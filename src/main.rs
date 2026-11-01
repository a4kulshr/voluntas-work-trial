use std::collections::{BTreeSet, HashMap};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context;
use chrono::Utc;
use clap::Parser;
use engineering_challenge::collector::{Action, Collector, Config};
use engineering_challenge::connection::{self, Command, ConnId};
use engineering_challenge::exchange::Exchange;
use engineering_challenge::kalshi::{Env, Kalshi};
use engineering_challenge::util::AlignedSbeWriter;
use tokio::sync::mpsc;
use tokio::task::JoinSet;

/// Must match AlignedSbeReader's chunk size so the output is readable with the repo's reader.
const CHUNK_SIZE_64_MEBIBYTES: usize = 64 << 20;

#[derive(Parser)]
#[command(about = "Collects every live Kalshi order book into an SBE file")]
struct Args {
    #[arg(long, env = "KALSHI_ENV", value_enum, default_value = "demo")]
    env: Env,
    #[arg(long, env = "KALSHI_KEY_ID")]
    key_id: String,
    #[arg(long, env = "KALSHI_KEY_PATH")]
    key_path: PathBuf,
    /// Where SBE output is written; logs always go to stderr.
    #[arg(long, default_value = "/dev/stdout")]
    output: PathBuf,
    /// Kalshi allows 200 concurrent websockets per user; leave headroom.
    #[arg(long, default_value_t = 190)]
    max_conns: usize,
    /// Only collect the first N markets (for testing).
    #[arg(long)]
    max_markets: Option<usize>,
    #[arg(long, default_value_t = 300)]
    full_discovery_secs: u64,
    #[arg(long, default_value_t = 10)]
    new_discovery_secs: u64,
    #[arg(long, default_value_t = 60)]
    snapshot_secs: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let args = Args::parse();

    let pem = std::fs::read_to_string(&args.key_path)
        .with_context(|| format!("reading key {}", args.key_path.display()))?;
    let exchange = Arc::new(Kalshi::new(args.env, args.key_id, &pem)?);
    let writer = AlignedSbeWriter::create(&args.output, CHUNK_SIZE_64_MEBIBYTES)?;
    let cfg = Config {
        max_conns: args.max_conns,
        ..Config::default()
    };
    let mut collector = Collector::new(cfg, writer);

    let (symbols_tx, mut symbols_rx) = mpsc::unbounded_channel();
    tokio::spawn(discover(
        exchange.clone(),
        symbols_tx,
        Duration::from_secs(args.full_discovery_secs),
        Duration::from_secs(args.new_discovery_secs),
        args.max_markets,
    ));

    let (inbound_tx, mut inbound_rx) = mpsc::unbounded_channel::<connection::Inbound>();
    // Keyed by id: the collector may spawn several groups at once, in any order.
    let mut conns: HashMap<ConnId, mpsc::UnboundedSender<Command>> = HashMap::new();
    let mut tasks = JoinSet::new();
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    let snapshot_every = Duration::from_secs(args.snapshot_secs);
    let mut snapshots =
        tokio::time::interval_at(tokio::time::Instant::now() + snapshot_every, snapshot_every);
    let mut max_queue_delay = chrono::TimeDelta::zero();
    let sigint = tokio::signal::ctrl_c();
    tokio::pin!(sigint);

    loop {
        let actions = tokio::select! {
            biased;
            _ = &mut sigint => break,
            Some(msg) = inbound_rx.recv() => {
                max_queue_delay = max_queue_delay.max(Utc::now() - msg.received_at);
                collector.on_inbound(msg, Instant::now())?
            }
            Some(found) = symbols_rx.recv() => match found {
                Discovered::All(live) => {
                    tracing::info!(markets = live.len(), "full discovery");
                    collector.set_symbols(&live, Instant::now())
                }
                Discovered::New(new) => collector.add_symbols(&new, Instant::now()),
            },
            _ = snapshots.tick() => {
                let n = collector.write_snapshots(Utc::now())?;
                tracing::info!(
                    snapshots = n,
                    conns = conns.len(),
                    queued = inbound_rx.len(),
                    max_queue_delay_ms = max_queue_delay.num_milliseconds(),
                    "periodic snapshot"
                );
                max_queue_delay = chrono::TimeDelta::zero();
                Vec::new()
            }
            _ = tick.tick() => collector.on_tick(Utc::now(), Instant::now())?,
        };
        for action in actions {
            match action {
                Action::Spawn { conn, symbols } => {
                    let (tx, rx) = mpsc::unbounded_channel();
                    conns.insert(conn, tx);
                    tasks.spawn(connection::run(
                        exchange.clone(),
                        conn,
                        symbols.into_iter().collect(),
                        rx,
                        inbound_tx.clone(),
                    ));
                }
                Action::Send { conn, cmd } => {
                    let _ = conns[&conn].send(cmd);
                }
            }
        }
    }

    tracing::info!("SIGINT: closing websockets and flushing output");
    for tx in conns.values() {
        let _ = tx.send(Command::Close);
    }
    if tokio::time::timeout(Duration::from_secs(5), tasks.join_all())
        .await
        .is_err()
    {
        tracing::warn!("some websockets did not close within 5s");
    }
    collector.sink_mut().flush()?;
    tracing::info!("output flushed and closed");
    Ok(())
}

enum Discovered {
    All(BTreeSet<String>),
    New(BTreeSet<String>),
}

/// Markets on Kalshi open within ~16 min of creation, so an hour of lookback is ample.
const NEW_MARKET_LOOKBACK: chrono::TimeDelta = chrono::TimeDelta::hours(1);

/// A cheap frequent poll for new listings, plus a slow full listing that also catches closures.
/// Each page is subscribed as it arrives; the full set at the end only drives removals.
async fn discover<E: Exchange>(
    exchange: Arc<E>,
    out: mpsc::UnboundedSender<Discovered>,
    full_every: Duration,
    new_every: Duration,
    max: Option<usize>,
) {
    let mut full = tokio::time::interval(full_every);
    let mut new = tokio::time::interval(new_every);
    // With --max-markets, stick to the first N we ever saw so test runs stay stable.
    let mut capped: BTreeSet<String> = BTreeSet::new();
    loop {
        let is_full = tokio::select! {
            biased;
            _ = full.tick() => true,
            _ = new.tick() => false,
        };
        if max.is_some() && !is_full {
            continue;
        }
        let since = (!is_full).then(|| Utc::now() - NEW_MARKET_LOOKBACK);
        let on_page = |page: Vec<String>| {
            let page: BTreeSet<String> = match max {
                Some(max) => page
                    .into_iter()
                    .filter(|s| {
                        capped.contains(s) || (capped.len() < max && capped.insert(s.clone()))
                    })
                    .collect(),
                None => page.into_iter().collect(),
            };
            if !page.is_empty() {
                let _ = out.send(Discovered::New(page));
            }
        };
        let found = match exchange.list_symbols(since, on_page).await {
            Ok(found) => found,
            Err(e) => {
                tracing::warn!(is_full, "discovery failed: {e:#}");
                continue;
            }
        };
        if !is_full {
            continue;
        }
        // An empty list is far likelier an API glitch than every market closing at once.
        if found.is_empty() {
            tracing::warn!("full discovery returned no markets; ignoring");
            continue;
        }
        let live = match max {
            Some(_) => found.intersection(&capped).cloned().collect(),
            None => found,
        };
        if out.send(Discovered::All(live)).is_err() {
            return;
        }
    }
}
