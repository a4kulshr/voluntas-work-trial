use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context;
use chrono::Utc;
use clap::Parser;
use engineering_challenge::collector::{Action, Collector, Config};
use engineering_challenge::connection::{self, Command};
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
    #[arg(long, default_value_t = 250)]
    markets_per_conn: usize,
    /// Only collect the first N markets (for testing).
    #[arg(long)]
    max_markets: Option<usize>,
    #[arg(long, default_value_t = 30)]
    discovery_secs: u64,
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
        markets_per_conn: args.markets_per_conn,
        ..Config::default()
    };
    let mut collector = Collector::new(cfg, writer);

    let (symbols_tx, mut symbols_rx) = mpsc::channel(1);
    tokio::spawn(discover(
        exchange.clone(),
        symbols_tx,
        Duration::from_secs(args.discovery_secs),
        args.max_markets,
    ));

    let (inbound_tx, mut inbound_rx) = mpsc::unbounded_channel();
    let mut conns: Vec<mpsc::UnboundedSender<Command>> = Vec::new();
    let mut tasks = JoinSet::new();
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    let snapshot_every = Duration::from_secs(args.snapshot_secs);
    let mut snapshots =
        tokio::time::interval_at(tokio::time::Instant::now() + snapshot_every, snapshot_every);
    let sigint = tokio::signal::ctrl_c();
    tokio::pin!(sigint);

    loop {
        let actions = tokio::select! {
            biased;
            _ = &mut sigint => break,
            Some(msg) = inbound_rx.recv() => collector.on_inbound(msg, Instant::now())?,
            Some(live) = symbols_rx.recv() => collector.set_symbols(&live, Instant::now()),
            _ = snapshots.tick() => {
                let n = collector.write_snapshots(Utc::now())?;
                tracing::info!(snapshots = n, conns = conns.len(), "periodic snapshot");
                Vec::new()
            }
            _ = tick.tick() => collector.on_tick(Utc::now(), Instant::now())?,
        };
        for action in actions {
            match action {
                Action::Spawn { conn, symbols } => {
                    debug_assert_eq!(conn, conns.len());
                    let (tx, rx) = mpsc::unbounded_channel();
                    conns.push(tx);
                    tasks.spawn(connection::run(
                        exchange.clone(),
                        conn,
                        symbols.into_iter().collect(),
                        rx,
                        inbound_tx.clone(),
                    ));
                }
                Action::Send { conn, cmd } => {
                    let _ = conns[conn].send(cmd);
                }
            }
        }
    }

    tracing::info!("SIGINT: closing websockets and flushing output");
    for tx in &conns {
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

async fn discover<E: Exchange>(
    exchange: Arc<E>,
    out: mpsc::Sender<BTreeSet<String>>,
    every: Duration,
    max: Option<usize>,
) {
    let mut interval = tokio::time::interval(every);
    loop {
        interval.tick().await;
        match exchange.list_symbols().await {
            // An empty list is far likelier an API glitch than every market closing at once.
            Ok(symbols) if symbols.is_empty() => {
                tracing::warn!("discovery returned no markets; ignoring")
            }
            Ok(symbols) => {
                let symbols = symbols
                    .into_iter()
                    .take(max.unwrap_or(usize::MAX))
                    .collect();
                if out.send(symbols).await.is_err() {
                    return;
                }
            }
            Err(e) => tracing::warn!("discovery failed: {e:#}"),
        }
    }
}
