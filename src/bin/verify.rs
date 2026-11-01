//! Replays an output file and checks each snapshot against the book rebuilt from prior deltas.
//! A differing snapshot is a correction: the collector replaced a stale or lagging source.
//! Corrections are expected under exchange lag, but never on a periodic snapshot: those come from
//! the current source, so they must equal the replay of the deltas we wrote.

use std::collections::HashMap;

use anyhow::Context;
use engineering_challenge::book::Book;
use engineering_challenge::util::dtos::challenge_orderbook::{
    AnyMessage, PriceLevelUpdateDomain, SnapshotDomain,
};
use engineering_challenge::util::{AlignedSbeReader, OrderbookSchema};

fn main() -> anyhow::Result<()> {
    let path = std::env::args()
        .nth(1)
        .context("usage: verify <output.sbe>")?;
    // A periodic batch snapshots every market under one timestamp. Failovers can share a timestamp
    // too (a socket dropping fails over all its markets at once), but cover a fraction of markets.
    let mut per_ts: HashMap<i64, u32> = HashMap::new();
    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(&path)?;
    while let Some(msg) = reader.read_msg()? {
        if let AnyMessage::Snapshot(d) = msg {
            let s = SnapshotDomain::try_from_decoder(d)?;
            *per_ts.entry(s.received_at.timestamp_micros()).or_default() += 1;
        }
    }
    let largest = per_ts.values().copied().max().unwrap_or(0);
    let is_periodic = |ts: i64| per_ts.get(&ts).is_some_and(|&n| 2 * n >= largest);

    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(&path)?;
    let mut periodic_corrections = 0u64;
    let mut delays_us: Vec<i64> = Vec::new();
    let mut books: HashMap<String, Book> = HashMap::new();
    let (mut snapshots, mut updates, mut matched, mut mismatched) = (0u64, 0u64, 0u64, 0u64);
    let (mut first, mut last) = (None, None);
    let mut max_delay_us = 0i64;

    while let Some(msg) = reader.read_msg()? {
        match msg {
            AnyMessage::Snapshot(d) => {
                let s = SnapshotDomain::try_from_decoder(d)?;
                snapshots += 1;
                let book = Book::from_levels(
                    s.bids.iter().map(|l| (l.price_level, l.quantity)),
                    s.asks.iter().map(|l| (l.price_level, l.quantity)),
                )?;
                if let Some(prev) = books.get(&s.symbol) {
                    if *prev == book {
                        matched += 1;
                    } else {
                        mismatched += 1;
                        let periodic = is_periodic(s.received_at.timestamp_micros());
                        periodic_corrections += u64::from(periodic);
                        eprintln!(
                            "correction: {} at {} periodic={periodic}",
                            s.symbol, s.received_at
                        );
                    }
                }
                first.get_or_insert(s.received_at);
                last = Some(s.received_at);
                books.insert(s.symbol, book);
            }
            AnyMessage::PriceLevelUpdate(d) => {
                let u = PriceLevelUpdateDomain::try_from_decoder(d)?;
                updates += 1;
                let delay = (u.backtest_timestamp - u.exchange_timestamp)
                    .num_microseconds()
                    .unwrap_or(0);
                max_delay_us = max_delay_us.max(delay);
                delays_us.push(delay);
                let book = books
                    .get_mut(&u.symbol)
                    .with_context(|| format!("update for {} before any snapshot", u.symbol))?;
                book.set(u.side, u.price_level, u.quantity)?;
                last = Some(u.backtest_timestamp);
            }
            _ => {}
        }
    }

    println!("symbols:   {}", books.len());
    println!("snapshots: {snapshots}");
    println!("updates:   {updates}");
    println!("span:      {first:?} .. {last:?}");
    delays_us.sort_unstable();
    let pct = |p: f64| {
        delays_us
            .get(((delays_us.len().max(1) - 1) as f64 * p) as usize)
            .map_or(f64::NAN, |&d| d as f64 / 1000.0)
    };
    println!(
        "receive - exchange ts (ms): p50 {:.0}, p99 {:.0}, max {:.0}",
        pct(0.5),
        pct(0.99),
        max_delay_us as f64 / 1000.0
    );
    println!("snapshot vs replayed book: {matched} consistent, {mismatched} corrections");
    println!("corrections on periodic snapshots: {periodic_corrections} (must be 0)");
    anyhow::ensure!(
        periodic_corrections == 0,
        "a periodic snapshot disagrees with the written deltas"
    );
    Ok(())
}
