//! Replays an output file and checks each snapshot against the book rebuilt from prior deltas.
//! A mismatch means deltas were lost (or a source switch hid a gap) between two snapshots.

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
    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(&path)?;

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
                        eprintln!("mismatch: {} at {}", s.symbol, s.received_at);
                    }
                }
                first.get_or_insert(s.received_at);
                last = Some(s.received_at);
                books.insert(s.symbol, book);
            }
            AnyMessage::PriceLevelUpdate(d) => {
                let u = PriceLevelUpdateDomain::try_from_decoder(d)?;
                updates += 1;
                max_delay_us = max_delay_us.max(
                    (u.backtest_timestamp - u.exchange_timestamp)
                        .num_microseconds()
                        .unwrap_or(0),
                );
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
    println!(
        "max receive delay vs exchange ts: {:.1} ms",
        max_delay_us as f64 / 1000.0
    );
    println!("snapshot vs replayed book: {matched} match, {mismatched} mismatch");
    anyhow::ensure!(mismatched == 0, "replayed books diverged from snapshots");
    Ok(())
}
