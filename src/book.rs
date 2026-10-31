//! Exchange-agnostic price-level order book. Prices and quantities are fixed point.

use std::collections::BTreeMap;

use anyhow::ensure;
use chrono::{DateTime, Utc};

use crate::Side;
use crate::util::dtos::challenge_orderbook::{
    SnapshotAsksEntryDomain, SnapshotBidsEntryDomain, SnapshotDomain,
};

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Book {
    bids: BTreeMap<i64, i64>,
    asks: BTreeMap<i64, i64>,
}

impl Book {
    pub fn from_levels(
        bids: impl IntoIterator<Item = (i64, i64)>,
        asks: impl IntoIterator<Item = (i64, i64)>,
    ) -> anyhow::Result<Self> {
        let mut book = Self::default();
        for (side, levels) in [
            (Side::Buy, bids.into_iter().collect::<Vec<_>>()),
            (Side::Sell, asks.into_iter().collect()),
        ] {
            for (price, qty) in levels {
                book.set(side, price, qty)?;
            }
        }
        Ok(book)
    }

    fn side_mut(&mut self, side: Side) -> &mut BTreeMap<i64, i64> {
        match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        }
    }

    pub fn qty(&self, side: Side, price: i64) -> i64 {
        let levels = match side {
            Side::Buy => &self.bids,
            Side::Sell => &self.asks,
        };
        levels.get(&price).copied().unwrap_or(0)
    }

    /// Sets the absolute quantity at a level; zero removes it.
    pub fn set(&mut self, side: Side, price: i64, qty: i64) -> anyhow::Result<()> {
        ensure!(qty >= 0, "negative quantity {qty} at {side} {price}");
        let levels = self.side_mut(side);
        if qty == 0 {
            levels.remove(&price);
        } else {
            levels.insert(price, qty);
        }
        Ok(())
    }

    /// Applies a signed change and returns the new absolute quantity. A negative
    /// result means we are out of sync with the exchange, so it errors untouched.
    pub fn apply_delta(&mut self, side: Side, price: i64, delta: i64) -> anyhow::Result<i64> {
        let qty = self.qty(side, price) + delta;
        ensure!(
            qty >= 0,
            "delta {delta} at {side} {price} leaves {qty}; book out of sync"
        );
        self.set(side, price, qty)?;
        Ok(qty)
    }

    /// Bids best (highest) first, asks best (lowest) first.
    pub fn to_snapshot(
        &self,
        symbol: &str,
        received_at: DateTime<Utc>,
        exchange_timestamp: DateTime<Utc>,
    ) -> SnapshotDomain {
        SnapshotDomain {
            received_at,
            exchange_timestamp,
            bids: self
                .bids
                .iter()
                .rev()
                .map(|(&price_level, &quantity)| SnapshotBidsEntryDomain {
                    price_level,
                    quantity,
                })
                .collect(),
            asks: self
                .asks
                .iter()
                .map(|(&price_level, &quantity)| SnapshotAsksEntryDomain {
                    price_level,
                    quantity,
                })
                .collect(),
            symbol: symbol.to_owned(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deltas_add_and_remove_levels() {
        let mut book = Book::default();
        assert_eq!(book.apply_delta(Side::Buy, 400_000, 10).unwrap(), 10);
        assert_eq!(book.apply_delta(Side::Buy, 400_000, 5).unwrap(), 15);
        assert_eq!(book.apply_delta(Side::Buy, 400_000, -15).unwrap(), 0);
        assert_eq!(book, Book::default());
    }

    #[test]
    fn over_removal_errors_without_mutating() {
        let mut book = Book::from_levels([(400_000, 10)], []).unwrap();
        let before = book.clone();
        assert!(book.apply_delta(Side::Buy, 400_000, -11).is_err());
        assert!(book.apply_delta(Side::Sell, 600_000, -1).is_err());
        assert_eq!(book, before);
    }

    #[test]
    fn snapshot_orders_best_first() {
        let book =
            Book::from_levels([(300_000, 1), (400_000, 2)], [(700_000, 3), (600_000, 4)]).unwrap();
        let ts = DateTime::from_timestamp_micros(1_700_000_000_000_000).unwrap();
        let snap = book.to_snapshot("X", ts, ts);
        let bids: Vec<_> = snap
            .bids
            .iter()
            .map(|l| (l.price_level, l.quantity))
            .collect();
        let asks: Vec<_> = snap
            .asks
            .iter()
            .map(|l| (l.price_level, l.quantity))
            .collect();
        assert_eq!(bids, [(400_000, 2), (300_000, 1)]);
        assert_eq!(asks, [(600_000, 4), (700_000, 3)]);
    }

    #[test]
    fn zero_levels_are_dropped() {
        let book = Book::from_levels([(400_000, 0)], [(600_000, 0)]).unwrap();
        assert_eq!(book, Book::default());
    }
}
