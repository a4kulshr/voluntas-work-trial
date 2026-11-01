//! The only exchange-specific surface. Everything else in the crate is generic over [`Exchange`].

use std::collections::BTreeSet;
use std::future::Future;

use chrono::{DateTime, Utc};
use tokio_tungstenite::tungstenite::handshake::client::Request;

use crate::Side;

/// `(price, quantity)` in fixed point.
pub type Levels = Vec<(i64, i64)>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Change {
    By(i64),
    To(i64),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    Snapshot {
        symbol: String,
        clock: u64,
        exchange_ts: Option<DateTime<Utc>>,
        bids: Levels,
        asks: Levels,
    },
    Level {
        symbol: String,
        clock: u64,
        exchange_ts: Option<DateTime<Utc>>,
        side: Side,
        price: i64,
        change: Change,
    },
    /// Messages were lost on this connection; all of its books are suspect.
    Gap,
}

pub trait Exchange: Send + Sync + 'static {
    type Session: Session;

    /// Currently tradeable symbols; `created_since` narrows it to recent listings for cheap polling.
    /// `on_page` sees each page as it arrives, so a slow full listing can start subscriptions early.
    fn list_symbols(
        &self,
        created_since: Option<DateTime<Utc>>,
        on_page: impl FnMut(Vec<String>) + Send,
    ) -> impl Future<Output = anyhow::Result<BTreeSet<String>>> + Send;

    /// A websocket handshake request, including any auth.
    fn ws_request(&self) -> anyhow::Result<Request>;

    fn session(&self) -> Self::Session;
}

/// Protocol state for one websocket: encodes commands into text frames and decodes frames into events.
/// `clock` must be a per-symbol exchange sequence that is identical across connections.
pub trait Session: Send + 'static {
    fn subscribe(&mut self, symbols: &[String]) -> Vec<String>;
    fn unsubscribe(&mut self, symbols: &[String]) -> Vec<String>;
    fn decode(&mut self, text: &str) -> anyhow::Result<Vec<Event>>;
}
