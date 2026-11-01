//! Kalshi adapter: Ed25519/RSA-PSS request signing, REST market discovery, and the `orderbook_delta` protocol.

use std::collections::BTreeSet;
use std::time::Duration;

use anyhow::{Context, bail};
use base64::Engine;
use chrono::{DateTime, Utc};
use ed25519_dalek::Signer;
use rsa::pkcs1::DecodeRsaPrivateKey;
use rsa::pkcs8::DecodePrivateKey;
use rsa::pss::BlindedSigningKey;
use rsa::sha2::Sha256;
use rsa::signature::{RandomizedSigner, SignatureEncoding};
use serde::Deserialize;
use serde_json::json;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::handshake::client::Request;

use crate::exchange::{Change, Event, Exchange, Levels, Session};
use crate::{Side, fixed};

const REST_PATH: &str = "/trade-api/v2";
const WS_PATH: &str = "/trade-api/ws/v2";
const MARKETS_PER_COMMAND: usize = 500;

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
pub enum Env {
    Demo,
    Prod,
}

impl Env {
    fn hosts(self) -> (&'static str, &'static str) {
        match self {
            Env::Demo => (
                "external-api.demo.kalshi.co",
                "external-api-ws.demo.kalshi.co",
            ),
            Env::Prod => ("external-api.kalshi.com", "external-api-ws.kalshi.com"),
        }
    }
}

/// Kalshi accepts Ed25519 (its recommendation) or RSA-PSS/SHA-256 keys.
enum Key {
    Ed25519(Box<ed25519_dalek::SigningKey>),
    Rsa(Box<BlindedSigningKey<Sha256>>),
}

impl Key {
    fn from_pem(pem: &str) -> anyhow::Result<Self> {
        if let Ok(k) =
            <ed25519_dalek::SigningKey as ed25519_dalek::pkcs8::DecodePrivateKey>::from_pkcs8_pem(
                pem,
            )
        {
            return Ok(Self::Ed25519(Box::new(k)));
        }
        let rsa = rsa::RsaPrivateKey::from_pkcs1_pem(pem)
            .or_else(|_| rsa::RsaPrivateKey::from_pkcs8_pem(pem))
            .context("Kalshi key must be an Ed25519 or RSA private key in PEM format")?;
        Ok(Self::Rsa(Box::new(BlindedSigningKey::new(rsa))))
    }

    fn sign(&self, msg: &[u8]) -> Vec<u8> {
        match self {
            Self::Ed25519(k) => k.sign(msg).to_bytes().to_vec(),
            Self::Rsa(k) => k.sign_with_rng(&mut rand::thread_rng(), msg).to_vec(),
        }
    }
}

pub struct Kalshi {
    rest_host: &'static str,
    ws_host: &'static str,
    key_id: String,
    key: Key,
    http: reqwest::Client,
}

impl Kalshi {
    pub fn new(env: Env, key_id: String, pem: &str) -> anyhow::Result<Self> {
        let key = Key::from_pem(pem)?;
        let (rest_host, ws_host) = env.hosts();
        Ok(Self {
            rest_host,
            ws_host,
            key_id,
            key,
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(20))
                .build()?,
        })
    }

    /// Signs `timestamp + method + path` (no query string), per Kalshi's auth spec.
    fn auth_headers(&self, method: &str, path: &str) -> [(&'static str, String); 3] {
        let ts = Utc::now().timestamp_millis().to_string();
        let sig = self.key.sign(format!("{ts}{method}{path}").as_bytes());
        [
            ("KALSHI-ACCESS-KEY", self.key_id.clone()),
            ("KALSHI-ACCESS-TIMESTAMP", ts),
            (
                "KALSHI-ACCESS-SIGNATURE",
                base64::engine::general_purpose::STANDARD.encode(sig),
            ),
        ]
    }

    async fn markets_page(
        &self,
        cursor: &str,
        created_since: Option<DateTime<Utc>>,
    ) -> anyhow::Result<MarketsPage> {
        let path = format!("{REST_PATH}/markets");
        let mut backoff = Duration::from_millis(500);
        loop {
            let mut req = self
                .http
                .get(format!("https://{}{path}", self.rest_host))
                .query(&[
                    ("status", "open"),
                    ("mve_filter", "exclude"),
                    ("limit", "1000"),
                ]);
            if !cursor.is_empty() {
                req = req.query(&[("cursor", cursor)]);
            }
            if let Some(t) = created_since {
                req = req.query(&[("min_created_ts", t.timestamp())]);
            }
            for (k, v) in self.auth_headers("GET", &path) {
                req = req.header(k, v);
            }
            let resp = req.send().await?;
            if resp.status() == reqwest::StatusCode::TOO_MANY_REQUESTS
                && backoff < Duration::from_secs(10)
            {
                tokio::time::sleep(backoff).await;
                backoff *= 2;
                continue;
            }
            return Ok(resp.error_for_status()?.json().await?);
        }
    }
}

#[derive(Deserialize)]
struct MarketsPage {
    markets: Vec<Market>,
    #[serde(default)]
    cursor: String,
}

#[derive(Deserialize)]
struct Market {
    ticker: String,
    #[serde(default)]
    mve_collection_ticker: Option<String>,
}

impl Exchange for Kalshi {
    type Session = KalshiSession;

    async fn list_symbols(
        &self,
        created_since: Option<DateTime<Utc>>,
        mut on_page: impl FnMut(Vec<String>) + Send,
    ) -> anyhow::Result<BTreeSet<String>> {
        let mut symbols = BTreeSet::new();
        let mut cursor = String::new();
        loop {
            let page = self.markets_page(&cursor, created_since).await?;
            // mve_filter=exclude should already drop these; filter again in case it ever doesn't.
            let tickers: Vec<String> = page
                .markets
                .into_iter()
                .filter(|m| m.mve_collection_ticker.as_deref().is_none_or(str::is_empty))
                .map(|m| m.ticker)
                .collect();
            symbols.extend(tickers.iter().cloned());
            on_page(tickers);
            if page.cursor.is_empty() {
                return Ok(symbols);
            }
            cursor = page.cursor;
        }
    }

    fn ws_request(&self) -> anyhow::Result<Request> {
        let mut req = format!("wss://{}{WS_PATH}", self.ws_host).into_client_request()?;
        for (k, v) in self.auth_headers("GET", WS_PATH) {
            req.headers_mut().insert(k, v.parse()?);
        }
        Ok(req)
    }

    fn session(&self) -> KalshiSession {
        KalshiSession::default()
    }
}

#[derive(Debug, Default)]
pub struct KalshiSession {
    next_id: u64,
    sid: Option<u64>,
    last_seq: Option<u64>,
}

impl KalshiSession {
    fn command(&mut self, cmd: &str, params: serde_json::Value) -> String {
        self.next_id += 1;
        json!({ "id": self.next_id, "cmd": cmd, "params": params }).to_string()
    }

    /// Kalshi numbers every message on a subscription; any skip means lost data.
    fn check_seq(&mut self, seq: u64, events: &mut Vec<Event>) {
        if self.last_seq.is_some_and(|last| seq != last + 1) {
            events.push(Event::Gap);
        }
        self.last_seq = Some(seq);
    }
}

impl Session for KalshiSession {
    fn subscribe(&mut self, symbols: &[String]) -> Vec<String> {
        // A repeated `subscribe` adds markets to the connection's single orderbook sid.
        symbols
            .chunks(MARKETS_PER_COMMAND)
            .map(|chunk| {
                self.command(
                    "subscribe",
                    json!({ "channels": ["orderbook_delta"], "market_tickers": chunk }),
                )
            })
            .collect()
    }

    fn unsubscribe(&mut self, symbols: &[String]) -> Vec<String> {
        let Some(sid) = self.sid else {
            tracing::warn!(
                count = symbols.len(),
                "unsubscribe before subscription ack; skipped"
            );
            return Vec::new();
        };
        symbols
            .chunks(MARKETS_PER_COMMAND)
            .map(|chunk| {
                self.command(
                    "update_subscription",
                    json!({ "sids": [sid], "market_tickers": chunk, "action": "delete_markets" }),
                )
            })
            .collect()
    }

    fn decode(&mut self, text: &str) -> anyhow::Result<Vec<Event>> {
        let frame: Frame =
            serde_json::from_str(text).with_context(|| format!("decoding {text}"))?;
        let mut events = Vec::new();
        match frame {
            Frame::OrderbookSnapshot {
                seq,
                sending_ts_ms,
                msg,
                ..
            } => {
                self.check_seq(seq, &mut events);
                let mut asks = parse_levels(&msg.no_dollars_fp)?;
                for (price, _) in &mut asks {
                    *price = no_to_yes(*price);
                }
                events.push(Event::Snapshot {
                    symbol: msg.market_ticker,
                    clock: msg.clock,
                    exchange_ts: sending_ts_ms.and_then(DateTime::from_timestamp_millis),
                    bids: parse_levels(&msg.yes_dollars_fp)?,
                    asks,
                });
            }
            Frame::OrderbookDelta {
                seq,
                sending_ts_ms,
                msg,
                ..
            } => {
                self.check_seq(seq, &mut events);
                let price = fixed::parse(&msg.price_dollars)?;
                let (side, price) = match msg.side.as_str() {
                    "yes" => (Side::Buy, price),
                    "no" => (Side::Sell, no_to_yes(price)),
                    other => bail!("unknown side {other:?}"),
                };
                events.push(Event::Level {
                    symbol: msg.market_ticker,
                    clock: msg.clock,
                    exchange_ts: msg
                        .ts_ms
                        .or(sending_ts_ms)
                        .and_then(DateTime::from_timestamp_millis),
                    side,
                    price,
                    change: Change::By(fixed::parse(&msg.delta_fp)?),
                });
            }
            Frame::Subscribed { msg } => {
                if msg.channel == "orderbook_delta" {
                    self.sid = Some(msg.sid);
                }
            }
            Frame::Ok { seq: Some(seq) } => self.check_seq(seq, &mut events),
            Frame::Error { msg } => {
                tracing::warn!(code = msg.code, msg = msg.msg, "kalshi error");
                if msg.code == BUFFER_OVERFLOW {
                    events.push(Event::Gap);
                }
            }
            Frame::Ok { seq: None } | Frame::Other => {}
        }
        Ok(events)
    }
}

/// Kalshi dropped messages during a burst.
const BUFFER_OVERFLOW: u32 = 25;

/// Kalshi only lists bids: a NO bid at `p` is a YES ask at `1 - p`.
fn no_to_yes(price: i64) -> i64 {
    fixed::SCALE - price
}

fn parse_levels(levels: &[(String, String)]) -> anyhow::Result<Levels> {
    levels
        .iter()
        .map(|(p, q)| Ok((fixed::parse(p)?, fixed::parse(q)?)))
        .collect()
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Frame {
    OrderbookSnapshot {
        seq: u64,
        sending_ts_ms: Option<i64>,
        msg: SnapshotMsg,
    },
    OrderbookDelta {
        seq: u64,
        sending_ts_ms: Option<i64>,
        msg: DeltaMsg,
    },
    Subscribed {
        msg: SubscribedMsg,
    },
    /// Acks to commands on an existing sid consume a sequence number (undocumented).
    Ok {
        seq: Option<u64>,
    },
    Error {
        msg: ErrorMsg,
    },
    #[serde(other)]
    Other,
}

#[derive(Deserialize)]
struct SnapshotMsg {
    market_ticker: String,
    clock: u64,
    #[serde(default)]
    yes_dollars_fp: Vec<(String, String)>,
    #[serde(default)]
    no_dollars_fp: Vec<(String, String)>,
}

#[derive(Deserialize)]
struct DeltaMsg {
    market_ticker: String,
    clock: u64,
    price_dollars: String,
    delta_fp: String,
    side: String,
    ts_ms: Option<i64>,
}

#[derive(Deserialize)]
struct SubscribedMsg {
    channel: String,
    sid: u64,
}

#[derive(Deserialize)]
struct ErrorMsg {
    code: u32,
    msg: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    const SNAPSHOT: &str = r#"{"type":"orderbook_snapshot","sending_ts_ms":1669149841234,"sid":2,"seq":1,
        "msg":{"market_ticker":"FED-23DEC-T3.00","market_id":"x","clock":4182057,
        "yes_dollars_fp":[["0.0800","300.00"],["0.2200","333.00"]],
        "no_dollars_fp":[["0.5400","20.00"],["0.5600","146.00"]]}}"#;

    fn delta(seq: u64, side: &str) -> String {
        format!(
            r#"{{"type":"orderbook_delta","sid":2,"seq":{seq},"msg":{{"market_ticker":"FED-23DEC-T3.00",
            "clock":4182061,"price_dollars":"0.9600","delta_fp":"-54.00","side":"{side}","ts_ms":1669149841000}}}}"#
        )
    }

    #[test]
    fn snapshot_maps_no_bids_to_yes_asks() {
        let events = KalshiSession::default().decode(SNAPSHOT).unwrap();
        let [
            Event::Snapshot {
                symbol,
                clock,
                bids,
                asks,
                ..
            },
        ] = events.as_slice()
        else {
            panic!("{events:?}")
        };
        assert_eq!(symbol, "FED-23DEC-T3.00");
        assert_eq!(*clock, 4182057);
        assert_eq!(bids, &[(80_000, 300_000_000), (220_000, 333_000_000)]);
        assert_eq!(asks, &[(460_000, 20_000_000), (440_000, 146_000_000)]);
    }

    #[test]
    fn deltas_decode_by_side() {
        let mut s = KalshiSession::default();
        s.decode(SNAPSHOT).unwrap();
        let yes = s.decode(&delta(2, "yes")).unwrap();
        let no = s.decode(&delta(3, "no")).unwrap();
        let level = |side, price| Event::Level {
            symbol: "FED-23DEC-T3.00".into(),
            clock: 4182061,
            exchange_ts: DateTime::from_timestamp_millis(1669149841000),
            side,
            price,
            change: Change::By(-54_000_000),
        };
        assert_eq!(yes, [level(Side::Buy, 960_000)]);
        assert_eq!(no, [level(Side::Sell, 40_000)]);
    }

    #[test]
    fn seq_skip_and_overflow_are_gaps() {
        let mut s = KalshiSession::default();
        s.decode(SNAPSHOT).unwrap();
        assert_eq!(s.decode(&delta(3, "yes")).unwrap()[0], Event::Gap);
        let overflow = r#"{"type":"error","msg":{"code":25,"msg":"Subscription buffer overflow"}}"#;
        assert_eq!(s.decode(overflow).unwrap(), [Event::Gap]);
    }

    #[test]
    fn command_acks_count_toward_seq() {
        let mut s = KalshiSession::default();
        s.decode(SNAPSHOT).unwrap();
        let ack = r#"{"type":"ok","id":2,"sid":1,"seq":2,"msg":{"market_tickers":["A"]}}"#;
        assert!(s.decode(ack).unwrap().is_empty());
        assert_eq!(s.decode(&delta(3, "yes")).unwrap().len(), 1);
    }

    #[test]
    fn unsubscribe_waits_for_sid() {
        let mut s = KalshiSession::default();
        assert!(s.unsubscribe(&["A".into()]).is_empty());
        s.decode(r#"{"type":"subscribed","msg":{"channel":"orderbook_delta","sid":7}}"#)
            .unwrap();
        let cmd: serde_json::Value =
            serde_json::from_str(&s.unsubscribe(&["A".into()])[0]).unwrap();
        assert_eq!(cmd["params"]["sids"], json!([7]));
        assert_eq!(cmd["params"]["action"], "delete_markets");
    }
}
