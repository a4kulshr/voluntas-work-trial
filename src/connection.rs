//! One websocket's lifecycle: connect, subscribe, stamp and forward frames, reconnect with backoff.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, anyhow, bail};
use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;

use crate::exchange::{Event, Exchange, Session};

pub type ConnId = usize;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// Kalshi pings every 10s, so this much silence means the socket is dead.
const SILENCE_TIMEOUT: Duration = Duration::from_secs(30);
const MIN_BACKOFF: Duration = Duration::from_millis(250);
const MAX_BACKOFF: Duration = Duration::from_secs(30);
/// Spaces out initial handshakes; opening every socket at once gets HTTP 429.
const STARTUP_SPACING: Duration = Duration::from_millis(25);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    Subscribe(Vec<String>),
    Unsubscribe(Vec<String>),
    Resubscribe(Vec<String>),
    Reconnect,
    Close,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnMsg {
    Up,
    Down,
    Event(Event),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Inbound {
    pub conn: ConnId,
    pub received_at: DateTime<Utc>,
    pub msg: ConnMsg,
}

enum Exit {
    Close,
    Reconnect,
}

pub async fn run<E: Exchange>(
    exchange: Arc<E>,
    id: ConnId,
    mut symbols: BTreeSet<String>,
    mut cmds: UnboundedReceiver<Command>,
    out: UnboundedSender<Inbound>,
) {
    let send = |msg| {
        out.send(Inbound {
            conn: id,
            received_at: Utc::now(),
            msg,
        })
    };
    tokio::time::sleep(STARTUP_SPACING * id as u32).await;
    let mut backoff = MIN_BACKOFF;
    loop {
        match stream(&*exchange, id, &mut symbols, &mut cmds, &out, &mut backoff).await {
            Ok(Exit::Close) => return,
            Ok(Exit::Reconnect) => tracing::info!(conn = id, "scheduled reconnect"),
            Err(e) => tracing::warn!(conn = id, "connection lost: {e:#}"),
        }
        if send(ConnMsg::Down).is_err() {
            return;
        }

        let sleep = tokio::time::sleep(backoff);
        tokio::pin!(sleep);
        loop {
            tokio::select! {
                _ = &mut sleep => break,
                cmd = cmds.recv() => match cmd {
                    None | Some(Command::Close) => return,
                    Some(cmd) => apply(&mut symbols, &cmd),
                },
            }
        }
        backoff = (backoff * 2).min(MAX_BACKOFF);
    }
}

/// Keeps the desired symbol set current so a reconnect resubscribes the right markets.
fn apply(symbols: &mut BTreeSet<String>, cmd: &Command) {
    match cmd {
        Command::Subscribe(s) => symbols.extend(s.iter().cloned()),
        Command::Unsubscribe(s) => s.iter().for_each(|x| {
            symbols.remove(x);
        }),
        _ => {}
    }
}

async fn stream<E: Exchange>(
    exchange: &E,
    id: ConnId,
    symbols: &mut BTreeSet<String>,
    cmds: &mut UnboundedReceiver<Command>,
    out: &UnboundedSender<Inbound>,
    backoff: &mut Duration,
) -> anyhow::Result<Exit> {
    let (ws, _) = timeout(
        CONNECT_TIMEOUT,
        tokio_tungstenite::connect_async(exchange.ws_request()?),
    )
    .await
    .context("connect timed out")??;
    let (mut tx, mut rx) = ws.split();
    let mut session = exchange.session();

    let initial: Vec<String> = symbols.iter().cloned().collect();
    for frame in session.subscribe(&initial) {
        tracing::trace!(conn = id, sent = frame.as_str());
        tx.send(Message::text(frame)).await?;
    }
    out.send(Inbound {
        conn: id,
        received_at: Utc::now(),
        msg: ConnMsg::Up,
    })?;
    tracing::info!(conn = id, markets = initial.len(), "connected");
    *backoff = MIN_BACKOFF;

    loop {
        tokio::select! {
            biased;
            frame = timeout(SILENCE_TIMEOUT, rx.next()) => {
                let received_at = Utc::now();
                let frame = frame
                    .map_err(|_| anyhow!("silent for {SILENCE_TIMEOUT:?}"))?
                    .ok_or_else(|| anyhow!("stream ended"))??;
                match frame {
                    Message::Text(text) => {
                        tracing::trace!(conn = id, frame = text.as_str());
                        for event in session.decode(text.as_str())? {
                            out.send(Inbound { conn: id, received_at, msg: ConnMsg::Event(event) })?;
                        }
                    }
                    Message::Close(reason) => bail!("server closed: {reason:?}"),
                    _ => {}
                }
            }
            cmd = cmds.recv() => {
                let cmd = cmd.unwrap_or(Command::Close);
                apply(symbols, &cmd);
                let frames = match cmd {
                    Command::Close | Command::Reconnect => {
                        let _ = tx.send(Message::Close(None)).await;
                        return Ok(if cmd == Command::Close { Exit::Close } else { Exit::Reconnect });
                    }
                    Command::Subscribe(s) => session.subscribe(&s),
                    Command::Unsubscribe(s) => session.unsubscribe(&s),
                    Command::Resubscribe(s) => {
                        let mut frames = session.unsubscribe(&s);
                        frames.extend(session.subscribe(&s));
                        frames
                    }
                };
                for frame in frames {
                    tracing::trace!(conn = id, sent = frame.as_str());
                    tx.send(Message::text(frame)).await?;
                }
            }
        }
    }
}
