//! Exchange-agnostic core. Every market is subscribed on `REPLICAS` sockets, each with its own book.
//! One replica is the source whose deltas we write; switching source always writes a fresh snapshot.
//! Replica health is judged by the exchange clock: a replica whose clock trails another's is missing data.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};

use crate::Side;
use crate::book::Book;
use crate::connection::{Command, ConnId, ConnMsg, Inbound};
use crate::exchange::{Change, Event, Levels};
use crate::util::AlignedSbeWriter;
use crate::util::dtos::challenge_orderbook::{PriceLevelUpdateDomain, SnapshotDomain};

pub const REPLICAS: usize = 2;

#[derive(Debug, Clone)]
pub struct Config {
    /// Exchange-side connection limit; past it, new markets go to the least-loaded group.
    pub max_conns: usize,
    /// How long the source may trail another replica's clock before we fail over.
    pub lag_switch: Duration,
    /// How long a backup may trail before we treat its subscription as dead and resubscribe.
    pub lag_resub: Duration,
    /// Re-verify a market with no updates for this long by resubscribing a backup and comparing books.
    pub idle_verify: Duration,
    /// Retry a resubscribe whose snapshot never arrived.
    pub resync_timeout: Duration,
    pub reconnect_every: Duration,
    /// Spreads resubscribes out so a quiet period doesn't trigger a storm.
    pub max_resubs_per_tick: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            max_conns: 190,
            lag_switch: Duration::from_millis(500),
            lag_resub: Duration::from_secs(5),
            idle_verify: Duration::from_secs(30 * 60),
            resync_timeout: Duration::from_secs(30),
            reconnect_every: Duration::from_secs(60 * 60),
            max_resubs_per_tick: 100,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    Spawn { conn: ConnId, symbols: Vec<String> },
    Send { conn: ConnId, cmd: Command },
}

pub trait Sink {
    fn snapshot(&mut self, msg: &SnapshotDomain) -> anyhow::Result<()>;
    fn update(&mut self, msg: &PriceLevelUpdateDomain) -> anyhow::Result<()>;
}

impl Sink for AlignedSbeWriter {
    fn snapshot(&mut self, msg: &SnapshotDomain) -> anyhow::Result<()> {
        self.write_msg(msg)
    }
    fn update(&mut self, msg: &PriceLevelUpdateDomain) -> anyhow::Result<()> {
        self.write_msg(msg)
    }
}

struct Replica {
    conn: ConnId,
    book: Book,
    synced: bool,
    unsynced_since: Instant,
    clock: u64,
    exchange_ts: Option<DateTime<Utc>>,
    behind_since: Option<Instant>,
}

impl Replica {
    fn new(conn: ConnId, now: Instant) -> Self {
        Self {
            conn,
            book: Book::default(),
            synced: false,
            unsynced_since: now,
            clock: 0,
            exchange_ts: None,
            behind_since: None,
        }
    }

    fn invalidate(&mut self, now: Instant) {
        self.synced = false;
        self.unsynced_since = now;
        self.behind_since = None;
        self.book = Book::default();
    }
}

struct Market {
    group: usize,
    replicas: Vec<Replica>,
    source: Option<usize>,
    last_activity: Instant,
}

impl Market {
    fn replica(&self, conn: ConnId) -> Option<usize> {
        self.replicas.iter().position(|r| r.conn == conn)
    }

    /// Marks every synced replica whose clock trails the newest one.
    fn refresh_lag(&mut self, now: Instant) {
        let newest = self
            .replicas
            .iter()
            .filter(|r| r.synced)
            .map(|r| r.clock)
            .max();
        for r in self.replicas.iter_mut().filter(|r| r.synced) {
            if Some(r.clock) < newest {
                r.behind_since.get_or_insert(now);
            } else {
                r.behind_since = None;
            }
        }
    }
}

struct Group {
    conns: Vec<ConnId>,
    size: usize,
}

struct Conn {
    group: usize,
    up: bool,
    next_reconnect: Instant,
}

pub struct Collector<S> {
    cfg: Config,
    sink: S,
    markets: HashMap<String, Market>,
    groups: Vec<Group>,
    conns: Vec<Conn>,
}

/// Resubscribes batched per connection.
#[derive(Default)]
struct Resubs(HashMap<ConnId, Vec<String>>);

impl Resubs {
    fn add(&mut self, conn: ConnId, symbol: &str) {
        self.0.entry(conn).or_default().push(symbol.to_owned());
    }
    fn len(&self) -> usize {
        self.0.values().map(Vec::len).sum()
    }
    fn into_actions(self) -> impl Iterator<Item = Action> {
        self.0.into_iter().map(|(conn, s)| Action::Send {
            conn,
            cmd: Command::Resubscribe(s),
        })
    }
}

impl<S: Sink> Collector<S> {
    pub fn new(cfg: Config, sink: S) -> Self {
        Self {
            cfg,
            sink,
            markets: HashMap::new(),
            groups: Vec::new(),
            conns: Vec::new(),
        }
    }

    pub fn sink_mut(&mut self) -> &mut S {
        &mut self.sink
    }

    pub fn conn_count(&self) -> usize {
        self.conns.len()
    }

    /// Reconciles the subscribed set with the exchange's full list of live symbols.
    pub fn set_symbols(&mut self, live: &BTreeSet<String>, now: Instant) -> Vec<Action> {
        let mut actions = Vec::new();

        let removed: Vec<String> = self
            .markets
            .keys()
            .filter(|s| !live.contains(*s))
            .cloned()
            .collect();
        let mut unsubs: HashMap<ConnId, Vec<String>> = HashMap::new();
        for symbol in removed {
            let market = self.markets.remove(&symbol).expect("key from map");
            self.groups[market.group].size -= 1;
            for r in market.replicas {
                unsubs.entry(r.conn).or_default().push(symbol.clone());
            }
        }
        actions.extend(unsubs.into_iter().map(|(conn, s)| Action::Send {
            conn,
            cmd: Command::Unsubscribe(s),
        }));
        actions.extend(self.add_symbols(live, now));
        actions
    }

    /// Subscribes any symbols we aren't collecting yet.
    pub fn add_symbols(&mut self, live: &BTreeSet<String>, now: Instant) -> Vec<Action> {
        let mut actions = Vec::new();
        let first_new_group = self.groups.len();
        let mut added: HashMap<usize, Vec<String>> = HashMap::new();
        let new: Vec<&String> = live
            .iter()
            .filter(|s| !self.markets.contains_key(*s))
            .collect();
        for symbol in new {
            let group = self.pick_group(now);
            self.groups[group].size += 1;
            let replicas = self.groups[group]
                .conns
                .iter()
                .map(|&c| Replica::new(c, now))
                .collect();
            self.markets.insert(
                symbol.clone(),
                Market {
                    group,
                    replicas,
                    source: None,
                    last_activity: now,
                },
            );
            added.entry(group).or_default().push(symbol.clone());
        }
        for (group, symbols) in added {
            for &conn in &self.groups[group].conns {
                actions.push(if group >= first_new_group {
                    Action::Spawn {
                        conn,
                        symbols: symbols.clone(),
                    }
                } else {
                    Action::Send {
                        conn,
                        cmd: Command::Subscribe(symbols.clone()),
                    }
                });
            }
        }
        actions
    }

    /// Opens the full pool of groups, then fills the least loaded one. Discovery returns markets
    /// clustered by series, so filling in order would pile a hot series onto a single socket pair.
    fn pick_group(&mut self, now: Instant) -> usize {
        let max_groups = (self.cfg.max_conns / REPLICAS).max(1);
        if self.groups.len() >= max_groups {
            return (0..self.groups.len())
                .min_by_key(|&g| self.groups[g].size)
                .expect("at least one group");
        }
        let group = self.groups.len();
        let conns = (0..REPLICAS)
            .map(|k| {
                // Spread scheduled reconnects evenly over the period across every socket.
                let slot = (group * REPLICAS + k + 1) as u32;
                let offset = self.cfg.reconnect_every * slot / (max_groups * REPLICAS) as u32;
                self.conns.push(Conn {
                    group,
                    up: false,
                    next_reconnect: now + offset,
                });
                self.conns.len() - 1
            })
            .collect();
        self.groups.push(Group { conns, size: 0 });
        group
    }

    pub fn on_inbound(&mut self, inbound: Inbound, now: Instant) -> anyhow::Result<Vec<Action>> {
        let Inbound {
            conn,
            received_at,
            msg,
        } = inbound;
        let mut resubs = Resubs::default();
        match msg {
            ConnMsg::Up => {
                self.conns[conn].up = true;
                // The resubscribe is in flight now; time the resync from here, not from the outage.
                for market in self.markets.values_mut() {
                    if let Some(i) = market.replica(conn) {
                        market.replicas[i].unsynced_since = now;
                    }
                }
            }
            ConnMsg::Down => {
                self.conns[conn].up = false;
                self.invalidate_conn(conn, received_at, now)?;
            }
            ConnMsg::Event(Event::Gap) => {
                tracing::warn!(conn, "data lost on connection; reconnecting");
                self.invalidate_conn(conn, received_at, now)?;
                return Ok(vec![Action::Send {
                    conn,
                    cmd: Command::Reconnect,
                }]);
            }
            ConnMsg::Event(Event::Snapshot {
                symbol,
                clock,
                exchange_ts,
                bids,
                asks,
            }) => {
                let snap = Snap {
                    clock,
                    exchange_ts,
                    bids,
                    asks,
                };
                self.on_snapshot(conn, received_at, now, &symbol, snap, &mut resubs)?
            }
            ConnMsg::Event(Event::Level {
                symbol,
                clock,
                exchange_ts,
                side,
                price,
                change,
            }) => {
                let level = Lvl {
                    clock,
                    exchange_ts,
                    side,
                    price,
                    change,
                };
                self.on_level(conn, received_at, now, &symbol, level, &mut resubs)?
            }
        }
        Ok(resubs.into_actions().collect())
    }

    fn invalidate_conn(
        &mut self,
        conn: ConnId,
        at: DateTime<Utc>,
        now: Instant,
    ) -> anyhow::Result<()> {
        let group = self.conns[conn].group;
        let mut uncovered = 0;
        for (symbol, market) in self.markets.iter_mut().filter(|(_, m)| m.group == group) {
            let Some(idx) = market.replica(conn) else {
                continue;
            };
            market.replicas[idx].invalidate(now);
            if market.source == Some(idx) {
                failover(&mut self.sink, symbol, market, at)?;
            }
            uncovered += usize::from(market.source.is_none());
        }
        if uncovered > 0 {
            tracing::warn!(
                conn,
                uncovered,
                "markets have no healthy replica until resync"
            );
        }
        Ok(())
    }

    fn on_snapshot(
        &mut self,
        conn: ConnId,
        at: DateTime<Utc>,
        now: Instant,
        symbol: &str,
        snap: Snap,
        resubs: &mut Resubs,
    ) -> anyhow::Result<()> {
        let Some(market) = self.markets.get_mut(symbol) else {
            return Ok(());
        };
        let Some(idx) = market.replica(conn) else {
            return Ok(());
        };
        let book = match Book::from_levels(snap.bids, snap.asks) {
            Ok(book) => book,
            Err(e) => {
                tracing::warn!(conn, symbol, "bad snapshot: {e:#}");
                market.replicas[idx].invalidate(now);
                resubs.add(conn, symbol);
                return Ok(());
            }
        };

        // Same clock on both sockets means the books must match; if not, the source drifted.
        let diverged = market.source.filter(|&s| {
            let src = &market.replicas[s];
            s != idx && src.synced && src.clock == snap.clock && src.book != book
        });
        if let Some(s) = diverged {
            tracing::warn!(
                symbol,
                conn,
                "source book diverged from a fresh snapshot; switching"
            );
            market.replicas[s].invalidate(now);
            resubs.add(market.replicas[s].conn, symbol);
        }

        let r = &mut market.replicas[idx];
        r.book = book;
        r.synced = true;
        r.clock = snap.clock;
        r.exchange_ts = snap.exchange_ts;
        market.refresh_lag(now);

        let take_source = match market.source {
            None => true,
            Some(s) => s == idx || diverged.is_some(),
        };
        if take_source {
            market.source = Some(idx);
            emit_snapshot(&mut self.sink, symbol, &market.replicas[idx], at)?;
        }
        Ok(())
    }

    fn on_level(
        &mut self,
        conn: ConnId,
        at: DateTime<Utc>,
        now: Instant,
        symbol: &str,
        lvl: Lvl,
        resubs: &mut Resubs,
    ) -> anyhow::Result<()> {
        let Some(market) = self.markets.get_mut(symbol) else {
            return Ok(());
        };
        let Some(idx) = market.replica(conn) else {
            return Ok(());
        };
        let r = &mut market.replicas[idx];
        if !r.synced {
            return Ok(());
        }

        let applied = match lvl.change {
            Change::By(delta) => r.book.apply_delta(lvl.side, lvl.price, delta),
            Change::To(qty) => r.book.set(lvl.side, lvl.price, qty).map(|()| qty),
        };
        let qty = match applied {
            Ok(qty) => qty,
            Err(e) => {
                tracing::warn!(conn, symbol, "{e:#}; resubscribing");
                r.invalidate(now);
                resubs.add(conn, symbol);
                if market.source == Some(idx) {
                    failover(&mut self.sink, symbol, market, at)?;
                }
                return Ok(());
            }
        };
        r.clock = lvl.clock;
        r.exchange_ts = lvl.exchange_ts;
        market.last_activity = now;
        market.refresh_lag(now);

        if market.source == Some(idx) {
            self.sink.update(&PriceLevelUpdateDomain {
                backtest_timestamp: at,
                exchange_timestamp: lvl.exchange_ts.unwrap_or(at),
                price_level: lvl.price,
                quantity: qty,
                side: lvl.side,
                symbol: symbol.to_owned(),
            })?;
        }
        Ok(())
    }

    /// Health checks: lagging replicas, stuck resyncs, idle-market verification, scheduled reconnects.
    pub fn on_tick(&mut self, at: DateTime<Utc>, now: Instant) -> anyhow::Result<Vec<Action>> {
        let cfg = &self.cfg;
        let mut resubs = Resubs::default();
        for (symbol, market) in &mut self.markets {
            if resubs.len() >= cfg.max_resubs_per_tick {
                break;
            }
            let lagging = |i: usize, r: &Replica| {
                let limit = if Some(i) == market.source {
                    cfg.lag_switch
                } else {
                    cfg.lag_resub
                };
                r.behind_since.is_some_and(|t| now - t >= limit)
            };
            let unhealthy = market.replicas.iter().enumerate().position(|(i, r)| {
                let stuck = !r.synced
                    && self.conns[r.conn].up
                    && now - r.unsynced_since >= cfg.resync_timeout;
                stuck || lagging(i, r)
            });
            let idle_backup = (now - market.last_activity >= cfg.idle_verify)
                .then(|| {
                    market
                        .replicas
                        .iter()
                        .enumerate()
                        .position(|(i, r)| r.synced && Some(i) != market.source)
                })
                .flatten();

            let Some(i) = unhealthy.or(idle_backup) else {
                continue;
            };
            if idle_backup == Some(i) {
                // Its fresh snapshot gets compared against the source in on_snapshot.
                market.last_activity = now;
            }
            tracing::info!(
                symbol,
                conn = market.replicas[i].conn,
                synced = market.replicas[i].synced,
                "resubscribing"
            );
            market.replicas[i].invalidate(now);
            resubs.add(market.replicas[i].conn, symbol);
            if market.source == Some(i) {
                failover(&mut self.sink, symbol, market, at)?;
            }
        }

        let mut actions: Vec<Action> = resubs.into_actions().collect();
        // Only take a socket down when every market in its group is fully covered by the others.
        let mut degraded: HashSet<usize> = self
            .markets
            .values()
            .filter(|m| m.replicas.iter().any(|r| !r.synced))
            .map(|m| m.group)
            .collect();
        for id in 0..self.conns.len() {
            let conn = &self.conns[id];
            if !conn.up || now < conn.next_reconnect || degraded.contains(&conn.group) {
                continue;
            }
            if self.groups[conn.group]
                .conns
                .iter()
                .all(|&c| self.conns[c].up)
            {
                self.conns[id].next_reconnect = now + self.cfg.reconnect_every;
                // One socket per group at a time, even when several are overdue.
                degraded.insert(self.conns[id].group);
                actions.push(Action::Send {
                    conn: id,
                    cmd: Command::Reconnect,
                });
            }
        }
        Ok(actions)
    }

    /// The periodic full snapshot of every market that currently has a healthy source.
    pub fn write_snapshots(&mut self, at: DateTime<Utc>) -> anyhow::Result<usize> {
        let mut written = 0;
        for (symbol, market) in &self.markets {
            if let Some(src) = market.source {
                emit_snapshot(&mut self.sink, symbol, &market.replicas[src], at)?;
                written += 1;
            }
        }
        Ok(written)
    }
}

struct Snap {
    clock: u64,
    exchange_ts: Option<DateTime<Utc>>,
    bids: Levels,
    asks: Levels,
}

struct Lvl {
    clock: u64,
    exchange_ts: Option<DateTime<Utc>>,
    side: Side,
    price: i64,
    change: Change,
}

/// Moves the source to the most up-to-date healthy replica and snapshots it.
fn failover(
    sink: &mut impl Sink,
    symbol: &str,
    market: &mut Market,
    at: DateTime<Utc>,
) -> anyhow::Result<()> {
    let old = market.source;
    market.source = market
        .replicas
        .iter()
        .enumerate()
        .filter(|(i, r)| r.synced && Some(*i) != old)
        .max_by_key(|(_, r)| r.clock)
        .map(|(i, _)| i);
    match market.source {
        Some(i) => emit_snapshot(sink, symbol, &market.replicas[i], at),
        None => {
            tracing::debug!(symbol, "no healthy replica; waiting for a snapshot");
            Ok(())
        }
    }
}

fn emit_snapshot(
    sink: &mut impl Sink,
    symbol: &str,
    replica: &Replica,
    at: DateTime<Utc>,
) -> anyhow::Result<()> {
    sink.snapshot(
        &replica
            .book
            .to_snapshot(symbol, at, replica.exchange_ts.unwrap_or(at)),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, PartialEq)]
    enum Rec {
        Snap(SnapshotDomain),
        Upd(PriceLevelUpdateDomain),
    }

    impl Sink for Vec<Rec> {
        fn snapshot(&mut self, msg: &SnapshotDomain) -> anyhow::Result<()> {
            self.push(Rec::Snap(msg.clone()));
            Ok(())
        }
        fn update(&mut self, msg: &PriceLevelUpdateDomain) -> anyhow::Result<()> {
            self.push(Rec::Upd(msg.clone()));
            Ok(())
        }
    }

    fn ts() -> DateTime<Utc> {
        DateTime::from_timestamp_micros(1_700_000_000_000_000).unwrap()
    }

    fn inbound(conn: ConnId, msg: ConnMsg) -> Inbound {
        Inbound {
            conn,
            received_at: ts(),
            msg,
        }
    }

    fn snap(conn: ConnId, clock: u64, bids: Levels) -> Inbound {
        let event = Event::Snapshot {
            symbol: "A".into(),
            clock,
            exchange_ts: None,
            bids,
            asks: vec![],
        };
        inbound(conn, ConnMsg::Event(event))
    }

    fn bid(conn: ConnId, clock: u64, price: i64, delta: i64) -> Inbound {
        let event = Event::Level {
            symbol: "A".into(),
            clock,
            exchange_ts: None,
            side: Side::Buy,
            price,
            change: Change::By(delta),
        };
        inbound(conn, ConnMsg::Event(event))
    }

    fn bids(rec: &Rec) -> Vec<(i64, i64)> {
        let Rec::Snap(s) = rec else {
            panic!("expected snapshot, got {rec:?}")
        };
        s.bids.iter().map(|l| (l.price_level, l.quantity)).collect()
    }

    /// One market "A" on conns 0 and 1, both up and synced at clock 1; conn 0 is the source.
    fn synced(now: Instant) -> Collector<Vec<Rec>> {
        let cfg = Config {
            max_conns: REPLICAS,
            ..Config::default()
        };
        let mut c = Collector::new(cfg, Vec::new());
        c.set_symbols(&BTreeSet::from(["A".to_owned()]), now);
        for conn in 0..2 {
            c.on_inbound(inbound(conn, ConnMsg::Up), now).unwrap();
            c.on_inbound(snap(conn, 1, vec![(400_000, 10)]), now)
                .unwrap();
        }
        assert_eq!(c.sink.len(), 1, "only the first snapshot is written");
        c.sink.clear();
        c
    }

    #[test]
    fn shards_symbols_across_replicated_groups() {
        let cfg = Config {
            max_conns: 4,
            ..Config::default()
        };
        let mut c = Collector::new(cfg, Vec::<Rec>::new());
        let now = Instant::now();
        let live: BTreeSet<String> = ["A", "B", "C"].map(String::from).into();
        let mut spawned: Vec<_> = c
            .set_symbols(&live, now)
            .into_iter()
            .map(|a| match a {
                Action::Spawn { conn, symbols } => (conn, symbols.len()),
                other => panic!("{other:?}"),
            })
            .collect();
        spawned.sort();
        assert_eq!(spawned, [(0, 2), (1, 2), (2, 1), (3, 1)]);

        let live: BTreeSet<String> = ["A", "C", "D"].map(String::from).into();
        let mut actions = c.set_symbols(&live, now);
        actions.sort_by_key(|a| format!("{a:?}"));
        let sub = |conn, s: &str| Action::Send {
            conn,
            cmd: Command::Subscribe(vec![s.into()]),
        };
        let unsub = |conn, s: &str| Action::Send {
            conn,
            cmd: Command::Unsubscribe(vec![s.into()]),
        };
        // Removing "B" empties group 1, so "D" goes there.
        assert_eq!(
            actions,
            [sub(2, "D"), unsub(2, "B"), sub(3, "D"), unsub(3, "B")]
        );
    }

    #[test]
    fn markets_round_robin_across_the_full_pool() {
        let cfg = Config {
            max_conns: 4,
            ..Config::default()
        };
        let mut c = Collector::new(cfg, Vec::<Rec>::new());
        let live: BTreeSet<String> = ["A", "B", "C", "D", "E"].map(String::from).into();
        c.set_symbols(&live, Instant::now());
        assert_eq!(c.conn_count(), 4);
        let sizes: Vec<_> = c.groups.iter().map(|g| g.size).collect();
        assert_eq!(sizes, [3, 2]);
    }

    #[test]
    fn overdue_partners_never_reconnect_in_the_same_tick() {
        let now = Instant::now();
        let mut c = synced(now);
        let late = now + 3 * Config::default().reconnect_every;
        c.on_inbound(bid(0, 2, 400_000, 1), late).unwrap();
        c.on_inbound(bid(1, 2, 400_000, 1), late).unwrap();
        let actions = c.on_tick(ts(), late).unwrap();
        assert_eq!(
            actions,
            [Action::Send {
                conn: 0,
                cmd: Command::Reconnect
            }]
        );
    }

    #[test]
    fn writes_only_source_deltas_as_absolute_quantities() {
        let now = Instant::now();
        let mut c = synced(now);
        c.on_inbound(bid(0, 2, 400_000, 5), now).unwrap();
        c.on_inbound(bid(1, 2, 400_000, 5), now).unwrap();
        let [Rec::Upd(u)] = c.sink.as_slice() else {
            panic!("{:?}", c.sink)
        };
        assert_eq!(
            (u.price_level, u.quantity, u.side),
            (400_000, 15, Side::Buy)
        );
    }

    #[test]
    fn source_disconnect_fails_over_with_backup_snapshot() {
        let now = Instant::now();
        let mut c = synced(now);
        c.on_inbound(bid(1, 2, 300_000, 7), now).unwrap();
        c.on_inbound(inbound(0, ConnMsg::Down), now).unwrap();
        assert_eq!(bids(&c.sink[0]), [(400_000, 10), (300_000, 7)]);

        // Backup is now the source; the old source's late deltas are ignored.
        c.sink.clear();
        c.on_inbound(bid(0, 3, 300_000, 1), now).unwrap();
        c.on_inbound(bid(1, 3, 300_000, 1), now).unwrap();
        let [Rec::Upd(u)] = c.sink.as_slice() else {
            panic!("{:?}", c.sink)
        };
        assert_eq!(u.quantity, 8);
    }

    #[test]
    fn lagging_source_is_replaced_after_threshold() {
        let now = Instant::now();
        let mut c = synced(now);
        c.on_inbound(bid(1, 2, 400_000, 1), now).unwrap();
        assert!(
            c.on_tick(ts(), now + Duration::from_millis(100))
                .unwrap()
                .is_empty()
        );

        let actions = c.on_tick(ts(), now + Duration::from_millis(600)).unwrap();
        assert_eq!(
            actions,
            [Action::Send {
                conn: 0,
                cmd: Command::Resubscribe(vec!["A".into()])
            }]
        );
        assert_eq!(bids(&c.sink[0]), [(400_000, 11)]);
    }

    #[test]
    fn catching_up_clears_lag() {
        let now = Instant::now();
        let mut c = synced(now);
        c.on_inbound(bid(1, 2, 400_000, 1), now).unwrap();
        c.on_inbound(bid(0, 2, 400_000, 1), now).unwrap();
        assert!(
            c.on_tick(ts(), now + Duration::from_secs(10))
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn diverged_source_is_detected_by_fresh_snapshot() {
        let now = Instant::now();
        let mut c = synced(now);
        c.on_inbound(snap(1, 1, vec![(400_000, 9)]), now).unwrap();
        assert_eq!(bids(&c.sink[0]), [(400_000, 9)]);
        // Same snapshot at the same clock is a no-op.
        c.sink.clear();
        c.on_inbound(snap(0, 1, vec![(400_000, 9)]), now).unwrap();
        assert!(c.sink.is_empty());
    }

    #[test]
    fn impossible_delta_on_source_fails_over_and_resubscribes() {
        let now = Instant::now();
        let mut c = synced(now);
        let actions = c.on_inbound(bid(0, 2, 400_000, -11), now).unwrap();
        assert_eq!(
            actions,
            [Action::Send {
                conn: 0,
                cmd: Command::Resubscribe(vec!["A".into()])
            }]
        );
        assert_eq!(bids(&c.sink[0]), [(400_000, 10)]);
    }

    #[test]
    fn gap_reconnects_and_fails_over() {
        let now = Instant::now();
        let mut c = synced(now);
        let actions = c
            .on_inbound(inbound(0, ConnMsg::Event(Event::Gap)), now)
            .unwrap();
        assert_eq!(
            actions,
            [Action::Send {
                conn: 0,
                cmd: Command::Reconnect
            }]
        );
        assert_eq!(c.sink.len(), 1);
    }

    #[test]
    fn scheduled_reconnects_never_leave_a_market_uncovered() {
        let now = Instant::now();
        let mut c = synced(now);
        let half = Config::default().reconnect_every / 2;
        let reconnect = |conn| Action::Send {
            conn,
            cmd: Command::Reconnect,
        };
        let resub = |conn| Action::Send {
            conn,
            cmd: Command::Resubscribe(vec!["A".into()]),
        };

        // Conn 0 is due, but the idle check resyncs backup conn 1 in the same tick, so conn 0 waits.
        let t = now + half;
        assert_eq!(c.on_tick(ts(), t).unwrap(), [resub(1)]);
        c.on_inbound(snap(1, 1, vec![(400_000, 10)]), t).unwrap();
        assert_eq!(c.on_tick(ts(), t).unwrap(), [reconnect(0)]);

        // Conn 1 is due next, but must wait until conn 0 is back *and* resynced.
        c.on_inbound(inbound(0, ConnMsg::Down), t).unwrap();
        let t = now + 2 * half;
        assert_eq!(c.on_tick(ts(), t).unwrap(), []);
        c.on_inbound(inbound(0, ConnMsg::Up), t).unwrap();
        assert!(c.on_tick(ts(), t).unwrap().is_empty());
        c.on_inbound(snap(0, 1, vec![(400_000, 10)]), t).unwrap();
        c.on_inbound(bid(1, 2, 400_000, 1), t).unwrap();
        c.on_inbound(bid(0, 2, 400_000, 1), t).unwrap();
        assert_eq!(c.on_tick(ts(), t).unwrap(), [reconnect(1)]);
    }

    #[test]
    fn stuck_resync_is_retried() {
        let now = Instant::now();
        let mut c = synced(now);
        c.on_inbound(bid(1, 2, 400_000, -11), now).unwrap();
        let resub = Action::Send {
            conn: 1,
            cmd: Command::Resubscribe(vec!["A".into()]),
        };
        assert!(
            c.on_tick(ts(), now + Duration::from_secs(5))
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            c.on_tick(ts(), now + Duration::from_secs(31)).unwrap(),
            [resub]
        );
    }
}
