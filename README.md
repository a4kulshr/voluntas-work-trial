# Kalshi order book collector

Subscribes to every live (non-MVE) Kalshi market, keeps a book per market on two
redundant websockets, and writes price-level deltas plus a full snapshot of every
book each minute using the challenge's `AlignedSbeWriter`.

All four extensions are implemented; see [Extensions](#extensions). Extension 4 findings are in
[LATENCY.md](LATENCY.md).

## Running

```sh
export KALSHI_KEY_ID=...                     # API key id
export KALSHI_KEY_PATH=~/.kalshi/demo.pem    # Ed25519 or RSA private key (PEM)
cargo run --release -- --env demo --output book.sbe   # default output is /dev/stdout
# Ctrl+C closes every websocket, flushes the final chunk, and closes the file.

cargo run --release --bin verify -- book.sbe          # replay + consistency check
```

Logs go to stderr (`RUST_LOG` controls verbosity). `--help` lists the tuning flags.

Output defaults to stdout, as the spec asks. The SIGINT note mentions closing "the file", so
`--output` also accepts a path. The writer keeps the template's 64 MiB chunk size, because
`AlignedSbeReader` only reads that size.

## Design

```
            REST /markets (full every 5 min, new listings every 10 s)
                              │
                         discover task
                              │ symbols
                              ▼
  ┌──────────── collector (single task, exchange-agnostic) ────────────┐
  │ market → group → [replica A, replica B], each with its own Book      │
  │ picks one source per market; writes only the source's deltas         │──► AlignedSbeWriter
  │ health: clock lag, stuck resyncs, idle verification, reconnects      │
  └───────▲──────────────────────────────────────────────────┬──────────┘
          │ events (stamped with receive time at the socket)  │ commands
   ┌──────┴─────┐  ┌────────────┐            ┌────────────┐   │
   │ socket 0   │  │ socket 1   │   ...      │ socket 189 │ ◄─┘
   └────────────┘  └────────────┘            └────────────┘
          each socket: connection.rs (generic) + kalshi::KalshiSession (protocol)
```

| File | What it does |
|---|---|
| `exchange.rs` | The adapter boundary: `Exchange` (discovery, auth'd handshake) and `Session` (encode commands, decode frames into `Snapshot` / `Level` / `Gap` events). |
| `kalshi.rs` | The only Kalshi-specific code: signing, `/markets` paging, the `orderbook_delta` protocol, NO bid → YES ask conversion, seq tracking. |
| `connection.rs` | One websocket's life: connect, subscribe, stamp each frame with its receive time, forward events, reconnect with backoff. |
| `collector.rs` | Books, source selection, failover, health checks, snapshots. Pure and synchronous, so it's unit-tested with fake time. |
| `book.rs`, `fixed.rs` | `BTreeMap` price levels; exact decimal-string → fixed-point parsing (no floats). |
| `bin/verify.rs` | Reads output with `AlignedSbeReader`, replays deltas, and compares each snapshot with the replayed book. |

### Correctness choices

- **Exact numbers.** Kalshi prices (`"0.9600"`) and sizes (`"54.00"`) are parsed straight into
  fixed point. Anything that can't be represented exactly is rejected.
- **Kalshi's book is two bid ladders.** A NO bid at `p` is written as an ask at `1 − p`.
- **Absolute quantities out.** Kalshi sends signed deltas. Each one is applied to our book and the
  resulting absolute size is written, as the schema asks. A delta that would leave a negative size
  proves the book is out of sync, so that replica is resubscribed instead of being trusted.
- **Timestamps.** `backtest_timestamp` / `received_at` is taken when the frame comes off the
  socket, before parsing or queueing. `exchange_timestamp` is Kalshi's `ts_ms`, falling back to
  `sending_ts_ms` (snapshots carry no change time).
- **Gaps.** Each socket tracks Kalshi's per-subscription `seq`. Any skip, or Kalshi's
  "subscription buffer overflow" error, marks every book on that socket as untrusted and
  reconnects it.

## Extensions

### 1. Redundant websockets

Every market is subscribed on two sockets (a group), and each socket keeps its own book. One replica
is the **source**, and only its deltas are written. The collector switches source when:

- the source disconnects, gaps, or receives an impossible delta;
- the source's Kalshi `clock` trails the other replica's for more than 500 ms. `clock` is identical
  across connections for the same change, so this catches a socket that's alive but delayed or
  silently missing updates;
- a fresh snapshot on the other replica, at the **same clock**, shows a different book. That means
  the source drifted.

**Every source switch writes a snapshot of the new source's book before any of its deltas**, so a
reader never mixes two sockets' views of a book.

### 2. Subscription monitoring

Health is judged with the exchange clock rather than message counts. A market that's quiet on both
sockets is just quiet, but a market that moves on one socket and not the other is broken.

- A backup whose clock trails for 5 s is unsubscribed and resubscribed.
- A resubscribe whose snapshot hasn't arrived after 30 s is retried.
- A market with no updates for 30 min is re-verified: its backup is resubscribed and the fresh
  snapshot is compared with the source's book (see above).
- Each socket is fully reconnected once an hour. Reconnects are spread evenly across all sockets,
  and a socket only goes down when its partner is connected **and** synced on every shared market,
  so a market is never left without a valid copy.
- Resubscribes are capped per tick so a burst of lag can't snowball.

### 3. Exchange-agnostic core

Everything except `kalshi.rs` is generic over `Exchange` / `Session`. A Polymarket adapter would
implement those two traits; `Change::To` already exists for exchanges that send absolute sizes
instead of deltas.

### 4. Latency

See [LATENCY.md](LATENCY.md).

## Sharding at Kalshi's scale

Kalshi demo lists ~255k open non-MVE markets and prod ~168k, while each user gets **200 concurrent
websockets**. The collector opens up to 190 sockets (95 groups of 2) up front and gives each new
market to the least-loaded group. Discovery returns markets clustered by series, so filling sockets
in order piled a hot series onto one pair, which then lagged constantly. Round-robin spreads them.

Discovery: a full listing (needed to notice closed markets) is ~250 sequential 1,000-market pages on
demo, so it runs every 5 minutes and each page is subscribed as it arrives. New listings are polled
every 10 s with `min_created_ts` (one request). In a sample, Kalshi markets opened at most ~16 min
after creation, so a 1-hour lookback is ample.

## Things I found out about Kalshi

- **Command acks consume a `seq` number (undocumented).** The `ok` reply to a repeated `subscribe`
  or `update_subscription` carries `sid` and `seq`. Ignoring it looks exactly like a lost message.
  At full scale this caused ~8,000 false gap reconnects before I found it.
- Opening ~190 websockets at once returns HTTP 429 on handshakes; spacing them by 25 ms avoids it.
- 100 small `subscribe` commands on one socket all get snapshots, paced or not (probed directly).

## Verification

`cargo test`: 23 tests covering parsing, book math, the Kalshi decoder (from the docs' example
frames and the real `ok` frame above), and the collector's failover, lag, divergence, reconnect
staggering and sharding logic.

Live, full scale on demo (all open markets, 190 sockets):

SOAK_RESULTS

"Corrections" are snapshots that differ from the replayed book. Each is the collector replacing a
lagging or drifted source, so the stream is repaired, but the individual changes during that lag
are collapsed into one snapshot. Periodic snapshots on a healthy source always match the replay.

## Known limits

- **Crash durability.** `AlignedSbeWriter` buffers a 64 MiB chunk before writing, so a crash
  (not SIGINT) loses up to one chunk. The real fix for process failure is running two collectors,
  which this design supports as-is.
- **Startup.** A full demo listing takes several minutes, so markets come online page by page.
- **Demo, not prod.** I only had a demo key. Demo's market list and activity are synthetic, which
  matters most for latency (LATENCY.md).
- **Single collector task.** It kept its queue under ~10 ms at full demo load. Sharding the
  collector by group would be the next step if it didn't.
