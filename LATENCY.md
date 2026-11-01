# Latency experiments (Extension 4)

Everything here was measured against **Kalshi demo** from a laptop on home internet, because I only had
a demo key. Treat the numbers as directional. The methods carry over to prod unchanged.

## How I measure it

Kalshi's `clock` is identical on every connection for the same book change, so the same change can be
matched across sockets. `examples/latency.rs` opens N sockets, subscribes all of them to the same
markets, and records each socket's receive time for every `(market, clock)`. For each change it
reports how far each socket trailed the first one to receive it.

```sh
cargo run --release --example latency -- <sockets> <series_ticker> <seconds>
```

## Result: connections to the same server differ

6 sockets, the 129 open `KXESOCCERGAME` markets (the busiest demo series in my full runs), 210 changes
seen on all 6. All handshakes completed within 311–319 ms of each other.

| socket | first to receive | behind fastest p50 | p90 | p99 |
|---|---|---|---|---|
| 0 | 36.2% | 0.08 ms | 27.6 ms | 91.3 ms |
| 1 | 1.9% | 4.86 ms | 27.8 ms | 91.6 ms |
| 2 | 15.7% | 0.66 ms | 4.65 ms | 8.46 ms |
| 3 | 1.4% | 5.12 ms | 14.4 ms | 15.9 ms |
| 4 | 41.4% | 0.05 ms | 3.45 ms | 4.79 ms |
| 5 | 3.3% | 1.41 ms | 13.7 ms | 14.4 ms |

Receive time minus Kalshi's `ts_ms` (my laptop's clock, so only roughly comparable): p50 65 ms, p90 110 ms,
p99 176 ms.

What it shows:

- **Two of six connections won 78% of the time**, even though all six connected within 8 ms of each other
  to the same host. Socket choice matters.
- **The tails are bad on some sockets.** Sockets 0 and 1 trailed by ~90 ms at p99, while socket 4 never
  trailed by more than ~5 ms. That points to per-connection queueing on Kalshi's side, not network
  distance.
- Caveat: this is a small sample. The machine went to sleep after about two minutes, so the run's second
  half has no data. I'd want a sample of tens of thousands of changes before tuning on it.

## What this means for the collector

The collector already uses this signal for correctness: a source whose clock trails its partner by
more than 500 ms is replaced. The same data suggests three latency improvements, in order of value:

1. **First arrival wins.** Both replicas are already subscribed, so write each change from whichever
   socket delivers it first, deduplicated by `(market, clock)`. Using the table above, racing sockets
   0 and 4 would cut p99 from ~91 ms to ~5 ms. The care needed: several deltas can share one clock, so
   dedup has to be per delta, not per clock. Kalshi also warns that clock-based merging is unsafe in
   conflated mode (I use real-time mode).
2. **Connection selection.** Open more sockets than needed, measure each one's lag with the method
   above for a minute, and keep the fastest. That only needs the existing reconnect machinery plus a
   ranking.
3. **Periodic re-ranking.** A connection's speed can change, so the hourly reconnect should prefer
   replacing the slowest socket rather than going round-robin.

## Ideas I didn't get to test

- **Source port / multiple TCP paths.** Different local ports hash onto different load-balancer paths.
  Binding sockets with `tokio::net::TcpSocket` before the TLS handshake would let me compare ports
  directly with the method above.
- **Connecting right after maintenance.** Kalshi has a weekly maintenance window, and being among the
  first connections after it may land on emptier servers. Worth testing with the latency example
  immediately after the window ends.
- **Prod and colocation.** Prod's latency profile will differ. The experiment should be rerun there,
  ideally from a server close to Kalshi's region rather than a home connection.
