//! Extension 4 experiment: N sockets subscribe to the same markets; for every change (market, clock)
//! record each socket's receive time and report how far each trails the fastest. See LATENCY.md.
//! Usage: latency <sockets> <series_ticker> <seconds>

use std::collections::HashMap;
use std::time::{Duration, Instant};

use engineering_challenge::exchange::{Event, Exchange, Session};
use engineering_challenge::kalshi::{Env, Kalshi};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let a: Vec<String> = std::env::args().skip(1).collect();
    let (sockets, series, secs): (usize, &str, u64) = (a[0].parse()?, &a[1], a[2].parse()?);
    let pem = std::fs::read_to_string(std::env::var("KALSHI_KEY_PATH")?)?;
    let k = std::sync::Arc::new(Kalshi::new(
        Env::Demo,
        std::env::var("KALSHI_KEY_ID")?,
        &pem,
    )?);
    let url = format!(
        "https://external-api.demo.kalshi.co/trade-api/v2/markets?status=open&limit=1000&series_ticker={series}"
    );
    let page: serde_json::Value = reqwest::get(url).await?.json().await?;
    let symbols: Vec<String> = page["markets"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["ticker"].as_str().unwrap().to_owned())
        .collect();

    let (tx, mut rx) = mpsc::unbounded_channel::<(usize, Instant, String, u64, Option<i64>)>();
    let start = Instant::now();
    for id in 0..sockets {
        let (k, tx, symbols) = (k.clone(), tx.clone(), symbols.clone());
        tokio::spawn(async move {
            let t0 = Instant::now();
            let (ws, _) = tokio_tungstenite::connect_async(k.ws_request().unwrap())
                .await
                .unwrap();
            eprintln!("socket {id}: handshake {:?}", t0.elapsed());
            let (mut w, mut r) = ws.split();
            let mut s = k.session();
            for f in s.subscribe(&symbols) {
                w.send(Message::text(f)).await.unwrap();
            }
            while let Some(Ok(Message::Text(t))) = r.next().await {
                let at = Instant::now();
                let ms = serde_json::from_str::<serde_json::Value>(t.as_str())
                    .ok()
                    .and_then(|v| v["msg"]["ts_ms"].as_i64());
                for e in s.decode(t.as_str()).unwrap_or_default() {
                    if let Event::Level { symbol, clock, .. } = e {
                        let _ = tx.send((id, at, symbol, clock, ms));
                    }
                }
            }
        });
    }
    drop(tx);

    // (symbol, clock) -> first arrival per socket; deltas sharing a clock count once.
    let mut seen: HashMap<(String, u64), Vec<Option<Instant>>> = HashMap::new();
    let mut exch_lag_ms: Vec<i64> = Vec::new();
    let deadline = tokio::time::sleep(Duration::from_secs(secs));
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            _ = &mut deadline => break,
            Some((id, at, sym, clock, ts)) = rx.recv() => {
                let row = seen.entry((sym, clock)).or_insert_with(|| vec![None; sockets]);
                if row[id].is_none() {
                    row[id] = Some(at);
                    if let Some(ts) = ts {
                        exch_lag_ms.push(chrono::Utc::now().timestamp_millis() - ts);
                    }
                }
            }
        }
    }

    let half = start + Duration::from_secs(secs / 2);
    let mut behind: Vec<[Vec<f64>; 2]> = vec![[Vec::new(), Vec::new()]; sockets];
    let mut wins = vec![0usize; sockets];
    let mut complete = 0;
    for row in seen.values() {
        let Some(times) = row.iter().copied().collect::<Option<Vec<Instant>>>() else {
            continue;
        };
        complete += 1;
        let first = *times.iter().min().unwrap();
        let h = usize::from(first >= half);
        wins[times.iter().position(|t| *t == first).unwrap()] += 1;
        for (id, t) in times.iter().enumerate() {
            behind[id][h].push((*t - first).as_secs_f64() * 1e3);
        }
    }
    let pct = |v: &mut Vec<f64>, p: f64| {
        v.sort_by(|a, b| a.partial_cmp(b).unwrap());
        if v.is_empty() {
            f64::NAN
        } else {
            v[((v.len() - 1) as f64 * p) as usize]
        }
    };
    println!(
        "{complete} changes seen on all {sockets} sockets ({} markets, {secs}s)",
        symbols.len()
    );
    println!(
        "socket | first% | behind fastest ms: p50 p90 p99 (1st half) | p50 p90 p99 (2nd half)"
    );
    for id in 0..sockets {
        let [a, b] = &mut behind[id];
        println!(
            "{id:>6} | {:>5.1}% | {:>5.2} {:>6.2} {:>7.2} | {:>5.2} {:>6.2} {:>7.2}",
            100.0 * wins[id] as f64 / complete.max(1) as f64,
            pct(a, 0.5),
            pct(a, 0.9),
            pct(a, 0.99),
            pct(b, 0.5),
            pct(b, 0.9),
            pct(b, 0.99)
        );
    }
    let mut e: Vec<f64> = exch_lag_ms.iter().map(|x| *x as f64).collect();
    println!(
        "receive - exchange ts_ms (local clock, ms): p50 {} p90 {} p99 {}",
        pct(&mut e, 0.5),
        pct(&mut e, 0.9),
        pct(&mut e, 0.99)
    );
    Ok(())
}
