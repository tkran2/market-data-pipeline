# Fault-Tolerant Market Data Pipeline

A public Coinbase Level 2 collector with a C++20 order-book core, bounded async ingestion, durable recording, verified replay, and reproducible fault injection.

The central rule: **after a sequence gap, disconnect, stale backlog, or recording failure, stop publishing quotes. Recover only from a new connection and a fresh snapshot.**

## What it does

- Reconstructs full aggregated bid/ask depth using exact fixed-point prices and quantities.
- Tracks the connection's sequence across book messages, subscription replies, and heartbeats.
- Fences old connections with generation IDs; rejects duplicate/out-of-order updates without mutating the book.
- Bounds ingestion by message count and bytes; overload closes the publication gate and resynchronizes.
- Commits raw frames, normalized commands, results, and publication markers to SQLite WAL with a SHA-256 hash chain.
- Replays a capture at maximum speed or at a multiple of its original receive-time spacing, checking every reconstructed result.
- Exports trusted top-of-book research features: midprice, spread, size imbalance, and microprice.
- Measures queue depth, queue dwell, native round-trip latency, journal commits, receive-to-publication latency, and recovery time.

## Build and try it

Requires a C++20 compiler, Make, and Python 3.11+. Only live capture and the local WebSocket transport test need the single Python dependency.

```bash
make -j2 test
make demo
make faults
make bench
```

`demo` and `faults` generate new files under `data/` and need no network. The fault suite validates every published best bid/ask against an independent synthetic exchange book and then checks exact replay.

For live public data:

```bash
make setup
.venv/bin/python -m mdpipe capture --product BTC-USD --seconds 60
```

No exchange account or API key is needed. The command prints trusted quotes, explicit untrusted notifications when the gate closes, and a final metrics report. A stopped session intentionally ends with an untrusted book; `last_trusted` is historical.

To record a capture at a chosen, unused path and export replayed research features:

```bash
.venv/bin/python -m mdpipe capture --seconds 60 --output data/btc-session.sqlite --quiet
.venv/bin/python -m mdpipe replay data/btc-session.sqlite --speed 20 --features data/btc-features.csv
.venv/bin/python -m mdpipe replay data/btc-session.sqlite --speed 0
```

Capture and CSV paths are exclusive-create: choose a new filename for each run.

On a Mac with the previously observed SDK/linker mismatch, select the compatible installed SDK before building:

```bash
export SDKROOT=/Library/Developer/CommandLineTools/SDKs/MacOSX26.5.sdk
make -j2 test
```

The Makefile honors `SDKROOT` on macOS. Full setup and publishing commands are in [docs/mac-and-github.md](docs/mac-and-github.md).

## Fault injection

```bash
make faults
```

| Scenario | Failure detected | Recovery assertion |
| --- | --- | --- |
| Baseline | None | All published quotes match the oracle |
| Drop one update | Sequence gap | New connection and fresh snapshot restore correct state |
| Disconnect the feed | Connection loss | No quote passes a closed gate; fresh snapshot resumes publication |
| Slow the consumer | Bounded queue overflow | Queue stays within its limits; consumer recovers |
| Age the backlog | Processing freshness deadline | Independent watchdog closes the gate before stale work can publish |

Additional tests cover a stalled disk commit, failed persistence, invalid decimal precision, crossed books, malformed messages, old connection generations, journal corruption, and missing snapshots despite ongoing heartbeats. Randomized reconstruction checks 6,000 updates against independent Python maps.

The same injection switches work on the live feed:

```bash
.venv/bin/python -m mdpipe capture --seconds 60 --drop-at 7 --quiet
.venv/bin/python -m mdpipe capture --seconds 60 --disconnect-at 7 --quiet
.venv/bin/python -m mdpipe capture --seconds 60 --slow-ms 20 --queue-messages 4 --quiet
```

`--drop-at` and `--disconnect-at` count Level 2 update messages, excluding snapshots and control envelopes. A persistent slowdown may exhaust the reconnect budget rather than catch up; the offline harness removes the injected slowdown after the first connection so it can verify recovery.

## Validation and measurements

The included baseline was measured on Linux/x86-64 with GCC 13.3, Python 3.12.14, and an AMD EPYC 9V74 virtual CPU. It is a measured baseline, not a promise of performance on another machine.

- **38 automated tests** passed in optimized and AddressSanitizer/UndefinedBehaviorSanitizer builds.
- A real public BTC-USD capture processed **505 book messages plus 28 control envelopes** in a 35-second session, with **zero detected gaps or reconnects**. Replay reproduced every recorded command result and publication.
- That live capture's median receive-to-publication latency was **0.601 ms**. Its p99 was **186.129 ms**, dominated by the initial full-depth snapshot and its queued burst. Reported latency starts when the client receives a frame; it excludes exchange-to-client network transit.
- A second real session deliberately dropped one update, detected the gap, reconnected once, and resumed from a fresh snapshot. Both live journals are included as compressed [examples](examples/README.md) for offline replay.
- Native book-only synthetic throughput and fault measurements are in [benchmarks/README.md](benchmarks/README.md). Native throughput excludes networking, JSON, subprocess IPC, and durable commits.

The GitHub Actions workflow is configured for Linux/macOS builds, sanitizer tests, recovery reports, and CMake/CTest. It will run when the repository is pushed; the validation above was run locally in the development environment.

## Design and walkthrough

- [Architecture and correctness rules](docs/design.md)
- [Code walkthrough and interview discussion](docs/walkthrough.md)
- [Mac setup and public GitHub publishing](docs/mac-and-github.md)
- [Benchmark method and recorded results](benchmarks/README.md)

## Scope

This reconstructs **aggregated Level 2 depth**, not individual orders or exchange matching. A recovery snapshot restores current state but cannot recreate missing historical updates; research should treat generation changes as discontinuities. The collector is a single process with a child book engine and durable local storage, not a replicated service.

The SHA-256 chain detects row corruption, reordering, and missing internal records. Without an external trusted head hash it cannot prove that a valid suffix was never deleted. Command durability precedes publication, but callbacks and publication markers are not an exactly-once delivery transaction. These tradeoffs are explained in the design document.

## Exchange references

- [Coinbase Advanced Trade WebSocket endpoints](https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/websocket/websocket-endpoints)
- [Coinbase WebSocket overview](https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/websocket/websocket-overview)
- [Coinbase Level 2 message format](https://docs.cdp.coinbase.com/api-reference/advanced-trade-api/websocket/level2)
- [websockets client API](https://websockets.readthedocs.io/en/stable/reference/asyncio/client.html)

Licensed under MIT.
