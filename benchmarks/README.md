# Measured baseline

Measured October 7, 2026 in a Linux/x86-64 virtualized development environment, AMD EPYC 9V74 virtual CPU, GCC 13.3, Python 3.12.14, websockets 16.0. The process was not pinned to a CPU and the machine was not isolated. Results on a Mac will differ.

## Native order-book microbenchmark

```bash
make -j2
./build/core-bench --events 100000 --trials 5 > benchmarks/my_core.csv
python3 scripts/summarize_bench.py benchmarks/my_core.csv
```

| Profile | Combined depth | Median throughput | Median of trial p99 update times |
| --- | ---: | ---: | ---: |
| Shallow | 200 levels | 16.45 million updates/s | 100 ns |
| Deep | 20,000 levels | 4.07 million updates/s | 461 ns |

Raw results: [linux_core.csv](linux_core.csv). Each profile has five trials of the same deterministic 100,000-update trace. The trace and initial snapshot are prepared before timing. Each event changes one existing level's absolute quantity. This does not benchmark deletion-heavy, insertion-heavy, or snapshot-heavy traffic.

Throughput uses a batch timing pass. Latency uses a separate pass with per-event clock reads; reported times include clock overhead, which is not subtracted. The final state fingerprints must match across the two passes. These are **book-only** figures and exclude JSON parsing, stdin/stdout IPC, WebSocket handling, recording, and publishing. They are not a claimed live ingestion rate.

## Full pipeline

- [live-baseline.json](live-baseline.json): 35-second public BTC-USD capture, 505 book messages and 28 control envelopes; no gaps or reconnects. Median receive-to-publication 600.567 microseconds, p99 186,128.934 microseconds. The initial roughly 41,000-level snapshot and queued startup burst dominate the tail.
- [live-gap-recovery.json](live-gap-recovery.json): deliberately dropped one live update; detected one gap, reconnected once, accepted two snapshots across generations, and resumed trusted publication. The measured gap-to-recovery interval was about 9.89 seconds, including a new network connection and snapshot.
- [synthetic-faults.json](synthetic-faults.json): baseline plus dropped-message, disconnect, queue-overflow, and stale-processing scenarios. Every published quote matched an independently maintained exchange oracle; every journal replay matched its capture.

The reports retain individual latency distributions and queue peaks. `processed` counts Level 2 envelopes; `control_processed` counts other sequenced envelopes. `received` can exceed their sum when a fault abandons queued frames, or when a deliberately dropped frame is audited separately.

Receive-to-publication latency begins at local receipt and includes queueing, normalization, native round-trip, and the command's durable commit. It stops at the publication callback, before the separate publication marker commit. It does not measure venue-to-client latency.

Metrics use at most 4,096 retained samples per distribution. Percentiles become reservoir estimates beyond that sample count; count, mean, and maximum still cover all observed values.

## Reproduce the included live sessions offline

Compressed real journals are under `examples/`. From the project root:

```bash
gzip -dk examples/live-baseline.sqlite.gz
gzip -dk examples/live-gap-recovery.sqlite.gz
python3 -m mdpipe replay examples/live-baseline.sqlite --speed 0
python3 -m mdpipe replay examples/live-gap-recovery.sqlite --speed 0 --features data/example-features.csv
```

The decompressed files are ignored by Git. The final state is intentionally untrusted because capture ended cleanly. `last_trusted.generation` is 2 for the recovered session, and replay verifies the recorded results across the invalidation and new snapshot.
