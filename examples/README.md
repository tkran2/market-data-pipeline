# Recorded live examples

These compressed SQLite journals contain public Coinbase Advanced Trade BTC-USD Level 2 messages captured October 7, 2026. No account credentials were used.

- `live-baseline.sqlite.gz`: 35 seconds, 505 book messages, 28 sequenced controls, zero detected gaps.
- `live-gap-recovery.sqlite.gz`: one update deliberately dropped, one detected gap, one reconnect, and a fresh snapshot in generation 2. This journal also contains the audited dropped raw frame as a fault record; replay does not apply it.

Decompress once and replay from the project root:

```bash
gzip -dk examples/live-baseline.sqlite.gz
gzip -dk examples/live-gap-recovery.sqlite.gz
python3 -m mdpipe replay examples/live-gap-recovery.sqlite --speed 0
```

The matching reports are in `benchmarks/`. These samples are protocol and recovery demonstrations, not a complete historical market dataset.
