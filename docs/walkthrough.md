# Code walkthrough

Start with the offline run and trace one event through the system:

```bash
make demo
```

## Files to read in order

| File | Responsibility | Question to answer while reading |
| --- | --- | --- |
| `mdpipe/feed.py` | Parse the exchange envelope into exact integer levels or a control tick | Why is `new_quantity` a replacement, and why can a heartbeat advance continuity? |
| `include/mdp/book.hpp` | Own depth, sequence, generation, and trust | What happens before any level changes when a sequence is missing? |
| `include/mdp/protocol.hpp` | Strict stdin commands and JSON replies | How are malformed commands prevented from retaining a trusted book? |
| `mdpipe/pipeline.py` | Coordinate reader, consumer, watchdog, publication, and reconnect | Can a completed disk commit reopen a gate after a disconnect? |
| `mdpipe/journal.py` | Durable ordered records and hash verification | Why is the recorder permanently disabled after a failed commit? |
| `mdpipe/replay.py` | Reconstruct and compare every recorded result | Why are publication markers separate from book application records? |
| `mdpipe/harness.py` | Fault scenarios and independent quote oracle | Does passing replay alone prove the original reconstruction was correct? |

## A normal update

1. The reader receives a UTF-8 frame, records local receive times, and enqueues it within both bounds.
2. The consumer checks its age and validates the complete exchange envelope.
3. The C++ book checks its generation and sequence, applies absolute level sizes, and rejects an invalid final book.
4. The recorder commits the raw frame, normalized command, and resulting state fingerprint.
5. The consumer checks freshness and the live gate again. Only then can it publish a quote and record a publication marker.

## A dropped message

The harness removes the seventh update before queuing it and records the injected fault. The next envelope is no longer consecutive. C++ clears trust and depth, the publication gate stays shut, and the supervisor reconnects. A fresh snapshot in a higher generation restores the authoritative synthetic exchange book; every resumed quote is checked against that independently maintained book.

## A slow commit and concurrent disconnect

The command/commit pair finishes so the journal retains replay ordering. Meanwhile, the reader or freshness watchdog closes the gate. Once the awaiting consumer resumes, it checks `failed` before publishing. The corresponding application record may exist without a publication marker; replay reconstructs it but does not pretend it was delivered as a quote.

## Tests worth discussing

- `test_core.py`: 6,000 randomized updates checked against independent maps, plus sequence, generation, crossing, and decimal-related invariants.
- `test_websocket.py`: real local WebSocket transport, subscription validation, sequenced controls, a deliberate gap, reconnect, and fresh-snapshot recovery.
- `test_pipeline.py`: failure/recovery assertions, journal failures, a disconnect during commit, snapshot/idle deadlines, and byte bounds.
- `test_freshness.py`: a stalled journal commit cannot keep a stale publication gate open.
- `test_journal.py`: corruption and missing internal records are rejected; committed prefixes replay.

## Useful interview discussion

**Why not keep publishing the old book during a reconnect?** It is a historical observation with unknown missing updates. The API emits an explicit untrusted state instead of presenting it as current.

**What was a real integration problem?** Initial tests treated only Level 2 messages as sequenced. A real feed capture revealed that subscription replies and heartbeats occupied the same sequence stream. Keeping those envelopes as no-op `TICK` commands fixed false gaps, and the transport test now reproduces that case.

**What is the performance bottleneck?** The C++ book is much faster than the full collector. JSON parsing, text IPC, per-event commits, and publication recording determine end-to-end latency. The benchmark reports these separately so native-only throughput is not presented as live pipeline capacity.

**What would you change next?** Measure long-running captures first; then evaluate group commits, direct native bindings, or binary transport. Preserve the same independent oracle, replay comparison, and cancellation/freshness failure tests as the implementation changes.

Use your own Mac measurements when describing performance. Read and modify the components until you can explain these design decisions and tests from the code.
