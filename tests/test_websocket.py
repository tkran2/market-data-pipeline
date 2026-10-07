"""Exercise the actual network adapter against a local, protocol-shaped exchange."""
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

from mdpipe.journal import Journal, Recorder
from mdpipe.native import NativeBook
from mdpipe.pipeline import Config, Pipeline
from mdpipe.replay import replay
from mdpipe.simulated import payload
from mdpipe.source import CoinbaseSource

@unittest.skipUnless(importlib.util.find_spec("websockets"), "install requirements.txt for real WebSocket transport test")
class WebSocketTests(unittest.IsolatedAsyncioTestCase):
    async def test_subscribe_gap_fresh_connection_and_snapshot(self):
        from websockets.asyncio.server import serve
        connections = []
        async def exchange(ws):
            subscriptions = [json.loads(await ws.recv()), json.loads(await ws.recv())]
            connections.append(subscriptions)
            generation = len(connections)
            await ws.send('{"channel":"subscriptions","sequence_num":0,"events":[]}')
            await ws.send(payload("BTC-USD", 1, "snapshot", [("B", 100, 2), ("A", 110, 3)]))
            await ws.send('{"channel":"heartbeats","sequence_num":2,"events":[]}')
            await ws.send(payload("BTC-USD", 3, "update", [("B", 100, generation + 2)]))
            if generation == 1:
                await ws.send(payload("BTC-USD", 5, "update", [("B", 100, 9)]))
            else:
                for seq in range(4, 12):
                    await ws.send(payload("BTC-USD", seq, "update", [("A", 110, seq)]))
            await ws.wait_closed()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "ws.sqlite"
            recorder = Recorder(Journal(path, "BTC-USD"))
            try:
                async with serve(exchange, "127.0.0.1", 0) as server:
                    port = server.sockets[0].getsockname()[1]
                    config = Config(stop_after=8, retry_seconds=0)
                    async with NativeBook() as core:
                        pipeline = Pipeline(core, recorder, config)
                        report = await pipeline.run(lambda _: CoinbaseSource(config, f"ws://127.0.0.1:{port}"))
            finally:
                await recorder.close()
            self.assertEqual(len(connections), 2)
            for subscriptions in connections:
                self.assertEqual([item["channel"] for item in subscriptions], ["level2", "heartbeats"])
                self.assertTrue(all(item["product_ids"] == ["BTC-USD"] and "jwt" not in item for item in subscriptions))
            self.assertEqual(report["metrics"]["invalidations"].get("sequence_gap"), 1)
            self.assertEqual(report["last_trusted"]["generation"], 2)
            self.assertEqual(report["metrics"]["control_processed"], 4)
            restored = await replay(path)
            self.assertEqual(restored["last_trusted"], report["last_trusted"])
