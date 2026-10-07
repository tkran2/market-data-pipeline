import random
import unittest

from mdpipe.native import NativeBook

class CoreTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.core = await NativeBook().__aenter__()

    async def asyncTearDown(self):
        await self.core.__aexit__()

    async def seed(self):
        await self.core.request("RESET 1")
        return await self.core.request("SNAP 1 0 2 B 100 20 A 110 30")

    async def test_cold_start_no_trusted_quote(self):
        state = await self.core.request("STATUS")
        self.assertFalse(state["trusted"])
        self.assertIsNone(state["bid"])

    async def test_snapshot_absolute_updates_and_delete(self):
        await self.seed()
        state = await self.core.request("UPD 1 1 2 B 100 5 A 110 0")
        self.assertEqual(state["bid"], [100, 5])
        self.assertIsNone(state["ask"])
        self.assertTrue(state["trusted"])

    async def test_controls_before_snapshot_and_between_updates_share_sequence(self):
        await self.core.request("RESET 1")
        control = await self.core.request("TICK 1 0")
        self.assertFalse(control["trusted"])
        self.assertEqual(control["status"], "tick")
        await self.core.request("SNAP 1 1 2 B 100 20 A 110 30")
        control = await self.core.request("TICK 1 2")
        self.assertTrue(control["trusted"])
        self.assertEqual(control["bid"], [100, 20])
        updated = await self.core.request("UPD 1 3 1 B 100 5")
        self.assertEqual(updated["bid"], [100, 5])
        self.assertEqual(updated["gaps"], 0)
        gap = await self.core.request("TICK 1 5")
        self.assertEqual(gap["status"], "gap")
        self.assertFalse(gap["trusted"])

    async def test_gap_before_snapshot_requires_new_connection(self):
        await self.core.request("RESET 1")
        await self.core.request("TICK 1 0")
        gap = await self.core.request("SNAP 1 2 2 B 100 20 A 110 30")
        self.assertEqual(gap["status"], "gap")
        self.assertFalse(gap["trusted"])

    async def test_gap_closes_trust_until_new_generation_snapshot(self):
        await self.seed()
        gap = await self.core.request("UPD 1 2 1 B 100 5")
        self.assertEqual(gap["status"], "gap")
        self.assertFalse(gap["trusted"])
        self.assertIsNone(gap["bid"])
        self.assertFalse((await self.core.request("SNAP 1 2 2 B 100 5 A 110 30"))["trusted"])
        await self.core.request("RESET 2")
        recovered = await self.core.request("SNAP 2 0 2 B 100 5 A 110 30")
        self.assertTrue(recovered["trusted"])

    async def test_duplicates_and_older_messages_do_not_mutate(self):
        await self.seed()
        first = await self.core.request("UPD 1 1 1 B 100 5")
        for command in ("UPD 1 1 1 B 100 99", "UPD 1 0 1 B 100 88"):
            result = await self.core.request(command)
            self.assertEqual(result["status"], "duplicate")
            self.assertEqual(result["digest"], first["digest"])
            self.assertEqual(result["bid"], [100, 5])

    async def test_old_generation_cannot_update_or_invalidate(self):
        await self.seed()
        await self.core.request("RESET 2")
        latest = await self.core.request("SNAP 2 0 2 B 105 1 A 110 1")
        for command in ("UPD 1 1 1 B 106 99", "INVALIDATE 1 disconnect", "RESET 1", "SNAP 1 0 0"):
            result = await self.core.request(command)
            self.assertEqual(result["status"], "stale_generation")
            self.assertEqual(result["digest"], latest["digest"])

    async def test_crossed_atomic_batch_never_publishes(self):
        await self.seed()
        result = await self.core.request("UPD 1 1 2 B 101 5 A 99 2")
        self.assertFalse(result["trusted"])
        self.assertEqual(result["reason"], "crossed_book")
        self.assertEqual(result["bid_levels"], 0)

    async def test_snapshot_duplicates_even_zero_rejected(self):
        await self.core.request("RESET 1")
        result = await self.core.request("SNAP 1 0 3 B 100 0 B 100 1 A 110 1")
        self.assertFalse(result["trusted"])

    async def test_malformed_packet_fails_closed(self):
        for generation, template in enumerate(("UPD {g} 1 2 B 100 5", "UPD {g} 1 1 B 100 -1", "UPD {g} 1 1 X 100 1", "WHAT", "UPD {g} 1 0 extra"), 1):
            await self.core.request(f"RESET {generation}")
            await self.core.request(f"SNAP {generation} 0 2 B 100 20 A 110 30")
            result = await self.core.request(template.format(g=generation))
            self.assertFalse(result["trusted"])
            self.assertIsNone(result["bid"])

    async def test_repeated_updates_within_one_packet_last_value_wins(self):
        await self.seed()
        result = await self.core.request("UPD 1 1 3 B 100 0 B 100 2 B 100 7")
        self.assertEqual(result["bid"], [100, 7])

    async def test_randomized_reconstruction_against_independent_maps(self):
        # Recompute sorted full depth every 25 updates. The oracle stores absolute
        # quantities directly and does not import any C++ hashing/matching code.
        for generation, seed in enumerate((7, 17, 29), 1):
            rng = random.Random(seed)
            bids, asks = {100: 20}, {200: 20}
            await self.core.request(f"RESET {generation}")
            await self.core.request(f"SNAP {generation} 0 2 B 100 20 A 200 20")
            for sequence in range(1, 2001):
                side = rng.choice(("B", "A"))
                price = rng.randrange(1, 100) if side == "B" else rng.randrange(201, 300)
                quantity = rng.randrange(0, 50)
                target = bids if side == "B" else asks
                if quantity:
                    target[price] = quantity
                else:
                    target.pop(price, None)
                result = await self.core.request(f"UPD {generation} {sequence} 1 {side} {price} {quantity}")
                self.assertTrue(result["trusted"])
                self.assertEqual(result["sequence"], sequence)
                self.assertEqual(result["bid"], [max(bids), bids[max(bids)]])
                self.assertEqual(result["ask"], [min(asks), asks[min(asks)]])
                if sequence % 25 == 0:
                    depth = await self.core.request("DUMP")
                    self.assertEqual(depth["bids"], [list(item) for item in sorted(bids.items())])
                    self.assertEqual(depth["asks"], [list(item) for item in sorted(asks.items())])
