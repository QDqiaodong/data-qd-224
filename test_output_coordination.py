"""Tests for single-production coordination and deadlock resolution."""
from contextlib import closing
from pathlib import Path
from tempfile import TemporaryDirectory
import hashlib
import sqlite3
import subprocess
import sys
import threading
import unittest

from objects import (
    CorruptBlob,
    CapacityExceeded,
    ObjectStore,
    OutputWaiting,
    StaleOutputClaim,
)


def digest(data):
    return hashlib.sha256(data).hexdigest()


class OutputCoordinationTest(unittest.TestCase):
    def setUp(self):
        self._tmp = TemporaryDirectory()
        self.path = Path(self._tmp.name, "coordination.db")
        self.store = ObjectStore(self.path)

    def tearDown(self):
        self._tmp.cleanup()

    def claim(self, key):
        status, value = self.store.claim_output(key)
        self.assertEqual(status, "claimed")
        return value

    # -- scenario 1: OR waits, an innocent lexicographically smaller key ----

    def test_or_wait_deadlock_spares_innocent_bystander(self):
        token_zero = self.claim("0")
        token_a = self.claim("A")
        token_b = self.claim("B")
        token_c = self.claim("C")

        # C starts with no waits and is therefore viable.  Its viability
        # propagates through A's OR clause to B and then to observer 0.
        self.assertEqual(self.store.set_output_waits(token_c, []), ())
        self.assertEqual(
            self.store.set_output_waits(token_a, [["B", "C"]]), ())
        self.assertEqual(
            self.store.set_output_waits(token_b, [["A"]]), ())
        self.assertEqual(
            self.store.set_output_waits(token_zero, [["A"]]), ())

        # C now needs both A and an unclaimed/free E.  E is viable, but A is
        # not; the fixed point leaves one sink SCC {A,B,C}.  "0" points into
        # it and is not a sink-SCC member.
        self.assertEqual(
            self.store.set_output_waits(token_c, [["A"], ["E"]]),
            ("A",))

        # The bystander and C/B keep their claims.  Replacing 0's wait again
        # performs resolution against the new state without revoking it.
        self.assertEqual(
            self.store.set_output_waits(token_zero, [["A"]]), ())
        self.assertEqual(
            self.store.set_output_waits(token_c, [["A"], ["E"]]), ())

        with self.assertRaises(OutputWaiting):
            self.store.complete_output(token_b, b"B-before-A")

        token_a2 = self.claim("A")
        self.assertIsNotNone(token_a2)
        self.assertNotEqual(token_a, token_a2)

        # An unproduced/free key or a fresh claim is only future possibility,
        # not a coordinated output.
        with self.assertRaises(OutputWaiting):
            self.store.complete_output(token_b, b"B-after-reclaim")

        self.assertEqual(self.store.complete_output(token_a2, b"A"),
                         digest(b"A"))
        self.assertEqual(self.store.complete_output(token_b, b"B"),
                         digest(b"B"))

        # Tokens and waits survive reopening; the sacrificed token stays dead
        # and cannot replace the new coordinated output.
        reopened = ObjectStore(self.path)
        with self.assertRaises(StaleOutputClaim):
            reopened.set_output_waits(token_a, [])
        with self.assertRaises(StaleOutputClaim):
            reopened.complete_output(token_a, b"old-A")
        self.assertFalse(reopened.cancel_output(token_a))
        self.assertEqual(
            reopened.claim_output("A"), ("ready", b"A"))

    def test_self_loop_is_sacrificed(self):
        token = self.claim("loop")
        self.assertEqual(
            self.store.set_output_waits(token, [["loop"]]), ("loop",))
        with self.assertRaises(StaleOutputClaim):
            self.store.set_output_waits(token, [])
        self.assertFalse(self.store.cancel_output(token))

        # The key is free and may be claimed by a new worker.
        new_token = self.claim("loop")
        self.store.complete_output(new_token, b"loop-output")
        self.assertEqual(
            self.store.claim_output("loop"), ("ready", b"loop-output"))

    def test_deterministic_minimum_among_sink_components(self):
        # Two direct self-loop sink components present in one committed graph.
        # The public API normally resolves the first loop immediately, so build
        # the two independent loops in one bulk fixture view and then trigger
        # resolution through a valid wait update.
        z = self.claim("z-loop")
        a = self.claim("a-loop")
        with closing(self.store.connection()) as db:
            db.execute(
                "INSERT INTO output_wait(token, clause_idx, dep_key) "
                "VALUES (?, 0, 'z-loop')", (z,))
            db.execute(
                "INSERT INTO output_wait(token, clause_idx, dep_key) "
                "VALUES (?, 0, 'a-loop')", (a,))
        trigger = self.claim("trigger")
        self.assertEqual(
            self.store.set_output_waits(trigger, []),
            ("a-loop", "z-loop"))

    # -- scenario 2: markers are not ordinary tags --------------------------

    def test_ordinary_publish_same_digest_invalidates_only_marker(self):
        data_a = b"coordinated-A"
        data_shared = b"shared-coordinated-bytes"

        token_a = self.claim("A")
        self.store.complete_output(token_a, data_a)
        token_shared_a = self.claim("sharedA")
        self.store.complete_output(token_shared_a, data_shared)
        token_shared_b = self.claim("sharedB")
        self.store.complete_output(token_shared_b, data_shared)

        # An ordinary content-addressed alias remains readable before and
        # after the coordinated name is ordinarily republished.
        self.store.publish("plain-alias", data_shared)

        waiter = self.claim("waiter")
        self.store.set_output_waits(waiter, [["A"]])

        # Ordinary same-name publication, even with identical bytes, kills
        # A's completion credential.
        self.store.publish("A", data_a)
        with self.assertRaises(OutputWaiting):
            self.store.complete_output(waiter, b"waiter-output")

        # A same-digest coordinated alias has its own independent marker.
        shared_waiter = self.claim("shared-waiter")
        self.store.set_output_waits(shared_waiter, [["sharedA"]])
        self.store.complete_output(shared_waiter, b"shared-waiter-output")

        # The ordinary name and unrelated same-digest aliases/markers survive.
        self.assertEqual(self.store.fetch("A"), data_a)
        self.assertEqual(self.store.fetch("plain-alias"), data_shared)
        self.assertEqual(
            self.store.claim_output("sharedA"), ("ready", data_shared))
        self.assertEqual(
            self.store.claim_output("sharedB"), ("ready", data_shared))

        # Re-coordinating A creates a fresh marker and admits the waiter.
        token_a2 = self.claim("A")
        self.store.complete_output(token_a2, data_a)
        self.assertEqual(
            self.store.complete_output(waiter, b"waiter-output"),
            digest(b"waiter-output"))

    def test_remove_invalidates_marker_but_alias_survives(self):
        data = b"coordinated-to-remove"
        token = self.claim("remove-me")
        self.store.complete_output(token, data)
        self.store.publish("same-digest-alias", data)

        waiter = self.claim("remove-waiter")
        self.store.set_output_waits(waiter, [["remove-me"]])

        self.assertTrue(self.store.remove("remove-me"))
        with self.assertRaises(OutputWaiting):
            self.store.complete_output(waiter, b"w1")
        self.assertEqual(self.store.fetch("same-digest-alias"), data)

        token2 = self.claim("remove-me")
        self.store.complete_output(token2, data)
        self.store.complete_output(waiter, b"w2")

    def test_capacity_eviction_invalidates_marker(self):
        data = b"evict"  # exactly five bytes
        token = self.claim("evict-me")
        self.store.complete_output(token, data)

        waiter = self.claim("eviction-waiter")
        self.store.set_output_waits(waiter, [["evict-me"]])

        self.store.configure_capacity(len(data))
        # Equal-length pressure forces the older coordinated label out under
        # the ordinary tagged-LRU rule.
        self.store.publish("pressure", b"xxxxx")

        with self.assertRaises(OutputWaiting):
            self.store.complete_output(waiter, b"w3")
        with self.assertRaises(KeyError):
            self.store.fetch("evict-me")
        self.assertEqual(self.store.fetch("pressure"), b"xxxxx")

    def test_corrupt_completed_blob_is_not_ready_but_marker_counts(self):
        data = b"intact-coordinated-bytes"
        token = self.claim("corrupt-key")
        expected_digest = self.store.complete_output(token, data)

        db = self.store.connection()
        try:
            db.execute(
                "UPDATE blob SET data=? WHERE digest=?",
                (b"corrupt-bytes!!", expected_digest))
        finally:
            db.close()

        with self.assertRaises(CorruptBlob):
            self.store.claim_output("corrupt-key")
        # The bad bytes are not replaced or repaired by the failed ready read.
        with self.assertRaises(CorruptBlob):
            self.store.claim_output("corrupt-key")

        # Dependency admission consumes marker+binding metadata only, so the
        # clause still exists; actual ready reads are where hash verification
        # matters.
        waiter_token = self.claim("metadata-waiter")
        self.store.set_output_waits(waiter_token, [["corrupt-key"]])
        self.store.complete_output(waiter_token, b"different-good-output")

    # -- scenario 3: capacity, claim ownership and concurrency -------------

    def test_capacity_failure_preserves_claim_until_pin_released(self):
        worker = ObjectStore(self.path)
        other = ObjectStore(self.path)

        old_data = b"aaaa"
        worker.publish("old", old_data)
        _, dep_token = worker.claim_output("dep-output")
        worker.complete_output(dep_token, b"")
        worker.configure_capacity(4)
        build_handle, _ = worker.begin_build(["old"])

        status, token = worker.claim_output("recipe")
        self.assertEqual(status, "claimed")
        worker.set_output_waits(token, [["dep-output"]])

        with self.assertRaises(CapacityExceeded):
            worker.complete_output(token, b"bbbb")

        # Neither the same store nor an independent instance may steal the
        # production right after failed admission.
        self.assertEqual(worker.claim_output("recipe"), ("busy", None))
        self.assertEqual(other.claim_output("recipe"), ("busy", None))

        self.assertTrue(worker.finish_build(build_handle))
        self.assertEqual(
            worker.complete_output(token, b"bbbb"), digest(b"bbbb"))
        self.assertEqual(worker.fetch("recipe"), b"bbbb")
        self.assertEqual(
            other.claim_output("recipe"), ("ready", b"bbbb"))
        with self.assertRaises(KeyError):
            worker.fetch("old")

    def test_two_connections_have_one_winner_with_barrier(self):
        first = ObjectStore(self.path)
        second = ObjectStore(self.path)
        barrier = threading.Barrier(2)
        results = []
        errors = []

        def race(store):
            try:
                barrier.wait(timeout=10)
                results.append(store.claim_output("raced-recipe"))
            except BaseException as exc:  # pragma: no cover - diagnostic
                errors.append(exc)

        threads = [
            threading.Thread(target=race, args=(first,)),
            threading.Thread(target=race, args=(second,)),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=30)

        self.assertEqual(errors, [])
        self.assertEqual(len(results), 2)
        claimed = [r for r in results if r[0] == "claimed"]
        busy = [r for r in results if r[0] == "busy"]
        self.assertEqual(len(claimed), 1)
        self.assertEqual(busy, [("busy", None)])
        self.assertIsInstance(claimed[0][1], str)

    # -- validation, limits, persistence and LRU ---------------------------

    def test_recipe_key_validation(self):
        valid = ["a", "Z", "0", "_", "-", "a" * 48, "A1_-x"]
        for key in valid:
            status, token = self.store.claim_output(key)
            self.assertEqual(status, "claimed", key)
            self.store.cancel_output(token)

        invalid = [
            "", "a" * 49, "has space", "dot.txt", "slash/x",
            "nonascii-é", None, 7, b"bytes",
        ]
        for key in invalid:
            with self.assertRaises(ValueError):
                self.store.claim_output(key)

    def test_wait_group_validation_is_all_or_nothing(self):
        token = self.claim("waiter")
        self.store.set_output_waits(token, [["ok-key"], ["a", "b"]])

        invalid_groups = [
            "not-a-sequence",
            42,
            [["x"] for _ in range(17)],
            [[]],
            [["a" * 49]],
            [["a", "a"]],
            [["valid"], "not-a-clause"],
            [["valid"], ("x", "x")],
        ]
        for groups in invalid_groups:
            with self.assertRaises(ValueError):
                self.store.set_output_waits(token, groups)

        # The previous valid declaration remains in force.
        with self.assertRaises(OutputWaiting):
            self.store.complete_output(token, b"too-soon")
        self.store.cancel_output(token)

    def test_stale_tokens_raise_stale_for_wait_and_complete(self):
        token = self.claim("stale-key")
        self.assertTrue(self.store.cancel_output(token))
        self.assertFalse(self.store.cancel_output(token))
        with self.assertRaises(StaleOutputClaim):
            self.store.set_output_waits(token, [])
        with self.assertRaises(StaleOutputClaim):
            self.store.complete_output(token, b"data")

        completed = self.claim("completed-key")
        self.store.complete_output(completed, b"done")
        with self.assertRaises(StaleOutputClaim):
            self.store.set_output_waits(completed, [])
        with self.assertRaises(StaleOutputClaim):
            self.store.complete_output(completed, b"again")

        with self.assertRaises(ValueError):
            self.store.set_output_waits(123, [])
        with self.assertRaises(ValueError):
            self.store.complete_output(None, b"x")
        with self.assertRaises(ValueError):
            self.store.complete_output(
                self.claim("too-large"), b"x" * ((1 << 20) + 1))

    def test_256_active_producer_limit_keeps_busy_and_ready(self):
        ready_token = self.claim("ready-key")
        self.store.complete_output(ready_token, b"ready")

        tokens = []
        for i in range(256):
            status, token = self.store.claim_output("producer-%03d" % i)
            self.assertEqual(status, "claimed")
            tokens.append(token)

        with self.assertRaises(ValueError):
            self.store.claim_output("one-too-many")

        # Full producer slots do not impair busy or coordinated reads.
        self.assertEqual(
            self.store.claim_output("producer-000"), ("busy", None))
        self.assertEqual(
            self.store.claim_output("ready-key"), ("ready", b"ready"))

        # Output claims are not build checkouts.
        handle, _ = self.store.begin_build(["ready-key"])
        self.assertTrue(self.store.finish_build(handle))

        self.assertTrue(self.store.cancel_output(tokens[0]))
        self.claim("one-too-many")

    def test_reopen_preserves_claims_waits_and_markers(self):
        token_a = self.claim("persist-A")
        token_b = self.claim("persist-B")
        self.store.set_output_waits(token_b, [["persist-A"]])
        self.store.complete_output(token_a, b"persisted-a")

        reopened = ObjectStore(self.path)
        self.assertEqual(
            reopened.claim_output("persist-A"), ("ready", b"persisted-a"))
        self.assertEqual(
            reopened.complete_output(token_b, b"persisted-b"),
            digest(b"persisted-b"))
        # token_a was used by complete_output and is therefore stale.
        with self.assertRaises(StaleOutputClaim):
            reopened.set_output_waits(token_a, [])

    def test_only_successful_complete_refreshes_lru(self):
        ordinary_1 = self.store.publish("ordinary-1", b"lru-first")
        ordinary_2 = self.store.publish("ordinary-2", b"lru-second")
        ready_data = b"ready-for-lru"
        ready_digest = digest(ready_data)
        ready_token = self.claim("ready-lru")
        self.store.complete_output(ready_token, ready_data)

        def use_seqs():
            with closing(sqlite3.connect(str(self.path))) as db:
                return dict(db.execute("SELECT digest, seq FROM digest_use"))

        before = use_seqs()
        self.assertEqual(
            self.store.claim_output("ready-lru"), ("ready", ready_data))
        waiter = self.claim("lru-waiter")
        self.store.set_output_waits(waiter, [["ready-lru"]])
        self.assertEqual(use_seqs(), before)

        new_digest = self.store.complete_output(waiter, b"coord-data")
        after = use_seqs()
        self.assertGreater(after[new_digest], max(before.values()))
        self.assertEqual(after[ready_digest], before[ready_digest])
        self.assertEqual(after[ordinary_1], before[ordinary_1])
        self.assertEqual(after[ordinary_2], before[ordinary_2])


MIB = 1 << 20


COLD_CHILD = r"""
import resource
import sys

project_dir, path, token, mib_text = sys.argv[1:5]
sys.path.insert(0, project_dir)
from objects import ObjectStore

mib = int(mib_text)
store = ObjectStore(path)

# The limit is measured only after imports and the first database open, as
# required by the cold acceptance harness.
with open("/proc/self/status", "r", encoding="ascii") as status:
    vmsize = next(
        int(line.split()[1]) * 1024
        for line in status
        if line.startswith("VmSize:")
    )
resource.setrlimit(
    resource.RLIMIT_AS, (vmsize + 24 * mib, vmsize + 24 * mib))

released = store.set_output_waits(token, [["L0-00"]])
assert released == ("L0-00",), released

# Reading every unrelated 1 MiB artifact sequentially must still work;
# fetch itself verifies SHA-256 and each result is released before the next.
for i in range(80):
    name = "blob-%03d" % i
    data = store.fetch(name)
    assert len(data) == mib, (name, len(data))

print("cold coordination ok")
"""


class ColdCoordinationAcceptanceTest(unittest.TestCase):
    @unittest.skipUnless(sys.platform.startswith("linux"),
                         "address-space acceptance is Linux-specific")
    def test_prebuilt_fixture_resolves_under_vmsize_plus_24mib(self):
        with TemporaryDirectory() as tmp:
            path = Path(tmp, "cold-coordination.db")
            store = ObjectStore(path)

            # Prebuild all bulky data in the unrestricted parent process.
            for i in range(80):
                data = ("%08d" % i).encode() * (MIB // 8)
                store.publish("blob-%03d" % i, data)

            wait_rows = []
            tokens = {}

            def add_chain(keys, free_endpoint):
                for current, nxt in zip(keys, keys[1:]):
                    wait_rows.append((tokens[current], 0, nxt))
                wait_rows.append((tokens[keys[-1]], 0, free_endpoint))

            # Eight independent 16-producer chains.
            for group in range(8):
                keys = ["C%d-%02d" % (group, i) for i in range(16)]
                for key in keys:
                    _, tokens[key] = store.claim_output(key)
                add_chain(keys, "free-C%d" % group)

            # One 128-producer chain crossing four named groups.  Its free end
            # makes it viable until the child closes L3-31 back to L0-00.
            long_chain = [
                "L%d-%02d" % (group, i)
                for group in range(4)
                for i in range(32)
            ]
            for key in long_chain:
                _, tokens[key] = store.claim_output(key)
            for current, nxt in zip(long_chain, long_chain[1:]):
                wait_rows.append((tokens[current], 0, nxt))
            wait_rows.append((tokens[long_chain[-1]], 0, "free-L"))

            with closing(store.connection()) as db:
                db.executemany(
                    "INSERT INTO output_wait(token, clause_idx, dep_key) "
                    "VALUES (?, ?, ?)", wait_rows)
                independent_before = db.execute("""
                    SELECT c.recipe_key, w.clause_idx, w.dep_key
                    FROM output_wait w
                    JOIN output_claim c ON c.token = w.token
                    WHERE c.recipe_key LIKE 'C%'
                    ORDER BY c.recipe_key, w.clause_idx, w.dep_key
                """).fetchall()

            result = subprocess.run(
                [sys.executable, "-B", "-c", COLD_CHILD,
                 str(Path(__file__).resolve().parent), str(path),
                 tokens[long_chain[-1]], str(MIB)],
                capture_output=True,
                text=True,
                timeout=120,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("cold coordination ok", result.stdout)

            with closing(sqlite3.connect(str(path))) as db:
                independent_after = db.execute("""
                    SELECT c.recipe_key, w.clause_idx, w.dep_key
                    FROM output_wait w
                    JOIN output_claim c ON c.token = w.token
                    WHERE c.recipe_key LIKE 'C%'
                    ORDER BY c.recipe_key, w.clause_idx, w.dep_key
                """).fetchall()
                self.assertEqual(independent_after, independent_before)
                self.assertIsNone(db.execute(
                    "SELECT 1 FROM output_claim WHERE recipe_key='L0-00'"
                ).fetchone())
                self.assertEqual(db.execute("""
                    SELECT w.dep_key
                    FROM output_wait w
                    JOIN output_claim c ON c.token=w.token
                    WHERE c.recipe_key='L3-31'
                """).fetchall(), [("L0-00",)])


if __name__ == "__main__":
    unittest.main()
