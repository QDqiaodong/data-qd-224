from contextlib import closing
from pathlib import Path
from tempfile import TemporaryDirectory
import hashlib
import io
import sqlite3
import unittest

from objects import (
    CorruptBlob,
    ObjectStore,
    PhysicalCapacityExceeded,
)

U = 64 << 10
MIB = 1 << 20
GIB = 1 << 30


def digest(data):
    return hashlib.sha256(data).hexdigest()


def repeated_block(byte):
    return bytes([byte]) * U


def pseudo_block(label, index):
    """Deterministic SHA-derived, effectively incompressible block."""
    seed = label + index.to_bytes(8, "big")
    return b"".join(
        hashlib.sha256(
            seed + counter.to_bytes(4, "big")).digest()
        for counter in range(U // hashlib.sha256().digest_size)
    )


def page_used_bytes(path):
    with sqlite3.connect(str(path)) as db:
        page_count = db.execute("PRAGMA page_count").fetchone()[0]
        freelist = db.execute("PRAGMA freelist_count").fetchone()[0]
        page_size = db.execute("PRAGMA page_size").fetchone()[0]
    return (page_count - freelist) * page_size


def corrupt_named_block(path, block, replacement=None):
    block_hash = digest(block)
    if replacement is None:
        replacement = b"\x5a" * len(block)
    with sqlite3.connect(str(path)) as db:
        cursor = db.execute(
            "UPDATE shared_block SET data=? WHERE block_hash=?",
            (replacement, block_hash))
        if cursor.rowcount != 1:
            raise AssertionError("test block was not found")


class SharedConfigurationTest(unittest.TestCase):
    def test_unconfigured_store_reports_whole_resident_bytes(self):
        with TemporaryDirectory() as tmp:
            path = Path(tmp, "legacy.db")
            store = ObjectStore(path)
            store.publish("one", b"one")
            store.publish("two", b"two-two")
            self.assertEqual(store.shared_payload_usage(), (10, 10))

            # Another instance on the same path sees the same DB default.
            other = ObjectStore(path)
            self.assertEqual(other.shared_payload_usage(), (10, 10))

            # Enabling is impossible once any whole artifact is resident.
            with self.assertRaises(ValueError):
                store.configure_shared_payload(10 << 20)
            self.assertEqual(store.shared_payload_usage(), (10, 10))

    def test_validation_db_wide_setting_and_empty_artifact(self):
        with TemporaryDirectory() as tmp:
            path = Path(tmp, "shared.db")
            store = ObjectStore(path)
            bad_values = (True, False, -1, GIB + 1, 1.5, "65536", None)
            for bad in bad_values:
                with self.assertRaises(ValueError):
                    store.configure_shared_payload(bad)

            store.configure_shared_payload(0)
            # A separately constructed instance is configured by the DB.
            self.assertEqual(ObjectStore(path).shared_payload_usage(), (0, 0))

            empty_digest = hashlib.sha256(b"").hexdigest()
            self.assertEqual(store.publish("empty", b""), empty_digest)
            self.assertEqual(store.shared_payload_usage(), (0, 0))
            self.assertEqual(store.fetch("empty"), b"")

            with self.assertRaises(PhysicalCapacityExceeded):
                store.publish("nonempty", b"x")
            self.assertEqual(store.shared_payload_usage(), (0, 0))
            with self.assertRaises(KeyError):
                store.fetch("nonempty")

            # Another connection may adjust the DB-wide limit.
            ObjectStore(path).configure_shared_payload(U)
            self.assertEqual(store.publish("one-block", b"x" * U),
                             digest(b"x" * U))
            self.assertEqual(store.shared_payload_usage(), (U, U))

    def test_failed_lower_limit_changes_nothing(self):
        with TemporaryDirectory() as tmp:
            path = Path(tmp, "shared.db")
            store = ObjectStore(path)
            store.configure_shared_payload(3 * U)
            x = repeated_block(1)
            y = repeated_block(2)
            store.publish("xy", x + y)
            self.assertEqual(store.shared_payload_usage(), (2 * U, 2 * U))

            with self.assertRaises(PhysicalCapacityExceeded):
                store.configure_shared_payload(2 * U - 1)

            with closing(sqlite3.connect(str(path))) as db:
                limit = db.execute(
                    "SELECT value FROM store_meta "
                    "WHERE key='shared_payload_capacity'").fetchone()[0]
            self.assertEqual(limit, 3 * U)
            self.assertEqual(store.shared_payload_usage(), (2 * U, 2 * U))
            # The old limit is still in force: two more unique blocks would
            # bring the store to four blocks.
            with self.assertRaises(PhysicalCapacityExceeded):
                store.publish("zw", repeated_block(4) + repeated_block(5))
            store.configure_shared_payload(3 * U)


class SharedEvictionAdmissionTest(unittest.TestCase):
    def setUp(self):
        self._tmp = TemporaryDirectory()
        self.path = Path(self._tmp.name, "objects.db")
        self.store = ObjectStore(self.path)

    def tearDown(self):
        self._tmp.cleanup()

    def _use_rows(self):
        with closing(sqlite3.connect(str(self.path))) as db:
            return dict(db.execute("SELECT digest, seq FROM digest_use"))

    def test_shared_victim_must_be_evaluated_before_physical_fit(self):
        x = repeated_block(11)
        a = repeated_block(12)
        b = repeated_block(13)
        y = repeated_block(14)
        c = repeated_block(15)
        A = x + a
        B = x + b
        rejected_C = y + c
        accepted_C = x + c

        self.store.configure_shared_payload(3 * U)
        self.store.configure_capacity(4 * U)
        self.store.publish("A-one", A)
        self.store.publish("A-two", A)
        self.store.publish("B-tag", B)
        handle, _ = self.store.begin_build(["B-tag"])
        digest_a = digest(A)
        digest_b = digest(B)
        digest_rejected = digest(rejected_C)
        digest_accepted = digest(accepted_C)

        self.assertEqual(self.store.shared_payload_usage(),
                         (4 * U, 3 * U))
        use_before = self._use_rows()

        # Y+c would make the original logical victim A, but deleting A only
        # frees its exclusive a block; X remains shared with protected B.
        with self.assertRaises(PhysicalCapacityExceeded):
            self.store.publish("C", rejected_C)

        # The rejected transaction left no use-order or payload changes.
        self.assertEqual(self._use_rows(), use_before)
        self.assertEqual(self.store.shared_payload_usage(),
                         (4 * U, 3 * U))

        # A second connection sees the complete pre-commit state.
        other = ObjectStore(self.path)
        self.assertEqual(other.fetch("A-one"), A)
        self.assertEqual(other.fetch("A-two"), A)
        self.assertEqual(other.fetch_for_build(handle, "B-tag"), B)
        self.assertEqual(other.shared_payload_usage(), (4 * U, 3 * U))
        with closing(sqlite3.connect(str(self.path))) as db:
            aliases = {
                row[0] for row in db.execute(
                    "SELECT name FROM tag WHERE digest=? ORDER BY name",
                    (digest_a,))
            }
            self.assertEqual(aliases, {"A-one", "A-two"})
            self.assertIsNone(db.execute(
                "SELECT 1 FROM blob WHERE digest=?",
                (digest_rejected,)).fetchone())

        # X+c can use the same logical eviction: A's exclusive a goes away and
        # c is the only new physical block, leaving exactly X, b and c.
        accepted = self.store.publish("C", accepted_C)
        self.assertEqual(accepted, digest_accepted)
        self.assertEqual(self.store.shared_payload_usage(),
                         (4 * U, 3 * U))

        reopened = ObjectStore(self.path)
        with self.assertRaises(KeyError):
            reopened.fetch("A-one")
        with self.assertRaises(KeyError):
            reopened.fetch("A-two")
        self.assertEqual(reopened.fetch("B-tag"), B)
        self.assertEqual(reopened.fetch("C"), accepted_C)
        self.assertEqual(
            reopened.fetch_for_build(handle, "B-tag"), B)
        self.assertEqual(reopened.shared_payload_usage(), (4 * U, 3 * U))
        self.assertEqual(digest_b, digest(B))

    def test_logical_room_but_physical_full_evicts_nothing(self):
        x = repeated_block(21)
        a = repeated_block(22)
        b = repeated_block(23)
        y = repeated_block(24)
        A = x + a
        B = x + b

        self.store.configure_capacity(100 * U)
        self.store.configure_shared_payload(3 * U)
        self.store.publish("A-one", A)
        self.store.publish("A-two", A)
        self.store.publish("B-tag", B)
        use_before = self._use_rows()

        with self.assertRaises(PhysicalCapacityExceeded):
            self.store.publish("Y", y)

        self.assertEqual(self._use_rows(), use_before)
        self.assertEqual(self.store.shared_payload_usage(),
                         (4 * U, 3 * U))
        self.assertEqual(self.store.fetch("A-one"), A)
        self.assertEqual(self.store.fetch("A-two"), A)
        self.assertEqual(self.store.fetch("B-tag"), B)
        with closing(sqlite3.connect(str(self.path))) as db:
            digests = {row[0] for row in db.execute("SELECT digest FROM blob")}
        self.assertEqual(digests, {digest(A), digest(B)})

        with self.assertRaises(PhysicalCapacityExceeded):
            self.store.configure_shared_payload(2 * U)
        with closing(sqlite3.connect(str(self.path))) as db:
            limit = db.execute(
                "SELECT value FROM store_meta "
                "WHERE key='shared_payload_capacity'").fetchone()[0]
        self.assertEqual(limit, 3 * U)

    def test_repeated_block_and_collection_preserve_other_users(self):
        q = repeated_block(31)
        s = repeated_block(32)
        repeated = q + q
        shared_user = q + s

        self.store.configure_shared_payload(10 * U)
        self.store.publish("repeated", repeated)
        self.store.publish("other", shared_user)
        self.assertEqual(self.store.shared_payload_usage(),
                         (4 * U, 2 * U))

        self.assertTrue(self.store.remove("repeated"))
        self.assertEqual(self.store.collect(), 1)
        # q is still used by the other artifact; only the repeated whole
        # artifact and its two order rows are gone.
        self.assertEqual(self.store.shared_payload_usage(),
                         (2 * U, 2 * U))
        self.assertEqual(self.store.fetch("other"), shared_user)

        self.assertTrue(self.store.remove("other"))
        self.assertEqual(self.store.collect(), 1)
        self.assertEqual(self.store.shared_payload_usage(), (0, 0))
        with closing(sqlite3.connect(str(self.path))) as db:
            self.assertEqual(
                db.execute("SELECT COUNT(*) FROM shared_block").fetchone()[0],
                0)


class SharedTransferAndRepairTest(unittest.TestCase):
    def setUp(self):
        self._tmp = TemporaryDirectory()
        self.tmp = Path(self._tmp.name)
        self.dest_path = self.tmp / "dest.db"
        self.source_path = self.tmp / "source.db"
        self.peer_path = self.tmp / "old-peer.db"
        self.dest = ObjectStore(self.dest_path)
        self.source = ObjectStore(self.source_path)

    def tearDown(self):
        self._tmp.cleanup()

    def test_corrupt_shared_block_repair_is_atomic_and_transferable(self):
        x = repeated_block(41)
        a = repeated_block(42)
        b = repeated_block(43)
        d_block = repeated_block(44)
        A = x + a
        B = x + b
        D = d_block

        self.dest.configure_capacity(10 * U)
        self.dest.configure_shared_payload(3 * U)
        digest_a = self.dest.publish("a", A)
        digest_b = self.dest.publish("b", B)
        old_handle, _ = self.dest.begin_build(["a", "b"])

        corrupt_named_block(self.dest_path, x)

        # Both whole artifacts sharing X now fail whole verification.
        with self.assertRaises(CorruptBlob):
            self.dest.fetch("a")
        with self.assertRaises(CorruptBlob):
            self.dest.fetch("b")
        with self.assertRaises(CorruptBlob):
            self.dest.fetch_for_build(old_handle, "a")

        # Ordinary publication of the good input must not overwrite X.
        with self.assertRaises(CorruptBlob):
            self.dest.publish("ordinary-repair", A)
        with self.assertRaises(KeyError):
            ObjectStore(self.dest_path).fetch("ordinary-repair")

        # Recipe completion has the same no-silent-repair rule.
        status, repair_token = self.dest.claim_output("repair-recipe")
        self.assertEqual(status, "claimed")
        with self.assertRaises(CorruptBlob):
            self.dest.complete_output(repair_token, A)
        self.assertEqual(self.dest.claim_output("repair-recipe")[0], "busy")
        self.assertTrue(self.dest.cancel_output(repair_token))
        with self.assertRaises(CorruptBlob):
            self.dest.fetch("a")

        # An unshared/default source sends A and a new D block.
        self.source.publish("src-a", A)
        self.source.publish("src-d", D)
        source_handle, _ = self.source.begin_build(["src-a", "src-d"])
        manifest = self.source.describe_build(source_handle)
        needed = self.dest.transfer_needs(manifest)
        self.assertEqual(set(needed), {digest_a, digest(D)})

        sink = io.BytesIO()
        self.source.send_build(source_handle, manifest, needed, sink)
        wire = sink.getvalue()

        # The physical ceiling is exactly the old X/a/b unique payload.  D's
        # new block cannot be admitted, even though A would repair X.
        with self.assertRaises(PhysicalCapacityExceeded):
            self.dest.install_build(
                manifest, needed, io.BytesIO(wire))

        unchanged = ObjectStore(self.dest_path)
        self.assertEqual(unchanged.shared_payload_usage(),
                         (4 * U, 3 * U))
        with self.assertRaises(CorruptBlob):
            unchanged.fetch("a")
        with self.assertRaises(CorruptBlob):
            unchanged.fetch("b")
        with closing(sqlite3.connect(str(self.dest_path))) as db:
            self.assertIsNone(db.execute(
                "SELECT 1 FROM blob WHERE digest=?",
                (digest(D),)).fetchone())
            self.assertEqual(
                db.execute("SELECT COUNT(*) FROM build").fetchone()[0], 1)
            self.assertEqual(
                {r[0] for r in db.execute("SELECT name FROM tag")},
                {"a", "b"})

        # Raising the physical ceiling allows the fully verified install.  A's
        # X upsert repairs B as well, without retransmitting B.
        self.dest.configure_shared_payload(4 * U)
        new_handle, bindings = self.dest.install_build(
            manifest, needed, io.BytesIO(wire))
        self.assertEqual(bindings,
                         {"src-a": digest_a, "src-d": digest(D)})
        self.assertEqual(self.dest.shared_payload_usage(),
                         (5 * U, 4 * U))
        self.assertEqual(self.dest.fetch("a"), A)
        self.assertEqual(self.dest.fetch("b"), B)
        self.assertEqual(self.dest.fetch_for_build(old_handle, "a"), A)
        self.assertEqual(self.dest.fetch_for_build(old_handle, "b"), B)
        self.assertEqual(self.dest.fetch_for_build(new_handle, "src-a"), A)
        self.assertEqual(self.dest.fetch_for_build(new_handle, "src-d"), D)

        with closing(sqlite3.connect(str(self.dest_path))) as db:
            self.assertEqual(
                {r[0] for r in db.execute("SELECT name FROM tag")},
                {"a", "b"})
            # Install/reuse/repair does not create LRU rows.
            self.assertIsNone(db.execute(
                "SELECT seq FROM digest_use WHERE digest=?",
                (digest(D),)).fetchone())

        # Reverse direction: a default/old-format peer can consume the shared
        # sender using exactly the same wire protocol.
        peer = ObjectStore(self.peer_path)
        reverse_manifest = self.dest.describe_build(new_handle)
        reverse_needed = peer.transfer_needs(reverse_manifest)
        reverse_sink = io.BytesIO()
        self.dest.send_build(
            new_handle, reverse_manifest, reverse_needed, reverse_sink)
        peer_handle, peer_bindings = peer.install_build(
            reverse_manifest, reverse_needed,
            io.BytesIO(reverse_sink.getvalue()))
        self.assertEqual(peer_bindings, bindings)
        self.assertEqual(
            peer.fetch_for_build(peer_handle, "src-a"), A)
        self.assertEqual(
            peer.fetch_for_build(peer_handle, "src-d"), D)

    def test_workflow_budget_and_recipe_retry_use_whole_length(self):
        # A two-whole-block artifact repeats one physical block.  Workflow
        # budgeting must count 2U, not the U unique stored bytes.
        q = repeated_block(51)
        data = q + q
        whole_digest = digest(data)

        workflow_path = self.tmp / "workflow.db"
        store = ObjectStore(workflow_path)
        store.configure_shared_payload(U)
        store.publish("repeated", data)

        actions = {"W": (1, (), "repeated", whole_digest)}
        too_small = 2 * U - 1
        handle, run_order, cached = store.prepare_workflow(
            actions, ["W"], too_small)
        self.assertIsNone(handle)
        self.assertEqual(run_order, ["W"])
        self.assertEqual(cached, {})

        handle, run_order, cached = store.prepare_workflow(
            actions, ["W"], 2 * U)
        self.assertEqual(run_order, [])
        self.assertEqual(cached, {"W": whole_digest})
        self.assertEqual(store.fetch_for_build(handle, "W"), data)

        # A physical-capacity failure preserves the recipe token and waits;
        # raising the ceiling lets the original token complete.
        dep_block = repeated_block(61)
        recipe_block = repeated_block(62)
        dep_path = self.tmp / "recipe.db"
        recipes = ObjectStore(dep_path)
        recipes.configure_shared_payload(10 * U)
        _, dep_token = recipes.claim_output("dep-key")
        recipes.complete_output(dep_token, dep_block)

        status, token = recipes.claim_output("recipe-key")
        self.assertEqual(status, "claimed")
        recipes.set_output_waits(token, [["dep-key"]])
        recipes.configure_shared_payload(U)
        with self.assertRaises(PhysicalCapacityExceeded):
            recipes.complete_output(token, recipe_block)
        self.assertEqual(recipes.claim_output("recipe-key")[0], "busy")

        recipes.configure_shared_payload(2 * U)
        recipe_digest = recipes.complete_output(token, recipe_block)
        self.assertEqual(recipe_digest, digest(recipe_block))
        self.assertEqual(recipes.fetch("recipe-key"), recipe_block)
        status, ready = recipes.claim_output("recipe-key")
        self.assertEqual(status, "ready")
        self.assertEqual(ready, recipe_block)


class SharedScaleFixtureTest(unittest.TestCase):
    def test_48_artifacts_page_budget_and_collection(self):
        with TemporaryDirectory() as tmp:
            path = Path(tmp, "scale-shared.db")
            store = ObjectStore(path)
            store.configure_capacity(GIB)
            store.configure_shared_payload(GIB)

            common = [pseudo_block(b"common", i) for i in range(15)]
            tails = [pseudo_block(b"tail", i) for i in range(48)]
            common_prefix = b"".join(common)
            artifacts = [common_prefix + tails[i] for i in range(48)]

            self.assertEqual(len({digest(block) for block in common + tails}),
                             63)
            self.assertEqual(len({digest(data) for data in artifacts}), 48)

            for i, data in enumerate(artifacts):
                store.publish("artifact-%02d" % i, data)

            self.assertEqual(store.shared_payload_usage(),
                             (48 * MIB, 63 * U))
            with closing(sqlite3.connect(str(path))) as db:
                self.assertEqual(db.execute(
                    "SELECT COALESCE(SUM(LENGTH(data)), 0) FROM blob"
                ).fetchone()[0], 0)
                self.assertEqual(db.execute("""
                    SELECT COUNT(*) FROM shared_block b
                    WHERE NOT EXISTS (
                        SELECT 1 FROM shared_block_map m
                        WHERE m.block_hash=b.block_hash)
                """).fetchone()[0], 0)
            store = ObjectStore(path)
            self.assertEqual(store.fetch("artifact-07"), artifacts[7])
            self.assertLess(page_used_bytes(path), 8 * MIB)

            keep = {3, 17}
            for i in range(48):
                if i not in keep:
                    self.assertTrue(store.remove("artifact-%02d" % i))
            self.assertEqual(store.collect(), 46)

            self.assertEqual(store.shared_payload_usage(),
                             (2 * MIB, 17 * U))
            self.assertEqual(store.fetch("artifact-03"), artifacts[3])
            self.assertEqual(store.fetch("artifact-17"), artifacts[17])
            self.assertLess(page_used_bytes(path), 3 * MIB)

            with closing(sqlite3.connect(str(path))) as db:
                self.assertEqual(
                    db.execute("SELECT COUNT(*) FROM shared_block").fetchone()[0],
                    17)
                self.assertEqual(
                    db.execute("SELECT COUNT(*) FROM shared_block_map")
                    .fetchone()[0],
                    32)  # two whole MiB artifacts, 16 order rows each


if __name__ == "__main__":
    unittest.main()
