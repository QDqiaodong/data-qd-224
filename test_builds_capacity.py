from contextlib import closing
from pathlib import Path
from tempfile import TemporaryDirectory
import hashlib
import sqlite3
import threading
import unittest

from objects import CapacityExceeded, CorruptBlob, ObjectStore


def digest(data):
    return hashlib.sha256(data).hexdigest()


class BuildCheckoutTest(unittest.TestCase):
    def setUp(self):
        self._tmp = TemporaryDirectory()
        self.path = Path(self._tmp.name, "objects.db")

    def tearDown(self):
        self._tmp.cleanup()

    def test_begin_build_returns_bindings_and_pins_digests(self):
        store = ObjectStore(self.path)
        a = store.publish("a1", b"aaa")
        store.publish("a2", b"aaa")
        store.publish("b", b"bbb")

        handle, bindings = store.begin_build(["a1", "b"])
        self.assertIsInstance(handle, str)
        self.assertEqual(bindings, {"a1": a, "b": digest(b"bbb")})

        # A rebound alias no longer affects an existing checkout.
        store.publish("a1", b"changed")
        self.assertEqual(store.fetch_for_build(handle, "a1"), b"aaa")
        self.assertEqual(store.fetch_for_build(handle, "b"), b"bbb")
        self.assertEqual(store.fetch("a1"), b"changed")

        # Reopening preserves the checkout and its pins.
        store = ObjectStore(self.path)
        self.assertEqual(store.fetch_for_build(handle, "a1"), b"aaa")

        self.assertTrue(store.finish_build(handle))
        self.assertFalse(store.finish_build(handle))
        with self.assertRaises(KeyError):
            store.fetch_for_build(handle, "a1")

    def test_handles_are_unique_and_opaque(self):
        store = ObjectStore(self.path)
        store.publish("a", b"a")
        handles = {store.begin_build(["a"])[0] for _ in range(5)}
        self.assertEqual(len(handles), 5)
        for handle in handles:
            store.finish_build(handle)

    def test_missing_tag_failure_leaves_no_references(self):
        store = ObjectStore(self.path)
        store.publish("present", b"p")
        with self.assertRaises(KeyError):
            store.begin_build(["present", "absent"])
        with closing(store.connection()) as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM build").fetchone()[0], 0)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM build_pin").fetchone()[0], 0)

    def test_invalid_arguments(self):
        store = ObjectStore(self.path)
        store.publish("a", b"a")
        for names in [None, "a", b"a", 42, [], ["a", "a"], ["a", ""], ["a", 3],
                      ["t%d" % i for i in range(25)], tuple(["a", "a"])]:
            with self.assertRaises(ValueError):
                store.begin_build(names)
        handle, bindings = store.begin_build(("a",))
        self.assertEqual(bindings, {"a": digest(b"a")})
        store.finish_build(handle)

    def test_active_build_limit(self):
        store = ObjectStore(self.path)
        store.publish("a", b"a")
        handles = [store.begin_build(["a"])[0] for _ in range(64)]
        with self.assertRaises(ValueError):
            store.begin_build(["a"])
        store.finish_build(handles[0])
        store.begin_build(["a"])  # a finished handle frees a slot

    def test_pin_survives_tag_removal_and_collect_until_last_release(self):
        store = ObjectStore(self.path)
        a = store.publish("a", b"payload")
        h1, _ = store.begin_build(["a"])
        h2, _ = store.begin_build(["a"])
        store.remove("a")
        self.assertEqual(store.collect(), 0)
        self.assertTrue(store.finish_build(h1))
        self.assertEqual(store.collect(), 0)
        self.assertTrue(store.finish_build(h2))
        self.assertEqual(store.collect(), 1)
        self.assertEqual(store.collect(), 0)

    def test_partially_removed_checkout_still_reads_all_bindings(self):
        store = ObjectStore(self.path)
        a = store.publish("a", b"aaa")
        b = store.publish("b", b"bbb")
        handle, bindings = store.begin_build(["a", "b"])
        store.remove("a")
        self.assertEqual(store.collect(), 0)  # A is still pinned
        with self.assertRaises(KeyError):
            store.fetch("a")
        self.assertEqual(store.fetch_for_build(handle, "a"), b"aaa")
        self.assertEqual(store.fetch_for_build(handle, "b"), b"bbb")
        # A reopened instance sees the same partial state.
        store = ObjectStore(self.path)
        self.assertEqual(store.collect(), 0)
        self.assertTrue(store.finish_build(handle))
        self.assertEqual(store.collect(), 1)  # only A
        self.assertEqual(store.fetch("b"), b"bbb")
        self.assertEqual(bindings, {"a": a, "b": b})

    def test_finish_does_not_delete_objects(self):
        store = ObjectStore(self.path)
        store.publish("a", b"payload")
        handle, _ = store.begin_build(["a"])
        self.assertTrue(store.finish_build(handle))
        self.assertEqual(store.fetch("a"), b"payload")

    def test_build_read_verifies_digest_and_keeps_pin_on_corruption(self):
        store = ObjectStore(self.path)
        a = store.publish("a", b"payload")
        handle, _ = store.begin_build(["a"])
        with closing(store.connection()) as db, db:
            db.execute("UPDATE blob SET data=? WHERE digest=?", (b"bad", a))
        with self.assertRaises(CorruptBlob):
            store.fetch_for_build(handle, "a")
        with closing(store.connection()) as db:
            self.assertEqual(
                db.execute("SELECT COUNT(*) FROM build_pin WHERE handle=?",
                           (handle,)).fetchone()[0], 1)
        self.assertTrue(store.finish_build(handle))

    def test_build_read_key_errors(self):
        store = ObjectStore(self.path)
        store.publish("a", b"a")
        handle, _ = store.begin_build(["a"])
        with self.assertRaises(KeyError):
            store.fetch_for_build("no-such-handle", "a")
        with self.assertRaises(KeyError):
            store.fetch_for_build(handle, "not-in-build")
        for bad in (None, 42, ["x"], ("x",)):
            with self.assertRaises(KeyError):
                store.fetch_for_build(bad, "a")
            self.assertFalse(store.finish_build(bad))

    def test_build_operations_do_not_refresh_use_order(self):
        store = ObjectStore(self.path)
        a = store.publish("a", b"aaaa")
        store.publish("b", b"bbbbb")
        before = self._use_rows()
        handle, _ = store.begin_build(["a"])
        store.fetch_for_build(handle, "a")
        store.finish_build(handle)
        self.assertEqual(self._use_rows(), before)
        self.assertIn(a, before)

    def test_checkout_versus_rebind_is_serialized(self):
        # Two independent connections race "resolve the current tag" against
        # "rebind that tag".  Every observation must equal some complete
        # serial order: entirely old or entirely new, never a mixed state.
        old, new = b"old-payload", b"new-payload"
        writer = ObjectStore(self.path)
        writer.publish("target", old)
        reader = ObjectStore(self.path)
        errors = []
        for _ in range(40):
            writer.publish("target", old)
            started = threading.Barrier(2)
            observed = []

            def checkout():
                try:
                    started.wait()
                    observed.append(reader.begin_build(["target"]))
                except BaseException as exc:  # pragma: no cover - diagnostic
                    errors.append(exc)

            def rebind():
                try:
                    started.wait()
                    writer.publish("target", new)
                except BaseException as exc:  # pragma: no cover - diagnostic
                    errors.append(exc)

            threads = [threading.Thread(target=checkout),
                       threading.Thread(target=rebind)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            self.assertEqual(errors, [])
            handle, bindings = observed.pop()
            self.assertIn(bindings, [{"target": digest(old)},
                                     {"target": digest(new)}])
            expected = old if bindings["target"] == digest(old) else new
            self.assertEqual(reader.fetch_for_build(handle, "target"), expected)
            reader.finish_build(handle)

    def _use_rows(self):
        with closing(sqlite3.connect(str(self.path))) as db:
            return dict(db.execute("SELECT digest, seq FROM digest_use"))


class CapacityTest(unittest.TestCase):
    def setUp(self):
        self._tmp = TemporaryDirectory()
        self.path = Path(self._tmp.name, "objects.db")

    def tearDown(self):
        self._tmp.cleanup()

    def _full_13_byte_store(self, pin_a=False):
        store = ObjectStore(self.path)
        a = store.publish("a1", b"aaaa")       # 4 bytes, oldest
        store.publish("a2", b"aaaa")
        b = store.publish("b1", b"bbbbb")      # 5 bytes, newer
        u = store.publish("u1", b"uuuu")       # 4 bytes, untagged after remove
        store.remove("u1")
        store.configure_capacity(13)
        handle = None
        if pin_a:
            handle, _ = store.begin_build(["a1"])
        return store, a, b, u, handle

    def test_unpinned_eviction_order_untagged_then_lru_tagged(self):
        store, a, b, u, _ = self._full_13_byte_store()
        c = store.publish("a1", b"ccccccc")  # 7 bytes, total 20 > 13

        with closing(store.connection()) as db:
            remaining = {r[0] for r in db.execute("SELECT digest FROM blob")}
        self.assertEqual(remaining, {b, c})
        self.assertEqual(store.fetch("a1"), b"ccccccc")
        with self.assertRaises(KeyError):
            store.fetch("a2")  # A's other alias vanished with A
        self.assertEqual(store.fetch("b1"), b"bbbbb")
        with self.assertRaises(KeyError):
            store.fetch("u1")
        self.assertEqual(store.collect(), 0)

    def test_pinned_eviction_skips_pinned_blob_and_rebinds_old_alias(self):
        store, a, b, u, handle = self._full_13_byte_store(pin_a=True)
        c = store.publish("a1", b"ccccccc")

        with closing(store.connection()) as db:
            remaining = {r[0] for r in db.execute("SELECT digest FROM blob")}
        self.assertEqual(remaining, {a, c})
        self.assertEqual(store.fetch("a1"), b"ccccccc")
        self.assertEqual(store.fetch("a2"), b"aaaa")
        with self.assertRaises(KeyError):
            store.fetch("b1")
        # The checkout still resolves the old binding via the rebound alias.
        self.assertEqual(store.fetch_for_build(handle, "a1"), b"aaaa")
        self.assertTrue(store.finish_build(handle))

    def test_failed_admission_rolls_back_everything(self):
        store, a, b, u, handle_a = self._full_13_byte_store(pin_a=True)
        handle_b, _ = store.begin_build(["b1"])
        before_tags = self._rows("SELECT name, digest FROM tag ORDER BY name")
        before_use = self._rows("SELECT digest, seq FROM digest_use ORDER BY digest")
        before_blobs = self._rows("SELECT digest, LENGTH(data) FROM blob ORDER BY digest")

        with self.assertRaises(CapacityExceeded):
            store.publish("a1", b"ccccccc")

        self.assertEqual(self._rows("SELECT name, digest FROM tag ORDER BY name"), before_tags)
        self.assertEqual(self._rows("SELECT digest, seq FROM digest_use ORDER BY digest"),
                         before_use)
        self.assertEqual(self._rows("SELECT digest, LENGTH(data) FROM blob ORDER BY digest"),
                         before_blobs)
        # Capacity, tag ownership and use order are untouched.
        with closing(store.connection()) as db:
            self.assertEqual(
                db.execute("SELECT value FROM store_meta WHERE key='capacity'").fetchone()[0],
                13)
        # Untagged U was not partially cleaned up either.
        self.assertEqual(store.collect(), 1)  # only U, now after rollback
        self.assertEqual(store.fetch("a2"), b"aaaa")
        self.assertEqual(store.fetch("b1"), b"bbbbb")
        self.assertEqual(store.fetch_for_build(handle_a, "a1"), b"aaaa")
        self.assertEqual(store.fetch_for_build(handle_b, "b1"), b"bbbbb")
        store.finish_build(handle_a)
        store.finish_build(handle_b)
        self.assertEqual(store.collect(), 0)

    def test_reading_any_alias_refreshes_whole_digest(self):
        store = ObjectStore(self.path)
        store.publish("a1", b"aaaa")
        store.publish("a2", b"aaaa")
        store.publish("b1", b"bbbbb")
        store.configure_capacity(13)
        self.assertEqual(store.fetch("a2"), b"aaaa")  # refresh A
        store.publish("a1", b"ccccccc")  # 16 -> evict B, keep A

        with closing(store.connection()) as db:
            remaining = {r[0] for r in db.execute("SELECT digest FROM blob")}
        self.assertEqual(remaining, {digest(b"aaaa"), digest(b"ccccccc")})
        self.assertEqual(store.fetch("a1"), b"ccccccc")
        self.assertEqual(store.fetch("a2"), b"aaaa")
        with self.assertRaises(KeyError):
            store.fetch("b1")

    def test_untouched_legacy_objects_tie_as_oldest_by_digest(self):
        # Build a legacy-format database without use records.
        with closing(sqlite3.connect(str(self.path))) as db, db:
            db.executescript("""CREATE TABLE blob (digest TEXT PRIMARY KEY, data BLOB NOT NULL);
                CREATE TABLE tag (name TEXT PRIMARY KEY,
                                  digest TEXT NOT NULL REFERENCES blob(digest));""")
            blobs = [("t%02d" % i, bytes([i]) * 10) for i in range(4)]
            for name, data in blobs:
                db.execute("INSERT INTO blob VALUES (?,?)", (digest(data), data))
                db.execute("INSERT INTO tag VALUES (?,?)", (name, digest(data)))
        store = ObjectStore(self.path)  # upgrade in place
        store.configure_capacity(30)    # drop 10 bytes: lowest digest first
        lowest = min(digest(data) for _, data in blobs)
        names_by_digest = {digest(data): name for name, data in blobs}
        with self.assertRaises(KeyError):
            store.fetch(names_by_digest[lowest])
        self.assertEqual(len({r[0] for r in self._rows("SELECT digest FROM blob")}), 3)

    def test_capacity_setting_validation_and_none_mode(self):
        store = ObjectStore(self.path)
        store.publish("a", b"a")
        for bad in [True, False, -1, 1 << 30 | 1, 1.5, "10", None]:
            with self.assertRaises(ValueError):
                store.configure_capacity(bad)
        self.assertIsNone(store.configure_capacity(0))
        # With capacity 0, an empty blob still fits; a non-empty publish cannot.
        self.assertIsNone(store.configure_capacity(1 << 30))
        store.publish("a", b"a")

    def test_zero_capacity_rejects_nonempty_publish(self):
        store = ObjectStore(self.path)
        store.configure_capacity(0)
        with self.assertRaises(CapacityExceeded):
            store.publish("a", b"x")
        with self.assertRaises(KeyError):
            store.fetch("a")

    def test_capacity_counts_unique_resident_bytes_not_tags(self):
        store = ObjectStore(self.path)
        store.configure_capacity(7)
        # 6 shared bytes + 1 other byte; the 100 aliases add no bytes.
        for i in range(100):
            store.publish("alias-%03d" % i, b"shared")
        store.publish("other", b"x")
        with closing(store.connection()) as db:
            self.assertEqual(
                db.execute("SELECT COUNT(*) FROM tag").fetchone()[0], 101)
            self.assertEqual(
                db.execute("SELECT SUM(LENGTH(data)) FROM blob").fetchone()[0], 7)

        # A fresh 5-byte blob no longer fits, forcing the shared blob out;
        # all 100 aliases pointing at it disappear together.
        store.publish("fresh", b"fresh")
        with closing(store.connection()) as db:
            self.assertEqual(
                db.execute("SELECT COUNT(*) FROM blob").fetchone()[0], 2)
            names = {r[0] for r in db.execute("SELECT name FROM tag")}
        self.assertEqual(names, {"other", "fresh"})
        with self.assertRaises(KeyError):
            store.fetch("alias-000")

    def test_failed_capacity_change_keeps_old_capacity(self):
        store = ObjectStore(self.path)
        store.publish("a", b"aaaa")
        handle, _ = store.begin_build(["a"])
        store.configure_capacity(4)
        with self.assertRaises(CapacityExceeded):
            store.configure_capacity(1)  # only the pinned 4-byte object is left
        # The old capacity (4) is intact: a small publish still succeeds,
        # which it could not under the failed limit of 1.
        self.assertEqual(store.publish("b", b""),
                         hashlib.sha256(b"").hexdigest())
        self.assertEqual(store.fetch("a"), b"aaaa")
        self.assertEqual(store.fetch("b"), b"")
        with closing(store.connection()) as db:
            self.assertEqual(
                db.execute("SELECT value FROM store_meta WHERE key='capacity'").fetchone()[0],
                4)
        store.finish_build(handle)

    def test_reopen_preserves_capacity_and_pins(self):
        store = ObjectStore(self.path)
        store.publish("a", b"aaaa")
        store.publish("b", b"bbb")
        store.configure_capacity(8)
        handle, _ = store.begin_build(["a"])
        store.publish("b", b"bbbb")  # 8 bytes, exactly at capacity
        store = ObjectStore(self.path)
        self.assertEqual(store.fetch_for_build(handle, "a"), b"aaaa")
        self.assertEqual(store.fetch("b"), b"bbbb")
        self.assertTrue(store.finish_build(handle))

    def test_configure_capacity_eviction_and_pin_protection(self):
        store = ObjectStore(self.path)
        store.publish("a", b"aaaa")
        store.publish("b", b"bbbbb")
        handle, _ = store.begin_build(["a"])
        store.configure_capacity(9)  # already fits, no eviction
        with closing(sqlite3.connect(str(self.path))) as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM blob").fetchone()[0], 2)
        store.configure_capacity(5)  # A is pinned; evict B (5) -> 4 bytes left
        self.assertEqual(store.fetch("a"), b"aaaa")
        with self.assertRaises(KeyError):
            store.fetch("b")
        # A 5-byte publish cannot fit while A is pinned and C is protected.
        with self.assertRaises(CapacityExceeded):
            store.publish("c", b"ccccc")
        store.finish_build(handle)
        store.configure_capacity(5)  # fits now
        self.assertEqual(store.fetch("a"), b"aaaa")

    def _rows(self, query):
        with closing(sqlite3.connect(str(self.path))) as db:
            return db.execute(query).fetchall()


if __name__ == "__main__":
    unittest.main()
