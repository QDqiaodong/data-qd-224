from contextlib import closing
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from objects import CorruptBlob, ObjectStore


class ObjectStoreTest(unittest.TestCase):
    def test_dedup_retain_and_collect_after_reopen(self):
        with TemporaryDirectory() as tmp:
            path = Path(tmp, "objects.db")
            store = ObjectStore(path)
            self.assertEqual(store.publish("release", b"v1"), store.publish("tested", b"v1"))
            store.publish("release", b"v2")
            store = ObjectStore(path)
            self.assertEqual(store.collect(), 0)
            self.assertEqual(store.fetch("release"), b"v2")
            self.assertEqual(store.fetch("tested"), b"v1")
            self.assertTrue(store.remove("tested"))
            self.assertEqual(store.collect(), 1)
            self.assertEqual(store.collect(), 0)
            with self.assertRaises(KeyError):
                store.fetch("tested")
            self.assertEqual(store.fetch("release"), b"v2")

    def test_integrity_error_and_publish_rollback(self):
        with TemporaryDirectory() as tmp:
            store = ObjectStore(Path(tmp, "objects.db"))
            digest = store.publish("old", b"a")
            with closing(store.connection()) as db, db:
                db.execute("UPDATE blob SET data=? WHERE digest=?", (b"bad", digest))
            with self.assertRaises(CorruptBlob):
                store.fetch("old")
            with self.assertRaises(CorruptBlob):
                store.publish("new", b"a")
            with self.assertRaises(KeyError):
                store.fetch("new")
