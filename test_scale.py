"""Scale and upgrade acceptance test.

A legacy database with 112 objects, 1 MiB each, is created in the parent.
A fresh Linux subprocess with a 96 MiB address-space limit then reopens it,
reduces the capacity to 64 MiB, evicts according to the documented rules and
reads back artifacts that must have survived.
"""
from pathlib import Path
from tempfile import TemporaryDirectory
import hashlib
import sqlite3
import subprocess
import sys
import unittest

from objects import ObjectStore

MIB = 1 << 20
COUNT = 112
TARGET = 64 * MIB
ADDRESS_SPACE = 96 * MIB


def artifact(index):
    data = ("%08d" % index).encode() * (MIB // 8)
    assert len(data) == MIB
    return data


def tag_for(index):
    return "old-%03d" % index


CHILD = r"""
import hashlib, resource, sqlite3, sys
sys.path.insert(0, sys.argv[2])
from objects import ObjectStore

limit = int(sys.argv[1])
path, target, count, mib = sys.argv[3], int(sys.argv[4]), int(sys.argv[5]), int(sys.argv[6])
# The module is loaded; from here on every allocation, including opening the
# database and evicting, happens inside the 96 MiB address space.
resource.setrlimit(resource.RLIMIT_AS, (limit, limit))
store = ObjectStore(path)
store.configure_capacity(target)

db = sqlite3.connect(path)
kept = {r[0] for r in db.execute("SELECT digest FROM blob")}
total, n = db.execute("SELECT SUM(LENGTH(data)), COUNT(*) FROM blob").fetchone()
tags = {r[0] for r in db.execute("SELECT digest FROM tag")}
db.close()
assert total == target, (total, target)
assert n == target // mib, n
assert kept == tags, "every surviving object still has its tag"
for i in range(count):
    data = ("%08d" % i).encode() * (mib // 8)
    digest = hashlib.sha256(data).hexdigest()
    if digest in kept:
        assert store.fetch("old-%03d" % i) == data
    else:
        try:
            store.fetch("old-%03d" % i)
        except KeyError:
            pass
        else:
            raise AssertionError("evicted artifact readable")
# A publish that fits the reduced capacity still works.
store.publish("fresh", b"small")
assert store.fetch("fresh") == b"small"
print("child ok", total)
"""


class ScaleTest(unittest.TestCase):
    def test_upgrade_and_reduce_under_96_mib_address_space(self):
        if not sys.platform.startswith("linux"):
            self.skipTest("address-space limit check is Linux-specific")
        with TemporaryDirectory() as tmp:
            path = Path(tmp, "legacy.db")
            # Legacy schema, one tag per object, no use-order records.
            db = sqlite3.connect(str(path))
            db.executescript("""CREATE TABLE blob (
                    digest TEXT PRIMARY KEY, data BLOB NOT NULL);
                CREATE TABLE tag (
                    name TEXT PRIMARY KEY,
                    digest TEXT NOT NULL REFERENCES blob(digest));""")
            for i in range(COUNT):
                data = artifact(i)
                db.execute("INSERT INTO blob VALUES (?,?)",
                           (hashlib.sha256(data).hexdigest(), data))
                db.execute("INSERT INTO tag VALUES (?,?)",
                           (tag_for(i), hashlib.sha256(data).hexdigest()))
            db.commit()
            db.close()

            result = subprocess.run(
                [sys.executable, "-B", "-c", CHILD, str(ADDRESS_SPACE),
                 str(Path(__file__).resolve().parent), str(path),
                 str(TARGET), str(COUNT), str(MIB)],
                capture_output=True, text=True, timeout=300)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("child ok", result.stdout)

            # The parent can still inspect the reduced database.
            store = ObjectStore(path)
            self.assertEqual(store.collect(), 0)
            kept = [tag_for(i) for i in range(COUNT) if _tag_exists(path, tag_for(i))]
            # The child reduced 112 untouched 1 MiB objects to the 64 largest
            # digests, then published a 5-byte blob at capacity.  Its read of
            # every survivor refreshed use order, so exactly one more old
            # object (the first one it read) had to go.
            largest = sorted(hashlib.sha256(artifact(i)).hexdigest()
                             for i in range(COUNT))[COUNT - TARGET // MIB:]
            survivors = {hashlib.sha256(artifact(int(t[4:]))).hexdigest()
                         for t in kept}
            self.assertLessEqual(len(survivors), TARGET // MIB)
            self.assertGreaterEqual(len(survivors), TARGET // MIB - 1)
            self.assertEqual(survivors - set(largest), set())
            self.assertEqual(store.fetch("fresh"), b"small")

    def test_many_tags_and_checkouts_at_support_scale(self):
        with TemporaryDirectory() as tmp:
            store = ObjectStore(Path(tmp, "scale.db"))
            store.configure_capacity(1 << 30)
            digests = {}
            for i in range(64):
                data = hashlib.sha256(str(i).encode()).digest()
                digests[i] = data
                for j in range(24):
                    store.publish("t%02d-%02d" % (i, j), data)
            # 64 active checkouts, 24 tags each, is the documented maximum.
            all_tags = ["t%02d-%02d" % (i, j) for i in range(64) for j in range(24)]
            checkouts = [store.begin_build(all_tags[i * 24:(i + 1) * 24])
                         for i in range(64)]
            with self.assertRaises(ValueError):
                store.begin_build(["t00-00"])  # 65th active checkout
            for handle, bindings in checkouts:
                self.assertEqual(len(bindings), 24)
                for name in bindings:
                    self.assertEqual(store.fetch_for_build(handle, name),
                                     digests[int(name[1:3])])
            for handle, _ in checkouts:
                self.assertTrue(store.finish_build(handle))
            store.begin_build(["t00-00"])  # a slot is free again


def _tag_exists(path, name):
    db = sqlite3.connect(str(path))
    try:
        return db.execute("SELECT 1 FROM tag WHERE name=?", (name,)).fetchone() is not None
    finally:
        db.close()


if __name__ == "__main__":
    unittest.main()
