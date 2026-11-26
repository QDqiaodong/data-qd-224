"""Acceptance tests for rebuild observations, verdicts and checked reads.

Everything runs against temporary SQLite files using only the standard
library.  Concurrency is ordered with events and real committed writes, never
with sleeps.
"""
from contextlib import closing
from pathlib import Path
from tempfile import TemporaryDirectory
import hashlib
import sqlite3
import threading
import unittest

from objects import CorruptBlob, ObjectStore, UnverifiedArtifact

U = 64 << 10


def digest(data):
    return hashlib.sha256(data).hexdigest()


class ReproducibilityTest(unittest.TestCase):
    def setUp(self):
        self._tmp = TemporaryDirectory()
        self.path = Path(self._tmp.name, "repro.db")
        self.store = ObjectStore(self.path)

    def tearDown(self):
        self._tmp.cleanup()

    # -- helpers ----------------------------------------------------------

    def _use_rows(self):
        with closing(sqlite3.connect(str(self.path))) as db:
            return dict(db.execute("SELECT digest, seq FROM digest_use"))

    def _build_count(self):
        with closing(sqlite3.connect(str(self.path))) as db:
            return db.execute("SELECT COUNT(*) FROM build").fetchone()[0]

    def _tag_digest(self, name):
        with closing(sqlite3.connect(str(self.path))) as db:
            row = db.execute(
                "SELECT digest FROM tag WHERE name=?", (name,)).fetchone()
            return None if row is None else row[0]

    def _assert_unverified(self, case_id, name, reason):
        with self.assertRaises(UnverifiedArtifact) as caught:
            self.store.fetch_reproduced(case_id, name)
        self.assertEqual(caught.exception.reason, reason)

    # -- scenario 1: matching, pending, contested, different_digest -------

    def test_independent_triple_verdict_and_checked_read(self):
        a_bytes = b"artifact-A-bytes"
        b_bytes = b"artifact-B-bytes"
        da = self.store.publish("artifact", a_bytes)
        db_ = digest(b_bytes)
        case = 7

        # A qualifying quartet where greedy combination-order selection would
        # pick (0,0) then find (1,0) blocked and stop short; (0,1),(1,0),(2,2)
        # is a valid independent triple.
        self.store.record_rebuilds(
            case, [(0, 0, da), (0, 1, da), (1, 0, da), (2, 2, da)])
        self.assertEqual(self.store.rebuild_verdict(case), ("verified", da))
        self.assertEqual(self.store.fetch_reproduced(case, "artifact"),
                         (da, a_bytes))

        # Withdraw (1,0) and add (1,2): three distinct runner and image
        # numbers remain on each side, but no independent triple exists.
        self.store.record_rebuilds(case, [(1, 0, None), (1, 2, da)])
        self.assertEqual(self.store.rebuild_verdict(case), ("pending", None))
        self._assert_unverified(case, "artifact", "pending")
        # Ordinary reads and the current binding are untouched by the
        # unverified outcome.
        self.assertEqual(self.store.fetch("artifact"), a_bytes)

        # Restore A's triple.  B then has its own three independent
        # observations even though its bytes are not resident at all.
        self.store.record_rebuilds(
            case, [(1, 0, da), (1, 2, None),
                   (3, 3, db_), (4, 4, db_), (5, 5, db_)])
        self.assertEqual(self.store.rebuild_verdict(case), ("contested", None))
        self._assert_unverified(case, "artifact", "contested")
        self.assertEqual(self.store.fetch("artifact"), a_bytes)

        # Dropping one of A's key observations makes B the unique result.
        # The old tag must neither fall back to A nor substitute B.
        self.store.record_rebuilds(case, [(2, 2, None)])
        self.assertEqual(self.store.rebuild_verdict(case), ("verified", db_))
        self._assert_unverified(case, "artifact", "different_digest")
        self.assertEqual(self._tag_digest("artifact"), da)

        # Publishing/rebinding writes no reproduction evidence, but once the
        # tag actually names B the checked read delivers B.
        self.assertEqual(self.store.publish("artifact", b_bytes), db_)
        self.assertEqual(self.store.rebuild_verdict(case), ("verified", db_))
        self.assertEqual(self.store.fetch_reproduced(case, "artifact"),
                         (db_, b_bytes))

        # Reopening the store keeps the verdict and the checked read.
        reopened = ObjectStore(self.path)
        self.assertEqual(reopened.rebuild_verdict(case), ("verified", db_))
        self.assertEqual(reopened.fetch_reproduced(case, "artifact"),
                         (db_, b_bytes))

    def test_invalid_batch_applies_nothing_and_does_not_touch_evidence(self):
        a_bytes = b"only-two-good-observations"
        da = self.store.publish("tag-a", a_bytes)
        case = 3
        self.store.record_rebuilds(case, [(0, 0, da), (1, 1, da)])
        self.assertEqual(self.store.rebuild_verdict(case), ("pending", None))

        # A batch whose valid-looking (2,2) would complete a triple is
        # rejected wholesale because another item is invalid.
        bad_batches = [
            [(2, 2, da), (99, 0, da)],
            [(2, 2, da), (2, 2, da)],          # repeated combination
            [(2, 2, da), (2, 2, None)],        # repeated combination
            [(2, 2, da), (0, 0)],              # wrong arity
            [(2, 2, da), (0, 0, "Z" * 64)],    # uppercase digest
            [(2, 2, da), (True, 0, da)],       # bool runner
            [(2, 2, da), (0, 0, 7)],           # non-string digest
        ]
        for batch in bad_batches:
            with self.assertRaises(ValueError):
                self.store.record_rebuilds(case, batch)
            self.assertEqual(
                self.store.rebuild_verdict(case), ("pending", None))

        # The rejected valid item never took effect: withdrawing it is a
        # no-op and the verdict does not change.
        self.store.record_rebuilds(case, [(2, 2, None)])
        self.assertEqual(self.store.rebuild_verdict(case), ("pending", None))

        # Cases are independent.
        other = 4
        self.store.record_rebuilds(
            other, [(0, 0, da), (1, 1, da), (2, 2, da)])
        self.assertEqual(self.store.rebuild_verdict(other), ("verified", da))
        self.assertEqual(self.store.rebuild_verdict(case), ("pending", None))

    def test_parameter_validation(self):
        data = b"validation"
        d = self.store.publish("v", data)
        for bad_case in (True, False, -1, 256, 1.0, "3", None):
            with self.assertRaises(ValueError):
                self.store.rebuild_verdict(bad_case)
            with self.assertRaises(ValueError):
                self.store.record_rebuilds(bad_case, [(0, 0, d)])

        good = (0, 0, d)
        for bad_updates in ("x", 42, [], [(0, 0, d)] * 129, [good, good],
                            [(0, 0, d), (1, 1, d.upper())],
                            [(0, 0, d), (1, 1, "not-hex")],
                            [(0, 0, d), (0, 0, None)]):
            with self.assertRaises(ValueError):
                self.store.record_rebuilds(1, bad_updates)
        # Valid bounds and a tuple batch are accepted.
        self.store.record_rebuilds(0, ((0, 0, d), (31, 31, None)))
        self.store.record_rebuilds(255, [(31, 31, d)])

        for bad_name in ("", "   ", None, 5):
            with self.assertRaises(ValueError):
                self.store.fetch_reproduced(0, bad_name)

    # -- scenario 2: an in-flight checked read against concurrent writers --

    def test_inflight_read_sees_one_complete_view(self):
        a_bytes = b"old-A-payload"
        b_bytes = b"new-B-payload"
        da = self.store.publish("artifact", a_bytes)
        db_ = digest(b_bytes)

        # Make B resident but untagged so the only tag still names A.
        self.store.publish("b-staging", b_bytes)
        self.assertTrue(self.store.remove("b-staging"))
        self.assertEqual(self._tag_digest("artifact"), da)
        self.assertIsNone(self._tag_digest("b-staging"))

        case = 11
        self.store.record_rebuilds(
            case, [(0, 0, da), (0, 1, da), (1, 0, da), (2, 2, da)])
        self.assertEqual(self.store.rebuild_verdict(case), ("verified", da))

        entered = threading.Event()
        gate = threading.Event()
        original_check = self.store._check_reproduced_payload
        errors = []
        result = {}

        def paused_check(verdict_digest, payload):
            entered.set()
            self.assertTrue(gate.wait(10))
            return original_check(verdict_digest, payload)

        self.store._check_reproduced_payload = paused_check

        def read():
            try:
                result["value"] = self.store.fetch_reproduced(
                    case, "artifact")
            except BaseException as exc:  # pragma: no cover - diagnostic
                errors.append(exc)

        worker = threading.Thread(target=read)
        worker.start()
        self.assertTrue(entered.wait(5))
        # The verdict input, binding and A's bytes were already captured; the
        # read snapshot has been released.  A second connection now performs
        # three writes that must all commit while the read is paused.
        self.assertTrue(worker.is_alive())

        writer = ObjectStore(self.path)
        # (1) One atomic observation batch: strip A's qualifying observation
        #     and give B three independent ones, making B unique.
        writer.record_rebuilds(
            case, [(2, 2, None), (3, 3, db_), (4, 4, db_), (5, 5, db_)])
        with closing(sqlite3.connect(str(self.path))) as db:
            self.assertIsNone(db.execute(
                "SELECT 1 FROM rebuild_observation "
                "WHERE case_id=? AND runner=2 AND image=2", (case,)).fetchone())
            self.assertEqual(db.execute(
                "SELECT COUNT(*) FROM rebuild_observation "
                "WHERE case_id=? AND digest=?", (case, db_)).fetchone()[0], 3)
        # (2) Rebind the only tag to B.
        self.assertEqual(writer.publish("artifact", b_bytes), db_)
        with closing(sqlite3.connect(str(self.path))) as db:
            self.assertEqual(db.execute(
                "SELECT digest FROM tag WHERE name='artifact'").fetchone()[0],
                db_)
        # (3) Collect the now-unreferenced A (no build checkout protects it).
        self.assertEqual(writer.collect(), 1)
        with closing(sqlite3.connect(str(self.path))) as db:
            self.assertIsNone(db.execute(
                "SELECT 1 FROM blob WHERE digest=?", (da,)).fetchone())
        self.assertTrue(worker.is_alive())

        gate.set()
        worker.join(10)
        self.assertFalse(worker.is_alive())
        self.assertEqual(errors, [])

        # Exactly one complete version is returned, never a mixture.
        verdict_digest, payload = result["value"]
        self.assertEqual(hashlib.sha256(payload).hexdigest(), verdict_digest)
        self.assertIn((verdict_digest, payload),
                      {(da, a_bytes), (db_, b_bytes)})

        # New calls see only the new committed state.
        self.assertEqual(self.store.rebuild_verdict(case), ("verified", db_))
        self.assertEqual(self.store.fetch_reproduced(case, "artifact"),
                         (db_, b_bytes))

    # -- scenario 3: shared-block damage, reclamation and no LRU ----------

    def test_shared_mode_corruption_reclamation_and_lru(self):
        shared_path = Path(self._tmp.name, "shared.db")
        store = ObjectStore(shared_path)
        store.configure_shared_payload(8 * U)
        a_bytes = b"shared-A" * 100            # spans several blocks
        da = store.publish("shared-artifact", a_bytes)
        nonce = 5
        store.record_rebuilds(
            nonce, [(0, 0, da), (1, 1, da), (2, 2, da)])
        self.assertEqual(store.rebuild_verdict(nonce), ("verified", da))
        use_before = dict(
            sqlite3.connect(str(shared_path)).execute(
                "SELECT digest, seq FROM digest_use"))
        self.assertIn(da, use_before)
        self.assertEqual(store.fetch_reproduced(nonce, "shared-artifact"),
                         (da, a_bytes))
        # The successful checked read did not refresh ordinary LRU state.
        self.assertEqual(dict(
            sqlite3.connect(str(shared_path)).execute(
                "SELECT digest, seq FROM digest_use")), use_before)

        # Corrupt the first physical block of A.
        first_block = a_bytes[:U]
        block_hash = digest(first_block)
        with closing(sqlite3.connect(str(shared_path))) as db:
            changed = db.execute(
                "UPDATE shared_block SET data=? WHERE block_hash=?",
                (b"\x5a" * len(first_block), block_hash)).rowcount
            db.commit()
        self.assertEqual(changed, 1)

        # The verdict is metadata-only and stays verified; the checked read
        # still verifies the whole artifact and reports the original error.
        self.assertEqual(store.rebuild_verdict(nonce), ("verified", da))
        with self.assertRaises(CorruptBlob):
            store.fetch_reproduced(nonce, "shared-artifact")

        # Removing the last tag and reclaiming drops the payload but not the
        # reproduction evidence.
        self.assertTrue(store.remove("shared-artifact"))
        self.assertEqual(store.collect(), 1)
        self.assertEqual(store.rebuild_verdict(nonce), ("verified", da))
        with self.assertRaises(KeyError):
            store.fetch_reproduced(nonce, "shared-artifact")

    def test_checked_read_does_not_refresh_use_order_or_create_handles(self):
        a_bytes = b"lru-neutral"
        da = self.store.publish("lru-artifact", a_bytes)
        self.store.publish("other", b"something-else")
        case = 21
        self.store.record_rebuilds(
            case, [(0, 0, da), (1, 1, da), (2, 2, da)])

        use_before = self._use_rows()
        builds_before = self._build_count()
        self.assertEqual(self.store.rebuild_verdict(case), ("verified", da))
        self.assertEqual(self.store.fetch_reproduced(case, "lru-artifact"),
                         (da, a_bytes))
        self.assertEqual(self._use_rows(), use_before)
        self.assertEqual(self._build_count(), builds_before)

        # A store opened without any configuration keeps all old entry points.
        legacy_path = Path(self._tmp.name, "legacy.db")
        legacy = ObjectStore(legacy_path)
        self.assertEqual(legacy.publish("plain", b"plain-bytes"),
                         digest(b"plain-bytes"))
        self.assertEqual(legacy.fetch("plain"), b"plain-bytes")
        self.assertEqual(legacy.collect(), 0)
        self.assertTrue(legacy.remove("plain"))
        self.assertEqual(legacy.collect(), 1)

    def test_tag_absence_and_contested_without_bytes(self):
        case = 30
        d1 = digest(b"never-stored-1")
        d2 = digest(b"never-stored-2")
        # Registering observations never requires resident bytes.
        self.store.record_rebuilds(
            case, [(0, 0, d1), (1, 1, d1), (2, 2, d1),
                   (3, 3, d2), (4, 4, d2), (5, 5, d2)])
        self.assertEqual(self.store.rebuild_verdict(case), ("contested", None))
        self.store.publish("present", b"present-bytes")
        # A missing tag is KeyError regardless of verdict.
        with self.assertRaises(KeyError):
            self.store.fetch_reproduced(case, "absent")
        # A present tag with a contested case refuses without bytes.
        self._assert_unverified(case, "present", "contested")


class SharedReproducibilityFixtureTest(unittest.TestCase):
    """The checked read also works over a shared-block store end to end."""

    def test_verified_fetch_in_shared_mode_returns_whole_artifact(self):
        with TemporaryDirectory() as tmp:
            path = Path(tmp, "shared-ok.db")
            store = ObjectStore(path)
            store.configure_shared_payload(4 * U)
            data = b"z" * (3 * U + 17)
            d = store.publish("whole", data)
            store.record_rebuilds(
                9, [(0, 0, d), (10, 4, d), (31, 20, d)])
            self.assertEqual(store.rebuild_verdict(9), ("verified", d))
            reopened = ObjectStore(path)
            self.assertEqual(reopened.fetch_reproduced(9, "whole"), (d, data))


if __name__ == "__main__":
    unittest.main()
