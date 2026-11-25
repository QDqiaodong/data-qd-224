"""Acceptance tests for ObjectStore.prepare_workflow.

These tests use temporary SQLite files and only the Python standard library.
Concurrency tests synchronize on real committed database writes and threading
events; they do not sleep to guess ordering.
"""
from contextlib import closing
from pathlib import Path
from tempfile import TemporaryDirectory
import hashlib
import sqlite3
import threading
import unittest

from objects import MAX_ACTIVE_BUILDS, ObjectStore


def digest(data):
    return hashlib.sha256(data).hexdigest()


class WorkflowPlanTest(unittest.TestCase):
    def setUp(self):
        self._tmp = TemporaryDirectory()
        self.path = Path(self._tmp.name, "workflow.db")
        self.store = ObjectStore(self.path)

    def tearDown(self):
        self._tmp.cleanup()

    def _db_rows(self, query, params=()):
        with closing(sqlite3.connect(str(self.path))) as db:
            return db.execute(query, params).fetchall()

    def _publish_headline(self):
        x = b"wxyz"  # exactly four bytes
        y = b"stuv"
        dx = digest(x)
        dy = digest(y)
        self.store.publish("A-source", x)
        self.store.publish("B-source", x)
        self.store.publish("C-source", y)
        actions = {
            "A": (6, ("P",), "A-source", dx),
            "B": (6, ("P",), "B-source", dx),
            "P": (10, (), None, None),
            "C": (9, (), "C-source", dy),
        }
        targets = ["A", "B", "C"]
        return x, y, dx, dy, actions, targets

    def test_shared_digest_budget_and_action_id_checkout(self):
        x, y, dx, dy, actions, targets = self._publish_headline()

        handle, run_order, cached = self.store.prepare_workflow(
            actions, targets, 4)

        self.assertEqual(cached, {"A": dx, "B": dx})
        self.assertEqual(run_order, ["C"])
        self.assertIsInstance(handle, str)

        # Reopen the database: the handle and action-ID bindings are durable.
        reopened = ObjectStore(self.path)
        self.assertEqual(reopened.fetch_for_build(handle, "A"), x)
        self.assertEqual(reopened.fetch_for_build(handle, "B"), x)
        self.assertEqual(
            reopened.describe_build(handle),
            {"A": (dx, 4), "B": (dx, 4)})
        with self.assertRaises(KeyError):
            reopened.fetch_for_build(handle, "A-source")
        self.assertNotIn("A-source", reopened.describe_build(handle))
        self.assertTrue(reopened.finish_build(handle))

    def test_missing_tag_changes_global_optimum_and_runs_shared_dependency(self):
        _x, _y, dx, dy, actions, targets = self._publish_headline()
        self.assertTrue(self.store.remove("A-source"))

        handle, run_order, cached = self.store.prepare_workflow(
            actions, targets, 4)

        # Caching only C leaves A+B+P to run (22 work).  Caching only B
        # still needs P+A and also C (25 work), so C is globally better.
        self.assertEqual(cached, {"C": dy})
        self.assertEqual(run_order, ["P", "A", "B"])
        self.assertIsInstance(handle, str)
        self.assertEqual(
            self.store.describe_build(handle), {"C": (dy, 4)})

    def test_rebound_tag_and_corrupt_blob_are_misses(self):
        _x, _y, dx, dy, actions, targets = self._publish_headline()

        # Rebind A's tag to a different digest.
        wrong_digest = self.store.publish("A-source", b"wrong!")
        self.assertNotEqual(wrong_digest, dx)
        _handle, run_order, cached = self.store.prepare_workflow(
            actions, targets, 4)
        self.assertEqual(cached, {"C": dy})
        self.assertEqual(run_order, ["P", "A", "B"])

        # Restore the correct tag, then corrupt the shared expected bytes.
        self.store.publish("A-source", b"wxyz")
        with closing(self.store.connection()) as db, db:
            db.execute("UPDATE blob SET data=? WHERE digest=?",
                       (b"bad!", dx))
        _handle, run_order, cached = self.store.prepare_workflow(
            actions, targets, 4)
        self.assertEqual(cached, {"C": dy})
        self.assertEqual(run_order, ["P", "A", "B"])

    def test_same_digest_through_another_tag_does_not_save_missing_tag(self):
        x, _y, dx, _dy, actions, _targets = self._publish_headline()
        self.store.remove("A-source")

        # B-source still names dx, but A's own declared tag is absent.
        handle, run_order, cached = self.store.prepare_workflow(
            actions, ["A"], 4)
        self.assertIsNone(handle)
        self.assertEqual(cached, {})
        self.assertEqual(run_order, ["P", "A"])

        # Direct access through the other action ID/tag is still independent.
        self.assertEqual(self.store.fetch("B-source"), x)

    def test_tie_breaks_by_bytes_then_cached_action_id_tuple(self):
        a = b"aaaa"
        b = b"bbbb"
        da = self.store.publish("ta", a)
        db_ = self.store.publish("tb", b)
        actions = {
            "A": (6, (), "ta", da),
            "B": (6, (), "tb", db_),
        }

        # Equal work and equal four-byte cost: ("A",) is lexicographically
        # smaller than ("B",).
        handle, run_order, cached = self.store.prepare_workflow(
            actions, ["A", "B"], 4)
        self.assertEqual(cached, {"A": da})
        self.assertEqual(run_order, ["B"])
        self.store.finish_build(handle)

        # Make A eight bytes and B four bytes.  At budget eight either one can
        # be selected, but B wins on reused bytes despite its larger ID.
        a8 = b"aaaaaaaa"
        da8 = self.store.publish("ta", a8)
        actions["A"] = (6, (), "ta", da8)
        handle, run_order, cached = self.store.prepare_workflow(
            actions, ["A", "B"], 8)
        self.assertEqual(cached, {"B": db_})
        self.assertEqual(run_order, ["A"])
        self.store.finish_build(handle)

    def test_zero_byte_candidate_uses_no_budget(self):
        d0 = digest(b"")
        self.store.publish("empty-tag", b"")
        actions = {"Z": (5, (), "empty-tag", d0)}

        handle, run_order, cached = self.store.prepare_workflow(
            actions, ["Z"], 0)
        self.assertIsInstance(handle, str)
        self.assertEqual(run_order, [])
        self.assertEqual(cached, {"Z": d0})
        self.assertEqual(self.store.fetch_for_build(handle, "Z"), b"")
        self.assertEqual(
            self.store.describe_build(handle), {"Z": (d0, 0)})

    def test_unreachable_actions_are_validated_but_not_planned(self):
        d = digest(b"used")
        self.store.publish("t", b"used")
        actions = {
            "A": (1, (), "t", d),
            "Ghost": (2, ("Missing",), None, None),
        }
        with self.assertRaises(ValueError):
            self.store.prepare_workflow(actions, ["A"], 1024)
        self.assertEqual(
            self._db_rows("SELECT COUNT(*) FROM build")[0][0], 0)

        cycle_actions = {
            "A": (1, (), None, None),
            "U": (1, ("V",), None, None),
            "V": (1, ("U",), None, None),
        }
        with self.assertRaises(ValueError):
            self.store.prepare_workflow(cycle_actions, ["A"], 1024)

        # A valid but unreachable expensive/cacheable node is not returned.
        valid = {
            "A": (1, (), "t", d),
            "Never": (100, (), "t", d),
        }
        handle, run_order, cached = self.store.prepare_workflow(
            valid, ["A"], 1024)
        self.assertEqual(cached, {"A": d})
        self.assertEqual(run_order, [])
        self.store.finish_build(handle)

    def test_invalid_declarations_raise_value_error_without_checkout(self):
        d = digest(b"x")
        valid_candidate = ("tag", d)
        bad_actions = [
            {}, [], None, "nope",
            {"": (1, (), None, None)},
            {"A" * 49: (1, (), None, None)},
            {"a.b": (1, (), None, None)},
            {"ä": (1, (), None, None)},
            {"A": [1, (), None, None]},
            {"A": (1,)},
            {"A": (0, (), None, None)},
            {"A": (True, (), None, None)},
            {"A": (1_000_001, (), None, None)},
            {"A": (1.5, (), None, None)},
            {"A": (1, [], None, None)},
            {"A": (1, ("missing",), None, None)},
            {"A": (1, ("A", "A"), None, None)},
            {"A": (1, (), "tag", None)},
            {"A": (1, (), None, d)},
            {"A": (1, (), " ", d)},
            {"A": (1, (), "tag", d.upper())},
            {"A": (1, (), "tag", "z" * 64)},
            {"A": (1, ("A",), None, None)},
        ]
        for actions in bad_actions:
            with self.assertRaises(ValueError):
                self.store.prepare_workflow(actions, ["A"], 0)

        nineteen = {
            "A%02d" % i: (1, (), "tag%d" % i,
                          digest(str(i).encode()))
            for i in range(19)
        }
        with self.assertRaises(ValueError):
            self.store.prepare_workflow(
                nineteen, ["A00"], 24 << 20)

        actions = {"A": (1, (), None, None)}
        for targets in ([], ["A", "A"], ["missing"], [1], tuple(["A"]),
                        ["A"] * 25):
            with self.assertRaises(ValueError):
                self.store.prepare_workflow(actions, targets, 0)

        for budget in (-1, (24 << 20) + 1, True, False, 1.0, "4"):
            with self.assertRaises(ValueError):
                self.store.prepare_workflow(actions, ["A"], budget)

        self.assertEqual(
            self._db_rows("SELECT COUNT(*) FROM build")[0][0], 0)

    def test_protected_action_id_pins_survive_tags_capacity_and_reopen(self):
        x = b"wxyz"
        dx = digest(x)
        self.store.publish("A-source", x)
        self.store.publish("B-source", x)
        actions = {
            "A": (6, (), "A-source", dx),
            "B": (6, (), "B-source", dx),
        }
        handle, order, cached = self.store.prepare_workflow(
            actions, ["A", "B"], 4)
        self.assertEqual(order, [])
        self.assertEqual(cached, {"A": dx, "B": dx})

        # Source aliases go away; the action-ID checkout remains.
        self.store.remove("A-source")
        self.store.remove("B-source")

        victim = b"0123456789"
        dv = self.store.publish("victim", victim)
        self.store.remove("victim")
        self.store.configure_capacity(4)

        resident = {
            row[0] for row in self._db_rows("SELECT digest FROM blob")}
        self.assertIn(dx, resident)
        self.assertNotIn(dv, resident)

        reopened = ObjectStore(self.path)
        self.assertEqual(reopened.fetch_for_build(handle, "A"), x)
        self.assertEqual(reopened.fetch_for_build(handle, "B"), x)
        self.assertEqual(
            reopened.describe_build(handle),
            {"A": (dx, 4), "B": (dx, 4)})

        self.assertTrue(reopened.finish_build(handle))
        self.assertEqual(reopened.collect(), 1)
        resident = {
            row[0] for row in self._db_rows("SELECT digest FROM blob")}
        self.assertNotIn(dx, resident)

    def test_full_slots_fail_nonempty_reuse_atomically_but_zero_reuse_plans(self):
        _x, _y, dx, dy, actions, targets = self._publish_headline()
        self.store.publish("slot-tag", b"s")
        slot_handles = [
            self.store.begin_build(["slot-tag"])[0]
            for _ in range(MAX_ACTIVE_BUILDS)
        ]
        self.assertEqual(
            self._db_rows("SELECT COUNT(*) FROM build")[0][0], 64)
        pins_before = self._db_rows("SELECT COUNT(*) FROM build_pin")[0][0]

        with self.assertRaises(ValueError):
            self.store.prepare_workflow(actions, targets, 4)
        self.assertEqual(
            self._db_rows("SELECT COUNT(*) FROM build")[0][0], 64)
        self.assertEqual(
            self._db_rows("SELECT COUNT(*) FROM build_pin")[0][0],
            pins_before)

        # No checkout is needed when the budget permits no cached output.
        handle, run_order, cached = self.store.prepare_workflow(
            actions, targets, 0)
        self.assertIsNone(handle)
        self.assertEqual(cached, {})
        self.assertEqual(run_order, ["C", "P", "A", "B"])
        self.assertEqual(
            self._db_rows("SELECT COUNT(*) FROM build")[0][0], 64)

        for slot_handle in slot_handles:
            self.store.finish_build(slot_handle)

    def test_planning_probe_and_admission_do_not_touch_lru(self):
        _x, _y, _dx, _dy, actions, targets = self._publish_headline()
        before = dict(self._db_rows("SELECT digest, seq FROM digest_use"))
        self.assertTrue(before)

        handle, _order, cached = self.store.prepare_workflow(
            actions, targets, 4)
        self.assertEqual(set(cached), {"A", "B"})
        after = dict(self._db_rows("SELECT digest, seq FROM digest_use"))
        self.assertEqual(after, before)
        self.store.finish_build(handle)
        finished = dict(self._db_rows("SELECT digest, seq FROM digest_use"))
        self.assertEqual(finished, before)


class WorkflowConcurrencyTest(unittest.TestCase):
    def setUp(self):
        self._tmp = TemporaryDirectory()
        self.path = Path(self._tmp.name, "workflow-concurrent.db")
        self.store = ObjectStore(self.path)

    def tearDown(self):
        self._tmp.cleanup()

    def test_final_admission_replans_candidate_restored_during_search(self):
        x = b"wxyz"
        y = b"stuv"
        dx = digest(x)
        dy = digest(y)
        # Deliberately do not publish A-source yet: the old candidate view has
        # A unavailable while B and C are available.
        self.store.publish("B-source", x)
        self.store.publish("C-source", y)
        actions = {
            "A": (6, ("P",), "A-source", dx),
            "B": (6, ("P",), "B-source", dx),
            "P": (10, (), None, None),
            "C": (9, (), "C-source", dy),
        }

        original_plan = self.store._plan_workflow
        old_view_taken = threading.Event()
        allow_search_to_finish = threading.Event()

        def paused_plan(spec, view):
            # prepare_workflow has already read and released its short SQLite
            # snapshot before calling the graph planner.
            old_view_taken.set()
            self.assertTrue(allow_search_to_finish.wait(5))
            return original_plan(spec, view)

        self.store._plan_workflow = paused_plan
        errors = []
        result = {}

        def prepare():
            try:
                result["value"] = self.store.prepare_workflow(
                    actions, ["A", "B", "C"], 4)
            except BaseException as exc:  # pragma: no cover - diagnostic
                errors.append(exc)

        worker = threading.Thread(target=prepare)
        worker.start()
        self.assertTrue(old_view_taken.wait(5))

        writer = ObjectStore(self.path)
        self.assertEqual(writer.publish("A-source", x), dx)
        # publish() returns only after its transaction commits; confirm a
        # fresh SQLite connection sees the restored candidate.
        with closing(sqlite3.connect(str(self.path))) as db:
            self.assertEqual(
                db.execute("SELECT digest FROM tag WHERE name=?",
                           ("A-source",)).fetchone()[0],
                dx)
        self.assertTrue(worker.is_alive())

        allow_search_to_finish.set()
        worker.join(10)
        self.assertFalse(worker.is_alive())
        self.assertEqual(errors, [])

        handle, run_order, cached = result["value"]
        self.assertEqual(cached, {"A": dx, "B": dx})
        self.assertEqual(run_order, ["C"])
        self.assertEqual(
            self.store.describe_build(handle),
            {"A": (dx, 4), "B": (dx, 4)})
        self.store.finish_build(handle)


if __name__ == "__main__":
    unittest.main()
