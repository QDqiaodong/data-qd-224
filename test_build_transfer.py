"""Acceptance tests for receiver-driven build transfer and one-shot install.

Two nodes keep separate SQLite files; tests only move manifests (plain dicts)
and byte streams between them, never opening the peer's database.  Slow
readers/writers are paused with explicit events rather than sleeps.
"""
from contextlib import closing
from pathlib import Path
from tempfile import TemporaryDirectory
import hashlib
import json
import os
import resource
import sqlite3
import subprocess
import sys
import threading
import unittest

from objects import (MAX_CAPACITY, ObjectStore, CapacityExceeded, CorruptBlob,
                     TransferBaseUnavailable)

MIB = 1 << 20
CHUNK = 1 << 16


def digest(data):
    return hashlib.sha256(data).hexdigest()


class NonSeekableBuffer:
    """A byte stream that rejects seek, like a pipe or socket."""

    def __init__(self, data=b"", chunk=CHUNK):
        self._buf = bytes(data)
        self._pos = 0
        self._chunk = chunk
        self.closed = False

    def seekable(self):
        return False

    def seek(self, *args):
        raise OSError("stream is not seekable")

    def write(self, data):
        raise NotImplementedError

    def read(self, n):
        if not isinstance(n, int) or n <= 0:
            raise ValueError("read size must be positive")
        if n > CHUNK:
            raise AssertionError("read requested more than 64 KiB: %d" % n)
        part = self._buf[self._pos:self._pos + min(n, self._chunk)]
        self._pos += len(part)
        return part


class BlockingSink:
    """write() blocks at a deterministic sync point the first time."""

    def __init__(self, gate, entered, limit=CHUNK):
        self._gate = gate
        self._entered = entered
        self._limit = limit
        self.parts = []
        self._blocked = False

    def seekable(self):
        return False

    def write(self, data):
        if len(data) > self._limit:
            raise AssertionError("write larger than 64 KiB: %d" % len(data))
        if not self._blocked:
            self._blocked = True
            self._entered.set()
            self._gate.wait()
        self.parts.append(bytes(data))
        return len(data)


class BlockingSource:
    """read() blocks at a deterministic sync point on its first call."""

    def __init__(self, data, gate, entered, chunk=CHUNK):
        self._inner = NonSeekableBuffer(data, chunk=chunk)
        self._gate = gate
        self._entered = entered
        self._blocked = False

    def seekable(self):
        return False

    def read(self, n):
        if not self._blocked:
            self._blocked = True
            self._entered.set()
            self._gate.wait()
        return self._inner.read(n)


class BuildTransferTest(unittest.TestCase):
    def setUp(self):
        self._tmp = TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.src_path = self.root / "src.db"
        self.dst_path = self.root / "dst.db"
        self.source = ObjectStore(self.src_path)
        self.dest = ObjectStore(self.dst_path)

    def tearDown(self):
        self._tmp.cleanup()

    def _rows(self, path, query, params=()):
        with closing(sqlite3.connect(str(path))) as db:
            return db.execute(query, params).fetchall()

    # ------------------------------------------------------------------
    # manifest validation and describe/needs semantics
    # ------------------------------------------------------------------

    def test_describe_build_pins_manifest_ignoring_current_tags(self):
        a = self.source.publish("a", b"aaa")
        b = self.source.publish("b", b"bbbb")
        handle, bindings = self.source.begin_build(["a", "b"])
        self.source.publish("a", b"changed-after-pin")
        manifest = self.source.describe_build(handle)
        self.assertEqual(manifest,
                         {"a": (a, 3), "b": (b, 4)})
        self.assertIsInstance(manifest, dict)
        # Unknown / released handles raise KeyError.
        with self.assertRaises(KeyError):
            self.source.describe_build("no-such-handle")
        with self.assertRaises(KeyError):
            self.source.describe_build(42)
        self.assertTrue(self.source.finish_build(handle))
        with self.assertRaises(KeyError):
            self.source.describe_build(handle)

    def test_manifest_validation_rejects_bad_shapes(self):
        good_d = digest(b"x")
        good = {"t": (good_d, 1)}
        # describe output itself is always valid.
        self.assertEqual(self.dest.transfer_needs(good), [good_d])
        bad_manifests = [
            None, {}, [], "x", 7,
            {("n",): (good_d, 1)},                       # non-string tag
            {"": (good_d, 1)},                            # blank tag
            {"t": (good_d,)},                             # wrong arity
            {"t": ("z" * 64, 1)},                         # non-hex digest
            {"t": (good_d.upper(), 1)},                   # uppercase hex
            {"t": (good_d, True)},                        # bool size
            {"t": (good_d, -1)},                          # negative size
            {"t": (good_d, 1.0)},                         # float size
            {"t": (good_d, MIB + 1)},                     # too large
            {"t": [good_d, 1]},                           # list, not tuple
            {"t%d" % i: (digest(bytes([i])), 1)
             for i in range(25)},                         # 25 tags
        ]
        # Same digest with conflicting sizes.
        bad_manifests.append({"t1": (good_d, 1), "t2": (good_d, 2)})
        for bad in bad_manifests:
            with self.assertRaises(ValueError):
                self.dest.transfer_needs(bad)

    def test_same_digest_may_back_several_tags_with_one_length(self):
        d = self.source.publish("a1", b"shared")
        self.source.publish("a2", b"shared")
        handle, _ = self.source.begin_build(["a1", "a2"])
        manifest = self.source.describe_build(handle)
        self.assertEqual(manifest, {"a1": (d, 6), "a2": (d, 6)})
        self.assertEqual(self.dest.transfer_needs(manifest), [d])

    def test_transfer_needs_detects_missing_wrong_length_and_bad_hash(self):
        d_missing = digest(b"missing")
        d_short = digest(b"12345")
        d_bad = digest(b"good-content")
        d_ok = digest(b"x")
        self.dest.publish("short", b"123")                 # wrong length
        with closing(self.dest.connection()) as db, db:
            db.execute("INSERT INTO blob VALUES (?,?)",
                       (d_bad, b"tampered"))               # wrong hash
            db.execute("INSERT INTO blob VALUES (?,?)", (d_ok, b"x"))
        manifest = {
            "m": (d_missing, 7),
            "s": (d_short, 5),
            "c": (d_bad, len(b"good-content")),
            "ok": (d_ok, 1),
        }
        self.assertEqual(self.dest.transfer_needs(manifest),
                         sorted([d_missing, d_short, d_bad]))
        # The advice establishes no retention and changes no LRU state.
        before = self._rows(self.dst_path,
                            "SELECT digest, seq FROM digest_use")
        self.dest.transfer_needs(manifest)
        self.assertEqual(
            self._rows(self.dst_path,
                       "SELECT digest, seq FROM digest_use"), before)

    def test_send_validation_writes_no_bytes(self):
        a = self.source.publish("a", b"aaa")
        handle, _ = self.source.begin_build(["a"])
        manifest = self.source.describe_build(handle)

        class FailSink:
            def __init__(self):
                self.written = False

            def write(self, data):
                self.written = True
                raise AssertionError("no bytes may be written")

        sink = FailSink()
        # unknown handle
        with self.assertRaises(KeyError):
            self.source.send_build("nope", manifest, [], sink)
        # mismatched digest
        wrong = dict(manifest)
        wrong["a"] = (digest(b"other"), 3)
        with self.assertRaises(ValueError):
            self.source.send_build(handle, wrong, [], sink)
        # mismatched tag set
        with self.assertRaises(ValueError):
            self.source.send_build(handle, {"other": (a, 3)}, [], sink)
        # needed list problems
        with self.assertRaises(ValueError):
            self.source.send_build(handle, manifest, [a, a], sink)
        with self.assertRaises(ValueError):
            self.source.send_build(handle, manifest, [digest(b"x")], sink)
        with self.assertRaises(ValueError):
            self.source.send_build(handle, manifest, (a,), sink)
        self.assertFalse(sink.written)
        # Empty needed sends nothing and is fine.
        sink2 = FailSink()
        self.assertIsNone(
            self.source.send_build(handle, manifest, [], sink2))
        self.assertFalse(sink2.written)

    # ------------------------------------------------------------------
    # headline scenario: dedup, reuse protection, no rebind, reclaim
    # ------------------------------------------------------------------

    def test_dedup_send_reuse_eviction_protection_and_reclaim(self):
        new_blob = b"brand-new-artifact"
        a_blob = b"artifact-A"
        d_new = self.source.publish("n1", new_blob)
        self.source.publish("n2", new_blob)        # two names, one blob
        d_a = self.source.publish("A", a_blob)
        handle, _ = self.source.begin_build(["n1", "n2", "A"])
        manifest = self.source.describe_build(handle)
        self.assertEqual(manifest["n1"], manifest["n2"])

        # Destination already holds A, with a same-named tag "A".
        self.dest.publish("A", a_blob)
        # One extra untagged object that admission will have to evict to fit
        # the incoming blob under a tight capacity.
        victim = b"victim-bytes-xx"                  # 14 bytes
        self.dest.publish("vtag", victim)
        d_victim = digest(victim)
        self.dest.remove("vtag")
        self.dest.configure_capacity(len(a_blob) + len(new_blob))

        needed = self.dest.transfer_needs(manifest)
        self.assertEqual(needed, [d_new])            # new blob exactly once

        captured = bytearray()

        class Collect:
            def write(self, data):
                if len(data) > CHUNK:
                    raise AssertionError("oversized write")
                captured.extend(data)
                return len(data)

        self.source.send_build(handle, manifest, needed, Collect())
        self.assertEqual(bytes(captured), new_blob)  # one copy only

        install_handle, bindings = self.dest.install_build(
            manifest, needed, NonSeekableBuffer(bytes(captured)))
        self.assertEqual(bindings, {"n1": d_new, "n2": d_new, "A": d_a})

        # The same-named destination tag was not rebound to the new blob.
        self.assertEqual(self.dest.fetch("A"), a_blob)
        # The reusable A was protected from eviction; the untagged victim went.
        resident = {r[0] for r in self._rows(
            self.dst_path, "SELECT digest FROM blob")}
        self.assertIn(d_a, resident)
        self.assertIn(d_new, resident)
        self.assertNotIn(d_victim, resident)
        # The new checkout reads exactly the manifest's versions after reopen.
        reopened = ObjectStore(self.dst_path)
        self.assertEqual(reopened.fetch_for_build(install_handle, "n1"),
                         new_blob)
        self.assertEqual(reopened.fetch_for_build(install_handle, "n2"),
                         new_blob)
        self.assertEqual(reopened.fetch_for_build(install_handle, "A"),
                         a_blob)
        # Releasing the checkout makes the unreferenced imported blob
        # collectable (it has no destination tag of its own).
        self.assertTrue(reopened.finish_build(install_handle))
        self.assertEqual(reopened.collect(), 1)
        remaining = {r[0] for r in self._rows(
            self.dst_path, "SELECT digest FROM blob")}
        self.assertNotIn(d_new, remaining)
        self.assertIn(d_a, remaining)

    # ------------------------------------------------------------------
    # concurrency at explicit synchronization points
    # ------------------------------------------------------------------

    def test_sender_survives_concurrent_release_rebind_reclaim(self):
        old = b"old-payload"
        new = b"new-payload-after-rebind"
        d_old = self.source.publish("t", old)
        handle, _ = self.source.begin_build(["t"])
        manifest = self.source.describe_build(handle)
        other = ObjectStore(self.src_path)        # second connection
        gate = threading.Event()
        entered = threading.Event()
        sink = BlockingSink(gate, entered)
        errors = []

        def send():
            try:
                self.source.send_build(handle, manifest, [d_old], sink)
            except BaseException as exc:  # pragma: no cover - diagnostic
                errors.append(exc)

        t = threading.Thread(target=send)
        t.start()
        self.assertTrue(entered.wait(5))
        # While the first sink write is paused, the peer releases the handle,
        # rebinds the tag and reclaims the original object.
        self.assertTrue(other.finish_build(handle))
        d_new = other.publish("t", new)
        self.assertNotEqual(d_old, d_new)
        self.assertEqual(other.collect(), 1)
        gate.set()
        t.join(5)
        self.assertFalse(t.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(b"".join(sink.parts), old)   # captured version only

    def test_receiver_reports_retransmit_after_local_copy_vanishes(self):
        a_blob = b"local-A-payload"
        b_blob = b"incoming-B-payload"
        d_a = digest(a_blob)
        d_b = digest(b_blob)
        self.dest.publish("A", a_blob)
        manifest = {"A": (d_a, len(a_blob)),
                    "B": (d_b, len(b_blob))}
        # Only B is on the wire; A was supposed to be reused locally.
        wire = BlockingSource(b_blob, threading.Event(), threading.Event())
        gate, entered = wire._gate, wire._entered
        other = ObjectStore(self.dst_path)
        errors = []

        def install():
            try:
                self.dest.install_build(manifest, [d_b], wire)
            except BaseException as exc:
                errors.append(exc)

        t = threading.Thread(target=install)
        t.start()
        self.assertTrue(entered.wait(5))
        # Another connection removes and reclaims A while the read is paused.
        self.assertTrue(other.remove("A"))
        self.assertEqual(other.collect(), 1)
        gate.set()
        t.join(5)
        self.assertFalse(t.is_alive())
        self.assertEqual(len(errors), 1)
        exc = errors[0]
        self.assertIsInstance(exc, TransferBaseUnavailable)
        self.assertEqual(exc.digests, (d_a,))
        # No half-finished checkout, and the streamed B was not installed.
        self.assertEqual(self._rows(self.dst_path,
                                    "SELECT COUNT(*) FROM build")[0][0], 0)
        self.assertEqual(self._rows(self.dst_path,
                                    "SELECT COUNT(*) FROM build_pin")[0][0], 0)
        resident = {r[0] for r in self._rows(
            self.dst_path, "SELECT digest FROM blob")}
        self.assertNotIn(d_b, resident)
        self.assertNotIn(d_a, resident)

        # Retransmitting with fresh advice succeeds.
        self.source.publish("A", a_blob)
        self.source.publish("B", b_blob)
        handle2, _ = self.source.begin_build(["A", "B"])
        manifest2 = self.source.describe_build(handle2)
        fresh_dest = ObjectStore(self.dst_path)
        needed = fresh_dest.transfer_needs(manifest2)
        self.assertEqual(needed, sorted([d_a, d_b]))
        buf = bytearray()

        class Sink:
            def write(self, data):
                buf.extend(data)
                return len(data)

        self.source.send_build(handle2, manifest2, needed, Sink())
        # Wire order is digest ascending.
        order = sorted([d_a, d_b])
        expected = b"".join(
            b_blob if d == d_b else a_blob for d in order)
        self.assertEqual(bytes(buf), expected)
        new_handle, bindings = fresh_dest.install_build(
            manifest2, needed, NonSeekableBuffer(bytes(buf)))
        self.assertEqual(fresh_dest.fetch_for_build(new_handle, "A"), a_blob)
        self.assertEqual(fresh_dest.fetch_for_build(new_handle, "B"), b_blob)

    def test_other_writes_commit_while_receiver_waits_on_source(self):
        a_blob = b"receiver-wait-A"
        d_a = digest(a_blob)
        manifest = {"A": (d_a, len(a_blob))}
        gate = threading.Event()
        entered = threading.Event()
        source = BlockingSource(a_blob, gate, entered)
        other = ObjectStore(self.dst_path)
        done = threading.Event()
        errors = []

        def install():
            try:
                self.dest.install_build(manifest, [d_a], source)
                done.set()
            except BaseException as exc:  # pragma: no cover - diagnostic
                errors.append(exc)
                done.set()

        t = threading.Thread(target=install)
        t.start()
        self.assertTrue(entered.wait(5))
        # Ordinary writes on another instance must commit during the wait.
        d = other.publish("parallel", b"while-waiting")
        self.assertEqual(other.fetch("parallel"), b"while-waiting")
        other.remove("parallel")
        gate.set()
        self.assertTrue(done.wait(5))
        t.join(5)
        self.assertEqual(errors, [])

    # ------------------------------------------------------------------
    # wire framing and error distinction
    # ------------------------------------------------------------------

    def test_truncated_stream_raises_value_error_and_changes_nothing(self):
        d = digest(b"0123456789")
        manifest = {"x": (d, 10)}
        before = self._snapshot()
        with self.assertRaises(ValueError):
            self.dest.install_build(
                manifest, [d], NonSeekableBuffer(b"01234"))  # short read
        self.assertEqual(self._snapshot(), before)

    def test_trailing_bytes_raise_value_error(self):
        d = digest(b"0123456789")
        manifest = {"x": (d, 10)}
        before = self._snapshot()
        with self.assertRaises(ValueError):
            self.dest.install_build(
                manifest, [d], NonSeekableBuffer(b"0123456789EXTRA"))
        self.assertEqual(self._snapshot(), before)

    def test_wrong_digest_stream_raises_corrupt_blob(self):
        claimed = digest(b"the-expected-content")
        manifest = {"x": (claimed, len(b"the-expected-content"))}
        before = self._snapshot()
        with self.assertRaises(CorruptBlob):
            self.dest.install_build(
                manifest, [claimed],
                NonSeekableBuffer(b"totally-different-bytes"))
        self.assertEqual(self._snapshot(), before)

    def test_missing_local_untransmitted_raises_transfer_base_unavailable(self):
        d = digest(b"never-sent")
        manifest = {"x": (d, 10)}
        before = self._snapshot()
        with self.assertRaises(TransferBaseUnavailable) as cm:
            self.dest.install_build(manifest, [], NonSeekableBuffer(b""))
        self.assertEqual(cm.exception.digests, (d,))
        self.assertEqual(self._snapshot(), before)

    def test_corrupt_local_untransmitted_is_distinct_from_bad_frame(self):
        good = b"good-local-copy"
        d_good = digest(good)
        local = b"should-be-here"
        d_bad = digest(local)
        self.dest.publish("good-tag", good)
        with closing(self.dest.connection()) as db, db:
            db.execute("INSERT INTO blob VALUES (?,?)", (d_bad, b"tampered"))
        manifest = {"ok": (d_good, len(good)),
                    "bad": (d_bad, len(local))}
        # A bad *frame* (the streamed digest does not match its bytes) is
        # reported as CorruptBlob, never as a local-base problem.
        with self.assertRaises(CorruptBlob):
            self.dest.install_build(
                manifest, [d_good],
                NonSeekableBuffer(b"wrong-frame-bytes"))
        # With nothing on the wire, verification reaches the bad local copy
        # and surfaces TransferBaseUnavailable, naming exactly that digest.
        with self.assertRaises(TransferBaseUnavailable) as cm:
            self.dest.install_build(manifest, [], NonSeekableBuffer(b""))
        self.assertEqual(cm.exception.digests, (d_bad,))

    def test_streamed_blob_atomically_repairs_bad_local_copy(self):
        payload = b"repair-me-please"
        d = digest(payload)
        with closing(self.dest.connection()) as db, db:
            db.execute("INSERT INTO blob VALUES (?,?)", (d, b"broken"))
        manifest = {"x": (d, len(payload))}
        before_use = dict(self._rows(
            self.dst_path, "SELECT digest, seq FROM digest_use"))
        handle, bindings = self.dest.install_build(
            manifest, [d], NonSeekableBuffer(payload))
        self.assertEqual(bindings, {"x": d})
        self.assertEqual(self.dest.fetch_for_build(handle, "x"), payload)
        # Even an ordinary fetch of a tag pointing at it now works.
        with closing(self.dest.connection()) as db, db:
            db.execute("INSERT INTO tag VALUES (?,?)", ("xtag", d))
        self.assertEqual(self.dest.fetch("xtag"), payload)
        # Repair/install did not refresh ordinary LRU: the transfer created
        # no use record for the repaired digest (the fetch above is what adds
        # one).
        self.assertEqual(before_use, {})
        self.dest.finish_build(handle)

    def test_capacity_failure_keeps_bad_copy_old_checkout_and_tags(self):
        # Two checkouts pin two good objects; nothing evictable can make room
        # for the incoming repair under the tight capacity.
        pinned1 = b"pinned-old-build-one"         # 20 bytes
        pinned2 = b"pinned-old-build-two"         # 20 bytes
        bad_payload = b"incoming-repair"          # 15 bytes
        d1 = digest(pinned1)
        d2 = digest(pinned2)
        d_bad = digest(bad_payload)
        assert (len(pinned1), len(pinned2), len(bad_payload)) == (20, 20, 15)
        self.dest.publish("pinned-tag-1", pinned1)
        self.dest.publish("pinned-tag-2", pinned2)
        old_handle, _ = self.dest.begin_build(["pinned-tag-1"])
        handle2, _ = self.dest.begin_build(["pinned-tag-2"])
        # A corrupt local copy of the digest the transfer would repair.
        with closing(self.dest.connection()) as db, db:
            db.execute("INSERT INTO blob VALUES (?,?)", (d_bad, b"broken"))
        # Resident: 20 + 20 (both pinned) + 6 (broken) = 46.  Repairing the
        # broken copy grows it to 15 bytes, so the resident set would be
        # 20 + 20 + 15 = 55; both 20-byte objects are pinned and the broken
        # object itself is part of the incoming manifest, hence protected.
        # Nothing is evictable, so admission must fail.
        self.dest.configure_capacity(46)
        before = self._snapshot()
        manifest = {"p1": (d1, len(pinned1)),
                    "p2": (d2, len(pinned2)),
                    "b": (d_bad, len(bad_payload))}
        with self.assertRaises(CapacityExceeded):
            self.dest.install_build(
                manifest, [d_bad], NonSeekableBuffer(bad_payload))
        self.assertEqual(self._snapshot(), before)
        # The corrupt copy, the old checkouts and every tag are untouched.
        with closing(sqlite3.connect(str(self.dst_path))) as db:
            self.assertEqual(db.execute(
                "SELECT data FROM blob WHERE digest=?",
                (d_bad,)).fetchone()[0], b"broken")
            self.assertEqual(db.execute(
                "SELECT COUNT(*) FROM build").fetchone()[0], 2)
            tags = {r[0] for r in db.execute("SELECT name FROM tag")}
        self.assertEqual(tags, {"pinned-tag-1", "pinned-tag-2"})
        self.assertEqual(
            self.dest.fetch_for_build(old_handle, "pinned-tag-1"), pinned1)
        self.assertEqual(
            self.dest.fetch_for_build(handle2, "pinned-tag-2"), pinned2)

    def test_reuse_and_install_do_not_refresh_lru(self):
        a = b"lru-a"
        b = b"lru-bbbbb"
        d_a = self.dest.publish("a", a)
        d_b = self.dest.publish("b", b)
        before = dict(self._rows(self.dst_path,
                                 "SELECT digest, seq FROM digest_use"))
        manifest = {"a": (d_a, len(a)), "b": (d_b, len(b))}
        handle, _ = self.dest.install_build(
            manifest, [], NonSeekableBuffer(b""))
        after = dict(self._rows(self.dst_path,
                                "SELECT digest, seq FROM digest_use"))
        self.assertEqual(before, after)
        self.dest.finish_build(handle)

    def test_slot_limit_and_dedup_space_accounting(self):
        # Fill all 64 checkout slots on the destination.
        filler = b"slot-filler"
        d = self.dest.publish("tag", filler)
        handles = [self.dest.begin_build(["tag"])[0] for _ in range(64)]
        manifest = {"tag": (d, len(filler))}
        with self.assertRaises(ValueError):
            self.dest.install_build(manifest, [], NonSeekableBuffer(b""))
        handles[0] and self.dest.finish_build(handles[0])
        handle, bindings = self.dest.install_build(
            manifest, [], NonSeekableBuffer(b""))
        self.assertEqual(bindings, {"tag": d})
        # Two checkouts pinning the same digest still occupy one blob's bytes.
        with closing(sqlite3.connect(str(self.dst_path))) as db:
            self.assertEqual(
                db.execute("SELECT COUNT(*) FROM blob WHERE digest=?",
                           (d,)).fetchone()[0], 1)
        self.dest.finish_build(handle)

    def _snapshot(self):
        with closing(sqlite3.connect(str(self.dst_path))) as db:
            return (
                db.execute("SELECT digest, data FROM blob ORDER BY digest")
                .fetchall(),
                db.execute("SELECT name, digest FROM tag ORDER BY name")
                .fetchall(),
                db.execute("SELECT handle FROM build ORDER BY handle")
                .fetchall(),
                db.execute("SELECT handle, name, digest FROM build_pin "
                           "ORDER BY handle, name").fetchall(),
                db.execute("SELECT digest, seq FROM digest_use ORDER BY digest")
                .fetchall(),
            )


# ----------------------------------------------------------------------
# Linux scale test: 24 x 1 MiB through non-seekable chunk-limited streams,
# with an address-space cap measured after module load in each child.
# ----------------------------------------------------------------------

SENDER_CHILD = r"""
import hashlib, json, os, resource, sys
sys.path.insert(0, sys.argv[1])
from objects import ObjectStore

db_path, manifest_path, fifo_path = sys.argv[2], sys.argv[3], sys.argv[4]
with open(manifest_path) as fh:
    manifest = json.load(fh)
manifest = {name: tuple(entry) for name, entry in manifest.items()}
needed = sorted({entry[0] for entry in manifest.values()})

class LimitedWriter:
    def seekable(self):
        return False
    def write(self, data):
        if len(data) > 65536:
            raise AssertionError("oversized write")
        return os.write(fd, data)

fd = os.open(fifo_path, os.O_WRONLY)
try:
    vm = int(open("/proc/self/status").read().split("VmSize:")[1].split()[0]) * 1024
    cap = vm + 16 * 1024 * 1024
    resource.setrlimit(resource.RLIMIT_AS, (cap, cap))
    store = ObjectStore(db_path)
    with open(sys.argv[5]) as meta:
        handle = meta.read().strip()
    store.send_build(handle, manifest, needed, LimitedWriter())
finally:
    os.close(fd)
print("sender ok")
"""

RECEIVER_CHILD = r"""
import hashlib, json, os, resource, sys
sys.path.insert(0, sys.argv[1])
from objects import ObjectStore

db_path, manifest_path, fifo_path = sys.argv[2], sys.argv[3], sys.argv[4]
with open(manifest_path) as fh:
    manifest = json.load(fh)
manifest = {name: tuple(entry) for name, entry in manifest.items()}
needed = sorted({entry[0] for entry in manifest.values()})

class LimitedReader:
    def __init__(self, fd):
        self.fd = fd
    def seekable(self):
        return False
    def read(self, n):
        if n > 65536:
            raise AssertionError("oversized read")
        return os.read(self.fd, n)

fd = os.open(fifo_path, os.O_RDONLY)
try:
    vm = int(open("/proc/self/status").read().split("VmSize:")[1].split()[0]) * 1024
    cap = vm + 16 * 1024 * 1024
    resource.setrlimit(resource.RLIMIT_AS, (cap, cap))
    store = ObjectStore(db_path)
    handle, bindings = store.install_build(
        manifest, needed, LimitedReader(fd))
finally:
    os.close(fd)
with open(sys.argv[5], "w") as out:
    json.dump({"handle": handle, "bindings": bindings}, out)
print("receiver ok")
"""


class FullScaleTransferTest(unittest.TestCase):
    @unittest.skipUnless(sys.platform.startswith("linux"),
                         "RLIMIT_AS test is Linux-specific")
    def test_24_one_mib_blobs_under_address_space_cap(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            src_db = root / "src.db"
            dst_db = root / "dst.db"
            fifo = root / "wire.fifo"
            manifest_path = root / "manifest.json"
            handle_path = root / "handle.txt"
            result_path = root / "result.json"
            os.mkfifo(fifo)

            source = ObjectStore(src_db)
            source.configure_capacity(MAX_CAPACITY)
            blobs = {}
            tags = []
            for i in range(24):
                base = ("artifact-%02d|" % i).encode()
                data = (base * ((MIB // len(base)) + 1))[:MIB]
                assert len(data) == MIB
                name = "tag-%02d" % i
                source.publish(name, data)
                blobs[hashlib.sha256(data).hexdigest()] = data
                tags.append(name)
            handle, bindings = source.begin_build(tags)
            manifest = source.describe_build(handle)
            self.assertEqual(len(manifest), 24)
            self.assertEqual({entry[1] for entry in manifest.values()}, {MIB})
            with open(manifest_path, "w") as fh:
                json.dump(manifest, fh)
            handle_path.write_text(handle)

            module_dir = str(Path(__file__).resolve().parent)
            sender = subprocess.Popen(
                [sys.executable, "-I", "-B", "-c", SENDER_CHILD, module_dir,
                 str(src_db), str(manifest_path), str(fifo), str(handle_path)],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            receiver = subprocess.Popen(
                [sys.executable, "-I", "-B", "-c", RECEIVER_CHILD, module_dir,
                 str(dst_db), str(manifest_path), str(fifo), str(result_path)],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            s_out, s_err = sender.communicate(timeout=300)
            r_out, r_err = receiver.communicate(timeout=300)
            self.assertEqual(sender.returncode, 0, s_err)
            self.assertEqual(receiver.returncode, 0, r_err)
            self.assertIn("sender ok", s_out)
            self.assertIn("receiver ok", r_out)

            result = json.loads(result_path.read_text())
            self.assertEqual(result["bindings"], bindings)
            dest = ObjectStore(dst_db)
            # Verify every installed artifact's actual bytes, not just counts.
            with closing(sqlite3.connect(str(dst_db))) as db:
                rows = db.execute(
                    "SELECT digest, LENGTH(data) FROM blob").fetchall()
            self.assertEqual(len(rows), 24)
            self.assertEqual({d for d, _ in rows}, set(blobs))
            for d, size in rows:
                self.assertEqual(size, MIB)
                self.assertEqual(dest.fetch_for_build(result["handle"],
                                                      _tag_for(d, manifest)),
                                 blobs[d])
            total = sum(size for _, size in rows)
            self.assertEqual(total, 24 * MIB)


def _tag_for(digest_value, manifest):
    for name, (d, _size) in manifest.items():
        if d == digest_value:
            return name
    raise KeyError(digest_value)


if __name__ == "__main__":
    unittest.main()
