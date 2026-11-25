"""Transactional content-addressed blob storage for a local build cache.

The cache lives in one SQLite file.  Every public operation runs in a
``BEGIN IMMEDIATE`` transaction, so independent instances and processes see
the operations in one serial order.

Tags are *mutable* aliases.  A build checkout resolves tags once and pins the
resulting digests; pins live in the database, not in a process, and protect
their blobs from collection and capacity eviction until the checkout is
finished.  Capacity is measured in bytes of distinct blobs actually resident
in the database, including unreferenced bytes that have not been collected
yet.

The build-transfer entry points (``describe_build``, ``transfer_needs``,
``send_build`` and ``install_build``) move one pinned build between two
independent databases through caller-provided byte streams.  Slow streams
never hold a database lock: selected blobs are staged in private temporary
files inside short transactions, and destination admission is a single final
transaction, so concurrent releases, rebinds, reclaims and ordinary writes
can commit while a peer is blocked on I/O.
"""
from contextlib import closing, contextmanager
import hashlib
import os
import re
import secrets
import shutil
import sqlite3
import tempfile

MAX_CAPACITY = 1 << 30
MAX_ACTIVE_BUILDS = 64
MAX_BUILD_TAGS = 24
MAX_BLOB_SIZE = 1 << 20
TRANSFER_CHUNK = 1 << 16
_DIGEST_RE = re.compile(r"[0-9a-f]{64}\Z")


class CorruptBlob(Exception):
    pass


class CapacityExceeded(Exception):
    pass


class TransferBaseUnavailable(Exception):
    """A blob that was not transmitted is missing or fails verification in
    the final admission view."""

    def __init__(self, digests):
        self.digests = tuple(sorted(digests))
        super().__init__(
            "local copies unavailable; retransmit: %s"
            % ",".join(self.digests))


def _valid_tag(name):
    return isinstance(name, str) and bool(name.strip())


def _validate_manifest(manifest):
    """Validate the wire manifest shape and return ``{digest: size}``.

    A manifest is an ordinary dict with 1-24 distinct, non-empty tag names;
    every value is ``(digest, size)`` where digest is a 64-character
    lowercase hex string and size is an int (never bool) in 0..1 MiB.  The
    same digest may back several tags but must keep one length.
    """
    if not isinstance(manifest, dict) or not 1 <= len(manifest) <= MAX_BUILD_TAGS:
        raise ValueError(
            "manifest must be a dict of 1 to %d tag entries" % MAX_BUILD_TAGS)
    sizes = {}
    for name, entry in manifest.items():
        if not _valid_tag(name):
            raise ValueError("manifest tags must be non-empty strings")
        if not isinstance(entry, tuple) or len(entry) != 2:
            raise ValueError(
                "manifest entry for %r must be (digest, size)" % name)
        digest, size = entry
        if not isinstance(digest, str) or _DIGEST_RE.match(digest) is None:
            raise ValueError(
                "digest for %r must be 64 lowercase hex characters" % name)
        if isinstance(size, bool) or not isinstance(size, int) \
                or not 0 <= size <= MAX_BLOB_SIZE:
            raise ValueError(
                "size for %r must be an int between 0 and 1 MiB" % name)
        if digest in sizes and sizes[digest] != size:
            raise ValueError(
                "digest %s is listed with inconsistent sizes" % digest)
        sizes[digest] = size
    return sizes


def _validate_needed(needed, known):
    """Validate the needed-digest list; return the given list."""
    if not isinstance(needed, list):
        raise ValueError("needed must be a list of manifest digests")
    if len(set(needed)) != len(needed):
        raise ValueError("needed digests must be unique")
    for digest in needed:
        if not isinstance(digest, str) or _DIGEST_RE.match(digest) is None \
                or digest not in known:
            raise ValueError(
                "every needed digest must occur in the manifest")
    return needed


def _rmtree(path):
    """Remove a private staging directory."""
    shutil.rmtree(path, ignore_errors=True)


class ObjectStore:
    def __init__(self, path):
        self.path = path
        with closing(self.connection()) as db:
            # Serialize schema setup too, so two processes opening the same
            # new path do not race on CREATE TABLE.
            db.execute("BEGIN IMMEDIATE")
            try:
                self._create_schema(db)
            except BaseException:
                db.execute("ROLLBACK")
                raise
            db.execute("COMMIT")

    @staticmethod
    def _create_schema(db):
        for statement in """
            CREATE TABLE IF NOT EXISTS blob (
                digest TEXT PRIMARY KEY, data BLOB NOT NULL);
            CREATE TABLE IF NOT EXISTS tag (
                name TEXT PRIMARY KEY,
                digest TEXT NOT NULL REFERENCES blob(digest));
            CREATE TABLE IF NOT EXISTS digest_use (
                digest TEXT PRIMARY KEY
                    REFERENCES blob(digest) ON DELETE CASCADE,
                seq INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS build (
                handle TEXT PRIMARY KEY,
                started INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS build_pin (
                handle TEXT NOT NULL
                    REFERENCES build(handle) ON DELETE CASCADE,
                name TEXT NOT NULL,
                digest TEXT NOT NULL REFERENCES blob(digest),
                PRIMARY KEY (handle, name));
            CREATE TABLE IF NOT EXISTS store_meta (
                key TEXT PRIMARY KEY,
                value INTEGER NOT NULL);
            CREATE INDEX IF NOT EXISTS tag_digest ON tag(digest);
            CREATE INDEX IF NOT EXISTS build_pin_digest
                ON build_pin(digest);
        """.split(";"):
            if statement.strip():
                db.execute(statement)

    def connection(self):
        db = sqlite3.connect(self.path, timeout=60.0, isolation_level=None)
        db.execute("PRAGMA foreign_keys=ON")
        db.execute("PRAGMA busy_timeout=60000")
        return db

    @contextmanager
    def _serialized(self):
        """Run one public operation as an exclusive SQLite transaction."""
        db = self.connection()
        try:
            db.execute("BEGIN IMMEDIATE")
            try:
                yield db
            except BaseException:
                try:
                    db.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
                raise
            else:
                db.execute("COMMIT")
        finally:
            db.close()

    # -- metadata helpers -------------------------------------------------

    @staticmethod
    def _meta_get(db, key):
        row = db.execute("SELECT value FROM store_meta WHERE key=?",
                         (key,)).fetchone()
        return None if row is None else row[0]

    @staticmethod
    def _next_counter(db, key):
        value = ObjectStore._meta_get(db, key)
        value = 1 if value is None else int(value) + 1
        db.execute("""INSERT INTO store_meta(key, value) VALUES (?,?)
                      ON CONFLICT(key) DO UPDATE SET value=excluded.value""",
                   (key, value))
        return value

    @staticmethod
    def _capacity(db):
        value = ObjectStore._meta_get(db, "capacity")
        return None if value is None else int(value)

    @staticmethod
    def _total_bytes(db):
        return db.execute(
            "SELECT COALESCE(SUM(LENGTH(data)), 0) FROM blob").fetchone()[0]

    @staticmethod
    def _touch(db, digest):
        seq = ObjectStore._next_counter(db, "next_use")
        db.execute("""INSERT INTO digest_use(digest, seq) VALUES (?,?)
                      ON CONFLICT(digest) DO UPDATE SET seq=excluded.seq""",
                   (digest, seq))

    # -- admission --------------------------------------------------------

    @staticmethod
    def _evict_to(db, limit, protected_digest=None, protected_digests=()):
        """Evict evictable blobs until the resident bytes fit *limit*.

        Callers run this inside their transaction.  If enough bytes cannot be
        freed, raise CapacityExceeded and let the caller roll everything back.
        """
        protected = set(protected_digests)
        if protected_digest is not None:
            protected.add(protected_digest)
        total = ObjectStore._total_bytes(db)
        if total <= limit:
            return

        # First: objects with no tags at all, digest ascending.
        if not protected:
            untagged = db.execute("""
                SELECT b.digest, LENGTH(b.data) FROM blob b
                WHERE NOT EXISTS (SELECT 1 FROM tag t WHERE t.digest=b.digest)
                  AND NOT EXISTS (SELECT 1 FROM build_pin p
                                  WHERE p.digest=b.digest)
                ORDER BY b.digest ASC""").fetchall()
        else:
            placeholders = ",".join("?" for _ in protected)
            untagged = db.execute("""
                SELECT b.digest, LENGTH(b.data) FROM blob b
                WHERE NOT EXISTS (SELECT 1 FROM tag t WHERE t.digest=b.digest)
                  AND NOT EXISTS (SELECT 1 FROM build_pin p
                                  WHERE p.digest=b.digest)
                  AND b.digest NOT IN (%s)
                ORDER BY b.digest ASC""" % placeholders,
                tuple(protected)).fetchall()
        for digest, size in untagged:
            if total <= limit:
                return
            db.execute("DELETE FROM blob WHERE digest=?", (digest,))
            total -= size

        if total <= limit:
            return

        # Then: objects that still have tags.  Least recently used first;
        # an untagged-era object with no use record is oldest, ties by digest.
        if not protected:
            tagged = db.execute("""
                SELECT b.digest, LENGTH(b.data) FROM blob b
                LEFT JOIN digest_use u ON u.digest=b.digest
                WHERE EXISTS (SELECT 1 FROM tag t WHERE t.digest=b.digest)
                  AND NOT EXISTS (SELECT 1 FROM build_pin p
                                  WHERE p.digest=b.digest)
                ORDER BY COALESCE(u.seq, 0) ASC, b.digest ASC""").fetchall()
        else:
            placeholders = ",".join("?" for _ in protected)
            tagged = db.execute("""
                SELECT b.digest, LENGTH(b.data) FROM blob b
                LEFT JOIN digest_use u ON u.digest=b.digest
                WHERE EXISTS (SELECT 1 FROM tag t WHERE t.digest=b.digest)
                  AND NOT EXISTS (SELECT 1 FROM build_pin p
                                  WHERE p.digest=b.digest)
                  AND b.digest NOT IN (%s)
                ORDER BY COALESCE(u.seq, 0) ASC, b.digest ASC"""
                % placeholders, tuple(protected)).fetchall()
        for digest, size in tagged:
            if total <= limit:
                return
            # Evicting a tagged object removes every alias for it at once.
            db.execute("DELETE FROM tag WHERE digest=?", (digest,))
            db.execute("DELETE FROM blob WHERE digest=?", (digest,))
            total -= size

        if total > limit:
            raise CapacityExceeded(
                "not enough evictable bytes to meet capacity")

    def configure_capacity(self, max_bytes):
        if (isinstance(max_bytes, bool)
                or not isinstance(max_bytes, int)
                or max_bytes < 0 or max_bytes > MAX_CAPACITY):
            raise ValueError("capacity must be an integer between 0 and 1 GiB")
        with self._serialized() as db:
            db.execute("""INSERT INTO store_meta(key, value) VALUES ('capacity', ?)
                          ON CONFLICT(key) DO UPDATE SET value=excluded.value""",
                       (max_bytes,))
            self._evict_to(db, max_bytes)
        return None

    # -- ordinary tag API -------------------------------------------------

    def publish(self, name, data):
        if not _valid_tag(name) or not isinstance(data, bytes):
            raise ValueError("a non-empty tag and bytes are required")
        digest = hashlib.sha256(data).hexdigest()
        with self._serialized() as db:
            db.execute("INSERT OR IGNORE INTO blob VALUES (?,?)",
                       (digest, data))
            stored, = db.execute("SELECT data FROM blob WHERE digest=?",
                                 (digest,)).fetchone()
            if stored != data:
                raise CorruptBlob(digest)
            db.execute("""INSERT INTO tag VALUES (?,?)
                          ON CONFLICT(name) DO UPDATE SET digest=excluded.digest""",
                       (name, digest))
            limit = self._capacity(db)
            if limit is not None and self._total_bytes(db) > limit:
                # Admission is part of this same transaction: the resulting
                # tag ownership and resident bytes decide what may go.
                self._evict_to(db, limit, protected_digest=digest)
            self._touch(db, digest)
        return digest

    def fetch(self, name):
        with self._serialized() as db:
            row = db.execute("""SELECT b.digest,b.data FROM tag t
                              JOIN blob b ON b.digest=t.digest
                              WHERE t.name=?""", (name,)).fetchone()
            if row is None:
                raise KeyError(name)
            digest, data = row
            if hashlib.sha256(data).hexdigest() != digest:
                raise CorruptBlob(digest)
            self._touch(db, digest)
            return data

    def remove(self, name):
        with self._serialized() as db:
            return db.execute("DELETE FROM tag WHERE name=?",
                              (name,)).rowcount == 1

    def collect(self):
        with self._serialized() as db:
            return db.execute("""
                DELETE FROM blob WHERE NOT EXISTS
                    (SELECT 1 FROM tag WHERE tag.digest=blob.digest)
                AND NOT EXISTS
                    (SELECT 1 FROM build_pin WHERE build_pin.digest=blob.digest)
                """).rowcount

    # -- build checkout ---------------------------------------------------

    def begin_build(self, names):
        if not isinstance(names, (list, tuple)):
            raise ValueError("names must be a list or tuple of tags")
        if not 1 <= len(names) <= MAX_BUILD_TAGS:
            raise ValueError("a build must name between 1 and 24 tags")
        seen = set()
        for name in names:
            if not _valid_tag(name) or name in seen:
                raise ValueError("tags must be distinct non-empty strings")
            seen.add(name)

        with self._serialized() as db:
            active, = db.execute("SELECT COUNT(*) FROM build").fetchone()
            if active >= MAX_ACTIVE_BUILDS:
                raise ValueError("too many active build checkouts")
            placeholders = ",".join("?" for _ in names)
            rows = db.execute(
                "SELECT name, digest FROM tag WHERE name IN (%s)"
                % placeholders, tuple(names)).fetchall()
            by_name = dict(rows)
            for name in names:
                if name not in by_name:
                    raise KeyError(name)
            bindings = {name: by_name[name] for name in names}

            seq = self._next_counter(db, "next_handle")
            # The counter guarantees that an issued handle is never issued
            # again; the random part makes it opaque.
            handle = "%x-%s" % (seq, secrets.token_hex(16))
            db.execute("INSERT INTO build(handle, started) VALUES (?,?)",
                       (handle, seq))
            db.executemany(
                "INSERT INTO build_pin(handle, name, digest) VALUES (?,?,?)",
                [(handle, name, bindings[name]) for name in names])
        return handle, bindings

    def fetch_for_build(self, handle, name):
        if not isinstance(handle, str) or not isinstance(name, str):
            raise KeyError(handle if not isinstance(handle, str) else name)
        with self._serialized() as db:
            row = db.execute("""SELECT digest FROM build_pin
                                WHERE handle=? AND name=?""",
                             (handle, name)).fetchone()
            if row is None:
                active = db.execute("SELECT 1 FROM build WHERE handle=?",
                                    (handle,)).fetchone()
                raise KeyError(name if active is not None else handle)
            digest, = row
            data_row = db.execute("SELECT data FROM blob WHERE digest=?",
                                  (digest,)).fetchone()
            if data_row is None:  # protected blobs must always be present
                raise CorruptBlob(digest)
            data, = data_row
            if hashlib.sha256(data).hexdigest() != digest:
                raise CorruptBlob(digest)
            return data

    def finish_build(self, handle):
        if not isinstance(handle, str):
            return False
        with self._serialized() as db:
            return db.execute("DELETE FROM build WHERE handle=?",
                              (handle,)).rowcount == 1

    # -- build transfer ---------------------------------------------------

    def describe_build(self, handle):
        """Return the manifest pinned by *handle*.

        The manifest is an ordinary dict ``{tag: (digest, size)}`` taken from
        the checkout's own bindings; current tag bindings are ignored.  No LRU
        or retention relationship changes.
        """
        if not isinstance(handle, str):
            raise KeyError(handle)
        with self._serialized() as db:
            if db.execute("SELECT 1 FROM build WHERE handle=?",
                          (handle,)).fetchone() is None:
                raise KeyError(handle)
            rows = db.execute("""SELECT p.name, p.digest, LENGTH(b.data)
                                 FROM build_pin p
                                 JOIN blob b ON b.digest=p.digest
                                 WHERE p.handle=?""", (handle,)).fetchall()
            manifest = {}
            for name, digest, size in rows:
                if size is None:
                    raise CorruptBlob(digest)
                manifest[name] = (digest, int(size))
            return manifest

    def transfer_needs(self, manifest):
        """Return the sorted unique digests the local view cannot reuse."""
        validated = _validate_manifest(manifest)
        with self._serialized() as db:
            missing = []
            for digest, size in validated.items():
                row = db.execute("SELECT data FROM blob WHERE digest=?",
                                 (digest,)).fetchone()
                if row is None or len(row[0]) != size \
                        or hashlib.sha256(row[0]).hexdigest() != digest:
                    missing.append(digest)
            return sorted(set(missing))

    def send_build(self, handle, manifest, needed, sink):
        """Stream the needed blobs of one pinned build to *sink*.

        A single short exclusive transaction captures the source view and
        stages the selected blobs in local temporary files; after it commits,
        the sink is fed from those files without holding any database lock, so
        concurrent release/rebind/reclaim work can commit while a slow sink
        blocks.  No bytes leave this function before every argument check and
        blob verification has passed.
        """
        if not isinstance(handle, str):
            raise KeyError(handle)
        validated = _validate_manifest(manifest)
        needed = _validate_needed(needed, set(validated))
        size_of = {digest: size for digest, size in validated.items()}
        workdir = tempfile.mkdtemp(prefix="build-send-")
        staged = []
        try:
            with self._serialized() as db:
                if db.execute("SELECT 1 FROM build WHERE handle=?",
                              (handle,)).fetchone() is None:
                    raise KeyError(handle)
                rows = db.execute("""SELECT name, digest FROM build_pin
                                     WHERE handle=?""", (handle,)).fetchall()
                if sorted(name for name, _ in rows) != sorted(manifest):
                    raise ValueError(
                        "manifest tags do not match the source checkout")
                pinned = {name: digest for name, digest in rows}
                for name, (digest, size) in manifest.items():
                    if pinned[name] != digest:
                        raise ValueError(
                            "manifest digest for %r does not match the "
                            "source checkout" % name)
                for digest in needed:
                    if size_of[digest] == 0:
                        # Verify the empty blob without staging a file.
                        row = db.execute(
                            "SELECT data FROM blob WHERE digest=?",
                            (digest,)).fetchone()
                        if row is None or row[0] != b"":
                            raise CorruptBlob(digest)
                        staged.append((digest, 0, None))
                        continue
                    row = db.execute("SELECT data FROM blob WHERE digest=?",
                                     (digest,)).fetchone()
                    if row is None:
                        raise CorruptBlob(digest)
                    data = row[0]
                    if len(data) != size_of[digest] \
                            or hashlib.sha256(data).hexdigest() != digest:
                        raise CorruptBlob(digest)
                    path = os.path.join(workdir, digest)
                    with open(path, "wb") as stage:
                        for offset in range(0, len(data), TRANSFER_CHUNK):
                            stage.write(data[offset:offset + TRANSFER_CHUNK])
                    staged.append((digest, len(data), path))
            # The exclusive transaction has committed and closed; nothing
            # below touches the database.
            for digest, size, path in staged:
                if size == 0:
                    continue
                with open(path, "rb") as stage:
                    hasher = hashlib.sha256()
                    written = 0
                    while True:
                        chunk = stage.read(TRANSFER_CHUNK)
                        if not chunk:
                            break
                        hasher.update(chunk)
                        written += len(chunk)
                        sink.write(chunk)
                    if written != size or hasher.hexdigest() != digest:
                        # Staged content is owned by this process; a mismatch
                        # means local failure, not a caller problem.
                        raise CorruptBlob(digest)
        finally:
            _rmtree(workdir)
        return None

    def install_build(self, manifest, needed, source):
        """Install one build checkout from streamed bytes and local blobs.

        The wire is read and staged in temporary files first, without any
        database write or long-lived lock, so other instances keep working
        while *source* blocks.  All admission, reuse, repair, eviction and
        binding work happens in one final exclusive transaction; any failure
        rolls the destination store back to its previous state.
        """
        validated = _validate_manifest(manifest)
        size_of = {digest: size for digest, size in validated.items()}
        needed = _validate_needed(needed, set(validated))
        workdir = tempfile.mkdtemp(prefix="build-install-")
        staged = {}
        try:
            # ---- phase 1: consume and verify the wire, no DB writes ------
            for digest in needed:
                expected = size_of[digest]
                path = os.path.join(workdir, digest)
                hasher = hashlib.sha256()
                remaining = expected
                with open(path, "wb") as stage:
                    while remaining:
                        ask = min(TRANSFER_CHUNK, remaining)
                        chunk = source.read(ask)
                        if not isinstance(chunk, bytes) or not chunk:
                            raise ValueError(
                                "short read for blob %s: %d of %d bytes"
                                % (digest, expected - remaining, expected))
                        if len(chunk) > ask:
                            raise ValueError(
                                "stream returned more bytes than requested")
                        remaining -= len(chunk)
                        hasher.update(chunk)
                        stage.write(chunk)
                if hasher.hexdigest() != digest:
                    raise CorruptBlob(digest)
                staged[digest] = path
            tail = source.read(1)
            if tail:
                raise ValueError("trailing bytes after the declared blobs")

            # ---- phase 2: one atomic admission transaction ---------------
            with self._serialized() as db:
                active, = db.execute("SELECT COUNT(*) FROM build").fetchone()
                # Untransmitted blobs must be good in this final view.
                unavailable = []
                for digest, expected in validated.items():
                    if digest in staged:
                        continue
                    row = db.execute("SELECT data FROM blob WHERE digest=?",
                                     (digest,)).fetchone()
                    if row is None or len(row[0]) != expected \
                            or hashlib.sha256(row[0]).hexdigest() != digest:
                        unavailable.append(digest)
                if unavailable:
                    raise TransferBaseUnavailable(unavailable)
                if active >= MAX_ACTIVE_BUILDS:
                    raise ValueError("too many active build checkouts")

                # Admit streamed blobs; the same verified bytes repair a bad
                # local copy of the same digest.  Nothing here touches the
                # ordinary LRU table.
                for digest, path in staged.items():
                    with open(path, "rb") as stage:
                        data = stage.read()
                    if len(data) != size_of[digest] \
                            or hashlib.sha256(data).hexdigest() != digest:
                        raise CorruptBlob(digest)
                    if db.execute("SELECT 1 FROM blob WHERE digest=?",
                                  (digest,)).fetchone() is None:
                        db.execute("INSERT INTO blob VALUES (?,?)",
                                   (digest, data))
                    else:
                        db.execute(
                            "UPDATE blob SET data=? WHERE digest=?",
                            (data, digest))

                limit = self._capacity(db)
                if limit is not None and self._total_bytes(db) > limit:
                    self._evict_to(
                        db, limit, protected_digests=set(validated))

                seq = self._next_counter(db, "next_handle")
                handle = "%x-%s" % (seq, secrets.token_hex(16))
                db.execute("INSERT INTO build(handle, started) VALUES (?,?)",
                           (handle, seq))
                db.executemany(
                    "INSERT INTO build_pin(handle, name, digest) "
                    "VALUES (?,?,?)",
                    [(handle, name, pin_digest)
                     for name, (pin_digest, _pin_size) in manifest.items()])

                bindings = {name: digest for name, (digest, _)
                            in manifest.items()}
                return handle, bindings
        finally:
            _rmtree(workdir)
