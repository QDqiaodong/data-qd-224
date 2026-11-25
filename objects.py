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
"""
from contextlib import closing, contextmanager
import hashlib
import secrets
import sqlite3

MAX_CAPACITY = 1 << 30
MAX_ACTIVE_BUILDS = 64
MAX_BUILD_TAGS = 24


class CorruptBlob(Exception):
    pass


class CapacityExceeded(Exception):
    pass


def _valid_tag(name):
    return isinstance(name, str) and bool(name.strip())


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
    def _evict_to(db, limit, protected_digest=None):
        """Evict evictable blobs until the resident bytes fit *limit*.

        Callers run this inside their transaction.  If enough bytes cannot be
        freed, raise CapacityExceeded and let the caller roll everything back.
        """
        total = ObjectStore._total_bytes(db)
        if total <= limit:
            return

        # First: objects with no tags at all, digest ascending.
        if protected_digest is None:
            untagged = db.execute("""
                SELECT b.digest, LENGTH(b.data) FROM blob b
                WHERE NOT EXISTS (SELECT 1 FROM tag t WHERE t.digest=b.digest)
                  AND NOT EXISTS (SELECT 1 FROM build_pin p
                                  WHERE p.digest=b.digest)
                ORDER BY b.digest ASC""").fetchall()
        else:
            untagged = db.execute("""
                SELECT b.digest, LENGTH(b.data) FROM blob b
                WHERE NOT EXISTS (SELECT 1 FROM tag t WHERE t.digest=b.digest)
                  AND NOT EXISTS (SELECT 1 FROM build_pin p
                                  WHERE p.digest=b.digest)
                  AND b.digest<>?
                ORDER BY b.digest ASC""", (protected_digest,)).fetchall()
        for digest, size in untagged:
            if total <= limit:
                return
            db.execute("DELETE FROM blob WHERE digest=?", (digest,))
            total -= size

        if total <= limit:
            return

        # Then: objects that still have tags.  Least recently used first;
        # an untagged-era object with no use record is oldest, ties by digest.
        if protected_digest is None:
            tagged = db.execute("""
                SELECT b.digest, LENGTH(b.data) FROM blob b
                LEFT JOIN digest_use u ON u.digest=b.digest
                WHERE EXISTS (SELECT 1 FROM tag t WHERE t.digest=b.digest)
                  AND NOT EXISTS (SELECT 1 FROM build_pin p
                                  WHERE p.digest=b.digest)
                ORDER BY COALESCE(u.seq, 0) ASC, b.digest ASC""").fetchall()
        else:
            tagged = db.execute("""
                SELECT b.digest, LENGTH(b.data) FROM blob b
                LEFT JOIN digest_use u ON u.digest=b.digest
                WHERE EXISTS (SELECT 1 FROM tag t WHERE t.digest=b.digest)
                  AND NOT EXISTS (SELECT 1 FROM build_pin p
                                  WHERE p.digest=b.digest)
                  AND b.digest<>?
                ORDER BY COALESCE(u.seq, 0) ASC, b.digest ASC""",
                (protected_digest,)).fetchall()
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
