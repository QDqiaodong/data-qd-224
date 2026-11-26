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
MAX_WORKFLOW_ACTIONS = 64
MAX_WORKFLOW_TARGETS = 24
MAX_WORKFLOW_CANDIDATES = 18
MAX_WORKFLOW_BUDGET = 24 << 20
MAX_OUTPUT_PRODUCERS = 256
MAX_WAIT_CLAUSES = 16
MAX_CLAUSE_KEYS = 16
MAX_REBUILD_CASES = 256
MAX_REBUILD_NODES = 32
MAX_REBUILD_UPDATES = 128
TRANSFER_CHUNK = 1 << 16
SHARED_BLOCK_SIZE = 1 << 16
SHARED_CAPACITY_KEY = "shared_payload_capacity"
_DIGEST_RE = re.compile(r"[0-9a-f]{64}\Z")
_ACTION_ID_RE = re.compile(r"[A-Za-z0-9_-]{1,48}\Z")
_RECIPE_KEY_RE = _ACTION_ID_RE


class CorruptBlob(Exception):
    pass


class CapacityExceeded(Exception):
    pass


class PhysicalCapacityExceeded(CapacityExceeded):
    """The unique shared-block payload would exceed its configured limit."""


class StaleOutputClaim(Exception):
    """An output-claim token has already completed, been cancelled or lost
    deadlock resolution."""


class OutputWaiting(Exception):
    """A completion was refused because a declared wait has no coordinated
    output yet."""


class UnverifiedArtifact(Exception):
    """A checked read has no unique reproduction-verified result to deliver.

    ``reason`` is ``"pending"`` when no digest in the case qualifies,
    ``"contested"`` when several digests qualify, and ``"different_digest"``
    when the single verified digest is not the one the tag is bound to.
    """

    def __init__(self, reason, case_id=None, digest=None):
        self.reason = reason
        self.case_id = case_id
        self.digest = digest
        super().__init__(
            "reproduction check %r%s%s"
            % (reason,
               "" if case_id is None else " for case %r" % (case_id,),
               "" if digest is None else " (%s)" % digest))


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


def _valid_recipe_key(key):
    return isinstance(key, str) and _RECIPE_KEY_RE.match(key) is not None


def _validate_wait_groups(groups):
    """Validate and normalize the AND-of-OR wait declaration.

    The outer sequence contains at most 16 clauses.  Each clause contains
    1-16 distinct recipe keys; candidates in one clause are alternatives.
    The same key may occur in several clauses and self-dependencies are
    allowed.
    """
    if not isinstance(groups, (list, tuple)):
        raise ValueError("wait groups must be a list or tuple")
    if len(groups) > MAX_WAIT_CLAUSES:
        raise ValueError(
            "at most %d wait clauses are allowed" % MAX_WAIT_CLAUSES)

    normalized = []
    for clause in groups:
        if not isinstance(clause, (list, tuple)):
            raise ValueError("each wait clause must be a list or tuple")
        if not 1 <= len(clause) <= MAX_CLAUSE_KEYS:
            raise ValueError(
                "each wait clause must contain 1 to %d recipe keys"
                % MAX_CLAUSE_KEYS)
        keys = []
        seen = set()
        for key in clause:
            if not _valid_recipe_key(key):
                raise ValueError(
                    "recipe keys must be 1-48 ASCII letters, digits, '_' or '-'")
            if key in seen:
                raise ValueError("recipe keys in one wait clause must be distinct")
            seen.add(key)
            keys.append(key)
        normalized.append(tuple(keys))
    return tuple(normalized)


def _validate_workflow(actions, targets, byte_budget):
    """Validate a workflow declaration and return its indexed graph.

    Validation covers every declared action, including actions unreachable
    from *targets*.  The graph is also checked for dependency cycles here,
    before any database checkout is created.
    """
    if isinstance(byte_budget, bool) \
            or not isinstance(byte_budget, int) \
            or not 0 <= byte_budget <= MAX_WORKFLOW_BUDGET:
        raise ValueError(
            "byte_budget must be an integer between 0 and 24 MiB")

    if not isinstance(actions, dict) \
            or not 1 <= len(actions) <= MAX_WORKFLOW_ACTIONS:
        raise ValueError(
            "actions must be a dict of 1 to %d actions"
            % MAX_WORKFLOW_ACTIONS)

    for action_id in actions:
        if not isinstance(action_id, str) \
                or _ACTION_ID_RE.match(action_id) is None:
            raise ValueError(
                "action IDs must be 1-48 ASCII letters, digits, '_' or '-'")

    ids = sorted(actions)
    index = {action_id: i for i, action_id in enumerate(ids)}
    works = []
    dep_masks = []
    candidates = {}
    candidate_count = 0

    for action_id in ids:
        declaration = actions[action_id]
        if not isinstance(declaration, tuple) or len(declaration) != 4:
            raise ValueError(
                "action %r must be (work, deps, tag, digest)" % action_id)
        work, deps, tag, digest = declaration

        if isinstance(work, bool) or not isinstance(work, int) \
                or not 1 <= work <= 1_000_000:
            raise ValueError(
                "work for %r must be an integer from 1 to 1,000,000"
                % action_id)
        works.append(work)

        if not isinstance(deps, tuple):
            raise ValueError("deps for %r must be a tuple" % action_id)
        dep_mask = 0
        seen_deps = set()
        for dep in deps:
            if not isinstance(dep, str) \
                    or _ACTION_ID_RE.match(dep) is None \
                    or dep not in index:
                raise ValueError(
                    "dependency %r of %r must be a declared action ID"
                    % (dep, action_id))
            if dep in seen_deps:
                raise ValueError(
                    "dependencies of %r must be distinct" % action_id)
            seen_deps.add(dep)
            dep_mask |= 1 << index[dep]
        dep_masks.append(dep_mask)

        has_candidate = tag is not None or digest is not None
        if has_candidate:
            candidate_count += 1
            if candidate_count > MAX_WORKFLOW_CANDIDATES:
                raise ValueError(
                    "at most %d actions may have cache candidates"
                    % MAX_WORKFLOW_CANDIDATES)
            if tag is None or digest is None \
                    or not _valid_tag(tag) \
                    or not isinstance(digest, str) \
                    or _DIGEST_RE.match(digest) is None:
                raise ValueError(
                    "cache candidate for %r must be a valid tag and "
                    "64-character lowercase hex digest" % action_id)
            candidates[index[action_id]] = (tag, digest)

    if not isinstance(targets, list) \
            or not 1 <= len(targets) <= MAX_WORKFLOW_TARGETS:
        raise ValueError(
            "targets must be a list of 1 to %d action IDs"
            % MAX_WORKFLOW_TARGETS)
    target_mask = 0
    seen_targets = set()
    for target in targets:
        if not isinstance(target, str) or target not in index:
            raise ValueError("every target must be a declared action ID")
        if target in seen_targets:
            raise ValueError("targets must be distinct")
        seen_targets.add(target)
        target_mask |= 1 << index[target]

    # Reject cycles anywhere in the declaration, not only in target-reachable
    # nodes.  Iterative-ish DFS with an explicit color array is sufficient at
    # the declared maximum of 64 actions.
    color = [0] * len(ids)

    def visit(node):
        if color[node] == 1:
            raise ValueError("workflow dependencies must not contain a cycle")
        if color[node] == 2:
            return
        color[node] = 1
        deps = dep_masks[node]
        while deps:
            bit = deps & -deps
            deps ^= bit
            visit(bit.bit_length() - 1)
        color[node] = 2

    for node in range(len(ids)):
        visit(node)

    return {
        "ids": ids,
        "index": index,
        "works": works,
        "dep_masks": dep_masks,
        "candidates": candidates,
        "target_mask": target_mask,
        "byte_budget": byte_budget,
    }


def _valid_rebuild_case(case_id):
    return not isinstance(case_id, bool) \
        and isinstance(case_id, int) \
        and 0 <= case_id < MAX_REBUILD_CASES


def _valid_rebuild_node(value):
    return not isinstance(value, bool) \
        and isinstance(value, int) \
        and 0 <= value < MAX_REBUILD_NODES


def _validate_rebuild_updates(updates):
    """Validate one batch of rebuild observations, all-or-nothing.

    Returns a list of ``(runner, image, digest_or_None)`` tuples in the order
    given.  A digest of ``None`` withdraws the current observation for that
    (runner, image) combination.  One batch may not name a combination twice.
    """
    if not isinstance(updates, (list, tuple)):
        raise ValueError("updates must be a list or tuple of observations")
    if not 1 <= len(updates) <= MAX_REBUILD_UPDATES:
        raise ValueError(
            "updates must contain 1 to %d observations" % MAX_REBUILD_UPDATES)

    normalized = []
    seen = set()
    for item in updates:
        if not isinstance(item, (list, tuple)) or len(item) != 3:
            raise ValueError(
                "each update must be (runner, image, digest-or-None)")
        runner, image, digest = item
        if not _valid_rebuild_node(runner):
            raise ValueError(
                "runner must be an integer between 0 and %d"
                % (MAX_REBUILD_NODES - 1))
        if not _valid_rebuild_node(image):
            raise ValueError(
                "image must be an integer between 0 and %d"
                % (MAX_REBUILD_NODES - 1))
        if digest is not None:
            if not isinstance(digest, str) \
                    or _DIGEST_RE.match(digest) is None:
                raise ValueError(
                    "digest must be 64 lowercase hex characters or None")
        combination = (runner, image)
        if combination in seen:
            raise ValueError(
                "one batch may not name a (runner, image) twice")
        seen.add(combination)
        normalized.append((runner, image, digest))
    return normalized


def _has_independent_triple(combinations):
    """True iff three observations are pairwise independent.

    A qualifying trio has three distinct runner groups *and* three distinct
    image lineages, which is exactly a matching of size three in the
    bipartite graph whose edges are the (runner, image) observations.
    """
    if len(combinations) < 3:
        return False
    adjacency = {}
    for runner, image in combinations:
        adjacency.setdefault(runner, set()).add(image)

    # Kuhn's augmenting-path matching, capped once size three is reached.
    match_image = {}  # image -> runner

    def augment(runner, visited):
        for image in adjacency[runner]:
            if image in visited:
                continue
            visited.add(image)
            if image not in match_image or augment(match_image[image], visited):
                match_image[image] = runner
                return True
        return False

    matched = 0
    for runner in sorted(adjacency):
        if augment(runner, set()):
            matched += 1
            if matched >= 3:
                return True
    return False


def _reproducible_triple(observations):
    """Return True iff any digest has three pairwise-independent observations.

    ``observations`` is ``{digest: {(runner, image), ...}}`` for the current
    observations in one case.  Two observations are independent only when
    both their runner groups and their image lineages differ.  Merely seeing
    three distinct numbers on each side of a set is not sufficient; the trio
    must be realizable as an actual set of three observations.
    """
    return any(_has_independent_triple(combinations)
               for combinations in observations.values())


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
            CREATE TABLE IF NOT EXISTS output_claim (
                token TEXT PRIMARY KEY,
                recipe_key TEXT NOT NULL UNIQUE,
                seq INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS output_wait (
                token TEXT NOT NULL
                    REFERENCES output_claim(token) ON DELETE CASCADE,
                clause_idx INTEGER NOT NULL,
                dep_key TEXT NOT NULL,
                PRIMARY KEY (token, clause_idx, dep_key));
            CREATE TABLE IF NOT EXISTS output_commit (
                recipe_key TEXT PRIMARY KEY,
                digest TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS shared_artifact (
                digest TEXT PRIMARY KEY
                    REFERENCES blob(digest) ON DELETE CASCADE,
                size INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS shared_block (
                block_hash TEXT PRIMARY KEY,
                data BLOB NOT NULL);
            CREATE TABLE IF NOT EXISTS shared_block_map (
                digest TEXT NOT NULL
                    REFERENCES blob(digest) ON DELETE CASCADE,
                block_idx INTEGER NOT NULL,
                block_hash TEXT NOT NULL
                    REFERENCES shared_block(block_hash),
                PRIMARY KEY (digest, block_idx));
            CREATE TABLE IF NOT EXISTS rebuild_observation (
                case_id INTEGER NOT NULL,
                runner INTEGER NOT NULL,
                image INTEGER NOT NULL,
                digest TEXT NOT NULL,
                PRIMARY KEY (case_id, runner, image));
            CREATE INDEX IF NOT EXISTS tag_digest ON tag(digest);
            CREATE INDEX IF NOT EXISTS build_pin_digest
                ON build_pin(digest);
            CREATE INDEX IF NOT EXISTS output_wait_dep
                ON output_wait(dep_key);
            CREATE INDEX IF NOT EXISTS output_commit_digest
                ON output_commit(digest);
            CREATE INDEX IF NOT EXISTS shared_block_map_hash
                ON shared_block_map(block_hash);
        """.split(";"):
            if statement.strip():
                db.execute(statement)
        # A coordinated completion is valid only while its ordinary label
        # binding remains.  Every label deletion path therefore invalidates the
        # marker in the same transaction.  Ordinary rebinding uses an upsert
        # and is handled explicitly by publish().
        db.execute("""
            CREATE TRIGGER IF NOT EXISTS output_tag_deleted
            AFTER DELETE ON tag
            BEGIN
                DELETE FROM output_commit WHERE recipe_key = OLD.name;
            END
        """)

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
    def _shared_capacity(db):
        value = ObjectStore._meta_get(db, SHARED_CAPACITY_KEY)
        return None if value is None else int(value)

    @staticmethod
    def _shared_mode(db):
        return ObjectStore._shared_capacity(db) is not None

    @staticmethod
    def _total_bytes(db):
        if not ObjectStore._shared_mode(db):
            return db.execute(
                "SELECT COALESCE(SUM(LENGTH(data)), 0) FROM blob"
            ).fetchone()[0]
        # Shared mode keeps blob.data empty.  Length is artifact metadata and
        # counts every distinct whole digest, including unreclaimed ones.  The
        # LEFT JOIN fallback only covers an externally inserted legacy row;
        # rows created by this implementation always have metadata.
        return db.execute("""
            SELECT COALESCE(SUM(COALESCE(a.size, LENGTH(b.data))), 0)
            FROM blob b
            LEFT JOIN shared_artifact a ON a.digest = b.digest
        """).fetchone()[0]

    @staticmethod
    def _stored_block_bytes(db):
        return db.execute(
            "SELECT COALESCE(SUM(LENGTH(data)), 0) FROM shared_block"
        ).fetchone()[0]

    @staticmethod
    def _ensure_shared_capacity(db):
        limit = ObjectStore._shared_capacity(db)
        if limit is None:
            return
        stored = int(ObjectStore._stored_block_bytes(db))
        if stored > limit:
            raise PhysicalCapacityExceeded(
                "unique shared blocks use %d bytes; limit is %d"
                % (stored, limit))

    @staticmethod
    def _blob_size(db, digest):
        if ObjectStore._shared_mode(db):
            row = db.execute("""
                SELECT COALESCE(a.size, LENGTH(b.data))
                FROM blob b
                LEFT JOIN shared_artifact a ON a.digest=b.digest
                WHERE b.digest=?
            """, (digest,)).fetchone()
            if row is None:
                raise CorruptBlob(digest)
            return int(row[0])
        row = db.execute("SELECT LENGTH(data) FROM blob WHERE digest=?",
                         (digest,)).fetchone()
        if row is None:
            raise CorruptBlob(digest)
        return int(row[0])

    @staticmethod
    def _read_blob(db, digest):
        """Return a whole artifact, or None when its blob row is absent.

        In shared mode the whole byte string is assembled from ordered blocks.
        Missing/corrupt blocks simply produce bytes that fail the caller's
        whole-digest verification; this helper does not repair them.
        """
        row = db.execute("SELECT data FROM blob WHERE digest=?",
                         (digest,)).fetchone()
        if row is None:
            return None
        if not ObjectStore._shared_mode(db):
            return row[0]
        if row[0]:
            # Supported shared rows use an empty blob.data.  A non-empty value
            # here is direct whole-column damage/legacy data; expose it so the
            # caller's whole-digest check fails rather than silently ignoring
            # the damaged column.
            return row[0]
        block_rows = db.execute("""
            SELECT b.data
            FROM shared_block_map m
            JOIN shared_block b ON b.block_hash = m.block_hash
            WHERE m.digest=?
            ORDER BY m.block_idx ASC
        """, (digest,)).fetchall()
        return b"".join(block_row[0] for block_row in block_rows)

    @staticmethod
    def _write_shared_blocks(db, digest, data, replace):
        db.execute("INSERT OR IGNORE INTO blob(digest, data) VALUES (?, X'')",
                   (digest,))
        # Shared mode never retains a second whole-artifact copy in blob.data.
        db.execute("UPDATE blob SET data=X'' WHERE digest=?", (digest,))
        db.execute("""
            INSERT INTO shared_artifact(digest, size) VALUES (?,?)
            ON CONFLICT(digest) DO UPDATE SET size=excluded.size
        """, (digest, len(data)))
        if replace:
            db.execute("DELETE FROM shared_block_map WHERE digest=?",
                       (digest,))

        for block_idx, start in enumerate(
                range(0, len(data), SHARED_BLOCK_SIZE)):
            block = data[start:start + SHARED_BLOCK_SIZE]
            block_hash = hashlib.sha256(block).hexdigest()
            if replace:
                # A fully verified incoming whole artifact is the one allowed
                # repair path.  Upserting also restores a shared block for
                # every other artifact that references its content hash.
                db.execute("""
                    INSERT INTO shared_block(block_hash, data) VALUES (?,?)
                    ON CONFLICT(block_hash)
                    DO UPDATE SET data=excluded.data
                """, (block_hash, block))
            else:
                existing_block = db.execute(
                    "SELECT 1 FROM shared_block WHERE block_hash=?",
                    (block_hash,)).fetchone()
                if existing_block is None:
                    # A map without its payload is pre-existing damage.  An
                    # ordinary write must not silently fill and repair that
                    # block; reject and roll this whole insertion back.
                    orphan_ref = db.execute(
                        "SELECT 1 FROM shared_block_map WHERE block_hash=? "
                        "LIMIT 1", (block_hash,)).fetchone()
                    if orphan_ref is not None:
                        raise CorruptBlob(digest)
                    db.execute(
                        "INSERT INTO shared_block VALUES (?,?)",
                        (block_hash, block))
            db.execute("""
                INSERT INTO shared_block_map(digest, block_idx, block_hash)
                VALUES (?,?,?)
                ON CONFLICT(digest, block_idx)
                DO UPDATE SET block_hash=excluded.block_hash
            """, (digest, block_idx, block_hash))

        if replace:
            ObjectStore._delete_orphan_blocks(db)

    @staticmethod
    def _put_blob_if_absent(db, digest, data):
        """Insert a new content-addressed blob; never repair an existing one."""
        shared = ObjectStore._shared_mode(db)
        exists = db.execute("SELECT 1 FROM blob WHERE digest=?",
                            (digest,)).fetchone() is not None
        if exists:
            return ObjectStore._read_blob(db, digest)
        if not shared:
            db.execute("INSERT INTO blob VALUES (?,?)", (digest, data))
            return data
        ObjectStore._write_shared_blocks(db, digest, data, replace=False)
        # Read back through the shared blocks.  INSERT OR IGNORE must not
        # overwrite a damaged block already used by another whole artifact; the
        # mismatch makes the caller reject and roll back this publication.
        return ObjectStore._read_blob(db, digest)

    @staticmethod
    def _delete_orphan_blocks(db):
        db.execute("""
            DELETE FROM shared_block
            WHERE block_hash NOT IN (
                SELECT block_hash FROM shared_block_map
            )
        """)

    @staticmethod
    def _delete_blob(db, digest):
        """Delete one whole artifact and blocks that lost their last user."""
        if not ObjectStore._shared_mode(db):
            db.execute("DELETE FROM blob WHERE digest=?", (digest,))
            return

        hashes = [row[0] for row in db.execute(
            "SELECT block_hash FROM shared_block_map WHERE digest=?",
            (digest,)).fetchall()]
        db.execute("DELETE FROM shared_block_map WHERE digest=?", (digest,))
        db.execute("DELETE FROM shared_artifact WHERE digest=?", (digest,))
        db.execute("DELETE FROM blob WHERE digest=?", (digest,))
        for block_hash in set(hashes):
            still_used = db.execute(
                "SELECT 1 FROM shared_block_map WHERE block_hash=? LIMIT 1",
                (block_hash,)).fetchone()
            if still_used is None:
                db.execute("DELETE FROM shared_block WHERE block_hash=?",
                           (block_hash,))

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
        shared = ObjectStore._shared_mode(db)
        size_select = ("COALESCE(a.size, LENGTH(b.data))" if shared
                       else "LENGTH(b.data)")
        size_join = (" LEFT JOIN shared_artifact a ON a.digest=b.digest"
                     if shared else "")
        total = ObjectStore._total_bytes(db)
        if total <= limit:
            return

        # First: objects with no tags at all, digest ascending.
        if not protected:
            untagged = db.execute("""
                SELECT b.digest, %s FROM blob b%s
                WHERE NOT EXISTS (SELECT 1 FROM tag t WHERE t.digest=b.digest)
                  AND NOT EXISTS (SELECT 1 FROM build_pin p
                                  WHERE p.digest=b.digest)
                ORDER BY b.digest ASC""" % (size_select, size_join)).fetchall()
        else:
            placeholders = ",".join("?" for _ in protected)
            untagged = db.execute("""
                SELECT b.digest, %s FROM blob b%s
                WHERE NOT EXISTS (SELECT 1 FROM tag t WHERE t.digest=b.digest)
                  AND NOT EXISTS (SELECT 1 FROM build_pin p
                                  WHERE p.digest=b.digest)
                  AND b.digest NOT IN (%s)
                ORDER BY b.digest ASC"""
                % (size_select, size_join, placeholders),
                tuple(protected)).fetchall()
        for digest, size in untagged:
            if total <= limit:
                return
            ObjectStore._delete_blob(db, digest)
            total -= size

        if total <= limit:
            return

        # Then: objects that still have tags.  Least recently used first;
        # an untagged-era object with no use record is oldest, ties by digest.
        if not protected:
            tagged = db.execute("""
                SELECT b.digest, %s FROM blob b%s
                LEFT JOIN digest_use u ON u.digest=b.digest
                WHERE EXISTS (SELECT 1 FROM tag t WHERE t.digest=b.digest)
                  AND NOT EXISTS (SELECT 1 FROM build_pin p
                                  WHERE p.digest=b.digest)
                ORDER BY COALESCE(u.seq, 0) ASC, b.digest ASC"""
                % (size_select, size_join)).fetchall()
        else:
            placeholders = ",".join("?" for _ in protected)
            tagged = db.execute("""
                SELECT b.digest, %s FROM blob b%s
                LEFT JOIN digest_use u ON u.digest=b.digest
                WHERE EXISTS (SELECT 1 FROM tag t WHERE t.digest=b.digest)
                  AND NOT EXISTS (SELECT 1 FROM build_pin p
                                  WHERE p.digest=b.digest)
                  AND b.digest NOT IN (%s)
                ORDER BY COALESCE(u.seq, 0) ASC, b.digest ASC"""
                % (size_select, size_join, placeholders),
                tuple(protected)).fetchall()
        for digest, size in tagged:
            if total <= limit:
                return
            # Evicting a tagged object removes every alias for it at once.
            db.execute("DELETE FROM tag WHERE digest=?", (digest,))
            ObjectStore._delete_blob(db, digest)
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

    def configure_shared_payload(self, max_bytes):
        if (isinstance(max_bytes, bool)
                or not isinstance(max_bytes, int)
                or max_bytes < 0 or max_bytes > MAX_CAPACITY):
            raise ValueError(
                "shared payload limit must be an integer between 0 and 1 GiB")
        with self._serialized() as db:
            current = self._shared_capacity(db)
            if current is None:
                resident, = db.execute("SELECT COUNT(*) FROM blob").fetchone()
                if resident:
                    raise ValueError(
                        "shared payload mode can only be enabled on a store "
                        "with no resident artifacts")
                db.execute(
                    "INSERT INTO store_meta(key, value) VALUES (?,?)",
                    (SHARED_CAPACITY_KEY, max_bytes))
                return None

            # Adjust an existing DB-wide setting.  If the new ceiling is too
            # low, change nothing and delete nothing.
            stored = int(self._stored_block_bytes(db))
            if stored > max_bytes:
                raise PhysicalCapacityExceeded(
                    "stored shared blocks use %d bytes; requested limit is %d"
                    % (stored, max_bytes))
            db.execute("""
                UPDATE store_meta SET value=? WHERE key=?
            """, (max_bytes, SHARED_CAPACITY_KEY))
        return None

    def shared_payload_usage(self):
        with self._serialized() as db:
            logical = int(self._total_bytes(db))
            if self._shared_capacity(db) is None:
                stored = logical
            else:
                stored = int(self._stored_block_bytes(db))
            return logical, stored

    # -- ordinary tag API -------------------------------------------------

    def publish(self, name, data):
        if not _valid_tag(name) or not isinstance(data, bytes):
            raise ValueError("a non-empty tag and bytes are required")
        digest = hashlib.sha256(data).hexdigest()
        with self._serialized() as db:
            stored = self._put_blob_if_absent(db, digest, data)
            if stored != data:
                raise CorruptBlob(digest)
            # Ordinary publication, even of the identical digest, is not a
            # coordinated completion credential.  This and the tag upsert are
            # atomic with the rest of publish().
            db.execute("DELETE FROM output_commit WHERE recipe_key=?",
                       (name,))
            db.execute("""INSERT INTO tag VALUES (?,?)
                          ON CONFLICT(name) DO UPDATE SET digest=excluded.digest""",
                       (name, digest))
            limit = self._capacity(db)
            if limit is not None and self._total_bytes(db) > limit:
                # Admission is part of this same transaction: the resulting
                # tag ownership and resident bytes decide what may go.
                self._evict_to(db, limit, protected_digest=digest)
            self._ensure_shared_capacity(db)
            self._touch(db, digest)
        return digest

    def fetch(self, name):
        with self._serialized() as db:
            row = db.execute("""SELECT t.digest FROM tag t
                                WHERE t.name=?""", (name,)).fetchone()
            if row is None:
                raise KeyError(name)
            digest = row[0]
            data = self._read_blob(db, digest)
            if data is None or hashlib.sha256(data).hexdigest() != digest:
                raise CorruptBlob(digest)
            self._touch(db, digest)
            return data

    def remove(self, name):
        with self._serialized() as db:
            # Also clear a marker left non-current by an out-of-band state;
            # normal tag deletion already fires the invalidation trigger.
            db.execute("DELETE FROM output_commit WHERE recipe_key=?",
                       (name,))
            return db.execute("DELETE FROM tag WHERE name=?",
                              (name,)).rowcount == 1

    def collect(self):
        with self._serialized() as db:
            deletable_sql = """
                SELECT digest FROM blob WHERE NOT EXISTS
                    (SELECT 1 FROM tag WHERE tag.digest=blob.digest)
                AND NOT EXISTS
                    (SELECT 1 FROM build_pin WHERE build_pin.digest=blob.digest)
            """
            if not self._shared_mode(db):
                return db.execute("""
                    DELETE FROM blob WHERE NOT EXISTS
                        (SELECT 1 FROM tag WHERE tag.digest=blob.digest)
                    AND NOT EXISTS
                        (SELECT 1 FROM build_pin
                         WHERE build_pin.digest=blob.digest)
                    """).rowcount

            digests = [row[0] for row in db.execute(deletable_sql)]
            for digest in digests:
                self._delete_blob(db, digest)
            return len(digests)

    # -- rebuild observations and checked reads ---------------------------

    @staticmethod
    def _case_observations(db, case_id):
        """Return ``{digest: {(runner, image), ...}}`` for one case.

        Metadata only: no blob row is read and no artifact byte is hashed.
        """
        observations = {}
        rows = db.execute("""
            SELECT runner, image, digest
            FROM rebuild_observation
            WHERE case_id=?
        """, (case_id,))
        for runner, image, digest in rows:
            observations.setdefault(digest, set()).add((runner, image))
        return observations

    @classmethod
    def _verdict_from_observations(cls, observations):
        """Return ``(status, digest_or_none)`` for a metadata-only view."""
        qualifying = sorted(
            digest for digest, combinations in observations.items()
            if _has_independent_triple(combinations))
        if not qualifying:
            return "pending", None
        if len(qualifying) == 1:
            return "verified", qualifying[0]
        return "contested", None

    def record_rebuilds(self, case_id, updates):
        """Register or withdraw rebuild observations for one case.

        ``updates`` is 1-128 items of ``(runner, image, digest)`` where digest
        is a 64-character lowercase hex SHA-256 or ``None`` to withdraw the
        current observation for that (runner, image) combination.  The whole
        batch is validated before touching the database and then applied in a
        single transaction.  No artifact bytes are required or inspected.
        """
        if not _valid_rebuild_case(case_id):
            raise ValueError(
                "case_id must be an integer between 0 and %d"
                % (MAX_REBUILD_CASES - 1))
        normalized = _validate_rebuild_updates(updates)
        with self._serialized() as db:
            for runner, image, digest in normalized:
                if digest is None:
                    db.execute("""
                        DELETE FROM rebuild_observation
                        WHERE case_id=? AND runner=? AND image=?
                    """, (case_id, runner, image))
                else:
                    db.execute("""
                        INSERT INTO rebuild_observation
                            (case_id, runner, image, digest)
                        VALUES (?,?,?,?)
                        ON CONFLICT(case_id, runner, image)
                        DO UPDATE SET digest=excluded.digest
                    """, (case_id, runner, image, digest))
        return None

    def rebuild_verdict(self, case_id):
        """Return ``(status, digest_or_none)`` for one reproduction case.

        ``status`` is ``"pending"`` when no digest has three pairwise
        independent current observations, ``"verified"`` when exactly one does
        (and its digest is returned), and ``"contested"`` when more than one
        does.  Observation metadata only; no artifact bytes are read.
        """
        if not _valid_rebuild_case(case_id):
            raise ValueError(
                "case_id must be an integer between 0 and %d"
                % (MAX_REBUILD_CASES - 1))
        with self._serialized() as db:
            observations = self._case_observations(db, case_id)
        return self._verdict_from_observations(observations)

    def _reproduced_view(self, case_id, name):
        """Capture observations, the tag binding and that one payload.

        All three are read in a single short read snapshot, so they describe a
        complete database view that existed during this call.  The snapshot is
        released before the caller computes independence or hashes bytes.
        """
        with closing(self.connection()) as db:
            db.execute("BEGIN")
            try:
                observations = self._case_observations(db, case_id)
                row = db.execute(
                    "SELECT digest FROM tag WHERE name=?", (name,)).fetchone()
                bound = None if row is None else row[0]
                data = None if bound is None else self._read_blob(db, bound)
            except BaseException:
                db.execute("ROLLBACK")
                raise
            db.execute("COMMIT")
        return observations, bound, data

    def _check_reproduced_payload(self, digest, data):
        """Verify the captured whole payload; CorruptBlob on absence/mismatch."""
        if data is None or hashlib.sha256(data).hexdigest() != digest:
            raise CorruptBlob(digest)

    def fetch_reproduced(self, case_id, name):
        """Read one tag's current payload only if reproduction verifies it.

        The current binding of *name* is used; no other tag is consulted.
        Raises ``KeyError`` when the tag is absent, ``UnverifiedArtifact`` when
        the case has no unique qualifying digest (reason ``"pending"`` or
        ``"contested"``) or when the unique digest is not the bound one
        (reason ``"different_digest"``), and ``CorruptBlob`` when the verified
        payload is missing or fails its whole SHA-256 check.  The read does not
        create a build handle and does not refresh ordinary LRU order.
        """
        if not _valid_rebuild_case(case_id):
            raise ValueError(
                "case_id must be an integer between 0 and %d"
                % (MAX_REBUILD_CASES - 1))
        if not _valid_tag(name):
            raise ValueError("a non-empty tag is required")

        observations, bound, data = self._reproduced_view(case_id, name)
        if bound is None:
            raise KeyError(name)

        status, digest = self._verdict_from_observations(observations)
        if status == "pending":
            raise UnverifiedArtifact("pending", case_id)
        if status == "contested":
            raise UnverifiedArtifact("contested", case_id)
        if digest != bound:
            raise UnverifiedArtifact("different_digest", case_id, digest)

        self._check_reproduced_payload(digest, data)
        return digest, data

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
            data = self._read_blob(db, digest)
            if data is None:  # protected blobs must always be present
                raise CorruptBlob(digest)
            if hashlib.sha256(data).hexdigest() != digest:
                raise CorruptBlob(digest)
            return data

    def finish_build(self, handle):
        if not isinstance(handle, str):
            return False
        with self._serialized() as db:
            return db.execute("DELETE FROM build WHERE handle=?",
                              (handle,)).rowcount == 1

    # -- single-production coordination -----------------------------------

    @staticmethod
    def _current_output_keys(db):
        """Return recipe keys with a valid commit marker and label binding.

        This is metadata-only: blob bytes are neither loaded nor rehashed for
        wait resolution or completion admission.
        """
        return {row[0] for row in db.execute("""
            SELECT oc.recipe_key
            FROM output_commit oc
            JOIN tag t
              ON t.name = oc.recipe_key
             AND t.digest = oc.digest
        """)}

    @staticmethod
    def _load_output_graph(db):
        producers = {}
        for token, key in db.execute(
                "SELECT token, recipe_key FROM output_claim"):
            producers[key] = token

        clauses_by_key = {key: {} for key in producers}
        rows = db.execute("""
            SELECT c.recipe_key, w.clause_idx, w.dep_key
            FROM output_wait w
            JOIN output_claim c ON c.token = w.token
        """)
        for key, clause_idx, dep_key in rows:
            clauses_by_key[key].setdefault(clause_idx, []).append(dep_key)

        waits = {}
        for key, clauses in clauses_by_key.items():
            waits[key] = tuple(
                tuple(clauses[index]) for index in sorted(clauses))

        outputs = ObjectStore._current_output_keys(db)
        return producers, waits, outputs

    @staticmethod
    def _choose_output_victim(producers, waits, outputs):
        """Choose one deterministic deadlock victim, or return None.

        The viability closure starts from coordinated outputs and every key
        which currently has no producer.  An active producer enters the closure
        only when every AND clause has at least one OR candidate already
        viable.  SCCs are then built only from clauses with no viable
        candidate.
        """
        viable = set(outputs)

        # A key without an active producer can be newly scheduled by the
        # caller, so it is not part of a local lock.
        for clauses in waits.values():
            for clause in clauses:
                for key in clause:
                    if key not in producers:
                        viable.add(key)

        while True:
            changed = False
            for producer in producers:
                if producer in viable:
                    continue
                clauses = waits.get(producer, ())
                if all(any(key in viable for key in clause)
                       for clause in clauses):
                    viable.add(producer)
                    changed = True
            if not changed:
                break

        nonviable = sorted(key for key in producers if key not in viable)
        adjacency = {key: set() for key in nonviable}
        for waiter in nonviable:
            for clause in waits.get(waiter, ()):
                if not any(key in viable for key in clause):
                    # Such a clause cannot contain an unproduced/output key:
                    # either would have made the clause viable.  Targets are
                    # therefore exactly other non-viable active producers.
                    for key in clause:
                        if key in adjacency:
                            adjacency[waiter].add(key)

        # Kosaraju SCC.  There are at most 256 producer vertices.
        seen = set()
        finish_order = []

        def visit(key):
            seen.add(key)
            for nxt in adjacency[key]:
                if nxt not in seen:
                    visit(nxt)
            finish_order.append(key)

        for key in nonviable:
            if key not in seen:
                visit(key)

        reverse = {key: set() for key in nonviable}
        for key in nonviable:
            for nxt in adjacency[key]:
                reverse[nxt].add(key)

        component = {}
        components = []

        def assign(key, component_id):
            component[key] = component_id
            components[component_id].append(key)
            for nxt in reverse[key]:
                if nxt not in component:
                    assign(nxt, component_id)

        for key in reversed(finish_order):
            if key not in component:
                component_id = len(components)
                components.append([])
                assign(key, component_id)

        sink_components = set(range(len(components)))
        for key in nonviable:
            key_component = component[key]
            for nxt in adjacency[key]:
                if component[nxt] != key_component:
                    sink_components.discard(key_component)

        candidates = []
        for component_id in sink_components:
            candidates.extend(components[component_id])
        return min(candidates) if candidates else None

    def _resolve_output_deadlocks(self, db):
        """Repeatedly revoke deterministic sink-SCC victims in one DB view."""
        producers, waits, outputs = self._load_output_graph(db)
        released = []
        while True:
            victim = self._choose_output_victim(producers, waits, outputs)
            if victim is None:
                return tuple(released)
            token = producers[victim]
            db.execute("DELETE FROM output_wait WHERE token=?", (token,))
            db.execute("DELETE FROM output_claim WHERE token=?", (token,))
            released.append(victim)
            producers.pop(victim, None)
            waits.pop(victim, None)

    def claim_output(self, key):
        if not _valid_recipe_key(key):
            raise ValueError(
                "recipe key must be 1-48 ASCII letters, digits, '_' or '-'")

        with self._serialized() as db:
            if db.execute(
                    "SELECT 1 FROM output_claim WHERE recipe_key=?",
                    (key,)).fetchone() is not None:
                return "busy", None

            row = db.execute("""
                SELECT oc.digest
                FROM output_commit oc
                JOIN tag t
                  ON t.name = oc.recipe_key
                 AND t.digest = oc.digest
                WHERE oc.recipe_key = ?
            """, (key,)).fetchone()
            if row is not None:
                digest = row[0]
                data = self._read_blob(db, digest)
                if data is None or \
                        hashlib.sha256(data).hexdigest() != digest:
                    raise CorruptBlob(digest)
                # Coordination reads deliberately do not touch digest_use.
                return "ready", data

            active, = db.execute("SELECT COUNT(*) FROM output_claim").fetchone()
            if active >= MAX_OUTPUT_PRODUCERS:
                raise ValueError("too many active output producers")

            seq = self._next_counter(db, "next_output_token")
            # The persisted counter prevents reissue; random bytes keep the
            # token opaque even to callers who know the recipe and sequence.
            token = "oc-%x-%s" % (seq, secrets.token_hex(16))
            db.execute(
                "INSERT INTO output_claim(token, recipe_key, seq) "
                "VALUES (?,?,?)", (token, key, seq))
            return "claimed", token

    def set_output_waits(self, token, groups):
        if not isinstance(token, str):
            raise ValueError("token must be an output-claim token string")
        normalized = _validate_wait_groups(groups)

        with self._serialized() as db:
            row = db.execute(
                "SELECT recipe_key FROM output_claim WHERE token=?",
                (token,)).fetchone()
            if row is None:
                raise StaleOutputClaim("output claim token is no longer active")

            db.execute("DELETE FROM output_wait WHERE token=?", (token,))
            db.executemany(
                "INSERT INTO output_wait(token, clause_idx, dep_key) "
                "VALUES (?,?,?)",
                [(token, clause_idx, key)
                 for clause_idx, clause in enumerate(normalized)
                 for key in clause])

            # Replacement and all resulting victim deletions commit together.
            return self._resolve_output_deadlocks(db)

    def complete_output(self, token, data):
        if not isinstance(token, str):
            raise ValueError("token must be an output-claim token string")
        if not isinstance(data, bytes) or len(data) > MAX_BLOB_SIZE:
            raise ValueError(
                "data must be bytes of at most %d bytes" % MAX_BLOB_SIZE)

        digest = hashlib.sha256(data).hexdigest()
        with self._serialized() as db:
            row = db.execute(
                "SELECT recipe_key FROM output_claim WHERE token=?",
                (token,)).fetchone()
            if row is None:
                raise StaleOutputClaim("output claim token is no longer active")
            key = row[0]

            clause_rows = db.execute("""
                SELECT clause_idx, dep_key
                FROM output_wait
                WHERE token=?
                ORDER BY clause_idx
            """, (token,)).fetchall()
            clauses = {}
            for clause_idx, dep_key in clause_rows:
                clauses.setdefault(clause_idx, []).append(dep_key)

            current_outputs = self._current_output_keys(db)
            for clause_idx in sorted(clauses):
                if not any(dep_key in current_outputs
                           for dep_key in clauses[clause_idx]):
                    # Rollback leaves the claim and its wait declaration in
                    # place for a later retry with the same token.
                    raise OutputWaiting(
                        "wait clause %d has no coordinated output"
                        % clause_idx)

            # Content-addressed admission, matching ordinary publish().
            stored = self._put_blob_if_absent(db, digest, data)
            if stored != data:
                raise CorruptBlob(digest)

            db.execute("""INSERT INTO tag VALUES (?,?)
                          ON CONFLICT(name) DO UPDATE
                          SET digest=excluded.digest""",
                       (key, digest))

            limit = self._capacity(db)
            if limit is not None and self._total_bytes(db) > limit:
                self._evict_to(db, limit, protected_digest=digest)
            self._ensure_shared_capacity(db)

            self._touch(db, digest)
            db.execute("""
                INSERT INTO output_commit(recipe_key, digest)
                VALUES (?,?)
                ON CONFLICT(recipe_key) DO UPDATE
                SET digest=excluded.digest
            """, (key, digest))
            db.execute("DELETE FROM output_wait WHERE token=?", (token,))
            db.execute("DELETE FROM output_claim WHERE token=?", (token,))
            return digest

    def cancel_output(self, token):
        if not isinstance(token, str):
            raise ValueError("token must be an output-claim token string")
        with self._serialized() as db:
            db.execute("DELETE FROM output_wait WHERE token=?", (token,))
            return db.execute(
                "DELETE FROM output_claim WHERE token=?",
                (token,)).rowcount == 1

    # -- workflow planning -------------------------------------------------

    def _workflow_candidate_view(self, db, spec):
        """Read and verify every declared cache candidate in one DB view.

        The returned signature contains the current tag binding, blob length
        and actual SHA-256 of every candidate.  It therefore changes not only
        when a selected digest disappears, but also when any previously
        unavailable candidate becomes useful.  This method never updates LRU
        state.
        """
        signature = []
        available = {}
        for node in sorted(spec["candidates"]):
            tag, expected_digest = spec["candidates"][node]
            row = db.execute("""
                SELECT t.digest
                FROM tag t
                WHERE t.name = ?
            """, (tag,)).fetchone()

            if row is None:
                signature.append((node, None, None, None))
                continue

            bound_digest = row[0]
            data = self._read_blob(db, bound_digest)
            if data is None:
                actual_digest = None
                size = None
            else:
                size = len(data)
                actual_digest = hashlib.sha256(data).hexdigest()

            signature.append(
                (node, bound_digest, size, actual_digest))

            # The action's own tag must name its confirmed digest.  Reading
            # the same bytes through some other tag does not make this
            # candidate a hit.
            if bound_digest == expected_digest \
                    and data is not None \
                    and size <= MAX_BLOB_SIZE \
                    and actual_digest == expected_digest:
                available[node] = (expected_digest, size)

        return tuple(signature), available

    def _read_workflow_view(self, spec):
        """Take a short read snapshot and release the database lock."""
        with closing(self.connection()) as db:
            db.execute("BEGIN")
            view = self._workflow_candidate_view(db, spec)
            db.execute("COMMIT")
        return view

    @staticmethod
    def _workflow_run_order(spec, run_mask):
        """Topologically order run nodes, breaking ties by action ID."""
        dep_masks = spec["dep_masks"]
        ids = spec["ids"]
        remaining = run_mask
        order = []
        while remaining:
            ready_bit = 0
            candidates = remaining
            while candidates:
                bit = candidates & -candidates
                candidates ^= bit
                node = bit.bit_length() - 1
                if (dep_masks[node] & remaining) == 0:
                    # Bits are visited from low to high, i.e. ascending ID.
                    ready_bit = bit
                    break
            if ready_bit == 0:  # validation already rejected cycles
                raise ValueError(
                    "workflow dependencies must not contain a cycle")
            node = ready_bit.bit_length() - 1
            order.append(ids[node])
            remaining ^= ready_bit
        return order

    def _plan_workflow(self, spec, view):
        """Find the globally optimal cache/run split without using the DB.

        There are at most 18 cache candidates, so enumerate those subsets.
        Bit masks keep graph closure, work and unique-digest accounting
        compact.  A selected cache node prunes its dependency closure; cache
        bits made irrelevant by an upstream cache hit are excluded from the
        actual returned reuse set.
        """
        _, available = view
        ids = spec["ids"]
        works = spec["works"]
        dep_masks = spec["dep_masks"]
        budget = spec["byte_budget"]

        # Candidates outside the targets' full dependency closure can never be
        # accessed; do not spend subset combinations on them.
        reachable = spec["target_mask"]
        pending = reachable
        while pending:
            bit = pending & -pending
            pending ^= bit
            node = bit.bit_length() - 1
            new_deps = dep_masks[node] & ~reachable
            reachable |= new_deps
            pending |= new_deps

        candidate_nodes = [
            node for node in sorted(available)
            if reachable & (1 << node)
        ]
        candidate_count = len(candidate_nodes)
        candidate_masks = [1 << node for node in candidate_nodes]

        best_key = None
        best_run_mask = 0
        best_cached_mask = 0

        for choice in range(1 << candidate_count):
            cache_mask = 0
            bits = choice
            candidate_index = 0
            while bits:
                if bits & 1:
                    cache_mask |= candidate_masks[candidate_index]
                bits >>= 1
                candidate_index += 1

            # Least closure starting at targets.  A cached node is satisfied
            # by the cache and therefore does not expose its dependencies.
            needed = spec["target_mask"]
            pending = needed
            while pending:
                bit = pending & -pending
                pending ^= bit
                if cache_mask & bit:
                    continue
                node = bit.bit_length() - 1
                new_deps = dep_masks[node] & ~needed
                needed |= new_deps
                pending |= new_deps

            cached_mask = needed & cache_mask
            run_mask = needed & ~cache_mask

            byte_count = 0
            seen_digests = set()
            cached_bits = cached_mask
            while cached_bits:
                bit = cached_bits & -cached_bits
                cached_bits ^= bit
                node = bit.bit_length() - 1
                digest, size = available[node]
                if digest not in seen_digests:
                    seen_digests.add(digest)
                    byte_count += size
            if byte_count > budget:
                continue

            work_total = 0
            run_bits = run_mask
            while run_bits:
                bit = run_bits & -run_bits
                run_bits ^= bit
                work_total += works[bit.bit_length() - 1]

            if best_key is not None:
                best_work, best_bytes, _best_ids = best_key
                if work_total > best_work \
                        or (work_total == best_work
                            and byte_count > best_bytes):
                    continue

            cached_ids = []
            cached_bits = cached_mask
            while cached_bits:
                bit = cached_bits & -cached_bits
                cached_bits ^= bit
                cached_ids.append(ids[bit.bit_length() - 1])

            key = (work_total, byte_count, tuple(cached_ids))
            if best_key is None or key < best_key:
                best_key = key
                best_run_mask = run_mask
                best_cached_mask = cached_mask

        return {
            "run_mask": best_run_mask,
            "cached_mask": best_cached_mask,
            "run_order": self._workflow_run_order(spec, best_run_mask),
        }

    def prepare_workflow(self, actions, targets, byte_budget):
        """Plan a target workflow and atomically pin its cached outputs.

        Candidate discovery and graph optimization do not hold a SQLite lock.
        A final ``BEGIN IMMEDIATE`` admission transaction re-reads the complete
        candidate view.  If any candidate changed during planning, the stale
        plan is discarded and replanned outside the lock.  Once the planning
        view equals the admission view, the checkout row and all action-ID
        bindings are inserted in that same transaction.
        """
        spec = _validate_workflow(actions, targets, byte_budget)
        view = self._read_workflow_view(spec)

        while True:
            plan = self._plan_workflow(spec, view)
            stale = False
            result = None

            with self._serialized() as db:
                current_view = self._workflow_candidate_view(db, spec)
                if current_view[0] != view[0]:
                    # Release the admission lock, replan from the new view,
                    # and try admission again.  Do not admit a plan that only
                    # rechecked its already-selected digests.
                    view = current_view
                    stale = True
                else:
                    cached_mask = plan["cached_mask"]
                    run_order = plan["run_order"]
                    if not cached_mask:
                        result = (None, run_order, {})
                    else:
                        active, = db.execute(
                            "SELECT COUNT(*) FROM build").fetchone()
                        if active >= MAX_ACTIVE_BUILDS:
                            raise ValueError(
                                "too many active build checkouts")

                        seq = self._next_counter(db, "next_handle")
                        handle = "%x-%s" % (seq, secrets.token_hex(16))
                        db.execute(
                            "INSERT INTO build(handle, started) VALUES (?,?)",
                            (handle, seq))

                        cached = {}
                        pins = []
                        bits = cached_mask
                        while bits:
                            bit = bits & -bits
                            bits ^= bit
                            node = bit.bit_length() - 1
                            action_id = spec["ids"][node]
                            digest, _size = current_view[1][node]
                            cached[action_id] = digest
                            pins.append((handle, action_id, digest))

                        db.executemany(
                            "INSERT INTO build_pin(handle, name, digest) "
                            "VALUES (?,?,?)", pins)
                        result = (handle, run_order, cached)

            if not stale:
                return result

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
            rows = db.execute("""SELECT p.name, p.digest
                                 FROM build_pin p
                                 WHERE p.handle=?""", (handle,)).fetchall()
            manifest = {}
            for name, digest in rows:
                size = self._blob_size(db, digest)
                manifest[name] = (digest, size)
            return manifest

    def transfer_needs(self, manifest):
        """Return the sorted unique digests the local view cannot reuse."""
        validated = _validate_manifest(manifest)
        with self._serialized() as db:
            missing = []
            for digest, size in validated.items():
                data = self._read_blob(db, digest)
                if data is None or len(data) != size \
                        or hashlib.sha256(data).hexdigest() != digest:
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
                    data = self._read_blob(db, digest)
                    if data is None:
                        raise CorruptBlob(digest)
                    if len(data) != size_of[digest] \
                            or hashlib.sha256(data).hexdigest() != digest:
                        raise CorruptBlob(digest)
                    if size_of[digest] == 0:
                        staged.append((digest, 0, None))
                        continue
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
                    data = self._read_blob(db, digest)
                    if data is None or len(data) != expected \
                            or hashlib.sha256(data).hexdigest() != digest:
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
                    if self._shared_mode(db):
                        self._write_shared_blocks(
                            db, digest, data, replace=True)
                    elif db.execute(
                            "SELECT 1 FROM blob WHERE digest=?",
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
                self._ensure_shared_capacity(db)

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
