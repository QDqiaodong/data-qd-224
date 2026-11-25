"""Transactional content-addressed blob storage for a local build cache."""
from contextlib import closing
import hashlib
import sqlite3


class CorruptBlob(Exception):
    pass


class ObjectStore:
    def __init__(self, path):
        self.path = path
        with closing(self.connection()) as db, db:
            db.executescript("""CREATE TABLE IF NOT EXISTS blob (digest TEXT PRIMARY KEY, data BLOB NOT NULL);
                CREATE TABLE IF NOT EXISTS tag (
                    name TEXT PRIMARY KEY, digest TEXT NOT NULL REFERENCES blob(digest));""")

    def connection(self):
        db = sqlite3.connect(self.path)
        db.execute("PRAGMA foreign_keys=ON")
        return db

    def publish(self, name, data):
        if not isinstance(data, bytes) or not isinstance(name, str) or not name.strip():
            raise ValueError("a non-empty tag and bytes are required")
        digest = hashlib.sha256(data).hexdigest()
        with closing(self.connection()) as db, db:
            db.execute("INSERT OR IGNORE INTO blob VALUES (?,?)", (digest, data))
            stored, = db.execute("SELECT data FROM blob WHERE digest=?", (digest,)).fetchone()
            if stored != data:
                raise CorruptBlob(digest)
            db.execute("""INSERT INTO tag VALUES (?,?)
                          ON CONFLICT(name) DO UPDATE SET digest=excluded.digest""", (name, digest))
        return digest

    def fetch(self, name):
        with closing(self.connection()) as db:
            row = db.execute("""SELECT b.digest,b.data FROM tag t JOIN blob b ON b.digest=t.digest
                              WHERE t.name=?""", (name,)).fetchone()
        if row is None:
            raise KeyError(name)
        digest, data = row
        if hashlib.sha256(data).hexdigest() != digest:
            raise CorruptBlob(digest)
        return data

    def remove(self, name):
        with closing(self.connection()) as db, db:
            return db.execute("DELETE FROM tag WHERE name=?", (name,)).rowcount == 1

    def collect(self):
        with closing(self.connection()) as db, db:
            return db.execute("""DELETE FROM blob WHERE NOT EXISTS
                (SELECT 1 FROM tag WHERE tag.digest=blob.digest)""").rowcount
