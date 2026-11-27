"""Disk-backed corpus store with exact-text deduplication and JSONL.zst export."""
from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import shutil
import unicodedata
import zlib
from pathlib import Path


class Store:
    MIN_FREE_BYTES = int(1.5 * 1024**3)
    DEFAULT_CACHE_MIB = 96

    def __init__(self, data_dir: str | os.PathLike[str]):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = self.data_dir / "corpus.sqlite3"
        self.conn = sqlite3.connect(self.db_path, timeout=60)
        cache_mib = max(8, int(os.environ.get("SQLITE_CACHE_MIB", self.DEFAULT_CACHE_MIB)))
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute(f"PRAGMA cache_size=-{cache_mib * 1024}")
        self.conn.execute("PRAGMA temp_store=FILE")
        self.conn.execute("""CREATE TABLE IF NOT EXISTS documents (
            id INTEGER PRIMARY KEY, digest BLOB NOT NULL, text BLOB NOT NULL, url TEXT NOT NULL,
            source TEXT NOT NULL, title TEXT NOT NULL DEFAULT '', date TEXT NOT NULL DEFAULT '',
            upstream_source TEXT NOT NULL DEFAULT '', source_id TEXT NOT NULL DEFAULT '',
            license TEXT NOT NULL DEFAULT ''
        )""")
        # Upgrade stores created by early importer revisions in place.
        cols = {row[1] for row in self.conn.execute("PRAGMA table_info(documents)")}
        for name in ("upstream_source", "source_id", "license"):
            if name not in cols:
                self.conn.execute(f"ALTER TABLE documents ADD COLUMN {name} TEXT NOT NULL DEFAULT ''")
        if "id" in cols:
            self.conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS documents_digest_uq ON documents(digest)")
            self.conn.execute("CREATE INDEX IF NOT EXISTS documents_by_source ON documents(source)")
        self.conn.execute("CREATE TABLE IF NOT EXISTS checkpoints (key TEXT PRIMARY KEY, value TEXT NOT NULL) WITHOUT ROWID")
        self._pending = 0
        self.conn.commit()

    def add(self, text: str, url: str, source: str, title: str = "", date: str = "", *,
            upstream_source: str = "", source_id: str = "", license: str = "", commit: bool = True) -> bool:
        text = unicodedata.normalize("NFC", (text or "")).strip()
        if not text:
            return False
        canonical = " ".join(text.split())
        digest = hashlib.sha256(canonical.encode("utf-8")).digest()
        cur = self.conn.execute(
            "INSERT OR IGNORE INTO documents(digest,text,url,source,title,date,upstream_source,source_id,license) VALUES(?,?,?,?,?,?,?,?,?)",
            (digest, zlib.compress(text.encode("utf-8"), level=3), url or "", source or "", title or "", date or "",
             upstream_source or "", source_id or "", license or ""),
        )
        self._pending += 1
        if commit or self._pending >= 512:
            self.commit()
        return cur.rowcount == 1

    def commit(self) -> None:
        self.ensure_space()
        self.conn.commit()
        self._pending = 0

    def ensure_space(self) -> None:
        free = shutil.disk_usage(self.data_dir).free
        if free < self.MIN_FREE_BYTES:
            raise OSError(f"Low disk space: {free / 1024**3:.2f} GiB free; pausing below 1.5 GiB reserve")

    def get_checkpoint(self, key: str, default=None):
        row = self.conn.execute("SELECT value FROM checkpoints WHERE key=?", (key,)).fetchone()
        return json.loads(row[0]) if row else default

    def set_checkpoint(self, key: str, value) -> None:
        self.conn.execute("INSERT INTO checkpoints(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                          (key, json.dumps(value, ensure_ascii=False)))
        self.commit()

    def export(self, output: str | os.PathLike[str] | None = None) -> Path:
        try:
            import zstandard as zstd
        except ImportError as exc:
            raise RuntimeError("JSONL.zst export requires `pip install zstandard`") from exc
        path = Path(output) if output else self.data_dir / "corpus.jsonl.zst"
        self.commit()
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(path.name + ".tmp")
        with tmp.open("wb") as raw:
            with zstd.ZstdCompressor(level=3).stream_writer(raw) as compressed:
                cursor = self.conn.execute("SELECT text,url,source,title,date,upstream_source,source_id,license FROM documents")
                for packed, url, source, title, date, upstream_source, source_id, license in cursor:
                    text = packed if isinstance(packed, str) else zlib.decompress(packed).decode("utf-8")
                    record = {"text": text, "url": url, "source": source, "title": title, "date": date,
                              "upstream_source": upstream_source, "source_id": source_id, "license": license}
                    compressed.write((json.dumps(record, ensure_ascii=False) + "\n").encode("utf-8"))
        tmp.replace(path)
        return path

    def close(self) -> None:
        try:
            self.commit()
        except OSError:
            self.conn.rollback()
        self.conn.close()

    def __enter__(self) -> "Store":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


def migrate_store(data_dir: str | os.PathLike[str]) -> None:
    """One-time, offline transactional conversion from digest-clustered storage.

    Stops if the current database does not match the expected schema or if a
    second copy plus the configured free-space reserve cannot fit.
    """
    directory = Path(data_dir)
    db_path = directory / "corpus.sqlite3"
    if not db_path.is_file():
        raise FileNotFoundError(f"Corpus database not found: {db_path}")
    live_bytes = sum(Path(str(db_path) + suffix).stat().st_size
                     for suffix in ("", "-wal", "-shm") if Path(str(db_path) + suffix).exists())
    free = shutil.disk_usage(directory).free
    required = live_bytes * 2 + Store.MIN_FREE_BYTES
    if free < required:
        raise OSError(f"Migration needs at least {required / 1024**3:.2f} GiB free including reserve; found {free / 1024**3:.2f} GiB")

    cache_mib = max(8, int(os.environ.get("SQLITE_CACHE_MIB", Store.DEFAULT_CACHE_MIB)))
    conn = sqlite3.connect(db_path, timeout=60)
    try:
        conn.execute(f"PRAGMA cache_size=-{cache_mib * 1024}")
        conn.execute("PRAGMA temp_store=FILE")
        conn.execute("PRAGMA busy_timeout=60000")
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        conn.execute("PRAGMA journal_mode=DELETE")
        table_sql = conn.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='documents'").fetchone()
        if not table_sql:
            raise RuntimeError("documents table is missing")
        info = list(conn.execute("PRAGMA table_info(documents)"))
        columns = {row[1] for row in info}
        if "id" in columns and "WITHOUT ROWID" not in table_sql[0].upper():
            conn.execute("PRAGMA journal_mode=WAL")
            print("Already rowid-backed; no migration needed.")
            return
        required_columns = {"digest", "text", "url", "source", "title", "date"}
        if not required_columns.issubset(columns):
            raise RuntimeError(f"Unexpected documents schema; columns found: {sorted(columns)}")
        conn.execute("BEGIN IMMEDIATE")
        for name in ("upstream_source", "source_id", "license"):
            if name not in columns:
                conn.execute(f"ALTER TABLE documents ADD COLUMN {name} TEXT NOT NULL DEFAULT ''")
        conn.execute("""CREATE TABLE documents_migrating (
            id INTEGER PRIMARY KEY, digest BLOB NOT NULL, text BLOB NOT NULL, url TEXT NOT NULL,
            source TEXT NOT NULL, title TEXT NOT NULL DEFAULT '', date TEXT NOT NULL DEFAULT '',
            upstream_source TEXT NOT NULL DEFAULT '', source_id TEXT NOT NULL DEFAULT '',
            license TEXT NOT NULL DEFAULT ''
        )""")
        conn.execute("CREATE UNIQUE INDEX documents_digest_uq ON documents_migrating(digest)")
        conn.execute("""INSERT INTO documents_migrating
            (digest,text,url,source,title,date,upstream_source,source_id,license)
            SELECT digest,text,url,source,title,date,upstream_source,source_id,license FROM documents ORDER BY digest""")
        old_count = conn.execute("SELECT COUNT(*) FROM documents").fetchone()[0]
        new_count = conn.execute("SELECT COUNT(*) FROM documents_migrating").fetchone()[0]
        if old_count != new_count:
            raise RuntimeError(f"Migration row-count mismatch: before={old_count}, after={new_count}")
        conn.execute("DROP TABLE documents")
        conn.execute("ALTER TABLE documents_migrating RENAME TO documents")
        # SQLite keeps the explicit index name across the table rename.
        conn.commit()
        conn.execute("PRAGMA journal_mode=WAL")
        print(f"Migrated {new_count:,} documents to rowid-backed storage; checkpoints preserved.")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
