import os
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from typing import Final

from pydantic import TypeAdapter


@dataclass(frozen=True, slots=True)
class ArchiveFailure:
    code: str


_KEY_ROWS: Final = TypeAdapter(tuple[tuple[str], ...])
_DATA_ROW: Final[TypeAdapter[tuple[bytes] | None]] = TypeAdapter(tuple[bytes] | None)
_USAGE: Final = TypeAdapter(tuple[int])


class ArchiveSpool:
    def __init__(self, path: Path, max_bytes: int = 1024 * 1024 * 1024) -> None:
        self.path = path
        self.max_bytes = max_bytes
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        os.close(os.open(path, os.O_CREAT | os.O_RDWR, 0o600))
        self.connection = sqlite3.connect(path, timeout=30, check_same_thread=False)
        self.lock = Lock()
        try:
            self.connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS pending (object_key TEXT PRIMARY KEY, payload BLOB NOT NULL);
                CREATE TABLE IF NOT EXISTS usage (id INTEGER PRIMARY KEY CHECK (id = 1), bytes INTEGER NOT NULL);
                INSERT OR IGNORE INTO usage VALUES (1, 0);
                CREATE TRIGGER IF NOT EXISTS charge AFTER INSERT ON pending BEGIN
                    UPDATE usage SET bytes = bytes + length(NEW.payload) WHERE id = 1;
                END;
                CREATE TRIGGER IF NOT EXISTS release AFTER DELETE ON pending BEGIN
                    UPDATE usage SET bytes = bytes - length(OLD.payload) WHERE id = 1;
                END;
                """
            )
        except sqlite3.Error:
            self.connection.close()
            raise

    def put(self, key: str, payload: bytes) -> ArchiveFailure | None:
        connection: Final = self.connection
        with self.lock, connection:
            connection.execute("BEGIN IMMEDIATE")
            existing: Final = _DATA_ROW.validate_python(
                connection.execute("SELECT payload FROM pending WHERE object_key = ?", (key,)).fetchone()
            )
            if existing is not None:
                return None if existing[0] == payload else ArchiveFailure("object_conflict")
            used: Final = _USAGE.validate_python(connection.execute("SELECT bytes FROM usage WHERE id = 1").fetchone())[
                0
            ]
            if used + len(payload) > self.max_bytes:
                return ArchiveFailure("spool_full")
            connection.execute("INSERT INTO pending VALUES (?, ?)", (key, payload))
        return None

    def pending_keys(self, limit: int) -> tuple[str, ...]:
        connection: Final = self.connection
        with self.lock:
            rows: Final = _KEY_ROWS.validate_python(
                connection.execute("SELECT object_key FROM pending ORDER BY rowid LIMIT ?", (limit,)).fetchall()
            )
            return tuple(row[0] for row in rows)

    def read(self, key: str) -> bytes | None:
        connection: Final = self.connection
        with self.lock:
            row: Final = _DATA_ROW.validate_python(
                connection.execute("SELECT payload FROM pending WHERE object_key = ?", (key,)).fetchone()
            )
            return None if row is None else row[0]

    def acknowledge(self, key: str, payload: bytes) -> None:
        connection: Final = self.connection
        with self.lock, connection:
            connection.execute("DELETE FROM pending WHERE object_key = ? AND payload = ?", (key, payload))

    def defer(self, key: str) -> None:
        connection: Final = self.connection
        with self.lock, connection:
            connection.execute(
                "UPDATE pending SET rowid = (SELECT max(rowid) + 1 FROM pending) WHERE object_key = ?", (key,)
            )

    def close(self) -> None:
        with self.lock:
            self.connection.close()
