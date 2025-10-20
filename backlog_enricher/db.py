from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterable, Iterator, Sequence

from .config import Config

SCHEMA_PATH = Path(__file__).resolve().parent.parent / "db.sql"


class Database:
    """Thin wrapper around sqlite3 with convenience helpers."""

    def __init__(self, path: Path):
        self.path = path
        self.connection = sqlite3.connect(path, timeout=30)
        self.connection.row_factory = sqlite3.Row
        self._apply_pragmas()

    def _apply_pragmas(self) -> None:
        cursor = self.connection.cursor()
        cursor.execute("PRAGMA foreign_keys = ON;")
        cursor.execute("PRAGMA journal_mode = WAL;")
        cursor.execute("PRAGMA synchronous = NORMAL;")
        cursor.close()

    def executemany(self, sql: str, seq_of_parameters: Iterable[Sequence[object]]) -> None:
        self.connection.executemany(sql, seq_of_parameters)
        self.connection.commit()

    def execute(self, sql: str, parameters: Sequence[object] | None = None) -> sqlite3.Cursor:
        cursor = self.connection.execute(sql, parameters or [])
        self.connection.commit()
        return cursor

    def query(self, sql: str, parameters: Sequence[object] | None = None) -> list[sqlite3.Row]:
        cursor = self.connection.execute(sql, parameters or [])
        rows = cursor.fetchall()
        cursor.close()
        return rows

    def __enter__(self) -> "Database":
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()

    def close(self) -> None:
        self.connection.close()


def init_database(cfg: Config) -> None:
    db_path = cfg.db_path()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    schema_sql = SCHEMA_PATH.read_text(encoding="utf-8")
    with Database(db_path) as db:
        db.connection.executescript(schema_sql)


@contextmanager
def connect_database(cfg: Config) -> Iterator[Database]:
    db = Database(cfg.db_path())
    try:
        yield db
    finally:
        db.close()

