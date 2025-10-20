from __future__ import annotations

import csv
from pathlib import Path
from typing import Iterable, Sequence

import orjson

from .config import Config
from .db import Database

import logging

LOG = logging.getLogger(__name__)


COLUMNS = [
    "title",
    "platform",
    "year",
    "status",
    "rating",
    "hltb_main",
    "hltb_main_extra",
    "hltb_complete",
    "hltb_votes",
    "confidence",
    "method",
    "decided_by",
]


def export_data(cfg: Config, db: Database, formats: Sequence[str]) -> dict[str, Path]:
    rows = db.query(
        """
        SELECT g.title, g.platform, g.year, g.status, g.rating,
               h.main AS hltb_main,
               h.main_extra AS hltb_main_extra,
               h.complete AS hltb_complete,
               h.votes AS hltb_votes,
               m.confidence,
               m.method,
               m.decided_by
        FROM games g
        LEFT JOIN matches m ON g.id = m.game_id
        LEFT JOIN hltb_results h ON m.hltb_id = h.id
        ORDER BY g.title_norm ASC
        """
    )
    data = [dict(row) for row in rows]
    export_dir = cfg.export_path()
    export_dir.mkdir(parents=True, exist_ok=True)
    produced: dict[str, Path] = {}
    for fmt in formats:
        fmt_lower = fmt.lower()
        if fmt_lower == "csv":
            produced["csv"] = _export_csv(data, export_dir / "backlog_enriched.csv")
        elif fmt_lower == "json":
            produced["json"] = _export_json(data, export_dir / "backlog_enriched.jsonl")
        elif fmt_lower == "parquet":
            produced["parquet"] = _export_parquet(data, export_dir / "backlog_enriched.parquet")
        else:
            LOG.warning("unsupported_export_format", extra={"format": fmt})
    return produced


def _export_csv(rows: Iterable[dict], path: Path) -> Path:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key) for key in COLUMNS})
    return path


def _export_json(rows: Iterable[dict], path: Path) -> Path:
    with path.open("wb") as handle:
        for row in rows:
            handle.write(orjson.dumps({key: row.get(key) for key in COLUMNS}))
            handle.write(b"\n")
    return path


def _export_parquet(rows: list[dict], path: Path) -> Path:
    try:
        import pyarrow as pa  # type: ignore
        import pyarrow.parquet as pq  # type: ignore
    except ModuleNotFoundError as exc:
        raise RuntimeError("pyarrow is required for parquet export") from exc
    table = pa.Table.from_pylist([{key: row.get(key) for key in COLUMNS} for row in rows])
    pq.write_table(table, path)
    return path
