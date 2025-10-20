from __future__ import annotations

import logging

from .db import Database

LOG = logging.getLogger(__name__)


def collect_stats(db: Database) -> dict[str, object]:
    games_total = _scalar(db, "SELECT COUNT(1) FROM games")
    matches_total = _scalar(db, "SELECT COUNT(1) FROM matches")
    queue_total = _scalar(db, "SELECT COUNT(1) FROM review_queue")
    unresolved = _scalar(
        db,
        """
        SELECT COUNT(1)
        FROM games g
        LEFT JOIN matches m ON g.id = m.game_id
        WHERE m.game_id IS NULL
        """,
    )
    match_methods = db.query(
        "SELECT COALESCE(method, 'unknown') AS method, COUNT(1) AS total FROM matches GROUP BY method"
    )
    cache_entries = _scalar(db, "SELECT COUNT(1) FROM hltb_results")
    return {
        "games": games_total,
        "matches": matches_total,
        "queue": queue_total,
        "unresolved": unresolved,
        "match_methods": {row["method"]: row["total"] for row in match_methods},
        "hltb_results": cache_entries,
    }


def print_stats(stats: dict[str, object]) -> None:
    LOG.info("stats_summary", extra=stats)
    for key, value in stats.items():
        if key == "match_methods":
            methods = ", ".join(f"{method}:{total}" for method, total in value.items())
            print(f"{key}: {methods}")
        else:
            print(f"{key}: {value}")


def _scalar(db: Database, sql: str) -> int:
    row = db.query(sql)
    if row:
        return int(list(row[0])[0])
    return 0
