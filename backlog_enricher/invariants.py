from __future__ import annotations

from typing import List

from .config import Config
from .db import Database
from .normalize import norm_platform


def run_validations(cfg: Config, db: Database) -> list[str]:
    errors: List[str] = []
    missing_games = db.query(
        "SELECT game_id FROM matches m LEFT JOIN games g ON g.id = m.game_id WHERE g.id IS NULL"
    )
    if missing_games:
        errors.append(f"{len(missing_games)} matches reference missing games.")

    missing_results = db.query(
        "SELECT hltb_id FROM matches m LEFT JOIN hltb_results h ON h.id = m.hltb_id WHERE h.id IS NULL"
    )
    if missing_results:
        errors.append(f"{len(missing_results)} matches reference missing hltb results.")

    duplicates = db.query(
        "SELECT game_id, COUNT(*) AS total FROM matches GROUP BY game_id HAVING total > 1"
    )
    if duplicates:
        errors.append("Duplicate match rows detected.")

    tolerance = cfg.match.year_tolerance
    auto_mismatches = db.query(
        """
        SELECT g.title, g.year AS game_year, h.year AS hltb_year, m.method
        FROM matches m
        JOIN games g ON g.id = m.game_id
        JOIN hltb_results h ON h.id = m.hltb_id
        WHERE m.method IN ('exact', 'exact_relaxed', 'fuzzy_auto')
        """
    )
    for row in auto_mismatches:
        game_year = row["game_year"]
        hltb_year = row["hltb_year"]
        if game_year is not None and hltb_year is not None and abs(game_year - hltb_year) > tolerance:
            errors.append(
                f"Year mismatch for '{row['title']}': game={game_year}, hltb={hltb_year}, method={row['method']}"
            )

    platform_conflicts = db.query(
        """
        SELECT g.title, g.platform_family, h.platforms
        FROM matches m
        JOIN games g ON g.id = m.game_id
        JOIN hltb_results h ON h.id = m.hltb_id
        WHERE g.platform_family IS NOT NULL
        """
    )
    for row in platform_conflicts:
        platform_family = row["platform_family"]
        raw_platforms = row["platforms"] or ""
        families = set()
        for token in raw_platforms.split(","):
            token = token.strip()
            if not token:
                continue
            _, fam = norm_platform(token)
            if fam:
                families.add(fam)
        if platform_family and families and platform_family not in families:
            errors.append(
                f"Platform conflict for '{row['title']}': expected {platform_family}, candidates families={sorted(families)}"
            )

    return errors
