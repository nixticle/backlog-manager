from backlog_enricher.config import config_from_mapping
from backlog_enricher.db import connect_database, init_database
from backlog_enricher.invariants import run_validations
from backlog_enricher.normalize import norm_platform, norm_title


def test_invariants_fail_for_missing_results(tmp_path):
    cfg = config_from_mapping(
        {
            "backloggd": {"username": "tester"},
            "paths": {"db_path": str(tmp_path / "db.sqlite"), "cache_dir": str(tmp_path / "cache")},
        }
    )
    init_database(cfg)

    with connect_database(cfg) as db:
        db.execute("PRAGMA foreign_keys = OFF")
        title = "Chrono Trigger"
        title_norm = norm_title(title)
        platform_norm, family = norm_platform("SNES")
        db.execute(
            "INSERT INTO games (title, platform, year, status, rating, title_norm, platform_norm, platform_family, source_id) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [title, "SNES", 1995, None, None, title_norm, platform_norm, family, "chrono"],
        )
        db.execute(
            "INSERT INTO matches (game_id, hltb_id, confidence, method, decided_by) VALUES (?, ?, ?, ?, ?)",
            [1, 999, 0.9, "exact", "auto"],
        )
        db.execute("PRAGMA foreign_keys = ON")
        errors = run_validations(cfg, db)

    assert errors
