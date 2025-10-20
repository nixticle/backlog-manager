from pathlib import Path

import pytest

from backlog_enricher.config import config_from_mapping
from backlog_enricher.db import connect_database, init_database
from backlog_enricher.hltb_client import HLTBCandidate, HLTBClient, enrich_games
from backlog_enricher.normalize import norm_platform, norm_title


@pytest.fixture()
def config(tmp_path: Path):
    data = {
        "backloggd": {"username": "tester"},
        "paths": {
            "cache_dir": str(tmp_path / "cache"),
            "db_path": str(tmp_path / "backlog.db"),
        },
        "export": {"formats": ["csv"]},
        "hltb": {
            "use_library": False,
            "fallback_html": False,
        },
    }
    cfg = config_from_mapping(data)
    init_database(cfg)
    return cfg


def test_enrich_uses_cache(monkeypatch: pytest.MonkeyPatch, config):
    call_count = 0

    def fake_fetch(self: HLTBClient, query):
        nonlocal call_count
        call_count += 1
        return [
            HLTBCandidate(
                title="Hollow Knight",
                platforms=["PC"],
                year=2017,
                main=25.0,
                main_extra=30.0,
                complete=40.0,
                votes=1000,
            )
        ]

    monkeypatch.setattr(HLTBClient, "_fetch_candidates", fake_fetch)

    with connect_database(config) as db:
        title = "Hollow Knight"
        norm = norm_title(title)
        platform_norm, family = norm_platform("PC")
        db.execute(
            "INSERT INTO games (title, platform, year, status, rating, title_norm, platform_norm, platform_family, source_id) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                title,
                "PC",
                2017,
                "backlog",
                None,
                norm,
                platform_norm,
                family,
                "hk",
            ],
        )
        stats_first = enrich_games(config, db, dry_run=False)
        assert call_count == 1
        cache_dir = config.cache_path() / "hltb"
        assert any(cache_dir.glob("*.json"))
        stats_second = enrich_games(config, db, dry_run=False)

    assert stats_first["processed"] == 1
    assert stats_second["skipped"] == 1
    assert call_count == 1

