from backlog_enricher.config import MatchConfig
from backlog_enricher.hltb_client import HLTBCandidate
from backlog_enricher.match import CandidateView, GameRow, decide_match
from backlog_enricher.normalize import norm_title


def build_candidate(title: str, platforms: list[str], year: int | None, families: set[str]) -> CandidateView:
    candidate = HLTBCandidate(
        title=title,
        platforms=platforms,
        year=year,
        main=None,
        main_extra=None,
        complete=None,
        votes=None,
    )
    return CandidateView(candidate=candidate, title_norm=norm_title(title), families=families)


def test_decide_match_exact():
    game = GameRow(id=1, title="Final Fantasy VII", title_norm="final fantasy 7", platform_family="playstation", year=1997)
    candidates = [
        build_candidate("Final Fantasy VII", ["PlayStation"], 1997, {"playstation"}),
    ]
    decision = decide_match(game, candidates, MatchConfig())
    assert decision.status == "match"
    assert decision.method == "exact"


def test_decide_match_collision_queues():
    game = GameRow(id=1, title="Resident Evil 2", title_norm="resident evil 2", platform_family="playstation", year=1998)
    candidates = [
        build_candidate("Resident Evil 2", ["PlayStation"], 1998, {"playstation"}),
        build_candidate("Resident Evil 2 Remake", ["PlayStation 4"], 2019, {"playstation"}),
    ]
    decision = decide_match(game, candidates, MatchConfig())
    assert decision.status == "queue"
    assert decision.reason == "collision"


def test_decide_match_fuzzy_accepts_clear_case():
    game = GameRow(id=1, title="Hollow Knight", title_norm="hollow knight", platform_family="pc", year=2017)
    candidates = [
        build_candidate("Hollow Knight", ["PC"], 2017, {"pc"}),
    ]
    config = MatchConfig(fuzzy_auto=90, fuzzy_queue_min=80)
    decision = decide_match(game, candidates, config)
    assert decision.status == "match"
    assert decision.method == "fuzzy_auto"

