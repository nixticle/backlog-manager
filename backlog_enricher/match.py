from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Iterable, Literal

import orjson
from rapidfuzz import fuzz

from .config import Config, MatchConfig
from .db import Database
from .hltb_client import HLTBCandidate, HLTBQuery, build_query
from .normalize import norm_title, platform_family

LOG = logging.getLogger(__name__)


@dataclass(slots=True)
class GameRow:
    id: int
    title: str
    title_norm: str
    platform_family: str | None
    year: int | None


@dataclass(slots=True)
class CandidateView:
    candidate: HLTBCandidate
    title_norm: str
    families: set[str]

    @property
    def year(self) -> int | None:
        return self.candidate.year


@dataclass(slots=True)
class Decision:
    status: Literal["match", "queue", "skip"]
    method: str | None = None
    confidence: float = 0.0
    candidate: CandidateView | None = None
    reason: str | None = None
    queue_payload: list[dict] | None = None


def match_games(cfg: Config, db: Database, dry_run: bool = False) -> dict[str, int]:
    rows = db.query(
        """
        SELECT g.id, g.title, g.title_norm, g.platform_family, g.year
        FROM games g
        LEFT JOIN matches m ON g.id = m.game_id
        WHERE m.game_id IS NULL
        """
    )
    total_matched = 0
    total_queued = 0
    total_skipped = 0

    for row in rows:
        game = GameRow(
            id=row["id"],
            title=row["title"],
            title_norm=row["title_norm"],
            platform_family=row["platform_family"],
            year=row["year"],
        )
        query = build_query(game.title_norm, game.year, game.platform_family)
        result_rows = db.query(
            "SELECT raw_json FROM hltb_results WHERE query_key = ?",
            [query.key()],
        )
        if not result_rows:
            LOG.info("hltb_missing", extra={"game_id": game.id, "query": query.key()})
            total_skipped += 1
            continue
        candidates = _load_candidates(result_rows[0]["raw_json"])
        decision = decide_match(game, candidates, cfg.match)
        LOG.info(
            "match_decision",
            extra={
                "game_id": game.id,
                "status": decision.status,
                "method": decision.method,
                "confidence": decision.confidence,
                "reason": decision.reason,
            },
        )
        if decision.status == "match" and decision.candidate:
            total_matched += 1
            if not dry_run:
                store_match(
                    db=db,
                    game_id=game.id,
                    query=query,
                    candidate=decision.candidate.candidate,
                    method=decision.method or "auto",
                    confidence=decision.confidence,
                    decided_by="auto",
                )
        elif decision.status == "queue" and decision.queue_payload:
            total_queued += 1
            if not dry_run:
                _queue_review(db, game.id, decision.queue_payload)
        else:
            total_skipped += 1

    return {"matched": total_matched, "queued": total_queued, "skipped": total_skipped}


def _load_candidates(raw_json: str) -> list[CandidateView]:
    data = orjson.loads(raw_json)
    candidates: list[CandidateView] = []
    for item in data:
        candidate = HLTBCandidate(
            title=item.get("title", ""),
            platforms=list(item.get("platforms") or []),
            year=item.get("year"),
            main=item.get("main"),
            main_extra=item.get("main_extra"),
            complete=item.get("complete"),
            votes=item.get("votes"),
            source_url=item.get("source_url"),
        )
        families: set[str] = set()
        for platform in candidate.platforms:
            norm, fam = _normalize_candidate_platform(platform)
            if fam:
                families.add(fam)
        candidates.append(CandidateView(candidate=candidate, title_norm=norm_title(candidate.title), families=families))
    return candidates


def _normalize_candidate_platform(platform: str) -> tuple[str, str | None]:
    normalized = platform.lower().strip()
    family = platform_family(normalized)
    return normalized, family


def decide_match(game: GameRow, candidates: list[CandidateView], config: MatchConfig) -> Decision:
    if not candidates:
        return Decision(status="skip", reason="no_candidates")

    if _detect_collision(game, candidates):
        return _queue_decision(game, candidates, config, reason="collision")

    exact = _deterministic_exact(game, candidates)
    if exact:
        return Decision(status="match", method="exact", confidence=1.0, candidate=exact)

    relaxed = _deterministic_relaxed(game, candidates, config.year_tolerance)
    if relaxed:
        return Decision(status="match", method="exact_relaxed", confidence=0.95, candidate=relaxed)

    fuzzy = _fuzzy_match(game, candidates, config)
    if fuzzy.status == "match":
        return fuzzy
    return _queue_decision(game, candidates, config, reason=fuzzy.reason)


def _deterministic_exact(game: GameRow, candidates: list[CandidateView]) -> CandidateView | None:
    exacts = [
        c
        for c in candidates
        if c.title_norm == game.title_norm and _year_distance(game.year, c.year) == 0 and _platform_overlap(game, c)
    ]
    if len(exacts) == 1:
        return exacts[0]
    return None


def _deterministic_relaxed(game: GameRow, candidates: list[CandidateView], tolerance: int) -> CandidateView | None:
    matches = [
        c
        for c in candidates
        if c.title_norm == game.title_norm
        and _year_distance(game.year, c.year) <= tolerance
        and (not game.platform_family or _platform_overlap(game, c))
    ]
    if len(matches) == 1:
        return matches[0]
    return None


def _fuzzy_match(game: GameRow, candidates: list[CandidateView], config: MatchConfig) -> Decision:
    best_candidate: CandidateView | None = None
    best_score = 0
    for candidate in candidates:
        score = fuzz.token_set_ratio(game.title_norm, candidate.title_norm)
        if score > best_score:
            best_score = score
            best_candidate = candidate
    if best_candidate and best_score >= config.fuzzy_auto:
        if config.require_platform_overlap and not _platform_overlap(game, best_candidate):
            return Decision(status="queue", reason="platform_mismatch")
        if _year_distance(game.year, best_candidate.year) > config.year_tolerance:
            return Decision(status="queue", reason="year_distance")
        return Decision(
            status="match",
            method="fuzzy_auto",
            confidence=round(best_score / 100, 4),
            candidate=best_candidate,
        )
    if best_candidate and best_score >= config.fuzzy_queue_min:
        return Decision(
            status="queue",
            reason="fuzzy_ambiguous",
        )
    return Decision(status="skip", reason="low_score")


def _queue_decision(
    game: GameRow,
    candidates: list[CandidateView],
    config: MatchConfig,
    reason: str | None,
) -> Decision:
    scored = []
    for candidate in candidates:
        score = fuzz.token_set_ratio(game.title_norm, candidate.title_norm)
        scored.append((score, candidate))
    scored.sort(key=lambda item: item[0], reverse=True)
    payload = [
        {
            "title": cand.candidate.title,
            "title_norm": cand.title_norm,
            "score": score,
            "year": cand.year,
            "platforms": cand.candidate.platforms,
            "source_url": cand.candidate.source_url,
            "main": cand.candidate.main,
            "main_extra": cand.candidate.main_extra,
            "complete": cand.candidate.complete,
            "votes": cand.candidate.votes,
        }
        for score, cand in scored[:5]
    ]
    return Decision(status="queue", reason=reason or "ambiguous", queue_payload=payload)


def _detect_collision(game: GameRow, candidates: list[CandidateView]) -> bool:
    same_title = [c for c in candidates if c.title_norm == game.title_norm]
    years = {c.year for c in same_title if c.year}
    if len(same_title) > 1 and len(years) > 1:
        return True
    collision_tokens = {"remake", "collection", "remaster", "redux", "definitive"}
    for candidate in candidates:
        if any(token in candidate.title_norm for token in collision_tokens):
            # Unless exact conditions later confirm, queue for manual review
            return True
    return False


def _platform_overlap(game: GameRow, candidate: CandidateView) -> bool:
    if not game.platform_family:
        return True
    if candidate.families:
        return game.platform_family in candidate.families
    return True


def _year_distance(game_year: int | None, candidate_year: int | None) -> int:
    if game_year is None or candidate_year is None:
        return 0
    return abs(game_year - candidate_year)


def store_match(
    db: Database,
    game_id: int,
    query: HLTBQuery,
    candidate: HLTBCandidate,
    method: str,
    confidence: float,
    decided_by: str,
) -> None:
    db.execute(
        """
        UPDATE hltb_results
        SET title = ?, platforms = ?, year = ?, main = ?, main_extra = ?, complete = ?, votes = ?
        WHERE query_key = ?
        """,
        [
            candidate.title,
            ",".join(candidate.platforms),
            candidate.year,
            candidate.main,
            candidate.main_extra,
            candidate.complete,
            candidate.votes,
            query.key(),
        ],
    )
    sql = (
        "INSERT OR REPLACE INTO matches (game_id, hltb_id, confidence, method, decided_by) "
        "SELECT ?, id, ?, ?, ? FROM hltb_results WHERE query_key = ?"
    )
    db.execute(
        sql,
        [
            game_id,
            confidence,
            method,
            decided_by,
            query.key(),
        ],
    )
    db.execute("DELETE FROM review_queue WHERE game_id = ?", [game_id])


def _queue_review(db: Database, game_id: int, payload: list[dict]) -> None:
    db.execute(
        "INSERT OR REPLACE INTO review_queue (game_id, candidates_json) VALUES (?, ?)",
        [game_id, orjson.dumps(payload).decode("utf-8")],
    )


def store_manual_match(db: Database, game: GameRow, candidate_payload: dict) -> None:
    candidate = HLTBCandidate(
        title=candidate_payload["title"],
        platforms=candidate_payload.get("platforms", []),
        year=candidate_payload.get("year"),
        main=candidate_payload.get("main"),
        main_extra=candidate_payload.get("main_extra"),
        complete=candidate_payload.get("complete"),
        votes=candidate_payload.get("votes"),
        source_url=candidate_payload.get("source_url"),
    )
    query = build_query(game.title_norm, game.year, game.platform_family)
    store_match(
        db=db,
        game_id=game.id,
        query=query,
        candidate=candidate,
        method="manual",
        confidence=round(candidate_payload.get("score", 0) / 100, 4) if candidate_payload.get("score") else 0.0,
        decided_by="manual",
    )
