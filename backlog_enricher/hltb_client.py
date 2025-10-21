from __future__ import annotations

import logging
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, List

import orjson
import requests
from tenacity import RetryError, Retrying, retry_if_exception_type, stop_after_attempt, wait_exponential

from .config import Config
from .normalize import norm_title

LOG = logging.getLogger(__name__)

try:
    from howlongtobeatpy import HowLongToBeat  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - optional dependency
    HowLongToBeat = None


@dataclass(slots=True)
class HLTBQuery:
    title_norm: str
    year: int | None
    platform_family: str | None

    def key(self) -> str:
        year_part = str(self.year or 0)
        platform_part = self.platform_family or ""
        return f"{self.title_norm}|{year_part}|{platform_part}"


@dataclass(slots=True)
class HLTBCandidate:
    title: str
    platforms: list[str]
    year: int | None
    main: float | None
    main_extra: float | None
    complete: float | None
    votes: int | None
    source_url: str | None = None


class HLTBClient:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.cache_dir = cfg.cache_path() / "hltb"
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self._last_request = 0.0
        self._library = HowLongToBeat() if HowLongToBeat and cfg.hltb.use_library else None
        self._retryer = Retrying(
            stop=stop_after_attempt(cfg.hltb.max_retries),
            wait=wait_exponential(
                multiplier=cfg.hltb.backoff_min_seconds,
                max=cfg.hltb.backoff_max_seconds,
            ),
            retry=retry_if_exception_type((requests.RequestException, RuntimeError)),
            reraise=True,
        )
        self.metrics = {"disk_cache": 0, "fetched": 0, "errors": 0}

    def search(self, query: HLTBQuery) -> tuple[list[HLTBCandidate], bool]:
        cache_path = self.cache_dir / f"{query.key().replace('/', '_')}.json"
        if cache_path.exists():
            try:
                cached = orjson.loads(cache_path.read_bytes())
                candidates = [self._candidate_from_dict(item) for item in cached]
                self.metrics["disk_cache"] += 1
                return candidates, True
            except orjson.JSONDecodeError:
                LOG.warning("cache_corrupt", extra={"query": query.key()})

        try:
            candidates = self._fetch_with_retry(query)
        except RetryError as exc:  # pragma: no cover - network failure path
            self.metrics["errors"] += 1
            raise
        self.metrics["fetched"] += 1
        cache_path.write_bytes(orjson.dumps([asdict(candidate) for candidate in candidates]))
        return candidates, False

    def _fetch_with_retry(self, query: HLTBQuery) -> list[HLTBCandidate]:
        for attempt in self._retryer:
            with attempt:
                self._respect_rate_limit()
                return self._fetch_candidates(query)
        raise RuntimeError("HLTB retry exhausted")

    def _fetch_candidates(self, query: HLTBQuery) -> list[HLTBCandidate]:
        title = query.title_norm
        if self._library:
            LOG.info("hltb_search_library", extra={"query": query.key()})
            results = self._library.search(title)  # type: ignore[no-untyped-call]
            if results:
                return [_candidate_from_library(entry) for entry in results]
            if not self.cfg.hltb.fallback_html:
                return []
        if not self.cfg.hltb.fallback_html:
            return []
        LOG.info("hltb_search_html", extra={"query": query.key()})
        return self._search_html(title)

    def _respect_rate_limit(self) -> None:
        interval = max(1 / self.cfg.hltb.rate_limit_per_sec, 0.5)
        now = time.monotonic()
        elapsed = now - self._last_request
        if elapsed < interval:
            sleep_for = interval - elapsed
            LOG.debug("hltb_throttle", extra={"sleep": round(sleep_for, 3)})
            time.sleep(sleep_for)
        self._last_request = time.monotonic()

    def _search_html(self, title: str) -> list[HLTBCandidate]:
        url = "https://howlongtobeat.com/search_results?page=1"
        payload = {"queryString": title, "t": "games", "sorthead": "popular", "sortd": "Normal"}
        headers = {
            "User-Agent": self.cfg.hltb.user_agent,
            "Referer": "https://howlongtobeat.com/",
            "Origin": "https://howlongtobeat.com",
            "Content-Type": "application/json",
        }
        response = requests.post(
            url,
            data=orjson.dumps(payload),
            headers=headers,
            timeout=(10, 30),
        )
        response.raise_for_status()
        data = response.json()
        html = data.get("data") if isinstance(data, dict) else None
        if not html:
            return []
        return parse_hltb_html(html)

    def _candidate_from_dict(self, data: dict[str, Any]) -> HLTBCandidate:
        return HLTBCandidate(
            title=data.get("title", ""),
            platforms=list(data.get("platforms") or []),
            year=data.get("year"),
            main=data.get("main"),
            main_extra=data.get("main_extra"),
            complete=data.get("complete"),
            votes=data.get("votes"),
            source_url=data.get("source_url"),
        )


def build_query(game_title_norm: str, year: int | None, platform_family: str | None) -> HLTBQuery:
    return HLTBQuery(game_title_norm, year, platform_family)


def parse_hltb_html(html: str) -> list[HLTBCandidate]:
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "html.parser")
    candidates: list[HLTBCandidate] = []
    selectors = [
        ".search_list_details",
        ".dl_search_result",
    ]
    entries: list[Any] = []
    for selector in selectors:
        entries = soup.select(selector)
        if entries:
            break
    for entry in entries:
        title_elem = entry.select_one("a") or entry.select_one(".search_list_t")
        if not title_elem:
            continue
        title = title_elem.text.strip()
        if not title:
            continue
        platforms_elem = entry.select_one(".search_list_tidbits, .search_list_details_block")
        platforms = _parse_platforms(platforms_elem.text if platforms_elem else "")
        year = _parse_year(entry.select_one(".search_list_rel, .search_list_details_block"))
        main = None
        main_extra = None
        complete = None
        for tidbit in entry.select(".search_list_tidbit"):
            text = tidbit.get_text(" ", strip=True).lower()
            if "main story" in text:
                main = _parse_hours_from_text(text)
            elif "main + extra" in text or "main + extras" in text:
                main_extra = _parse_hours_from_text(text)
            elif "completionist" in text:
                complete = _parse_hours_from_text(text)
        votes = _parse_votes(entry.select_one(".search_list_details_block"))
        href = title_elem.get("href")
        source_url = f"https://howlongtobeat.com{href}" if href else None
        candidates.append(
            HLTBCandidate(
                title=title,
                platforms=platforms,
                year=year,
                main=main,
                main_extra=main_extra,
                complete=complete,
                votes=votes,
                source_url=source_url,
            )
        )
    return candidates


def _parse_platforms(raw: str) -> list[str]:
    parts = [part.strip() for part in raw.split(",") if part.strip()]
    return parts


def _parse_year(element: Any) -> int | None:
    if not element:
        return None
    text = element.text if hasattr(element, "text") else str(element)
    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) == 4:
        return int(digits)
    return None


def _parse_hours(element: Any) -> float | None:
    if not element:
        return None
    text = element.text if hasattr(element, "text") else str(element)
    return _parse_hours_from_text(text)


def _parse_hours_from_text(text: str) -> float | None:
    digits = "".join(ch for ch in text if ch.isdigit() or ch == ".")
    try:
        return float(digits)
    except ValueError:
        return None


def _parse_votes(element: Any) -> int | None:
    if not element:
        return None
    text = element.text if hasattr(element, "text") else str(element)
    digits = "".join(ch for ch in text if ch.isdigit())
    if digits:
        return int(digits)
    return None


def _candidate_from_library(entry: Any) -> HLTBCandidate:
    title = getattr(entry, "game_name", "")
    platforms = getattr(entry, "platforms", []) or getattr(entry, "platform", [])
    year = getattr(entry, "release_world", None)
    main = _safe_float(getattr(entry, "main_story", None))
    main_extra = _safe_float(getattr(entry, "main_extra", None))
    complete = _safe_float(getattr(entry, "completionist", None))
    votes = getattr(entry, "profile_steam", None)
    source_url = getattr(entry, "profile_url", None)
    return HLTBCandidate(
        title=title,
        platforms=[str(p) for p in platforms] if platforms else [],
        year=int(year) if isinstance(year, int) else None,
        main=main,
        main_extra=main_extra,
        complete=complete,
        votes=int(votes) if isinstance(votes, int) else None,
        source_url=source_url,
    )


def _safe_float(value: Any) -> float | None:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def store_results(
    db,
    query: HLTBQuery,
    candidates: list[HLTBCandidate],
) -> None:
    if not candidates:
        title = norm_title(query.title_norm)
        row = {
            "title": title,
            "platforms": "",
            "year": None,
            "main": None,
            "main_extra": None,
            "complete": None,
            "votes": None,
        }
    else:
        top = candidates[0]
        row = {
            "title": top.title,
            "platforms": ",".join(top.platforms),
            "year": top.year,
            "main": top.main,
            "main_extra": top.main_extra,
            "complete": top.complete,
            "votes": top.votes,
        }
    raw_json = orjson.dumps([asdict(candidate) for candidate in candidates]).decode("utf-8")
    fetched_at = datetime.now(tz=timezone.utc).isoformat()
    sql = (
        "INSERT OR REPLACE INTO hltb_results "
        "(query_key, title, platforms, year, main, main_extra, complete, votes, raw_json, fetched_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    )
    db.execute(
        sql,
        [
            query.key(),
            row["title"],
            row["platforms"],
            row["year"],
            row["main"],
            row["main_extra"],
            row["complete"],
            row["votes"],
            raw_json,
            fetched_at,
        ],
    )


def enrich_games(cfg: Config, db, dry_run: bool = False) -> dict[str, int]:
    client = HLTBClient(cfg)
    rows = db.query(
        """
        SELECT id, title_norm, year, platform_family
        FROM games
        """
    )
    stats = {"processed": 0, "skipped": 0, "cached": 0, "fetched": 0}
    for row in rows:
        query = build_query(row["title_norm"], row["year"], row["platform_family"])
        existing = db.query("SELECT 1 FROM hltb_results WHERE query_key = ?", [query.key()])
        if existing:
            stats["skipped"] += 1
            continue
        candidates, from_cache = client.search(query)
        stats["processed"] += 1
        if from_cache:
            stats["cached"] += 1
        else:
            stats["fetched"] += 1
        if not dry_run:
            store_results(db, query, candidates)
    return stats
