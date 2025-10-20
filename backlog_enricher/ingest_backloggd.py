from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Iterable, Optional

import requests
from bs4 import BeautifulSoup

from .config import Config
from .db import Database
from .normalize import norm_platform, norm_title, platform_family

LOG = logging.getLogger(__name__)
BACKLOGGD_BASE_URL = "https://www.backloggd.com"


@dataclass(slots=True)
class BackloggdGame:
    title: str
    platform: str | None
    year: int | None
    status: str | None
    rating: float | None
    source_id: str | None


class BackloggdIngestError(Exception):
    """Raised when Backloggd ingestion fails."""


def ingest_backlog(cfg: Config, db: Database, dry_run: bool = False) -> dict[str, int]:
    session = _build_session(cfg)
    username = cfg.backloggd.username
    page = 1
    total_inserted = 0
    total_seen = 0
    pages_processed = 0

    while True:
        LOG.info("fetch_backlog_page", extra={"page": page})
        html = _fetch_page(session, username, page)
        games = list(parse_backloggd_page(html))
        if not games:
            LOG.info("no_more_results", extra={"page": page})
            break

        pages_processed += 1
        total_seen += len(games)
        if not dry_run:
            inserted = _insert_games(db, games)
        else:
            inserted = 0
        total_inserted += inserted
        LOG.info(
            "page_processed",
            extra={"page": page, "games": len(games), "inserted": inserted, "dry_run": dry_run},
        )
        page += 1
        time.sleep(max(0.5, 1 / cfg.hltb.rate_limit_per_sec))

    return {
        "pages": pages_processed,
        "parsed": total_seen,
        "inserted": total_inserted,
    }


def _build_session(cfg: Config) -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": cfg.hltb.user_agent,
            "Accept": "text/html,application/xhtml+xml",
        }
    )
    return session


def _fetch_page(session: requests.Session, username: str, page: int) -> str:
    url = f"{BACKLOGGD_BASE_URL}/u/{username}/games/?page={page}"
    response = session.get(url, timeout=(10, 30))
    if response.status_code >= 400:
        raise BackloggdIngestError(f"Failed to fetch Backloggd page {page}: {response.status_code}")
    return response.text


def parse_backloggd_page(html: str) -> Iterable[BackloggdGame]:
    soup = BeautifulSoup(html, "html.parser")
    card_selectors = [
        ".card.game-card",
        ".game-card.card",
        ".gamedetails-card",
    ]
    cards = []
    for selector in card_selectors:
        cards = soup.select(selector)
        if cards:
            break
    if not cards:
        LOG.warning("no_cards_found")
        return []
    for card in cards:
        title_elem = card.select_one(".game-title, .card-title a, h2 a")
        if not title_elem or not title_elem.text.strip():
            continue
        title = title_elem.text.strip()

        platform_elem = card.select_one(".platform, .game-platform, .badge-platform")
        platform = platform_elem.text.strip() if platform_elem and platform_elem.text else None

        year_elem = card.select_one(".release-year, .year, .meta span")
        year = _parse_year(year_elem.text if year_elem else None)

        status_elem = card.select_one(".status, .badge-status, .game-status")
        status = status_elem.text.strip() if status_elem and status_elem.text else None

        rating_elem = card.select_one(".rating, .score, .game-rating")
        rating = _parse_rating(rating_elem.text if rating_elem else None)

        source_link = card.select_one("a[href*='/games/']")
        source_id = _extract_source_id(source_link["href"]) if source_link and source_link.get("href") else None

        yield BackloggdGame(
            title=title,
            platform=platform,
            year=year,
            status=status,
            rating=rating,
            source_id=source_id,
        )


def _parse_year(value: Optional[str]) -> Optional[int]:
    if not value:
        return None
    digits = "".join(ch for ch in value if ch.isdigit())
    if len(digits) == 4:
        return int(digits)
    return None


def _parse_rating(value: Optional[str]) -> Optional[float]:
    if not value:
        return None
    try:
        return float(value.strip())
    except ValueError:
        return None


def _extract_source_id(href: str) -> Optional[str]:
    parts = href.rstrip("/").split("/")
    if parts:
        return parts[-1]
    return None


def _insert_games(db: Database, games: Iterable[BackloggdGame]) -> int:
    rows = []
    for game in games:
        title_norm = norm_title(game.title)
        platform_norm, family = norm_platform(game.platform)
        rows.append(
            (
                game.title,
                game.platform,
                game.year,
                game.status,
                game.rating,
                title_norm,
                platform_norm,
                family,
                game.source_id,
            )
        )
    sql = (
        "INSERT OR IGNORE INTO games "
        "(title, platform, year, status, rating, title_norm, platform_norm, platform_family, source_id) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
    )
    before = _count_rows(db)
    db.executemany(sql, rows)
    after = _count_rows(db)
    return after - before


def _count_rows(db: Database) -> int:
    row = db.query("SELECT COUNT(1) AS total FROM games")
    return int(row[0]["total"]) if row else 0
