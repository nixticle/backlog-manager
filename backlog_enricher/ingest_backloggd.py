from __future__ import annotations

import json
import logging
import re
import subprocess
import time
from dataclasses import dataclass
from typing import Any, Iterable, Iterator, Optional

import requests
from bs4 import BeautifulSoup

from .config import Config
from .db import Database
from .normalize import norm_platform, norm_title, platform_family

LOG = logging.getLogger(__name__)
BACKLOGGD_BASE_URL = "https://backloggd.com"


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
    collection = cfg.backloggd.collection
    page = 1
    total_inserted = 0
    total_seen = 0
    pages_processed = 0

    while True:
        LOG.info("fetch_backlog_page", extra={"page": page, "collection": collection})
        html = _fetch_page(session, username, collection, page)
        games = list(parse_backloggd_page(html))
        if not games:
            LOG.info(
                "no_more_results",
                extra={"page": page, "html_length": len(html), "has_content": bool(html.strip())},
            )
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
    if cfg.backloggd.host_override_ip:
        session.trust_env = False
        setattr(session, "_override_ip", cfg.backloggd.host_override_ip)
    return session


def _fetch_page(session: requests.Session, username: str, collection: str, page: int) -> str:
    slug = _slugify_collection_path(collection)
    base_host = BACKLOGGD_BASE_URL.replace("https://", "")
    headers: dict[str, str] = {}
    if getattr(session, "_override_ip", None):
        headers["Host"] = base_host
    override_ip = getattr(session, "_override_ip", None)
    candidate_paths = [
        f"/u/{username}/{slug}/?page={page}",
        f"/u/{username}/games/{slug}/?page={page}",
    ]
    if slug != "games" and "/" in slug:
        # Backloggd historically allowed both `/u/<user>/<collection>/` and
        # `/u/<user>/games/<collection>/`. When the collection contains nested
        # segments we also try the final segment by itself to preserve backwards
        # compatibility with older configs.
        tail = slug.rsplit("/", 1)[-1]
        candidate_paths.append(f"/u/{username}/{tail}/?page={page}")
    candidate_paths.extend(
        f"/u/{username}/{raw.strip('/') or 'games'}/?page={page}"
        for raw in {collection, collection.lower()}
        if raw and raw.strip("/")
    )
    candidate_paths = list(dict.fromkeys(candidate_paths))
    last_exc: Exception | None = None
    for path in candidate_paths:
        url = f"https://{override_ip or base_host}{path}"
        try:
            response = session.get(url, timeout=(10, 30), headers=headers or None)
            if response.status_code == 404:
                last_exc = BackloggdIngestError(f"Backloggd page not found: {path}")
                continue
            if response.status_code >= 400:
                last_exc = BackloggdIngestError(
                    f"Failed to fetch Backloggd page {page} ({path}): {response.status_code}"
                )
                continue
            LOG.debug(
                "backloggd_fetch_success",
                extra={"url": url, "status": response.status_code, "length": len(response.text)},
            )
            return response.text
        except requests.RequestException as exc:  # pragma: no cover - network
            last_exc = exc
            LOG.warning("requests_fetch_failed", extra={"url": url, "error": str(exc)})
            html = _fetch_page_via_curl(base_host, path, override_ip)
            if html is not None:
                return html
    if last_exc:
        raise last_exc
    raise BackloggdIngestError("Failed to fetch Backloggd page with any known path.")


def _fetch_page_via_curl(host: str, path: str, override_ip: str | None) -> str | None:
    url = f"https://{host}{path}"
    cmd = ["curl", "-fsSL", url]
    if override_ip:
        cmd = ["curl", "-fsSL", "--resolve", f"{host}:443:{override_ip}", url]
    try:
        result = subprocess.run(cmd, capture_output=True, check=False, text=True, timeout=30)
    except (OSError, subprocess.SubprocessError) as exc:  # pragma: no cover - curl missing
        LOG.error("curl_fetch_failed", extra={"url": url, "error": str(exc)})
        return None
    if result.returncode != 0:
        LOG.warning(
            "curl_nonzero_exit",
            extra={"url": url, "code": result.returncode, "stderr": result.stderr[:200]},
        )
        return None
    return result.stdout


def parse_backloggd_page(html: str) -> Iterable[BackloggdGame]:
    soup = BeautifulSoup(html, "html.parser")
    nuxt_games = list(_parse_games_from_nuxt_payload(soup))
    if nuxt_games:
        LOG.debug("parsed_backloggd_games", extra={"source": "nuxt", "count": len(nuxt_games)})
        yield from nuxt_games
        return

    card_selectors = [
        ".card.game-card",
        ".game-card.card",
        ".gamedetails-card",
        ".game-card",
        ".games-list > li",
    ]
    cards: list[Any] = []
    for selector in card_selectors:
        cards = soup.select(selector)
        if cards:
            break
    if not cards:
        cards = soup.select("[data-game-id]")
    if not cards:
        LOG.warning(
            "backloggd_no_cards_found",
            extra={"html_excerpt": html[:200], "nuxt_detected": bool(nuxt_games)},
        )
        return []
    yielded = 0
    for card in cards:
        title = _extract_title(card)
        if not title:
            continue
        platform = _extract_platform(card)
        year = _extract_year(card)
        status = _extract_status(card)
        rating = _extract_rating(card)
        source_id = _extract_source_from_card(card)

        yielded += 1
        yield BackloggdGame(
            title=title,
            platform=platform,
            year=year,
            status=status,
            rating=rating,
            source_id=source_id,
        )
    if yielded:
        LOG.debug("parsed_backloggd_games", extra={"source": "dom", "count": yielded})


def _extract_title(card: Any) -> str | None:
    attr_title = getattr(card, "get", lambda *_: None)("data-title") or getattr(card, "get", lambda *_: None)("data-game-title")
    if attr_title:
        return attr_title.strip()
    title_elem = card.select_one(
        ".game-title, .card-title a, .card-title, h2 a, h2, h3 a, h3, .media-title, a.title"
    )
    if title_elem and title_elem.text:
        return title_elem.text.strip()
    return None


def _extract_platform(card: Any) -> str | None:
    attr = getattr(card, "get", lambda *_: None)("data-platform")
    if attr:
        return attr.strip()
    platform_elem = card.select_one(
        ".platform, .game-platform, .badge-platform, .platform-badge, .meta-platform, .platforms span"
    )
    if platform_elem and platform_elem.text:
        return platform_elem.text.strip()
    return None


def _extract_year(card: Any) -> int | None:
    attr = getattr(card, "get", lambda *_: None)("data-year")
    if attr:
        return _parse_year(str(attr))
    year_elem = card.select_one(".release-year, .year, .meta span, .meta-year, .year-tag")
    return _parse_year(year_elem.text if year_elem else None)


def _extract_status(card: Any) -> str | None:
    attr = getattr(card, "get", lambda *_: None)("data-status")
    if attr:
        return str(attr).strip()
    status_elem = card.select_one(".status, .badge-status, .game-status, .status-tag, .play-state")
    if status_elem and status_elem.text:
        return status_elem.text.strip()
    return None


def _extract_rating(card: Any) -> float | None:
    attr = getattr(card, "get", lambda *_: None)("data-rating")
    if attr:
        return _parse_rating(str(attr))
    rating_elem = card.select_one(".rating, .score, .game-rating, .rating-value")
    return _parse_rating(rating_elem.text if rating_elem else None)


def _extract_source_from_card(card: Any) -> str | None:
    link = card.select_one("a[href*='/game']") or card.select_one("a[href*='/games/']")
    if link and link.get("href"):
        return _extract_source_id(link["href"])
    return None


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


def _slugify_collection_path(collection: str) -> str:
    collection = collection or ""
    stripped = collection.strip("/")
    if not stripped:
        return "games"

    parts = [segment for segment in re.split(r"/+", stripped) if segment]
    if not parts:
        return "games"

    slugged: list[str] = []
    for part in parts:
        normalized = re.sub(r"[^a-z0-9\-]+", "-", part.lower())
        normalized = re.sub(r"-+", "-", normalized).strip("-")
        slugged.append(normalized or part.lower())
    return "/".join(slugged)


def _parse_games_from_nuxt_payload(soup: BeautifulSoup) -> Iterator[BackloggdGame]:
    scripts = soup.find_all("script")
    seen: set[tuple[str, str | None, str | None]] = set()
    for script in scripts:
        text = script.string or script.get_text(strip=True)
        if not text or "__NUXT__" not in text:
            continue
        match = re.search(r"window\.__NUXT__\s*=\s*", text)
        if not match:
            continue
        snippet = text[match.end():].lstrip()
        if not snippet:
            continue
        try:
            payload, _ = json.JSONDecoder().raw_decode(snippet)
        except json.JSONDecodeError:
            LOG.debug("nuxt_payload_decode_failed")
            continue
        for node in _walk_nuxt_payload(payload):
            game = _game_from_nuxt_node(node)
            if not game:
                continue
            fingerprint = (game.title.lower(), game.platform or "", game.source_id or "")
            if fingerprint in seen:
                continue
            seen.add(fingerprint)
            yield game


def _walk_nuxt_payload(value: Any) -> Iterator[dict[str, Any]]:
    if isinstance(value, dict):
        yield value
        for sub in value.values():
            yield from _walk_nuxt_payload(sub)
    elif isinstance(value, list):
        for item in value:
            yield from _walk_nuxt_payload(item)


def _game_from_nuxt_node(node: dict[str, Any]) -> BackloggdGame | None:
    title = node.get("title") or node.get("name") or node.get("gameTitle")
    if not title:
        return None

    potential_keys = {
        "platform",
        "platforms",
        "releaseYear",
        "year",
        "status",
        "playState",
        "rating",
        "score",
        "slug",
        "id",
        "objectID",
        "gameId",
        "game_id",
    }
    if not potential_keys.intersection(node.keys()):
        return None

    platform = _extract_platform_from_node(node)
    year_value = node.get("year") or node.get("releaseYear") or node.get("release_year")
    if isinstance(year_value, int):
        year = year_value
    else:
        year = _parse_year(str(year_value)) if year_value else None

    status = node.get("status") or node.get("playState") or node.get("play_state")
    rating_value = node.get("rating") or node.get("score")
    if isinstance(rating_value, (int, float)):
        rating = float(rating_value)
    else:
        rating = _parse_rating(str(rating_value)) if rating_value is not None else None

    source_id = _extract_source_id_from_node(node)

    return BackloggdGame(
        title=title,
        platform=platform,
        year=year,
        status=status,
        rating=rating,
        source_id=source_id,
    )


def _extract_platform_from_node(node: dict[str, Any]) -> str | None:
    platform = node.get("platform")
    if isinstance(platform, dict):
        platform = platform.get("title") or platform.get("name")
    if isinstance(platform, list):
        platform = platform[0] if platform else None
    if not platform:
        platforms = node.get("platforms")
        if isinstance(platforms, list) and platforms:
            first = platforms[0]
            if isinstance(first, dict):
                platform = first.get("title") or first.get("name")
            else:
                platform = first
    if isinstance(platform, str):
        return platform.strip() or None
    return None


def _extract_source_id_from_node(node: dict[str, Any]) -> str | None:
    for key in ("slug", "id", "objectID", "gameId", "game_id"):
        value = node.get(key)
        if value is None:
            continue
        if isinstance(value, dict):
            value = value.get("slug") or value.get("id")
        if value is None:
            continue
        return str(value)
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
