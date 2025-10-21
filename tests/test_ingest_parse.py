from pathlib import Path

from backlog_enricher.ingest_backloggd import (
    BackloggdGame,
    _slugify_collection_path,
    parse_backloggd_page,
)


def _read_fixture(name: str) -> str:
    html_path = Path(__file__).parent / "data" / name
    return html_path.read_text(encoding="utf-8")


def test_parse_backloggd_page_extracts_games_from_dom():
    html = _read_fixture("backloggd_page_dom.html")

    games = list(parse_backloggd_page(html))

    assert len(games) == 2
    first = games[0]
    assert isinstance(first, BackloggdGame)
    assert first.title == "The Legend of Zelda: Breath of the Wild"
    assert first.platform == "Nintendo Switch"
    assert first.year == 2017
    assert first.status == "Completed"
    assert first.rating == 4.5


def test_parse_backloggd_page_extracts_games_from_nuxt_payload():
    html = _read_fixture("backloggd_page_nuxt.html")

    games = list(parse_backloggd_page(html))

    assert {(g.title, g.platform, g.status) for g in games} == {
        ("Outer Wilds", "Xbox One", "Completed"),
        ("Citizen Sleeper", "PC", "Backlog"),
    }
    outer_wilds = next(game for game in games if game.title == "Outer Wilds")
    assert outer_wilds.year == 2019
    assert outer_wilds.rating == 4.7
    citizen = next(game for game in games if game.title == "Citizen Sleeper")
    assert citizen.year == 2022
    assert citizen.rating == 4.5
    assert citizen.source_id == "12345"


def test_slugify_collection_path_handles_whitespace_and_case():
    assert _slugify_collection_path("  Now Playing ") == "now-playing"
    assert _slugify_collection_path("games/Now Playing") == "games/now-playing"
    assert _slugify_collection_path("Games") == "games"
    assert _slugify_collection_path("") == "games"
