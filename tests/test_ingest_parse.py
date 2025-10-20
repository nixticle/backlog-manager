from pathlib import Path

from backlog_enricher.ingest_backloggd import BackloggdGame, parse_backloggd_page


def test_parse_backloggd_page_extracts_games():
    html_path = Path(__file__).parent / "data" / "backloggd_page.html"
    html = html_path.read_text(encoding="utf-8")

    games = list(parse_backloggd_page(html))

    assert len(games) == 2
    first = games[0]
    assert isinstance(first, BackloggdGame)
    assert first.title == "The Legend of Zelda: Breath of the Wild"
    assert first.platform == "Nintendo Switch"
    assert first.year == 2017
    assert first.status == "Completed"
    assert first.rating == 4.5
