from backlog_enricher.normalize import norm_platform, norm_title, platform_family


def test_norm_title_strips_trademarks_and_diacritics():
    assert norm_title("Pokémon™ Red (Game)") == "pokemon red"


def test_norm_title_converts_roman_numerals():
    assert norm_title("Final Fantasy VII") == "final fantasy 7"


def test_norm_title_removes_editions_and_brackets():
    assert norm_title("Resident Evil 2 (Remake)") == "resident evil 2"


def test_norm_platform_family_detection():
    platform_norm, family = norm_platform("PlayStation 5")
    assert platform_norm == "playstation 5"
    assert family == "playstation"
    assert platform_family(platform_norm) == "playstation"

