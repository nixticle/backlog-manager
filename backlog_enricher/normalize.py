from __future__ import annotations

import re
import unicodedata
from typing import Iterable


EDITION_PATTERNS = [
    "remaster",
    "remastered",
    "hd",
    "definitive",
    "goty",
    "game of the year",
    "complete",
    "complete edition",
    "director's cut",
    "directors cut",
    "anniversary",
    "collection",
    "royal",
    "ultimate",
    "redux",
    "legendary",
    "remake",
]

GENERIC_SUBTITLES = {"origins", "legends", "redux"}

ROMAN_NUMERAL_MAP = {
    "i": "1",
    "ii": "2",
    "iii": "3",
    "iv": "4",
    "v": "5",
    "vi": "6",
    "vii": "7",
    "viii": "8",
    "ix": "9",
    "x": "10",
}

PLATFORM_FAMILIES = {
    "playstation": [
        "playstation",
        "psone",
        "ps1",
        "ps2",
        "ps3",
        "ps4",
        "ps5",
        "ps vita",
        "psp",
    ],
    "xbox": [
        "xbox",
        "xbox one",
        "xbox series",
        "xbox 360",
    ],
    "nintendo": [
        "nintendo",
        "switch",
        "3ds",
        "ds",
        "wii",
        "wii u",
        "gamecube",
        "gba",
        "game boy",
        "nes",
        "snes",
        "n64",
    ],
    "pc": [
        "pc",
        "windows",
        "steam",
        "dos",
        "linux",
        "mac",
    ],
    "mobile": ["ios", "android", "mobile"],
    "sega": ["saturn", "dreamcast", "genesis", "mega drive", "sega"],
    "atari": ["atari"],
    "neo-geo": ["neo-geo", "neogeo"],
}

TITLE_PUNCTUATION_RE = re.compile(r"[^a-z0-9\s]")
BRACKETS_RE = re.compile(r"(\[[^\]]*\]|\([^\)]*\))")
WHITESPACE_RE = re.compile(r"\s+")
ROMAN_RE = re.compile(r"\b(?P<roman>(?=[ivx]+\b)[ivx]+)\b", re.IGNORECASE)


def norm_title(raw: str) -> str:
    value = _normalize_unicode(raw)
    value = BRACKETS_RE.sub(" ", value)
    value = _strip_trademarks(value)
    value = value.lower()
    value = _replace_roman_numerals(value)
    value = _remove_edition_markers(value)
    value = TITLE_PUNCTUATION_RE.sub(" ", value)
    parts = [part for part in WHITESPACE_RE.split(value) if part]
    parts = [part for part in parts if part not in GENERIC_SUBTITLES]
    return " ".join(parts)


def _normalize_unicode(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    return normalized.encode("ascii", "ignore").decode("ascii")


def _strip_trademarks(value: str) -> str:
    symbols = ["™", "®", "©"]
    for symbol in symbols:
        value = value.replace(symbol, " ")
    value = value.replace("(tm)", " ").replace("(TM)", " ")
    value = value.replace("(r)", " ").replace("(R)", " ")
    value = value.replace("(c)", " ").replace("(C)", " ")
    value = re.sub(r"(?i)(?<=\w)tm\b", " ", value)
    return value


def _remove_edition_markers(value: str) -> str:
    lowered = value.lower()
    for pattern in EDITION_PATTERNS:
        lowered = re.sub(rf"\b{re.escape(pattern)}\b", " ", lowered)
    return lowered


def _replace_roman_numerals(value: str) -> str:
    def repl(match: re.Match[str]) -> str:
        roman = match.group("roman").lower()
        return ROMAN_NUMERAL_MAP.get(roman, roman)

    return ROMAN_RE.sub(repl, value)


def norm_platform(raw: str | None) -> tuple[str | None, str | None]:
    if raw is None:
        return None, None
    cleaned = _normalize_unicode(raw).lower()
    cleaned = TITLE_PUNCTUATION_RE.sub(" ", cleaned)
    cleaned = WHITESPACE_RE.sub(" ", cleaned).strip()
    if not cleaned:
        return None, None
    fam = platform_family(cleaned)
    return cleaned, fam


def platform_family(platform_norm: str | None) -> str | None:
    if not platform_norm:
        return None
    for family, tokens in PLATFORM_FAMILIES.items():
        if any(_contains_token(platform_norm, token) for token in tokens):
            return family
    return None


def _contains_token(string: str, token: str) -> bool:
    normalized_token = WHITESPACE_RE.sub(" ", token).strip()
    pattern = r"\b" + re.escape(normalized_token) + r"\b"
    return re.search(pattern, string) is not None


def normalize_tokens(values: Iterable[str]) -> list[str]:
    return [norm_title(value) for value in values]
