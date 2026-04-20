"""Match a list of site candidates to the user's query.

Pipeline:
  1. Platform filter (PS5 required)
  2. Edition filter (only when query specifies one; else keep all editions)
  3. Used/new filter: if query specifies, honor it; else both allowed
  4. rapidfuzz score ≥ 80 on token_set_ratio over normalized strings
  5. Pick cheapest among survivors
"""
from __future__ import annotations

import re
from dataclasses import dataclass

from rapidfuzz import fuzz

from .normalize import normalize_query

MIN_SCORE = 80

# Romanian + English filler words that show up in marketing titles and don't
# carry matching signal. Keep small; over-stripping hurts precision.
STOPWORDS: frozenset[str] = frozenset({
    "joc", "jocul", "pentru", "de", "pe", "cu", "si", "la",
    "the", "a", "an", "and", "edition", "standard",
})

# Order matters for detection — longer/more specific keywords first.
EDITION_KEYWORDS: tuple[str, ...] = (
    "deluxe steelbook",
    "game of the year",
    "day one",
    "collector's",
    "collectors",
    "collector",
    "steelbook",
    "lenticular",
    "ultimate",
    "definitive",
    "anniversary",
    "complete",
    "premium",
    "goty",
    "gold",
    "deluxe",
    "game key card",
    "standard",
)

# A "base" edition = title contains no edition keyword OR contains 'standard'.
_BASE_EDITION_SENTINEL = "standard"

PS5_MARKERS: tuple[str, ...] = (
    "ps5",
    "playstation 5",
    "play station 5",
)

USED_MARKERS: tuple[str, ...] = (
    "second hand",
    "second-hand",
    "sh",
)

_USED_RE = re.compile(
    r"\b(" + "|".join(re.escape(m) for m in USED_MARKERS) + r")\b",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class Candidate:
    """A raw product returned by a site scraper, pre-match."""
    title: str
    price_ron: float
    url: str
    availability: str | None = None
    is_used: bool = False


@dataclass(frozen=True)
class MatchResult:
    winners: list[Candidate]  # 0, 1, or 2: cheapest-new and/or cheapest-SH

    @property
    def winner(self) -> Candidate | None:
        """Backward-compat: single cheapest across new and SH."""
        if not self.winners:
            return None
        return min(self.winners, key=lambda c: c.price_ron)


def _norm(s: str) -> str:
    return normalize_query(s)


def _contains_any(haystack: str, needles: tuple[str, ...]) -> bool:
    return any(n in haystack for n in needles)


def detect_editions(text_norm: str) -> set[str]:
    """Return edition keywords present in a normalized string."""
    found: set[str] = set()
    for kw in EDITION_KEYWORDS:
        if re.search(rf"\b{re.escape(kw)}\b", text_norm):
            found.add(kw)
    return found


def detect_platform_ps5(text_norm: str) -> bool:
    """True if the title mentions PS5 in any form."""
    # match as a whole token to avoid false positives on 'ps500' etc.
    return any(re.search(rf"\b{re.escape(m)}\b", text_norm) for m in PS5_MARKERS)


def detect_used(text_norm: str) -> bool:
    """True if the text has a used/SH marker as a whole word (not a substring
    of e.g. 'Shin' or 'Sheepshead')."""
    return bool(_USED_RE.search(text_norm))


def _significant_tokens(text_norm: str) -> list[str]:
    return [t for t in text_norm.split() if len(t) >= 2 and t not in STOPWORDS]


def all_query_tokens_present(query_norm: str, title_norm: str) -> bool:
    """Every significant query token must appear in the title."""
    for qt in _significant_tokens(query_norm):
        if qt not in title_norm:
            return False
    return True


def pre_filter_matches(query: str, title: str, url: str = "") -> bool:
    """Lightweight filter for scrapers deciding whether to fetch a PDP for price.

    Applies platform, edition, and token-coverage gates — the same ones `match()`
    uses — but skips the fuzzy score and cheapest-pick stages.
    """
    q_norm = _norm(query)
    t_norm = _norm(title)
    platform_text = _norm(f"{title} {url}")

    if not detect_platform_ps5(platform_text):
        return False
    if not all_query_tokens_present(q_norm, t_norm):
        return False
    q_editions = detect_editions(q_norm)
    if q_editions:
        t_editions = detect_editions(t_norm)
        if not q_editions.issubset(t_editions):
            return False
    return True


def match(query: str, candidates: list[Candidate]) -> MatchResult:
    """Pick the cheapest new and/or cheapest SH candidate for the query.

    Returns up to two winners: one with is_used=False, one with is_used=True.
    Both pass the platform/token/edition filters; the cheapest in each bucket wins.
    If the query itself specifies SH, only SH candidates are considered.
    """
    if not candidates:
        return MatchResult(winners=[])

    q_norm = _norm(query)
    q_editions = detect_editions(q_norm)
    q_wants_used: bool | None = True if detect_used(q_norm) else None

    # Per-candidate pre-processing
    enriched: list[tuple[Candidate, str, set[str], float]] = []
    for c in candidates:
        t_norm = _norm(c.title)
        platform_text = _norm(f"{c.title} {c.url or ''}")

        # 1) platform filter — check title + URL slug (some sites bury platform in slug only)
        if not detect_platform_ps5(platform_text):
            continue

        # 2) token coverage — every significant query token must be in the title
        if not all_query_tokens_present(q_norm, t_norm):
            continue

        # 3) edition filter
        t_editions = detect_editions(t_norm)
        if q_editions:
            if not q_editions.issubset(t_editions):
                continue

        # 4) hard used filter (only if the query explicitly asks for SH)
        if q_wants_used is True and not c.is_used:
            continue

        # 5) fuzzy score (ranking signal, not a hard gate)
        score = fuzz.token_set_ratio(q_norm, t_norm)
        if score < MIN_SCORE:
            continue

        enriched.append((c, t_norm, t_editions, score))

    if not enriched:
        return MatchResult(winners=[])

    def sort_key(item: tuple[Candidate, str, set[str], float]) -> tuple[float, int, float]:
        c, _t, edns, score = item
        is_non_base = 0 if (not edns or _BASE_EDITION_SENTINEL in edns) else 1
        return (c.price_ron, is_non_base, -score)

    winners: list[Candidate] = []
    for used_flag in (False, True):
        bucket = [e for e in enriched if e[0].is_used == used_flag]
        if not bucket:
            continue
        bucket.sort(key=sort_key)
        winners.append(bucket[0][0])

    return MatchResult(winners=winners)
