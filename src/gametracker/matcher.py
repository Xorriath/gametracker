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

# Tokens that disambiguate game versions but that some storefronts drop from
# the official title (e.g. PS Store calls "Resident Evil 4 Remake" just
# "Resident Evil 4" — Capcom rebranded on the storefront). Treat these as
# OPTIONAL for token-coverage: a candidate missing them isn't disqualified, but
# candidates that DO contain them are preferred via the match sort key.
SOFT_TOKENS: frozenset[str] = frozenset({
    "remake", "remaster", "remastered",
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
    # Multi-game bundles — also non-base, so a "Trilogy" or "Collection" SKU
    # never outranks the standalone game when the user queries the standalone.
    "trilogy",
    "bundle",
    "collection",
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

# Product categories that are NOT the game itself but often carry the game's
# name in their title (cases, merch, DLC, etc.). Normalized form used (no
# diacritics) since detection runs after `normalize_query`.
# Multi-word phrases MUST be whole-word matches; single words are matched as
# whole tokens to avoid false positives (e.g. "case" as a substring of "case").
ACCESSORY_MARKERS: tuple[str, ...] = (
    # Cases / covers (RO + EN)
    "husa", "husa de protectie", "carcasa", "case", "cover",
    # Skins / stickers / themes
    "skin", "sticker", "autocolant", "theme",
    # Grips / accessories
    "grip", "maner", "manere",
    # Collectibles / merchandise
    "figurina", "figurine", "figure", "funko", "pop vinyl",
    "artbook", "poster", "keychain", "breloc",
    "mug", "cana", "tricou", "t shirt", "tshirt",
    # PS Store in-game cosmetic DLC packs (very common, all carry the game name)
    "charm", "costume", "ticket", "accessory", "accessoire",
    # Audio / OST / guides (not the game)
    "soundtrack", "coloana sonora", "strategy guide",
    # Stands / controllers / peripherals
    "stand", "headset", "casca",
    # DLC / season passes / upgrades (base-game search shouldn't pick these up)
    "dlc", "season pass", "expansion pass",
    "add on", "add-on", "addon",
    "upgrade",  # PS Store "Digital Deluxe Edition Upgrade", cross-gen upgrades, etc.
)

_ACCESSORY_RE = re.compile(
    # Allow an optional 's' or 'es' suffix so "costumes"/"charms"/"accessories"
    # match the singular markers without us having to enumerate plurals.
    r"\b(" + "|".join(re.escape(m) for m in ACCESSORY_MARKERS) + r")(?:s|es)?\b",
    re.IGNORECASE,
)


def detect_accessory(text_norm: str) -> bool:
    """True if the title describes an accessory, merchandise, or DLC rather
    than the game itself. Used to drop false matches where a case/figurine/OST
    carries the game's name."""
    return bool(_ACCESSORY_RE.search(text_norm))


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
    # Keep short numeric tokens (sequel numbers like "4" in "Ninja Gaiden 4")
    # that would otherwise be filtered out by the length-2 cutoff.
    # SOFT_TOKENS are excluded too — coverage shouldn't fail just because a
    # storefront dropped a "remake" suffix. Soft tokens are still used as a
    # ranking signal in `match()` to prefer candidates that DO carry them.
    return [
        t for t in text_norm.split()
        if t not in STOPWORDS and t not in SOFT_TOKENS and (len(t) >= 2 or t.isdigit())
    ]


def _query_soft_tokens(query_norm: str) -> set[str]:
    return {t for t in query_norm.split() if t in SOFT_TOKENS}


def _adjacent_pairs_present(query_norm: str, title_norm: str) -> bool:
    """Numeric query tokens must be adjacent to the preceding word in the title.

    Prevents a title like "2× silicone grips Death Stranding PS5" from matching
    "Death Stranding 2": both contain the tokens {death, stranding, 2, ps5}, but
    only the real sequel has "stranding 2" as an adjacent token pair.

    Only enforced for digit tokens that aren't the first token of the query —
    those are the ones that carry sequel/version semantics.
    """
    q_tokens = query_norm.split()
    title_tokens = title_norm.split()
    if len(title_tokens) < 2:
        return True
    # Build the set of adjacent bigrams in the title for O(1) lookup.
    title_bigrams = {
        (title_tokens[i], title_tokens[i + 1])
        for i in range(len(title_tokens) - 1)
    }
    for i, qt in enumerate(q_tokens):
        if i == 0 or not qt.isdigit():
            continue
        prev = q_tokens[i - 1]
        # Skip bigram check when the preceding token is a filler word — the
        # query "the 5" makes no sense, but being defensive here avoids surprises.
        if prev in STOPWORDS or len(prev) < 2:
            continue
        if (prev, qt) not in title_bigrams:
            return False
    return True


def all_query_tokens_present(query_norm: str, title_norm: str) -> bool:
    """Every significant query token must appear in the title, and any numeric
    sequel token must sit adjacent to its preceding query word."""
    for qt in _significant_tokens(query_norm):
        if qt not in title_norm:
            return False
    return _adjacent_pairs_present(query_norm, title_norm)


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
    # Accessory/merch filter — only reject when the query isn't itself an accessory.
    if detect_accessory(t_norm) and not detect_accessory(q_norm):
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
    q_is_accessory = detect_accessory(q_norm)
    q_soft = _query_soft_tokens(q_norm)

    # Per-candidate pre-processing
    enriched: list[tuple[Candidate, str, set[str], float, int]] = []
    for c in candidates:
        t_norm = _norm(c.title)
        platform_text = _norm(f"{c.title} {c.url or ''}")

        # 1) platform filter — check title + URL slug (some sites bury platform in slug only)
        if not detect_platform_ps5(platform_text):
            continue

        # 2) token coverage — every significant query token must be in the title
        if not all_query_tokens_present(q_norm, t_norm):
            continue

        # 3) accessory/merch filter — drop cases, grips, figurines, DLC, OSTs, etc.
        #    unless the user was explicitly searching for an accessory.
        if detect_accessory(t_norm) and not q_is_accessory:
            continue

        # 4) edition filter
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

        # Soft-token coverage: query "Resident Evil 4 Remake" prefers candidates
        # whose titles also contain "remake" over candidates that omit it.
        # Penalty=0 when query has no soft tokens or the candidate carries them
        # all; penalty=1 when the candidate is missing one or more.
        soft_penalty = 0 if (not q_soft or q_soft.issubset(set(t_norm.split()))) else 1
        enriched.append((c, t_norm, t_editions, score, soft_penalty))

    if not enriched:
        return MatchResult(winners=[])

    def sort_key(
        item: tuple[Candidate, str, set[str], float, int],
    ) -> tuple[int, int, float, float]:
        c, _t, edns, score, soft_penalty = item
        is_non_base = 0 if (not edns or _BASE_EDITION_SENTINEL in edns) else 1
        # Base editions always beat non-base — matching the base game is
        # semantically correct even if a Deluxe/Premium tier happens to be on
        # sale for less. Within the same edition tier: prefer titles that carry
        # the query's soft tokens (e.g. "remake"), then cheapest, then highest
        # fuzzy score.
        return (is_non_base, soft_penalty, c.price_ron, -score)

    winners: list[Candidate] = []
    for used_flag in (False, True):
        bucket = [e for e in enriched if e[0].is_used == used_flag]
        if not bucket:
            continue
        bucket.sort(key=sort_key)
        winners.append(bucket[0][0])

    return MatchResult(winners=winners)
