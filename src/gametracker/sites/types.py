"""Shared types for site scrapers."""
from __future__ import annotations

from dataclasses import dataclass, field

from ..matcher import Candidate

# Status values returned by a scraper (pre-match).
OK = "ok"              # returned candidates (possibly empty list = "no results on site")
BLOCKED = "blocked"    # 403, challenge page, rate-limited
ERROR = "error"        # network, parse, or unexpected failure


@dataclass
class SiteResult:
    """Raw candidates returned by a single site, pre-matcher."""
    site: str
    status: str
    candidates: list[Candidate] = field(default_factory=list)
    strategy_used: str | None = None
    error: str | None = None  # human-readable reason when status != 'ok'
