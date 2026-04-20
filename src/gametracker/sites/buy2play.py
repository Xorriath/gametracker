"""Buy2play.ro scraper — Shopify public search API.

Uses /search/suggest.json — the supported, public Shopify endpoint. No auth.
Detects "Second-Hand SH" items and marks them as used.
"""
from __future__ import annotations

import logging
import re
from urllib.parse import quote_plus

from ..client import BlockedError, RateLimited, SiteClient
from ..matcher import Candidate
from ..price import PriceParseError, parse_price
from .types import BLOCKED, ERROR, OK, SiteResult

log = logging.getLogger(__name__)

NAME = "buy2play"
BASE = "https://buy2play.ro"

# Detect SH / Second-Hand markers in buy2play titles.
# Observed in scout: "Joc Resident Evil 6 Second-Hand SH"
_SH_RE = re.compile(r"\b(sh|second[\s-]?hand)\b", re.IGNORECASE)


def _build_url(query: str, limit: int = 10) -> str:
    q = quote_plus(query)
    return (
        f"{BASE}/search/suggest.json?q={q}"
        f"&resources%5Btype%5D=product&resources%5Blimit%5D={limit}"
    )


def is_used(title: str) -> bool:
    return bool(_SH_RE.search(title))


def parse_suggest(data: dict) -> list[Candidate]:
    products = (
        data.get("resources", {}).get("results", {}).get("products", [])
        if isinstance(data, dict)
        else []
    )
    out: list[Candidate] = []
    for p in products:
        title = p.get("title")
        url = p.get("url")
        if not title or not url:
            continue
        try:
            price = parse_price(p.get("price"))
        except PriceParseError:
            continue
        available = p.get("available")
        availability = (
            "in_stock" if available is True else ("out_of_stock" if available is False else None)
        )
        out.append(Candidate(
            title=str(title),
            price_ron=price,
            url=url if str(url).startswith("http") else f"{BASE}{url}",
            availability=availability,
            is_used=is_used(str(title)),
        ))
    return out


async def search(client: SiteClient, query: str) -> SiteResult:
    url = _build_url(query)
    try:
        r = await client.get(
            url,
            headers={"Accept": "application/json"},
            referer=f"{BASE}/",
        )
    except BlockedError as e:
        return SiteResult(site=NAME, status=BLOCKED, error=str(e))
    except RateLimited as e:
        return SiteResult(site=NAME, status=BLOCKED, error=str(e))
    except Exception as e:
        return SiteResult(site=NAME, status=ERROR, error=f"{type(e).__name__}: {e}")

    try:
        data = r.json()
    except Exception as e:
        return SiteResult(site=NAME, status=ERROR, error=f"bad json: {e}")

    try:
        candidates = parse_suggest(data)
    except Exception as e:
        return SiteResult(site=NAME, status=ERROR, error=f"parse: {e}")

    return SiteResult(site=NAME, status=OK, candidates=candidates, strategy_used="shopify-suggest")
