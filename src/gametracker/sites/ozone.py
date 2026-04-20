"""Ozone.ro scraper — uses Fast Simon's public full_text_search API.

The UUID is the store's public Fast Simon identifier, exposed in Ozone's site HTML.
No auth, no referer check required.
"""
from __future__ import annotations

import logging
from typing import Any
from urllib.parse import quote_plus

from ..client import BlockedError, RateLimited, SiteClient
from ..matcher import Candidate
from ..price import PriceParseError, parse_price
from .types import BLOCKED, ERROR, OK, SiteResult

log = logging.getLogger(__name__)

NAME = "ozone"
UUID = "71de66ee-d2e2-4de2-b0bd-f51e3f0ee99e"
STORE_ID = 1

_API = "https://api.fastsimon.com/full_text_search"


def _build_url(query: str, per_page: int = 24, page: int = 1) -> str:
    return (
        f"{_API}?UUID={UUID}&store_id={STORE_ID}&q={quote_plus(query)}"
        f"&products_per_page={per_page}&page_num={page}"
        f"&sort_by=relevency&with_product_attributes=true"
        f"&facets_required=1&api_type=json"
    )


def parse_items(data: Any) -> list[Candidate]:
    """Convert Fast Simon API response JSON into Candidates."""
    if not isinstance(data, dict):
        return []
    items = data.get("items") or []
    out: list[Candidate] = []
    for it in items:
        title = it.get("l")
        url = it.get("u")
        if not title or not url:
            continue
        try:
            price = parse_price(it.get("p"))
        except PriceParseError:
            continue
        currency = it.get("c", "RON")
        if currency and currency != "RON":
            # Defensive: skip non-RON listings.
            continue
        iso = it.get("iso")
        availability = "out_of_stock" if iso else "in_stock"
        out.append(Candidate(
            title=str(title),
            price_ron=price,
            url=str(url),
            availability=availability,
            is_used=False,
        ))
    return out


async def search(client: SiteClient, query: str) -> SiteResult:
    url = _build_url(query)
    try:
        r = await client.get(
            url,
            headers={"Accept": "application/json"},
            referer="https://www.ozone.ro/",
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
        candidates = parse_items(data)
    except Exception as e:
        return SiteResult(site=NAME, status=ERROR, error=f"parse: {e}")

    return SiteResult(site=NAME, status=OK, candidates=candidates, strategy_used="fastsimon")
