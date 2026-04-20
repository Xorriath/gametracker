"""Jocurinoi.ro scraper — OpenCart SERP HTML with visible prices."""
from __future__ import annotations

import logging
from urllib.parse import quote_plus

from selectolax.parser import HTMLParser

from ..client import BlockedError, RateLimited, SiteClient
from ..matcher import Candidate
from ..price import PriceParseError, parse_price
from .types import BLOCKED, ERROR, OK, SiteResult

log = logging.getLogger(__name__)

NAME = "jocurinoi"
BASE = "https://www.jocurinoi.ro"


def _build_search_url(query: str) -> str:
    return f"{BASE}/toate-jocurile?search={quote_plus(query)}"


def _best_title_link(layout) -> tuple[str, str] | None:
    """Pick the anchor with a title attribute (OpenCart product link) and return (title, href)."""
    titled = layout.css("a[title]")
    for a in titled:
        title = (a.attributes.get("title") or "").strip()
        href = a.attributes.get("href") or ""
        if title and href:
            return title, href
    # Fallback: an anchor whose text is non-empty.
    for a in layout.css("a[href]"):
        text = a.text(strip=True)
        href = a.attributes.get("href") or ""
        if text and href:
            return text, href
    return None


def parse_serp(html: str) -> list[Candidate]:
    tree = HTMLParser(html)
    out: list[Candidate] = []
    for layout in tree.css(".product-layout"):
        price_node = layout.css_first(".price")
        if not price_node:
            continue
        got = _best_title_link(layout)
        if not got:
            continue
        title, href = got
        try:
            price = parse_price(price_node.text(strip=True))
        except PriceParseError:
            continue
        out.append(Candidate(
            title=title,
            price_ron=price,
            url=href if href.startswith("http") else f"{BASE}{href}",
            availability=None,
            is_used=False,
        ))
    return out


async def search(client: SiteClient, query: str) -> SiteResult:
    url = _build_search_url(query)
    try:
        r = await client.get(url, referer=f"{BASE}/")
    except BlockedError as e:
        return SiteResult(site=NAME, status=BLOCKED, error=str(e))
    except RateLimited as e:
        return SiteResult(site=NAME, status=BLOCKED, error=str(e))
    except Exception as e:
        return SiteResult(site=NAME, status=ERROR, error=f"{type(e).__name__}: {e}")

    try:
        candidates = parse_serp(r.text)
    except Exception as e:
        return SiteResult(site=NAME, status=ERROR, error=f"parse: {e}")

    return SiteResult(site=NAME, status=OK, candidates=candidates, strategy_used="serp")
