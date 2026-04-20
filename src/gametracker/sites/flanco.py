"""Flanco.ro scraper — Magento SERP → PDP microdata.

SERP has product names + URLs but client-side prices; PDP has schema.org microdata.
"""
from __future__ import annotations

import logging
from urllib.parse import quote_plus

from selectolax.parser import HTMLParser

from ..client import BlockedError, RateLimited, SiteClient
from ..matcher import Candidate, pre_filter_matches
from ..schema_org import extract_product
from .types import BLOCKED, ERROR, OK, SiteResult

log = logging.getLogger(__name__)

NAME = "flanco"
BASE = "https://www.flanco.ro"

MAX_PDP_FETCHES = 3


def _build_search_url(query: str) -> str:
    return f"{BASE}/catalogsearch/result/?q={quote_plus(query)}"


def parse_serp_previews(html: str) -> list[tuple[str, str]]:
    """Return list of (title, pdp_url) pairs from a Flanco search page."""
    tree = HTMLParser(html)
    seen: set[str] = set()
    out: list[tuple[str, str]] = []
    for a in tree.css("a.product-item-link"):
        href = (a.attributes.get("href") or "").strip()
        title = a.text(strip=True)
        if not href or not title or href in seen:
            continue
        seen.add(href)
        out.append((title, href))
    return out


async def _fetch_pdp(client: SiteClient, url: str, fallback_title: str) -> Candidate | None:
    r = await client.get(url)
    product = extract_product(r.text)
    if product is None or product.price is None:
        return None
    title = product.name or fallback_title
    return Candidate(
        title=title,
        price_ron=product.price,
        url=url,
        availability=product.availability,
        is_used=False,
    )


async def search(client: SiteClient, query: str) -> SiteResult:
    serp_url = _build_search_url(query)
    try:
        r = await client.get(serp_url, referer=f"{BASE}/")
    except BlockedError as e:
        return SiteResult(site=NAME, status=BLOCKED, error=str(e))
    except RateLimited as e:
        return SiteResult(site=NAME, status=BLOCKED, error=str(e))
    except Exception as e:
        return SiteResult(site=NAME, status=ERROR, error=f"{type(e).__name__}: {e}")

    previews = parse_serp_previews(r.text)
    if not previews:
        return SiteResult(site=NAME, status=OK, candidates=[], strategy_used="serp+pdp")

    # Pre-filter by title+url (no prices yet) — limit PDP fetches.
    filtered: list[tuple[str, str]] = [
        (t, u) for (t, u) in previews if pre_filter_matches(query, t, u)
    ][:MAX_PDP_FETCHES]

    if not filtered:
        return SiteResult(site=NAME, status=OK, candidates=[], strategy_used="serp+pdp")

    candidates: list[Candidate] = []
    for title, url in filtered:
        try:
            c = await _fetch_pdp(client, url, title)
        except BlockedError as e:
            return SiteResult(site=NAME, status=BLOCKED, error=str(e))
        except RateLimited as e:
            return SiteResult(site=NAME, status=BLOCKED, error=str(e))
        except Exception as e:
            log.debug("flanco PDP %s failed: %s", url, e)
            continue
        if c is not None:
            candidates.append(c)

    return SiteResult(site=NAME, status=OK, candidates=candidates, strategy_used="serp+pdp")
