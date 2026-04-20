"""Trendyol.com/ro scraper — SERP → PDP ld+json."""
from __future__ import annotations

import logging
from urllib.parse import quote_plus

from selectolax.parser import HTMLParser

from ..client import BlockedError, RateLimited, SiteClient
from ..matcher import Candidate, pre_filter_matches
from ..schema_org import extract_product
from .types import BLOCKED, ERROR, OK, SiteResult

log = logging.getLogger(__name__)

NAME = "trendyol"
BASE = "https://www.trendyol.com"

MAX_PDP_FETCHES = 3


def _build_search_url(query: str) -> str:
    return f"{BASE}/ro/sr?q={quote_plus(query)}"


def parse_serp_previews(html: str) -> list[tuple[str, str]]:
    """Return (title, url) previews from Trendyol RO SERP."""
    tree = HTMLParser(html)
    out: list[tuple[str, str]] = []
    seen: set[str] = set()
    for card in tree.css("a.product-card"):
        href = (card.attributes.get("href") or "").strip()
        if not href:
            continue
        # Drop query string — boutiqueId / merchantId vary per session
        canonical = href.split("?")[0]
        if canonical in seen:
            continue
        seen.add(canonical)
        brand = card.css_first(".product-brand")
        name = card.css_first(".product-name")
        brand_t = brand.text(strip=True) if brand else ""
        name_t = name.text(strip=True) if name else ""
        title = f"{brand_t} {name_t}".strip()
        if not title:
            continue
        url = canonical if canonical.startswith("http") else f"{BASE}{canonical}"
        out.append((title, url))
    return out


async def _fetch_pdp(client: SiteClient, url: str, fallback_title: str) -> Candidate | None:
    r = await client.get(url)
    product = extract_product(r.text)
    if product is None or product.price is None:
        return None
    if product.currency and product.currency != "RON":
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
        r = await client.get(serp_url, referer=f"{BASE}/ro")
    except BlockedError as e:
        return SiteResult(site=NAME, status=BLOCKED, error=str(e))
    except RateLimited as e:
        return SiteResult(site=NAME, status=BLOCKED, error=str(e))
    except Exception as e:
        return SiteResult(site=NAME, status=ERROR, error=f"{type(e).__name__}: {e}")

    previews = parse_serp_previews(r.text)
    filtered = [(t, u) for (t, u) in previews if pre_filter_matches(query, t, u)][:MAX_PDP_FETCHES]

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
            log.debug("trendyol PDP %s failed: %s", url, e)
            continue
        if c is not None:
            candidates.append(c)

    return SiteResult(site=NAME, status=OK, candidates=candidates, strategy_used="serp+pdp")
