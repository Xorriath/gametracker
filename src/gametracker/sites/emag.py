"""eMAG.ro scraper — SERP embedded JSON + inline prices; PDP ld+json fallback."""
from __future__ import annotations

import json
import logging
import re
from urllib.parse import quote

from selectolax.parser import HTMLParser

from ..client import BlockedError, RateLimited, SiteClient
from ..matcher import Candidate, pre_filter_matches
from ..price import PriceParseError, parse_price
from ..schema_org import extract_product
from .types import BLOCKED, ERROR, OK, SiteResult

log = logging.getLogger(__name__)

NAME = "emag"
BASE = "https://www.emag.ro"

MAX_PDP_FALLBACK = 3

_LISTING_ITEMS_RE = re.compile(
    r"EM\.listingGlobals\.items\s*=\s*(\[.*?\]);", re.DOTALL
)

# eMAG availability strings we've seen and their normalized form.
_AVAILABILITY_MAP: dict[str, str] = {
    "în stoc": "in_stock",
    "in stoc": "in_stock",
    "stoc limitat": "limited",
    "ultimul produs in stoc": "limited",
    "ultimul produs în stoc": "limited",
    "indisponibil": "out_of_stock",
    "la precomandă": "preorder",
    "la precomanda": "preorder",
    "la comandă": "preorder",
    "la comanda": "preorder",
}


def _slugify(query: str) -> str:
    """Convert a free-text query into eMAG's URL slug ('resident evil requiem' → 'resident-evil-requiem')."""
    q = query.strip().lower()
    q = re.sub(r"\s+", "-", q)
    q = re.sub(r"[^a-z0-9\-]", "", q)
    return q


def _build_search_url(query: str) -> str:
    return f"{BASE}/search/{quote(_slugify(query), safe='-')}"


def _normalize_availability(text: str | None) -> str | None:
    if not text:
        return None
    t = text.strip().lower()
    if t in _AVAILABILITY_MAP:
        return _AVAILABILITY_MAP[t]
    # Partial match: "Ultimele 3 produse" → limited
    if "ultimul" in t or "ultimele" in t:
        return "limited"
    if "precomand" in t or "comand" in t:
        return "preorder"
    if "indisponibil" in t or "stoc epuizat" in t:
        return "out_of_stock"
    if "stoc" in t:
        return "in_stock"
    return None


def _parse_listing_json(html: str) -> list[dict]:
    m = _LISTING_ITEMS_RE.search(html)
    if not m:
        return []
    try:
        data = json.loads(m.group(1))
    except json.JSONDecodeError:
        return []
    return data if isinstance(data, list) else []


def _extract_offer_map(html: str) -> dict[str, tuple[str | None, str]]:
    """Build a map offer_id → (price_text, pdp_url) by walking the outermost
    product cards (those that carry a data-url pointing at a /pd/ page).
    """
    tree = HTMLParser(html)
    result: dict[str, tuple[str | None, str]] = {}
    for card in tree.css("[data-url]"):
        data_url = (card.attributes.get("data-url") or "").strip()
        if "/pd/" not in data_url:
            continue
        url = data_url if data_url.startswith("http") else f"{BASE}{data_url}"
        offer_id = card.attributes.get("data-offer-id") or card.attributes.get("data-product-id")
        if not offer_id:
            # Fall back to using URL as the key
            offer_id = url
        if offer_id in result:
            continue
        price_node = card.css_first(".product-new-price")
        price_text = price_node.html if price_node else None
        result[str(offer_id)] = (price_text, url)
    return result


def parse_serp(html: str) -> list[Candidate]:
    """Combine EM.listingGlobals.items (names + availability) with DOM prices+URLs.

    Pairs the JSON item and DOM card by offer.id when possible; falls back to
    DOM order otherwise.
    """
    items = _parse_listing_json(html)
    offer_map = _extract_offer_map(html)
    out: list[Candidate] = []

    for i, item in enumerate(items):
        name = item.get("name")
        if not name:
            continue
        offer_id = str((item.get("offer") or {}).get("id") or "")
        price_text, url = (None, None)
        if offer_id and offer_id in offer_map:
            price_text, url = offer_map[offer_id]
        else:
            # Fall back to DOM order (i-th outer card)
            try:
                price_text, url = list(offer_map.values())[i]
            except IndexError:
                pass
        if not url:
            continue
        try:
            price = parse_price(price_text) if price_text else None
        except PriceParseError:
            price = None
        if price is None:
            continue
        avail_text = (item.get("offer") or {}).get("availability", {}).get("text")
        availability = _normalize_availability(avail_text)
        out.append(Candidate(
            title=str(name),
            price_ron=price,
            url=url,
            availability=availability,
            is_used=False,
        ))
    return out


async def _pdp_fallback(client: SiteClient, url: str, fallback_title: str) -> Candidate | None:
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

    candidates = parse_serp(r.text)
    strategy = "serp-json"

    # If SERP extraction yielded nothing useful (e.g. markup shift), fall back to
    # finding PDP URLs on the page and extracting ld+json from each.
    if not candidates:
        tree = HTMLParser(r.text)
        pdp_urls: list[str] = []
        seen: set[str] = set()
        for a in tree.css("a[href]"):
            href = a.attributes.get("href") or ""
            if "/pd/" in href:
                full = href if href.startswith("http") else f"{BASE}{href}"
                if full not in seen:
                    seen.add(full)
                    pdp_urls.append(full)
        # Pre-filter by URL alone (titles unknown here); apply platform check via URL
        picked: list[str] = []
        for u in pdp_urls:
            if pre_filter_matches(query, "", u):
                picked.append(u)
                if len(picked) >= MAX_PDP_FALLBACK:
                    break
        if picked:
            strategy = "pdp-fallback"
            for u in picked:
                try:
                    c = await _pdp_fallback(client, u, fallback_title="")
                except BlockedError as e:
                    return SiteResult(site=NAME, status=BLOCKED, error=str(e))
                except RateLimited as e:
                    return SiteResult(site=NAME, status=BLOCKED, error=str(e))
                except Exception as e:
                    log.debug("emag PDP %s failed: %s", u, e)
                    continue
                if c is not None:
                    candidates.append(c)

    return SiteResult(site=NAME, status=OK, candidates=candidates, strategy_used=strategy)
