"""PlayStation Store (en-ro) scraper.

Sony's storefront server-renders search results to HTML for SEO, so we can pull
prices in a single GET against `store.playstation.com/en-ro/search/<query>`.
Each product tile carries a `data-telemetry-meta` JSON attribute containing the
canonical id, name, and locale-formatted price — that's our parse target,
because it's the same blob the frontend reports to telemetry, not free-text
prone to layout drift.

Only PS5 listings are kept. Free items, DLC/upgrades, costumes, demos, and
tiles without a numeric price are filtered out — `parse_price` raises on
non-numeric strings like "Free" so those tiles drop naturally.
"""
from __future__ import annotations

import json
import logging
from typing import Any
from urllib.parse import quote

from selectolax.parser import HTMLParser

from ..client import BlockedError, RateLimited, SiteClient
from ..matcher import Candidate
from ..price import PriceParseError, parse_price
from .types import BLOCKED, ERROR, OK, SiteResult

log = logging.getLogger(__name__)

NAME = "psstore"
BASE = "https://store.playstation.com"


def _build_search_url(query: str) -> str:
    return f"{BASE}/en-ro/search/{quote(query, safe='')}"


def _product_url(product_id: str) -> str:
    return f"{BASE}/en-ro/product/{product_id}"


# PS Store tags every tile with a type label (Costume, Item, Add-on, …) shown
# under the title. Empty label = full game. We only accept these two — every
# other label is in-game-cosmetic / DLC / merch with the game's name attached.
_PRODUCT_TYPES_ALLOWED = frozenset({"", "Game Bundle"})


def _extract_type_label(tile) -> str:
    """Return the small product-type label PS Store puts under each title.

    PS Store renders this as a span with class `psw-c-t-2` (sometimes paired
    with `psw-t-secondary`). For full games the span is absent or empty; for
    cosmetics/DLC it carries text like "Costume", "Item", "Add-on", "Weapons".
    """
    for span in tile.css("span"):
        cls = span.attributes.get("class") or ""
        if "psw-c-t-2" not in cls:
            continue
        text = span.text(strip=True)
        if text:
            return text
    return ""


def parse_search_results(html: str) -> list[Candidate]:
    """Extract PS5 product candidates from a /en-ro/search HTML page."""
    tree = HTMLParser(html)
    out: list[Candidate] = []
    seen_ids: set[str] = set()

    # Top-level tile wrappers carry both `data-qa^="search#productTile"` and
    # `data-qa-index` — the inner <a> shares the same data-qa, so the index
    # attribute is what disambiguates the wrapper from its descendants.
    for tile in tree.css('[data-qa^="search#productTile"][data-qa-index]'):
        anchor = tile.css_first("a[data-telemetry-meta]")
        if anchor is None:
            continue
        href = (anchor.attributes.get("href") or "").strip()
        if not href.startswith("/en-ro/product/"):
            continue

        # Only full games (empty label) and explicit Game Bundles get through.
        # Costumes, Items, Add-ons, Weapons, Demos, etc. are dropped at parse
        # time so the matcher never even sees them.
        type_label = _extract_type_label(tile)
        if type_label not in _PRODUCT_TYPES_ALLOWED:
            continue

        meta_raw = anchor.attributes.get("data-telemetry-meta") or ""
        try:
            meta: dict[str, Any] = json.loads(meta_raw)
        except (json.JSONDecodeError, TypeError):
            continue

        product_id = str(meta.get("id") or "").strip()
        name = str(meta.get("name") or "").strip()
        price_raw = meta.get("price")
        if not product_id or not name or product_id in seen_ids:
            continue

        # PS Store tiles include "PS5"/"PS4" badges as plain text inside the
        # tile. We only want PS5 listings — drop pure PS4 results so the
        # matcher doesn't have to.
        tile_text = tile.text(separator=" ", strip=True)
        if "PS5" not in tile_text:
            continue

        try:
            price_ron = parse_price(price_raw)
        except PriceParseError:
            # Non-numeric price (e.g. "Free", "Add-On Pack"): not a sellable
            # game listing in the sense we care about.
            continue

        url = _product_url(product_id)
        # Encode platform in the title so the matcher's PS5 detection passes
        # without us having to special-case the URL slug.
        display_title = f"{name} (PS5)" if "PS5" not in name.upper() else name

        seen_ids.add(product_id)
        out.append(Candidate(
            title=display_title,
            price_ron=price_ron,
            url=url,
            availability=None,
            is_used=False,
        ))

    return out


async def search(client: SiteClient, query: str) -> SiteResult:
    url = _build_search_url(query)
    try:
        r = await client.get(url, referer=f"{BASE}/en-ro/")
    except BlockedError as e:
        return SiteResult(site=NAME, status=BLOCKED, error=str(e))
    except RateLimited as e:
        return SiteResult(site=NAME, status=BLOCKED, error=str(e))
    except Exception as e:
        return SiteResult(site=NAME, status=ERROR, error=f"{type(e).__name__}: {e}")

    try:
        candidates = parse_search_results(r.text)
    except Exception as e:
        return SiteResult(site=NAME, status=ERROR, error=f"parse: {e}")

    return SiteResult(site=NAME, status=OK, candidates=candidates, strategy_used="serp")
