"""Altex.ro scraper — uses fenrir.altex.ro JSON API.

Generous cooldown (60s default) to stay under Akamai's radar.

Also recognises Altex's "-N% extra in app" promotional labels and folds that
discount into the price we compare against history, since that is the real
price a shopper pays by ordering from Altex.
"""
from __future__ import annotations

import logging
import re
from typing import Any
from urllib.parse import quote

from ..client import BlockedError, RateLimited, SiteClient
from ..matcher import Candidate
from ..price import PriceParseError, parse_price
from .types import BLOCKED, ERROR, OK, SiteResult

log = logging.getLogger(__name__)

NAME = "altex"
BASE = "https://altex.ro"
_API = "https://fenrir.altex.ro/v2/catalog/search"

# Matches labels like "-20% extra in app" / "-25% extra aplicatie" / "-10% EXTRA"
_APP_DISCOUNT_RE = re.compile(r"-\s*(\d{1,2})\s*%\s*extra", re.IGNORECASE)


def _build_api_url(query: str, size: int = 48) -> str:
    return f"{_API}/{quote(query, safe='')}?size={size}"


def _product_url(url_key: str | None, sku: str | None) -> str:
    if url_key and sku:
        return f"{BASE}/{url_key}/cpp/{sku}/"
    if url_key:
        return f"{BASE}/{url_key}/"
    return BASE


def extract_app_discount_pct(labels: Any) -> int | None:
    """Scan fenrir's label_actions_serialized for a '-N% extra in app' label."""
    if not isinstance(labels, list):
        return None
    for lbl in labels:
        title = (lbl or {}).get("title") if isinstance(lbl, dict) else None
        if not isinstance(title, str):
            continue
        m = _APP_DISCOUNT_RE.search(title)
        if m:
            try:
                pct = int(m.group(1))
            except ValueError:
                continue
            if 0 < pct < 100:
                return pct
    return None


def parse_products(data: Any) -> list[Candidate]:
    if not isinstance(data, dict):
        return []
    prods = data.get("products") or []
    out: list[Candidate] = []
    for p in prods:
        name = p.get("name")
        if not name:
            continue
        try:
            price = parse_price(p.get("price"))
        except PriceParseError:
            continue

        display_title = str(name)
        pct = extract_app_discount_pct(p.get("label_actions_serialized"))
        if pct is not None:
            effective = round(price * (1 - pct / 100), 2)
            # Sanity check — apply only if it actually lowers the price.
            if effective < price:
                price = effective
                display_title = f"{display_title} (−{pct}% app)"

        stock = p.get("stock_status")
        if stock == 1:
            availability = "in_stock"
        elif stock == 0:
            availability = "out_of_stock"
        else:
            availability = None
        url = _product_url(p.get("url_key"), p.get("sku"))
        out.append(Candidate(
            title=display_title,
            price_ron=price,
            url=url,
            availability=availability,
            is_used=False,
        ))
    return out


async def search(client: SiteClient, query: str) -> SiteResult:
    url = _build_api_url(query)
    try:
        r = await client.get(
            url,
            headers={"Accept": "application/json", "x-client-type": "web"},
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
        candidates = parse_products(data)
    except Exception as e:
        return SiteResult(site=NAME, status=ERROR, error=f"parse: {e}")

    return SiteResult(site=NAME, status=OK, candidates=candidates, strategy_used="fenrir")
