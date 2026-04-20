"""Extract schema.org Product data from HTML.

Supports both encodings seen in our research:
  - ld+json Product (jocurinoi, eMAG, trendyol)
  - microdata <meta itemprop="..."> (flanco)
"""
from __future__ import annotations

import html as html_mod
import json
import re
from dataclasses import dataclass

from .price import PriceParseError, parse_price

_LDJSON_RE = re.compile(
    r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
    re.DOTALL | re.IGNORECASE,
)

_META_ITEMPROP_RE = re.compile(
    r'<meta\s+[^>]*itemprop=["\']([^"\']+)["\'][^>]*content=["\']([^"\']*)["\']',
    re.IGNORECASE,
)
_META_ITEMPROP_ALT_RE = re.compile(
    r'<meta\s+[^>]*content=["\']([^"\']*)["\'][^>]*itemprop=["\']([^"\']+)["\']',
    re.IGNORECASE,
)
_SPAN_ITEMPROP_RE = re.compile(
    r'<([a-z0-9]+)[^>]*itemprop=["\']([^"\']+)["\'][^>]*>([^<]*)</\1>',
    re.IGNORECASE,
)


@dataclass(frozen=True)
class SchemaProduct:
    name: str | None
    price: float | None
    currency: str | None
    availability: str | None
    sku: str | None
    brand: str | None
    url: str | None


_STOCK_URL_MAP = {
    "instock": "in_stock",
    "outofstock": "out_of_stock",
    "preorder": "preorder",
    "discontinued": "out_of_stock",
    "limitedavailability": "limited",
    "onlineonly": "in_stock",
    "soldout": "out_of_stock",
    "backorder": "preorder",
}


def _normalize_availability(raw: str | None) -> str | None:
    if not raw:
        return None
    # schema.org URLs: http://schema.org/InStock, https://schema.org/OutOfStock
    tail = raw.strip().lower().rsplit("/", 1)[-1]
    return _STOCK_URL_MAP.get(tail, None)


def _as_product(obj) -> dict | None:
    """Return obj (or a sub-node) if it's a Product, else None."""
    if not isinstance(obj, dict):
        return None
    t = obj.get("@type")
    if t == "Product" or (isinstance(t, list) and "Product" in t):
        return obj
    # Some sites wrap Product inside @graph
    graph = obj.get("@graph")
    if isinstance(graph, list):
        for g in graph:
            got = _as_product(g)
            if got:
                return got
    return None


def _flatten_offers(offers) -> dict:
    if isinstance(offers, list):
        return offers[0] if offers else {}
    if isinstance(offers, dict):
        return offers
    return {}


def _extract_ldjson(html: str) -> SchemaProduct | None:
    for block in _LDJSON_RE.findall(html):
        try:
            data = json.loads(block.strip())
        except json.JSONDecodeError:
            continue
        items = data if isinstance(data, list) else [data]
        for item in items:
            prod = _as_product(item)
            if not prod:
                continue
            offers = _flatten_offers(prod.get("offers"))
            name = prod.get("name")
            if isinstance(name, list):
                name = name[0] if name else None
            brand = prod.get("brand")
            if isinstance(brand, dict):
                brand = brand.get("name")
            try:
                price = parse_price(offers.get("price"))
            except PriceParseError:
                price = None
            return SchemaProduct(
                name=str(name).strip() if name else None,
                price=price,
                currency=offers.get("priceCurrency"),
                availability=_normalize_availability(offers.get("availability")),
                sku=str(prod.get("sku")) if prod.get("sku") else None,
                brand=str(brand).strip() if brand else None,
                url=prod.get("url") if isinstance(prod.get("url"), str) else None,
            )
    return None


def _extract_microdata(html: str) -> SchemaProduct | None:
    props: dict[str, str] = {}
    for name, value in _META_ITEMPROP_RE.findall(html):
        props.setdefault(name.strip().lower(), html_mod.unescape(value))
    for value, name in _META_ITEMPROP_ALT_RE.findall(html):
        props.setdefault(name.strip().lower(), html_mod.unescape(value))
    for _tag, name, value in _SPAN_ITEMPROP_RE.findall(html):
        key = name.strip().lower()
        val = value.strip()
        if key and val:
            props.setdefault(key, html_mod.unescape(val))

    if not props.get("price") and not props.get("priceCurrency".lower()):
        return None

    try:
        price = parse_price(props.get("price")) if props.get("price") else None
    except PriceParseError:
        price = None

    name = props.get("name")
    return SchemaProduct(
        name=name.strip() if name else None,
        price=price,
        currency=props.get("pricecurrency"),
        availability=_normalize_availability(props.get("availability")),
        sku=props.get("sku"),
        brand=props.get("brand"),
        url=props.get("url"),
    )


def extract_product(html: str) -> SchemaProduct | None:
    """Extract a schema.org Product from HTML, trying ld+json first then microdata."""
    prod = _extract_ldjson(html)
    if prod and (prod.price is not None or prod.name):
        return prod
    return _extract_microdata(html)
