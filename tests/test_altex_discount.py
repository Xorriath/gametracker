"""Test altex in-app discount extraction and application."""
from __future__ import annotations

import json
from pathlib import Path

from gametracker.sites.altex import (
    extract_app_discount_pct,
    parse_products,
)

FIXTURES = Path(__file__).parent / "fixtures"


def test_extract_pct_basic():
    labels = [{"title": "-20% extra in app"}]
    assert extract_app_discount_pct(labels) == 20


def test_extract_pct_romanian():
    labels = [{"title": "-25% extra aplicatie"}]
    assert extract_app_discount_pct(labels) == 25


def test_extract_pct_case_insensitive():
    labels = [{"title": "-10% EXTRA in APP"}]
    assert extract_app_discount_pct(labels) == 10


def test_extract_pct_no_match():
    labels = [{"title": "Gratuit livrare"}, {"title": "Stoc epuizat"}]
    assert extract_app_discount_pct(labels) is None


def test_extract_pct_empty_or_none():
    assert extract_app_discount_pct(None) is None
    assert extract_app_discount_pct([]) is None
    assert extract_app_discount_pct([{"image": "/img.png"}]) is None


def test_extract_pct_rejects_bogus_values():
    # Out-of-range percentages should be rejected.
    assert extract_app_discount_pct([{"title": "-0% extra in app"}]) is None
    assert extract_app_discount_pct([{"title": "-100% extra in app"}]) is None


def test_parse_products_applies_discount():
    data = json.loads((FIXTURES / "altex_stellar_blade.json").read_text())
    cands = parse_products(data)
    assert len(cands) == 1
    c = cands[0]
    # 239.94 with -20% extra = 191.95
    assert c.price_ron == 191.95
    assert "−20% app" in c.title
    assert "Stellar Blade PS5" in c.title
    assert c.availability == "in_stock"


def test_parse_products_no_discount_leaves_price_alone():
    data = json.loads((FIXTURES / "altex_stellar_blade.json").read_text())
    # strip the promo label and re-parse
    data["products"][0]["label_actions_serialized"] = []
    cands = parse_products(data)
    assert cands[0].price_ron == 239.94
    assert "app" not in cands[0].title
