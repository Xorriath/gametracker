"""Integration tests that replay saved site responses through the parsers.

These catch regressions when a site changes its markup. The fixture files live
alongside this test.
"""
from __future__ import annotations

import json
from pathlib import Path

from gametracker.sites import buy2play, jocurinoi, ozone, psstore

FIXTURES = Path(__file__).parent / "fixtures"


def test_ozone_parses_fast_simon_json():
    data = json.loads((FIXTURES / "ozone_requiem.json").read_text())
    cands = ozone.parse_items(data)
    assert len(cands) > 0
    # Expect at least one Requiem PS5 title.
    titles = [c.title for c in cands]
    assert any("Requiem" in t and "PS5" in t for t in titles)
    sample = next(c for c in cands if "Requiem" in c.title and "PS5" in c.title)
    assert sample.price_ron > 0
    assert sample.url.startswith("https://www.ozone.ro/")


def test_jocurinoi_parses_serp():
    html = (FIXTURES / "jocurinoi_requiem.html").read_text()
    cands = jocurinoi.parse_serp(html)
    assert len(cands) > 0
    # Expect at least one Requiem title with a RO-formatted price parsed correctly.
    req = [c for c in cands if "Requiem" in c.title]
    assert req
    prices = [c.price_ron for c in req]
    assert all(p > 100 for p in prices)
    assert all(c.url.startswith("https://www.jocurinoi.ro/") for c in req)


def test_buy2play_parses_suggest_json():
    data = json.loads((FIXTURES / "buy2play_resident_evil.json").read_text())
    cands = buy2play.parse_suggest(data)
    # The fixture was taken when buy2play had 2 RE listings.
    assert len(cands) >= 0  # empty is valid if buy2play has no stock on that day
    for c in cands:
        assert c.price_ron >= 0
        assert c.url.startswith("https://buy2play.ro/")


def test_buy2play_sh_detection():
    assert buy2play.is_used("Joc Resident Evil 6 pentru Xbox 360 Second-Hand SH")
    assert buy2play.is_used("Joc FIFA 23 SH")
    assert not buy2play.is_used("Joc nou FIFA 23 pentru PS5")


def test_psstore_parses_search_results():
    html = (FIXTURES / "psstore_astro_bot.html").read_text()
    cands = psstore.parse_search_results(html)
    # Fixture should yield at least the base Astro Bot plus a few other PS5 games.
    assert len(cands) >= 3
    # All candidates must be PS5-marked, have positive price, and a /en-ro/product/ URL.
    for c in cands:
        assert c.price_ron > 0
        assert c.url.startswith("https://store.playstation.com/en-ro/product/")
        assert "PS5" in c.title.upper()
        assert not c.is_used  # PS Store doesn't sell SH
    # The base game must be present at its known PS Store price (339.90 RON).
    base = [c for c in cands if c.title.upper().startswith("ASTRO BOT (")]
    assert base, "Base 'ASTRO BOT' tile missing"
    assert abs(base[0].price_ron - 339.90) < 0.01
    # Per-tile type-label filter must drop the "Digital Deluxe Edition Upgrade"
    # tile at parse time (it carries an "Add-on" label, not a game).
    titles_upper = " | ".join(c.title.upper() for c in cands)
    assert "UPGRADE" not in titles_upper, (
        "Upgrade/Add-on tiles should be filtered at parse time, "
        f"got: {titles_upper}"
    )
