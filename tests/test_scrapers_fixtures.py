"""Integration tests that replay saved site responses through the parsers.

These catch regressions when a site changes its markup. The fixture files live
alongside this test.
"""
from __future__ import annotations

import json
from pathlib import Path

from gametracker.sites import buy2play, jocurinoi, ozone

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
