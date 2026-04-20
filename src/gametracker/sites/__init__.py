"""Site scrapers registry."""
from __future__ import annotations

from typing import Awaitable, Callable

from . import altex, buy2play, emag, flanco, jocurinoi, ozone, trendyol
from .types import SiteResult

SearchFn = Callable[..., Awaitable[SiteResult]]

REGISTRY: dict[str, SearchFn] = {
    altex.NAME: altex.search,
    buy2play.NAME: buy2play.search,
    emag.NAME: emag.search,
    flanco.NAME: flanco.search,
    jocurinoi.NAME: jocurinoi.search,
    ozone.NAME: ozone.search,
    trendyol.NAME: trendyol.search,
}


def all_sites() -> list[str]:
    return list(REGISTRY.keys())
