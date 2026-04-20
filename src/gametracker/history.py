"""History summary per site, with sparkline trend."""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass

from . import db
from .normalize import normalize_query

SPARK_CHARS = "▁▂▃▄▅▆▇█"


@dataclass
class SiteHistory:
    site: str
    latest: float
    low: float
    high: float
    count: int
    sparkline: str


def sparkline(values: list[float]) -> str:
    if not values:
        return ""
    if len(values) == 1:
        return SPARK_CHARS[0]
    lo, hi = min(values), max(values)
    if hi - lo < 0.005:
        return SPARK_CHARS[0] * len(values)
    span = hi - lo
    n = len(SPARK_CHARS) - 1
    out = []
    for v in values:
        idx = int(round((v - lo) / span * n))
        idx = max(0, min(n, idx))
        out.append(SPARK_CHARS[idx])
    return "".join(out)


def build_history(conn, display_query: str) -> list[SiteHistory]:
    nq = normalize_query(display_query)
    by_site: dict[str, list[float]] = defaultdict(list)
    for obs in db.iter_history(conn, nq):
        if obs.price_ron is None:
            continue
        by_site[obs.site].append(obs.price_ron)

    out: list[SiteHistory] = []
    for site, vals in by_site.items():
        out.append(SiteHistory(
            site=site,
            latest=vals[-1],
            low=min(vals),
            high=max(vals),
            count=len(vals),
            sparkline=sparkline(vals),
        ))
    out.sort(key=lambda h: h.site)
    return out
