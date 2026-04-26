"""Orchestrates a `check` run: cache → concurrent fetch → match → record → rows."""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Callable

from . import db
from .client import SiteClient, site_config
from .db import Observation
from .matcher import match
from .normalize import normalize_query
from .sites import REGISTRY
from .sites.types import OK, SiteResult

ProgressFn = Callable[[str, str], None]

log = logging.getLogger(__name__)

CACHE_TTL = timedelta(minutes=30)


@dataclass
class DisplayRow:
    site: str
    status: str
    matched_title: str | None
    price_ron: float | None
    url: str | None
    availability: str | None
    is_used: bool
    low: float | None
    is_first_check: bool
    strategy_used: str | None
    from_cache: bool
    error: str | None = None
    scraped_at: str | None = None  # when this observation was recorded
    low_at: str | None = None      # when the historic low for this (site, variant) was hit


def _fresh(iso_str: str, now: datetime) -> bool:
    try:
        then = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
    except ValueError:
        return False
    if then.tzinfo is None:
        then = then.replace(tzinfo=timezone.utc)
    return (now - then) < CACHE_TTL


async def _fetch_one(site: str, query: str, overrides: dict[str, float]) -> tuple[str, SiteResult]:
    cfg = site_config(site, overrides)
    try:
        async with SiteClient(cfg) as client:
            return site, await REGISTRY[site](client, query)
    except Exception as e:
        log.debug("%s: top-level exception %s", site, e, exc_info=True)
        return site, SiteResult(site=site, status="error", error=f"{type(e).__name__}: {e}")


async def _fetch_one_with_client(site: str, client: SiteClient, query: str) -> tuple[str, SiteResult]:
    """Use a pre-opened client (for batched runs like favorites)."""
    try:
        return site, await REGISTRY[site](client, query)
    except Exception as e:
        log.debug("%s: top-level exception %s", site, e, exc_info=True)
        return site, SiteResult(site=site, status="error", error=f"{type(e).__name__}: {e}")


def _row_from_observation(
    conn,
    site: str,
    obs: Observation,
    *,
    from_cache: bool,
    prior_low: float | None = None,
    prior_count: int | None = None,
) -> DisplayRow:
    # Historic low and count are tracked per (query, site, is_used) so new and SH
    # have independent baselines.
    low_at: str | None = None
    if obs.status == "ok":
        detail = db.historic_low_with_date(conn, obs.normalized_query, site, is_used=obs.is_used)
        if detail is not None:
            # If caller supplied prior_low (pre-insert snapshot), keep it so the
            # "new low!" flag stays correct; otherwise use the fresh value.
            if prior_low is None:
                prior_low = detail[0]
            low_at = detail[1]
        if prior_count is None:
            prior_count = db.observation_count(conn, obs.normalized_query, site, is_used=obs.is_used)
    first = (prior_count == 0) if (prior_count is not None and obs.status == "ok") else False
    return DisplayRow(
        site=site,
        status=obs.status,
        matched_title=obs.matched_title,
        price_ron=obs.price_ron,
        url=obs.url,
        availability=obs.availability,
        is_used=obs.is_used,
        low=prior_low,
        is_first_check=first,
        strategy_used=obs.strategy_used,
        from_cache=from_cache,
        scraped_at=obs.scraped_at,
        low_at=low_at,
    )


async def run_check(
    query: str,
    *,
    sites: list[str] | None = None,
    force: bool = False,
    cooldown_overrides: dict[str, float] | None = None,
    clients: dict[str, SiteClient] | None = None,
    progress: "ProgressFn | None" = None,
    run_id: int | None = None,
) -> tuple[str, list[DisplayRow]]:
    """Run a lookup of `query` across `sites` (defaults to all). Returns (display_query, rows).

    If `clients` is provided, those pre-opened SiteClients are reused (used by the
    batched `favorites` path to keep cooldown state across games).

    `progress(site, state)` is called with states 'fetching', 'cache', 'ok',
    'no_match', 'blocked', 'error' as each site transitions.
    """
    display = query.strip()
    normalized = normalize_query(display)
    if not normalized:
        raise ValueError("empty query")

    targeted = sites or list(REGISTRY.keys())
    targeted = [s for s in targeted if s in REGISTRY]
    if not targeted:
        return display, []

    conn = db.connect()
    now = datetime.now(timezone.utc)

    # Pull cached observations inside TTL (unless --force). Each site can have up to
    # two cached observations: one new and one SH.
    cached_variants: dict[str, list[Observation]] = {}
    if not force:
        for s in targeted:
            variants = db.latest_observations(conn, normalized, s)
            fresh_variants = [o for o in variants if _fresh(o.scraped_at, now)]
            if fresh_variants:
                cached_variants[s] = fresh_variants

    to_fetch = [s for s in targeted if s not in cached_variants]

    if progress:
        for s in targeted:
            progress(s, "cache" if s in cached_variants else "fetching")

    async def _run(site: str) -> tuple[str, SiteResult]:
        if clients:
            site_key, res = await _fetch_one_with_client(site, clients[site], display)
        else:
            site_key, res = await _fetch_one(site, display, cooldown_overrides or {})
        if progress:
            # Translate scraper status + candidates into a more granular progress state.
            if res.status != "ok":
                progress(site, res.status)
            # For 'ok' we don't know yet whether matcher will pick something; emit 'ok' for now,
            # the matcher's no_match outcome is surfaced in the final row.
            else:
                progress(site, "ok")
        return site_key, res

    fetched: dict[str, SiteResult] = {}
    if to_fetch:
        results = await asyncio.gather(*[_run(s) for s in to_fetch])
        for site, res in results:
            fetched[site] = res

    rows: list[DisplayRow] = []
    for s in targeted:
        if s in cached_variants:
            for obs in cached_variants[s]:
                rows.append(_row_from_observation(conn, s, obs, from_cache=True))
            continue

        site_result = fetched.get(s)
        if site_result is None:
            continue

        site_rows: list[DisplayRow] = []
        final_status = site_result.status

        if site_result.status == OK:
            mr = match(display, site_result.candidates)
            if mr.winners:
                for winner in mr.winners:
                    prior_low = db.historic_low(conn, normalized, s, is_used=winner.is_used)
                    prior_count = db.observation_count(conn, normalized, s, is_used=winner.is_used)
                    obs = Observation(
                        normalized_query=normalized,
                        site=s,
                        matched_title=winner.title,
                        price_ron=winner.price_ron,
                        url=winner.url,
                        availability=winner.availability,
                        is_used=winner.is_used,
                        status="ok",
                        strategy_used=site_result.strategy_used,
                        scraped_at=db.now_iso(),
                    )
                    db.record_observation(conn, obs, run_id=run_id)
                    row = _row_from_observation(
                        conn, s, obs, from_cache=False,
                        prior_low=prior_low, prior_count=prior_count,
                    )
                    site_rows.append(row)
                final_status = "ok"
            else:
                obs = Observation(
                    normalized_query=normalized,
                    site=s,
                    matched_title=None,
                    price_ron=None,
                    url=None,
                    availability=None,
                    is_used=False,
                    status="no_match",
                    strategy_used=site_result.strategy_used,
                    scraped_at=db.now_iso(),
                )
                db.record_observation(conn, obs, run_id=run_id)
                site_rows.append(_row_from_observation(conn, s, obs, from_cache=False))
                final_status = "no_match"
        else:
            obs = Observation(
                normalized_query=normalized,
                site=s,
                matched_title=None,
                price_ron=None,
                url=None,
                availability=None,
                is_used=False,
                status=site_result.status,
                strategy_used=None,
                scraped_at=db.now_iso(),
            )
            db.record_observation(conn, obs, run_id=run_id)
            site_rows.append(_row_from_observation(conn, s, obs, from_cache=False))

        for r in site_rows:
            r.error = site_result.error
        if progress and final_status in ("ok", "no_match"):
            progress(s, final_status)

        rows.extend(site_rows)

    return display, rows


async def run_favorites(
    *,
    sites: list[str] | None = None,
    force: bool = False,
    cooldown_overrides: dict[str, float] | None = None,
    per_game_progress: Callable[[str, list[str]], "ProgressFn"] | None = None,
    on_game_complete: Callable[[str, list[DisplayRow]], None] | None = None,
    target_sites_per_game: dict[str, list[str]] | None = None,
) -> list[tuple[str, list[DisplayRow]]]:
    """Check every favorite sequentially using long-lived per-site clients.

    Long-lived clients mean per-site cooldown state persists across games, so we
    never hit the same domain twice within its cooldown window.

    `target_sites_per_game` (used by --fix-missing): if provided, restricts each
    favorite to only the sites in its list (keyed by `normalized_query`). Games
    not present in the dict are skipped entirely.
    """
    conn = db.connect()  # always open to read favorites
    favs = db.list_favorites(conn)
    if not favs:
        conn.close()
        return []

    targeted = sites or list(REGISTRY.keys())
    targeted = [s for s in targeted if s in REGISTRY]
    if not targeted:
        conn.close()
        return [(f.display_query, []) for f in favs]

    if target_sites_per_game is not None:
        favs = [f for f in favs if target_sites_per_game.get(f.normalized_query)]
        if not favs:
            conn.close()
            return []

    # Run lifecycle: tag every observation produced in this batch with one run id
    # so `--fix-missing` can later identify exactly which (game, site) failed.
    run_id = db.start_run(conn, "favorites")
    conn.close()

    from contextlib import AsyncExitStack

    out: list[tuple[str, list[DisplayRow]]] = []
    try:
        async with AsyncExitStack() as stack:
            clients: dict[str, SiteClient] = {}
            for s in targeted:
                c = SiteClient(site_config(s, cooldown_overrides or {}))
                clients[s] = await stack.enter_async_context(c)

            for fav in favs:
                game_sites = targeted
                if target_sites_per_game is not None:
                    allowed = set(target_sites_per_game.get(fav.normalized_query, []))
                    game_sites = [s for s in targeted if s in allowed]
                    if not game_sites:
                        continue
                pg = per_game_progress(fav.display_query, game_sites) if per_game_progress else None
                display, rows = await run_check(
                    fav.display_query,
                    sites=game_sites,
                    force=force,
                    cooldown_overrides=cooldown_overrides,
                    clients=clients,
                    progress=pg,
                    run_id=run_id,
                )
                out.append((display, rows))
                if on_game_complete:
                    on_game_complete(display, rows)
    finally:
        end_conn = db.connect()
        db.end_run(end_conn, run_id)
        end_conn.close()
    return out
