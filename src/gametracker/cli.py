"""Gametracker CLI — typer entry point."""
from __future__ import annotations

import asyncio
import logging
from typing import Annotated

import typer
from rich.console import Console

from . import db
from .normalize import normalize_query
from .client import SITE_COOLDOWN_DEFAULTS
from .history import build_history
from .render import StatusDisplay, render_errors, render_history, render_summary, render_table
from .runner import run_check, run_favorites
from .sites import REGISTRY

app = typer.Typer(
    name="gametracker",
    help="Romanian PS5 price tracker.",
    no_args_is_help=True,
)

console = Console()
err_console = Console(stderr=True)


class CtxState:
    """Holds global flag state, attached to typer.Context.obj."""

    def __init__(self) -> None:
        self.force: bool = False
        self.sites: list[str] | None = None
        self.verbose: bool = False
        self.cooldown: dict[str, float] = {}


def _parse_sites(value: str | None) -> list[str] | None:
    if not value:
        return None
    return [s.strip().lower() for s in value.split(",") if s.strip()]


def _parse_cooldown(value: str | None) -> dict[str, float]:
    if not value:
        return {}
    out: dict[str, float] = {}
    for chunk in value.split(","):
        chunk = chunk.strip()
        if not chunk or "=" not in chunk:
            continue
        k, v = chunk.split("=", 1)
        try:
            out[k.strip().lower()] = float(v)
        except ValueError:
            raise typer.BadParameter(f"invalid cooldown value: {chunk!r}")
    return out


@app.callback()
def _root(
    ctx: typer.Context,
    force: Annotated[bool, typer.Option("--force", help="Bypass the 30-minute recheck cache.")] = False,
    sites: Annotated[
        str | None,
        typer.Option("--sites", help="Comma-separated site allowlist (e.g. altex,emag).")
    ] = None,
    verbose: Annotated[bool, typer.Option("--verbose", "-v", help="Print debug info to stderr.")] = False,
    cooldown: Annotated[
        str | None,
        typer.Option(
            "--cooldown",
            help="Per-site cooldown overrides, e.g. 'altex=60,emag=15'."
        )
    ] = None,
) -> None:
    state = CtxState()
    state.force = force
    state.sites = _parse_sites(sites)
    state.verbose = verbose
    state.cooldown = _parse_cooldown(cooldown)
    ctx.obj = state

    level = logging.DEBUG if verbose else logging.WARNING
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
        stream=err_console.file,
    )


@app.command()
def check(
    ctx: typer.Context,
    game: Annotated[str, typer.Argument(help="Game name, e.g. 'Resident Evil Requiem'.")],
) -> None:
    """Look up one game across all sites."""
    state: CtxState = ctx.obj
    target_sites = state.sites or list(REGISTRY.keys())

    async def _go() -> tuple[str, list]:
        with StatusDisplay(console, game, target_sites) as live:
            return await run_check(
                game,
                sites=state.sites,
                force=state.force,
                cooldown_overrides=state.cooldown,
                progress=live.update,
            )

    try:
        display, rows = asyncio.run(_go())
    except ValueError as e:
        err_console.print(f"[red]{e}[/red]")
        raise typer.Exit(code=2)

    render_table(display, rows, console)
    if state.verbose:
        render_errors(rows, err_console)


@app.command()
def favorites(
    ctx: typer.Context,
    site: Annotated[
        str | None,
        typer.Argument(help="Optional: limit to a single site (e.g. 'buy2play')."),
    ] = None,
    summary: Annotated[
        bool,
        typer.Option("--summary", help="After all games check, show a compact best-price table."),
    ] = False,
) -> None:
    """Check all saved favorites."""
    state: CtxState = ctx.obj

    sites_override = state.sites
    if site:
        name = site.strip().lower()
        if name not in REGISTRY:
            err_console.print(
                f"[red]unknown site {name!r}[/red]; known: {', '.join(sorted(REGISTRY.keys()))}"
            )
            raise typer.Exit(code=2)
        sites_override = [name]

    # One live display per game: created on start, updates, closes to flush when the
    # game's table is ready to print. The next game opens its own display.
    current: list[StatusDisplay | None] = [None]

    def per_game_progress(display: str, sites_list: list[str]):
        if current[0] is not None:
            current[0].__exit__(None, None, None)
        sd = StatusDisplay(console, display, sites_list).__enter__()
        current[0] = sd
        return sd.update

    def on_game_complete(display: str, rows: list) -> None:
        if current[0] is not None:
            current[0].__exit__(None, None, None)
            current[0] = None
        render_table(display, rows, console)
        if state.verbose:
            render_errors(rows, err_console)

    try:
        results = asyncio.run(
            run_favorites(
                sites=sites_override,
                force=state.force,
                cooldown_overrides=state.cooldown,
                per_game_progress=per_game_progress,
                on_game_complete=on_game_complete,
            )
        )
    finally:
        if current[0] is not None:
            current[0].__exit__(None, None, None)
            current[0] = None

    if not results:
        console.print("[dim]no favorites yet — add some with `gametracker add \"Game Name\"`[/dim]")
        return
    if summary:
        render_summary(results, console)


@app.command()
def add(
    ctx: typer.Context,
    game: Annotated[str, typer.Argument(help="Game name to save.")],
) -> None:
    """Save a game to the favorites list."""
    nq = normalize_query(game)
    if not nq:
        err_console.print("[red]empty game name[/red]")
        raise typer.Exit(code=2)
    with db.connect() as conn:
        added = db.add_favorite(conn, nq, game)
    if added:
        console.print(f"[green]added[/green] {game}")
    else:
        console.print(f"[yellow]already tracked[/yellow] {game}")


@app.command()
def remove(
    ctx: typer.Context,
    game: Annotated[str, typer.Argument(help="Game name to drop.")],
) -> None:
    """Drop a game from the favorites list."""
    nq = normalize_query(game)
    with db.connect() as conn:
        removed = db.remove_favorite(conn, nq)
    if removed:
        console.print(f"[green]removed[/green] {game}")
    else:
        console.print(f"[yellow]not in favorites[/yellow] {game}")


@app.command(name="list")
def list_cmd(ctx: typer.Context) -> None:
    """Show saved favorites."""
    with db.connect() as conn:
        favs = db.list_favorites(conn)
    if not favs:
        console.print("[dim]no favorites yet[/dim]")
        return
    for f in favs:
        console.print(f"- {f.display_query}")


@app.command()
def sites(ctx: typer.Context) -> None:
    """Show the sites being tracked (and each site's default cooldown)."""
    for name in sorted(REGISTRY.keys()):
        cd = SITE_COOLDOWN_DEFAULTS.get(name, 5.0)
        console.print(f"- {name}  [dim]({cd:g}s default cooldown)[/dim]")


@app.command()
def history(
    ctx: typer.Context,
    game: Annotated[str, typer.Argument(help="Game name to show history for.")],
) -> None:
    """Show price timeline per site."""
    with db.connect() as conn:
        histories = build_history(conn, game)
    render_history(game.strip(), histories, console)


@app.command()
def best(
    ctx: typer.Context,
    game: Annotated[
        str | None,
        typer.Argument(help="Optional: show best price for a single game only."),
    ] = None,
) -> None:
    """Show the best known price per game from the DB (no scraping)."""
    from .runner import DisplayRow  # local import to avoid a cycle

    with db.connect() as conn:
        if game:
            nq = normalize_query(game)
            if not nq:
                err_console.print("[red]empty game name[/red]")
                raise typer.Exit(code=2)
            queries = [(nq, game.strip())]
        else:
            favs = db.list_favorites(conn)
            if not favs:
                console.print("[dim]no favorites yet — add some with `gametracker add \"Game Name\"`[/dim]")
                return
            queries = [(f.normalized_query, f.display_query) for f in favs]

        games: list[tuple[str, list[DisplayRow]]] = []
        global_lows: dict[tuple[str, bool], tuple[float, str]] = {}
        for nq, display_name in queries:
            rows: list[DisplayRow] = []
            for site in REGISTRY.keys():
                for obs in db.latest_observations(conn, nq, site):
                    lo = db.historic_low(conn, nq, site, is_used=obs.is_used)
                    cnt = db.observation_count(conn, nq, site, is_used=obs.is_used)
                    rows.append(DisplayRow(
                        site=site,
                        status=obs.status,
                        matched_title=obs.matched_title,
                        price_ron=obs.price_ron,
                        url=obs.url,
                        availability=obs.availability,
                        is_used=obs.is_used,
                        low=lo,
                        is_first_check=(cnt <= 1),
                        strategy_used=obs.strategy_used,
                        from_cache=True,
                    ))
            # Cross-site historic low per variant for this game.
            for used_flag in (False, True):
                gl = db.historic_low_global(conn, nq, used_flag)
                if gl is not None:
                    global_lows[(display_name, used_flag)] = gl
            games.append((display_name, rows))

    render_summary(games, console, single_game=(game is not None), global_lows=global_lows)


@app.command(name="all")
def all_cmd(
    ctx: typer.Context,
    game: Annotated[
        str | None,
        typer.Argument(help="Optional: limit the dump to a single game."),
    ] = None,
) -> None:
    """Dump every recorded price observation — game × site × current × low."""
    from .render import render_all

    filter_nq: str | None = None
    if game:
        filter_nq = normalize_query(game)
        if not filter_nq:
            err_console.print("[red]empty game name[/red]")
            raise typer.Exit(code=2)

    with db.connect() as conn:
        render_all(conn, console, filter_normalized=filter_nq)


if __name__ == "__main__":
    app()
