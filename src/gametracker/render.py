"""Rich table rendering for check results."""
from __future__ import annotations

from rich.console import Console
from rich.live import Live
from rich.table import Table

from .history import SiteHistory
from .runner import DisplayRow


class StatusDisplay:
    """Transient per-site progress table. Clears when the context exits."""

    _ICON = {
        "fetching": "[dim]…[/dim]",
        "cache":    "[dim]✓ cache[/dim]",
        "ok":       "[green]✓[/green]",
        "no_match": "[dim]—[/dim]",
        "blocked":  "[red]✗ blocked[/red]",
        "error":    "[red]✗ error[/red]",
    }

    def __init__(self, console: Console, game: str, sites: list[str]) -> None:
        self.console = console
        self.game = game
        self.sites = sorted(sites)
        self.states: dict[str, str] = {s: "fetching" for s in self.sites}
        self._live: Live | None = None

    def _build(self) -> Table:
        t = Table(show_header=False, box=None, padding=(0, 2), title=f"[dim]checking[/dim] {self.game}")
        t.add_column(justify="left")
        t.add_column(justify="left")
        for site in self.sites:
            icon = self._ICON.get(self.states[site], self.states[site])
            t.add_row(site, icon)
        return t

    def __enter__(self) -> "StatusDisplay":
        self._live = Live(
            self._build(),
            console=self.console,
            transient=True,
            refresh_per_second=6,
        )
        self._live.__enter__()
        return self

    def __exit__(self, *exc) -> None:
        if self._live is not None:
            self._live.__exit__(*exc)
            self._live = None

    def update(self, site: str, state: str) -> None:
        if site not in self.states:
            return
        self.states[site] = state
        if self._live is not None:
            self._live.update(self._build())

STATUS_ICON = {
    "ok": "✓",
    "no_match": "—",
    "blocked": "✗",
    "error": "✗",
}


def _fmt_price(p: float | None) -> str:
    if p is None:
        return "—"
    return f"{p:.2f}"


def _fmt_date(iso: str | None) -> str:
    """Shorten an ISO timestamp to YYYY-MM-DD for compact display."""
    if not iso:
        return ""
    s = iso.strip().replace("Z", "")
    # Accept both 'YYYY-MM-DD' and 'YYYY-MM-DDTHH:MM:SS' forms.
    return s.split("T", 1)[0][:10]


def _fmt_vs_low(row: DisplayRow) -> str:
    if row.status != "ok" or row.price_ron is None:
        return ""
    if row.is_first_check or row.low is None:
        return "⭐ first check"
    delta = row.price_ron - row.low
    if delta < -0.005:  # strictly below prior low
        return f"[bold green]−{abs(delta):.2f} new low![/bold green]"
    if abs(delta) <= 0.005:  # matches prior low
        return "= low"
    return f"+{delta:.2f} vs low"


def _fmt_title(row: DisplayRow) -> str:
    return row.matched_title if row.matched_title else "—"


def _fmt_site(row: DisplayRow) -> str:
    return f"{row.site} (SH)" if row.is_used else row.site


def _fmt_status(row: DisplayRow) -> str:
    icon = STATUS_ICON.get(row.status, "?")
    extras = []
    if row.from_cache:
        extras.append("cache")
    if row.status == "blocked":
        extras.append("blocked")
    elif row.status == "error":
        extras.append("error")
    suffix = f" ({', '.join(extras)})" if extras else ""
    return f"{icon}{suffix}"


def _row_style(row: DisplayRow) -> str | None:
    # Column-level coloring is handled via Column(style=...); no per-row overrides.
    return None


def render_table(display_query: str, rows: list[DisplayRow], console: Console) -> None:
    console.print(f"\n[bold]{display_query}[/bold]  [dim](PS5)[/dim]\n")

    table = Table(show_header=True, header_style="bold", box=None)
    table.add_column("Site", style="yellow")
    table.add_column("Matched title", overflow="fold", max_width=55, style="cyan")
    table.add_column("Price RON", justify="right", style="green")
    table.add_column("vs low", justify="right")
    table.add_column("Last seen", justify="right", style="dim")
    table.add_column("Low on", justify="right", style="dim")
    table.add_column("Status")

    # Sort: ok first, then no_match, then errors; within each, by price ascending.
    def sort_key(r: DisplayRow):
        status_order = {"ok": 0, "no_match": 1, "blocked": 2, "error": 3}.get(r.status, 9)
        price = r.price_ron if r.price_ron is not None else float("inf")
        return (status_order, price, r.site)

    for row in sorted(rows, key=sort_key):
        last_seen = _fmt_date(row.scraped_at) if row.status == "ok" else ""
        low_on = _fmt_date(row.low_at) if (row.status == "ok" and row.low is not None) else ""
        table.add_row(
            _fmt_site(row),
            _fmt_title(row),
            _fmt_price(row.price_ron),
            _fmt_vs_low(row),
            last_seen,
            low_on,
            _fmt_status(row),
            style=_row_style(row),
        )

    console.print(table)


def render_errors(rows: list[DisplayRow], console: Console) -> None:
    """Verbose error listing to stderr."""
    for r in rows:
        if r.status in ("blocked", "error") and r.error:
            console.print(f"[dim]{r.site}:[/dim] [red]{r.status}[/red] — {r.error}")


def _classify_reason(status: str, error: str | None) -> str:
    """Shorten the raw error to a stable reason code (403, 429, timeout, parse, …)."""
    text = (error or "").strip()
    low = text.lower()
    if "403" in low:
        return "403"
    if "429" in low or "ratelimited" in low:
        return "429"
    if "timeout" in low:
        return "timeout"
    if low.startswith("bad json"):
        return "bad json"
    if low.startswith("parse"):
        return "parse"
    # Fall back to the first token of the error (usually "TypeName:").
    head = text.split(":", 1)[0].strip()
    if head:
        return head[:24]
    return status or "error"


def _fmt_duration(seconds: float) -> str:
    """Format a wall-clock duration compactly: '4.2s', '1m 12s', '1h 03m 05s'."""
    if seconds < 0:
        seconds = 0.0
    if seconds < 60:
        return f"{seconds:.1f}s"
    mins, secs = divmod(int(seconds), 60)
    if mins < 60:
        return f"{mins}m {secs:02d}s"
    hours, mins = divmod(mins, 60)
    return f"{hours}h {mins:02d}m {secs:02d}s"


def render_run_summary(
    games: list[tuple[str, list[DisplayRow]]],
    console: Console,
    *,
    elapsed_seconds: float | None = None,
) -> None:
    """Summarize retrieval failures at the end of a run.

    A failure is any row whose status is 'blocked' or 'error'. Rows with
    status 'no_match' are ignored — the game simply isn't sold on that site.
    Results are grouped by (site, reason) so a site-wide outage collapses to
    one line.
    """
    # (site, reason) -> list of games
    buckets: dict[tuple[str, str], list[str]] = {}
    total_failures = 0
    affected_games: set[str] = set()
    for display, rows in games:
        # De-dupe within a game: one (site, reason) entry even if multiple rows.
        seen: set[tuple[str, str]] = set()
        for r in rows:
            if r.status not in ("blocked", "error"):
                continue
            key = (r.site, _classify_reason(r.status, r.error))
            if key in seen:
                continue
            seen.add(key)
            buckets.setdefault(key, []).append(display)
            total_failures += 1
            affected_games.add(display)

    elapsed_tag = f" [dim](took {_fmt_duration(elapsed_seconds)})[/dim]" if elapsed_seconds is not None else ""

    if not buckets:
        console.print(
            f"\n[green]Retrieval summary:[/green] all sites returned data for every game.{elapsed_tag}"
        )
        return

    console.print(
        f"\n[bold]Retrieval summary[/bold]  "
        f"[dim]({total_failures} failures across {len(affected_games)} game(s))[/dim]"
        f"{elapsed_tag}\n"
    )
    table = Table(show_header=True, header_style="bold", box=None)
    table.add_column("Site", style="yellow")
    table.add_column("Reason", style="red")
    table.add_column("Games", justify="right")
    table.add_column("Affected", overflow="fold", max_width=70, style="cyan")

    for (site, reason), names in sorted(buckets.items(), key=lambda kv: (-len(kv[1]), kv[0])):
        table.add_row(site, reason, str(len(names)), ", ".join(sorted(names)))
    console.print(table)


def _cheapest(rows: list[DisplayRow], used_flag: bool) -> DisplayRow | None:
    bucket = [r for r in rows if r.status == "ok" and r.price_ron is not None and r.is_used == used_flag]
    if not bucket:
        return None
    return min(bucket, key=lambda r: r.price_ron)  # type: ignore[arg-type,return-value]


def _fmt_vs_low_symbol(price: float | None, low: float | None) -> str:
    """Render '<' / '=' / '>' showing where the current price sits vs the historic low.

    '<' (green) — current is below the recorded low (a fresh new low).
    '=' (dim)   — current matches the low.
    '>' (red)   — current is above the low.
    '' if either value is missing.
    """
    if price is None or low is None:
        return ""
    eps = 0.005
    if price < low - eps:
        return "[green]<[/green]"
    if price > low + eps:
        return "[red]>[/red]"
    return "[dim]=[/dim]"


def _fmt_low_with_site(
    low: float | None, site: str | None, at: str | None = None
) -> str:
    if low is None:
        return "—"
    date = _fmt_date(at)
    parts: list[str] = [f"{low:.2f}"]
    tag_bits: list[str] = []
    if site:
        tag_bits.append(site)
    if date:
        tag_bits.append(date)
    if tag_bits:
        parts.append(f"[dim]({', '.join(tag_bits)})[/dim]")
    return " ".join(parts)


def _render_summary_rows(
    games: list[tuple[str, list[DisplayRow]]],
    console: Console,
    *,
    global_lows: dict[tuple[str, bool], tuple[float, str, str]] | None = None,
) -> None:
    """One row per (game, condition) — used for single-game best lookups."""
    console.print("\n[bold]Best price per game[/bold]\n")
    table = Table(show_header=True, header_style="bold", box=None)
    table.add_column("Game", overflow="fold", max_width=55, style="cyan")
    table.add_column("Site", style="yellow")
    table.add_column("Current", justify="right", style="green")
    table.add_column("", justify="center", max_width=2)
    table.add_column("Last seen", justify="right", style="dim")
    table.add_column("Historic low", justify="right")

    no_match: list[str] = []
    matched: list[tuple[str, str, float, str | None, float | None, str | None, str | None]] = []
    for display, rows in games:
        any_row = False
        for used_flag in (False, True):
            ch = _cheapest(rows, used_flag)
            if ch is None:
                continue
            any_row = True
            site_label = f"{ch.site} (SH)" if ch.is_used else ch.site
            gl = (global_lows or {}).get((display, used_flag))
            if gl is not None:
                low_val, low_site, low_at = gl
            else:
                low_val, low_site, low_at = ch.low, None, ch.low_at
            matched.append((site_label, display, ch.price_ron, ch.scraped_at, low_val, low_site, low_at))
        if not any_row:
            no_match.append(display)

    matched.sort(key=lambda t: (t[0], t[2], t[1]))
    for site_label, game, price, seen_at, low, low_site, low_at in matched:
        table.add_row(
            game, site_label, f"{price:.2f}",
            _fmt_vs_low_symbol(price, low),
            _fmt_date(seen_at),
            _fmt_low_with_site(low, low_site, low_at),
        )
    for g in no_match:
        table.add_row(g, "[dim]—[/dim]", "[dim]no match[/dim]", "", "", "")
    console.print(table)


def _render_summary_columns(
    games: list[tuple[str, list[DisplayRow]]],
    console: Console,
    *,
    global_lows: dict[tuple[str, bool], tuple[float, str, str]] | None = None,
) -> None:
    """One row per game with dedicated SH columns — used for the multi-game best view."""
    console.print("\n[bold]Best price per game[/bold]\n")
    table = Table(show_header=True, header_style="bold", box=None)
    table.add_column("Game", overflow="fold", max_width=40, style="cyan")
    table.add_column("Site", style="yellow")
    table.add_column("Current", justify="right", style="green")
    table.add_column("", justify="center", max_width=2)
    table.add_column("Seen", justify="right", style="dim")
    table.add_column("Hist. low (site, date)", justify="right")
    table.add_column("SH site", style="yellow")
    table.add_column("SH current", justify="right", style="green")
    table.add_column("", justify="center", max_width=2)
    table.add_column("SH seen", justify="right", style="dim")
    table.add_column("SH hist. low (site, date)", justify="right")

    entries: list[tuple[str, str, DisplayRow | None, DisplayRow | None]] = []
    no_match: list[str] = []
    for display, rows in games:
        new = _cheapest(rows, False)
        sh = _cheapest(rows, True)
        if new is None and sh is None:
            no_match.append(display)
            continue
        key_site = new.site if new else (sh.site if sh else "zzz")
        entries.append((key_site, display, new, sh))

    entries.sort(
        key=lambda e: (
            e[0],
            e[2].price_ron if e[2] is not None else (e[3].price_ron if e[3] is not None else float("inf")),
            e[1].lower(),
        )
    )
    for _key_site, display, new, sh in entries:
        new_low_val: float | None = None
        new_low_cell = "[dim]—[/dim]"
        if new is not None:
            gl = (global_lows or {}).get((display, False))
            if gl:
                new_low_val = gl[0]
                new_low_cell = _fmt_low_with_site(*gl)
            else:
                new_low_val = new.low
                new_low_cell = _fmt_low_with_site(new.low, None, new.low_at)
        sh_low_val: float | None = None
        sh_low_cell = "[dim]—[/dim]"
        if sh is not None:
            gl = (global_lows or {}).get((display, True))
            if gl:
                sh_low_val = gl[0]
                sh_low_cell = _fmt_low_with_site(*gl)
            else:
                sh_low_val = sh.low
                sh_low_cell = _fmt_low_with_site(sh.low, None, sh.low_at)
        table.add_row(
            display,
            new.site if new else "[dim]—[/dim]",
            f"{new.price_ron:.2f}" if new else "[dim]—[/dim]",
            _fmt_vs_low_symbol(new.price_ron if new else None, new_low_val),
            _fmt_date(new.scraped_at) if new else "",
            new_low_cell,
            sh.site if sh else "[dim]—[/dim]",
            f"{sh.price_ron:.2f}" if sh else "[dim]—[/dim]",
            _fmt_vs_low_symbol(sh.price_ron if sh else None, sh_low_val),
            _fmt_date(sh.scraped_at) if sh else "",
            sh_low_cell,
        )
    for g in no_match:
        table.add_row(g, "[dim]no match[/dim]", "", "", "", "", "", "", "", "", "")
    console.print(table)


def render_summary(
    games: list[tuple[str, list[DisplayRow]]],
    console: Console,
    *,
    single_game: bool = False,
    global_lows: dict[tuple[str, bool], tuple[float, str, str]] | None = None,
) -> None:
    """Render the best-price summary.

    single_game=True → one row per (game, condition). Used when the user asked
    about one specific game and expects to see both new and SH as separate rows.

    single_game=False → one row per game with dedicated SH columns. Used for
    multi-game overviews (`gametracker best`, `gametracker favorites --summary`).

    global_lows maps (display_query, is_used) → (price, site) for the all-time
    cheapest observation across sites. When present, the historic-low column
    shows that cross-site low plus the site that recorded it.
    """
    if single_game:
        _render_summary_rows(games, console, global_lows=global_lows)
    else:
        _render_summary_columns(games, console, global_lows=global_lows)


def _icon_for(price: float | None, low: float | None, cnt: int) -> str:
    if price is None or low is None:
        return ""
    if abs(price - low) < 0.005 and cnt >= 2:
        return "⭐"
    if abs(price - low) < 0.005:
        return "·"
    return "[dim]↑[/dim]"


def render_all(
    conn, console: Console, *, filter_normalized: str | None = None,
) -> None:
    """List every recorded (game × site). Each row packs both the new and the SH
    latest observations side by side; independent hist-lows per variant.

    Pass filter_normalized to limit the dump to a single normalized query.
    """
    fav_map: dict[str, str] = {}
    for row in conn.execute("SELECT normalized_query, display_query FROM favorites").fetchall():
        fav_map[row[0]] = row[1]

    # Latest `ok` observation per (query, site, is_used) with its per-variant low/count.
    # variant_low_at is the scraped_at of the earliest row that hit the variant_low price.
    rows = conn.execute("""
        SELECT o.normalized_query, o.site, o.price_ron, o.is_used, o.scraped_at AS seen_at,
               (SELECT MIN(price_ron) FROM price_observations
                  WHERE normalized_query = o.normalized_query
                    AND site = o.site
                    AND is_used = o.is_used
                    AND status = 'ok') AS variant_low,
               (SELECT MIN(scraped_at) FROM price_observations
                  WHERE normalized_query = o.normalized_query
                    AND site = o.site
                    AND is_used = o.is_used
                    AND status = 'ok'
                    AND price_ron = (
                      SELECT MIN(price_ron) FROM price_observations
                        WHERE normalized_query = o.normalized_query
                          AND site = o.site
                          AND is_used = o.is_used
                          AND status = 'ok'
                    )) AS variant_low_at,
               (SELECT COUNT(*) FROM price_observations
                  WHERE normalized_query = o.normalized_query
                    AND site = o.site
                    AND is_used = o.is_used
                    AND status = 'ok') AS variant_count
        FROM price_observations o
        INNER JOIN (
            SELECT normalized_query, site, is_used, MAX(scraped_at) AS max_at
            FROM price_observations WHERE status = 'ok'
            GROUP BY normalized_query, site, is_used
        ) latest
          ON latest.normalized_query = o.normalized_query
         AND latest.site = o.site
         AND latest.is_used = o.is_used
         AND latest.max_at = o.scraped_at
        WHERE o.status = 'ok' AND o.price_ron IS NOT NULL
    """).fetchall()

    if filter_normalized:
        rows = [r for r in rows if r[0] == filter_normalized]

    if not rows:
        if filter_normalized:
            console.print(f"[dim]no recorded observations for {filter_normalized!r}[/dim]")
        else:
            console.print("[dim]no recorded observations yet[/dim]")
        return

    # Fold new + SH for the same (game, site) into a single entry.
    # merged[(display, site)] = {'new': (price, seen_at, low, low_at, cnt), ...}
    merged: dict[
        tuple[str, str],
        dict[str, tuple[float, str | None, float | None, str | None, int]],
    ] = {}
    for nq, site, price, is_used, seen_at, low, low_at, cnt in rows:
        display = fav_map.get(nq, nq)
        key = (display, site)
        merged.setdefault(key, {})
        merged[key]["sh" if is_used else "new"] = (
            price, seen_at, low, low_at, int(cnt or 1),
        )

    table = Table(show_header=True, header_style="bold", box=None)
    table.add_column("Game", overflow="fold", max_width=45, style="cyan")
    table.add_column("Site", style="yellow")
    table.add_column("Current", justify="right", style="green")
    table.add_column("Seen", justify="right", style="dim")
    table.add_column("Hist. low", justify="right")
    table.add_column("Low on", justify="right", style="dim")
    table.add_column("", max_width=4)
    table.add_column("SH current", justify="right", style="green")
    table.add_column("SH seen", justify="right", style="dim")
    table.add_column("SH hist. low", justify="right")
    table.add_column("SH low on", justify="right", style="dim")
    table.add_column("", max_width=4)

    # Per-game minima across sites — the site carrying this value gets a crown.
    # Tuple layout: (price, seen_at, low, low_at, cnt)
    game_min_new: dict[str, float] = {}
    game_min_sh: dict[str, float] = {}
    for (display, _site), entry in merged.items():
        if "new" in entry:
            game_min_new[display] = min(entry["new"][0], game_min_new.get(display, float("inf")))
        if "sh" in entry:
            game_min_sh[display] = min(entry["sh"][0], game_min_sh.get(display, float("inf")))

    # Group by game (alphabetical), then by new-price ascending within each game.
    def cheapest_visible(entry: dict) -> float:
        vals = [v[0] for v in entry.values() if v is not None]
        return min(vals) if vals else float("inf")

    ordered_keys = sorted(
        merged.keys(),
        key=lambda k: (k[0].lower(), cheapest_visible(merged[k]), k[1]),
    )

    CROWN = "[bold yellow]👑[/bold yellow]"

    last_game: str | None = None
    for display, site in ordered_keys:
        entry = merged[(display, site)]
        new = entry.get("new")
        sh = entry.get("sh")
        game_cell = display if display != last_game else ""
        last_game = display

        new_price = f"{new[0]:.2f}" if new else "[dim]—[/dim]"
        new_seen = _fmt_date(new[1]) if new else ""
        new_low = f"{new[2]:.2f}" if (new and new[2] is not None) else "[dim]—[/dim]"
        new_low_at = _fmt_date(new[3]) if (new and new[2] is not None) else ""
        new_icon = _icon_for(new[0], new[2], new[4]) if new else ""
        if new and abs(new[0] - game_min_new.get(display, float("inf"))) < 0.005:
            new_icon = f"{CROWN}{new_icon}"

        sh_price = f"{sh[0]:.2f}" if sh else "[dim]—[/dim]"
        sh_seen = _fmt_date(sh[1]) if sh else ""
        sh_low = f"{sh[2]:.2f}" if (sh and sh[2] is not None) else "[dim]—[/dim]"
        sh_low_at = _fmt_date(sh[3]) if (sh and sh[2] is not None) else ""
        sh_icon = _icon_for(sh[0], sh[2], sh[4]) if sh else ""
        if sh and abs(sh[0] - game_min_sh.get(display, float("inf"))) < 0.005:
            sh_icon = f"{CROWN}{sh_icon}"

        table.add_row(
            game_cell, site,
            new_price, new_seen, new_low, new_low_at, new_icon,
            sh_price, sh_seen, sh_low, sh_low_at, sh_icon,
        )

    console.print(table)


def render_history(display_query: str, histories: list[SiteHistory], console: Console) -> None:
    console.print(f"\n[bold]{display_query}[/bold]  [dim](PS5)[/dim]\n")

    if not histories:
        console.print("[dim]no history yet — run `gametracker check` first[/dim]")
        return

    table = Table(show_header=True, header_style="bold", box=None)
    table.add_column("Site")
    table.add_column("Latest", justify="right")
    table.add_column("Seen", justify="right", style="dim")
    table.add_column("Low", justify="right")
    table.add_column("Low on", justify="right", style="dim")
    table.add_column("High", justify="right")
    table.add_column("Trend")

    for h in histories:
        table.add_row(
            h.site,
            f"{h.latest:.2f}",
            _fmt_date(h.latest_at),
            f"{h.low:.2f}",
            _fmt_date(h.low_at),
            f"{h.high:.2f}",
            f"{h.sparkline}  [dim]({h.count})[/dim]",
        )
    console.print(table)


def render_graph(
    display_query: str,
    observations: list,
    console: Console,
    *,
    site_filter: str | None = None,
) -> None:
    """Plot price evolution per (site, variant) for a single game using plotext.

    observations: list of db.Observation — pre-filtered to status='ok' with price.
    site_filter: optional site name to restrict the plot to one series.
    """
    import plotext as plt

    console.print(f"\n[bold]{display_query}[/bold]  [dim](PS5, price evolution)[/dim]\n")

    if not observations:
        console.print("[dim]no history yet — run `gametracker check` first[/dim]")
        return

    # Group observations into per-series time series.
    # Series key is a human label: 'site' or 'site (SH)'.
    series: dict[str, list[tuple[str, float]]] = {}
    for obs in observations:
        if obs.price_ron is None:
            continue
        if site_filter and obs.site != site_filter:
            continue
        day = _fmt_date(obs.scraped_at)
        if not day:
            continue
        key = f"{obs.site} (SH)" if obs.is_used else obs.site
        series.setdefault(key, []).append((day, float(obs.price_ron)))

    if not series:
        console.print("[dim]no data points to plot[/dim]")
        return

    plt.clf()
    plt.date_form("Y-m-d")
    plt.theme("clear")
    # Plotext picks its own terminal size by default; cap the height so the chart
    # stays scannable even on very tall terminals.
    try:
        term_w = console.size.width
    except Exception:
        term_w = 100
    plt.plotsize(max(60, min(term_w, 140)), 22)

    for label, pts in sorted(series.items()):
        pts.sort(key=lambda p: p[0])
        xs = [p[0] for p in pts]
        ys = [p[1] for p in pts]
        if len(xs) == 1:
            plt.scatter(xs, ys, label=label, marker="braille")
        else:
            plt.plot(xs, ys, label=label, marker="braille")

    plt.title(f"{display_query} — price (RON) over time")
    plt.xlabel("date")
    plt.ylabel("RON")
    # plotext.build() returns the rendered chart as a string; print through the
    # rich console so it lives in the same output stream as everything else.
    console.print(plt.build())
