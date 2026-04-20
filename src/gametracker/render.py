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
    if row.matched_title is None:
        return "—"
    t = row.matched_title
    if row.is_used:
        t = f"{t} (SH)"
    return t


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
    if row.status != "ok" or row.price_ron is None or row.low is None:
        return None
    if row.price_ron - row.low < -0.005:
        return "green"
    return None


def render_table(display_query: str, rows: list[DisplayRow], console: Console) -> None:
    console.print(f"\n[bold]{display_query}[/bold]  [dim](PS5)[/dim]\n")

    table = Table(show_header=True, header_style="bold", box=None)
    table.add_column("Site")
    table.add_column("Matched title", overflow="fold", max_width=55)
    table.add_column("Price RON", justify="right")
    table.add_column("vs low", justify="right")
    table.add_column("Status")

    # Sort: ok first, then no_match, then errors; within each, by price ascending.
    def sort_key(r: DisplayRow):
        status_order = {"ok": 0, "no_match": 1, "blocked": 2, "error": 3}.get(r.status, 9)
        price = r.price_ron if r.price_ron is not None else float("inf")
        return (status_order, price, r.site)

    for row in sorted(rows, key=sort_key):
        table.add_row(
            row.site,
            _fmt_title(row),
            _fmt_price(row.price_ron),
            _fmt_vs_low(row),
            _fmt_status(row),
            style=_row_style(row),
        )

    console.print(table)


def render_errors(rows: list[DisplayRow], console: Console) -> None:
    """Verbose error listing to stderr."""
    for r in rows:
        if r.status in ("blocked", "error") and r.error:
            console.print(f"[dim]{r.site}:[/dim] [red]{r.status}[/red] — {r.error}")


def render_history(display_query: str, histories: list[SiteHistory], console: Console) -> None:
    console.print(f"\n[bold]{display_query}[/bold]  [dim](PS5)[/dim]\n")

    if not histories:
        console.print("[dim]no history yet — run `gametracker check` first[/dim]")
        return

    table = Table(show_header=True, header_style="bold", box=None)
    table.add_column("Site")
    table.add_column("Latest", justify="right")
    table.add_column("Low", justify="right")
    table.add_column("High", justify="right")
    table.add_column("Trend")

    for h in histories:
        table.add_row(
            h.site,
            f"{h.latest:.2f}",
            f"{h.low:.2f}",
            f"{h.high:.2f}",
            f"{h.sparkline}  [dim]({h.count})[/dim]",
        )
    console.print(table)
