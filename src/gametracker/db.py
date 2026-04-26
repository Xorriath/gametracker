"""SQLite storage for favorites and price observations."""
from __future__ import annotations

import os
import shutil
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterator


def _default_db_path() -> Path:
    """Return the DB path. GAMETRACKER_DB env var overrides — useful for tests
    so real user data in ~/.gametracker never gets touched."""
    override = os.environ.get("GAMETRACKER_DB")
    if override:
        return Path(override)
    return Path.home() / ".gametracker" / "db.sqlite"


DB_PATH = _default_db_path()

SCHEMA_VERSION = 2

_SCHEMA = """
CREATE TABLE IF NOT EXISTS favorites (
    id               INTEGER PRIMARY KEY,
    normalized_query TEXT UNIQUE NOT NULL,
    display_query    TEXT NOT NULL,
    added_at         TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS runs (
    id          INTEGER PRIMARY KEY,
    kind        TEXT NOT NULL,
    started_at  TEXT NOT NULL DEFAULT (datetime('now')),
    ended_at    TEXT
);

CREATE TABLE IF NOT EXISTS price_observations (
    id               INTEGER PRIMARY KEY,
    normalized_query TEXT NOT NULL,
    site             TEXT NOT NULL,
    matched_title    TEXT,
    price_ron        REAL,
    url              TEXT,
    availability     TEXT,
    is_used          INTEGER NOT NULL DEFAULT 0,
    status           TEXT NOT NULL,
    strategy_used    TEXT,
    scraped_at       TEXT NOT NULL DEFAULT (datetime('now')),
    run_id           INTEGER
);

CREATE INDEX IF NOT EXISTS idx_obs_query_site ON price_observations(normalized_query, site);
CREATE INDEX IF NOT EXISTS idx_obs_scraped_at ON price_observations(scraped_at);
CREATE INDEX IF NOT EXISTS idx_runs_kind       ON runs(kind, id);
"""


def _migrate(conn: sqlite3.Connection) -> None:
    """Idempotent migration for older DBs that pre-date schema v2.

    Adds `run_id` to `price_observations` if missing, then ensures the related
    index exists. Safe to run on fresh installs too — both operations are no-ops
    when the column/index already exist.
    """
    cols = {row[1] for row in conn.execute("PRAGMA table_info(price_observations)").fetchall()}
    if "run_id" not in cols:
        conn.execute("ALTER TABLE price_observations ADD COLUMN run_id INTEGER")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_obs_run ON price_observations(run_id)")
    conn.commit()


@dataclass(frozen=True)
class Favorite:
    normalized_query: str
    display_query: str
    added_at: str


@dataclass(frozen=True)
class Observation:
    normalized_query: str
    site: str
    matched_title: str | None
    price_ron: float | None
    url: str | None
    availability: str | None
    is_used: bool
    status: str  # ok | no_match | blocked | error
    strategy_used: str | None
    scraped_at: str


def connect(path: Path | None = None) -> sqlite3.Connection:
    p = path or _default_db_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    _rotate_backup(p)
    conn = sqlite3.connect(p, detect_types=sqlite3.PARSE_DECLTYPES)
    conn.row_factory = sqlite3.Row
    conn.executescript(_SCHEMA)
    _migrate(conn)
    conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
    return conn


def _rotate_backup(db_path: Path) -> None:
    """Copy the existing DB to <name>.bak before we open it.

    Cheap defensive backup — a single .bak overwritten each run. If the tool
    corrupts or someone rm's the DB, the .bak still has the pre-run state.
    """
    try:
        if db_path.exists() and db_path.stat().st_size > 0:
            shutil.copyfile(db_path, db_path.with_suffix(db_path.suffix + ".bak"))
    except Exception:
        # Backup failures must never block the app.
        pass


def add_favorite(conn: sqlite3.Connection, normalized: str, display: str) -> bool:
    """Return True if newly added, False if it already existed."""
    cur = conn.execute(
        "INSERT OR IGNORE INTO favorites(normalized_query, display_query) VALUES (?, ?)",
        (normalized, display),
    )
    conn.commit()
    return cur.rowcount > 0


def remove_favorite(conn: sqlite3.Connection, normalized: str) -> bool:
    cur = conn.execute("DELETE FROM favorites WHERE normalized_query = ?", (normalized,))
    conn.commit()
    return cur.rowcount > 0


def list_favorites(conn: sqlite3.Connection) -> list[Favorite]:
    rows = conn.execute(
        "SELECT normalized_query, display_query, added_at FROM favorites ORDER BY added_at"
    ).fetchall()
    return [Favorite(**dict(r)) for r in rows]


def record_observation(
    conn: sqlite3.Connection, obs: Observation, *, run_id: int | None = None,
) -> None:
    conn.execute(
        """INSERT INTO price_observations
           (normalized_query, site, matched_title, price_ron, url, availability,
            is_used, status, strategy_used, scraped_at, run_id)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            obs.normalized_query, obs.site, obs.matched_title, obs.price_ron,
            obs.url, obs.availability, int(obs.is_used), obs.status,
            obs.strategy_used, obs.scraped_at, run_id,
        ),
    )
    conn.commit()


def start_run(conn: sqlite3.Connection, kind: str) -> int:
    """Open a new run row and return its id. `kind` is e.g. 'favorites' or 'check'."""
    cur = conn.execute("INSERT INTO runs(kind) VALUES (?)", (kind,))
    conn.commit()
    return int(cur.lastrowid)


def end_run(conn: sqlite3.Connection, run_id: int) -> None:
    conn.execute(
        "UPDATE runs SET ended_at = datetime('now') WHERE id = ?", (run_id,),
    )
    conn.commit()


def latest_run_id(conn: sqlite3.Connection, kind: str) -> int | None:
    """Most recently started run of the given kind, or None if no run yet."""
    row = conn.execute(
        "SELECT id FROM runs WHERE kind = ? ORDER BY id DESC LIMIT 1",
        (kind,),
    ).fetchone()
    return int(row["id"]) if row else None


def failed_targets_for_run(
    conn: sqlite3.Connection, run_id: int,
) -> list[tuple[str, str]]:
    """Return the (normalized_query, site) pairs that failed (blocked or error)
    in the given run. Used by `--fix-missing` to retry only those slots."""
    rows = conn.execute(
        """SELECT DISTINCT normalized_query, site FROM price_observations
           WHERE run_id = ? AND status IN ('blocked', 'error')""",
        (run_id,),
    ).fetchall()
    return [(str(r["normalized_query"]), str(r["site"])) for r in rows]


def latest_observation(
    conn: sqlite3.Connection, normalized: str, site: str,
    is_used: bool | None = None,
) -> Observation | None:
    sql = """SELECT normalized_query, site, matched_title, price_ron, url, availability,
                    is_used, status, strategy_used, scraped_at
             FROM price_observations
             WHERE normalized_query = ? AND site = ?"""
    args: list = [normalized, site]
    if is_used is not None:
        sql += " AND is_used = ?"
        args.append(int(is_used))
    sql += " ORDER BY scraped_at DESC LIMIT 1"
    row = conn.execute(sql, args).fetchone()
    if row is None:
        return None
    d = dict(row)
    d["is_used"] = bool(d["is_used"])
    return Observation(**d)


def latest_observations(
    conn: sqlite3.Connection, normalized: str, site: str
) -> list[Observation]:
    """Return the latest `ok` observation per is_used variant (up to 2 rows)."""
    out: list[Observation] = []
    for is_used in (False, True):
        obs = latest_observation(conn, normalized, site, is_used=is_used)
        if obs is not None and obs.status == "ok":
            out.append(obs)
    return out


def historic_low(
    conn: sqlite3.Connection, normalized: str, site: str,
    is_used: bool | None = None,
) -> float | None:
    sql = """SELECT MIN(price_ron) AS lo FROM price_observations
             WHERE normalized_query = ? AND site = ? AND price_ron IS NOT NULL
               AND status = 'ok'"""
    args: list = [normalized, site]
    if is_used is not None:
        sql += " AND is_used = ?"
        args.append(int(is_used))
    row = conn.execute(sql, args).fetchone()
    return row["lo"] if row and row["lo"] is not None else None


def historic_low_with_date(
    conn: sqlite3.Connection, normalized: str, site: str,
    is_used: bool | None = None,
) -> tuple[float, str] | None:
    """Per-site historic low plus the date it was recorded. Ties break by earliest date."""
    sql = """SELECT price_ron, scraped_at FROM price_observations
             WHERE normalized_query = ? AND site = ? AND price_ron IS NOT NULL
               AND status = 'ok'"""
    args: list = [normalized, site]
    if is_used is not None:
        sql += " AND is_used = ?"
        args.append(int(is_used))
    sql += " ORDER BY price_ron ASC, scraped_at ASC LIMIT 1"
    row = conn.execute(sql, args).fetchone()
    if row is None or row["price_ron"] is None:
        return None
    return float(row["price_ron"]), str(row["scraped_at"])


def observation_count(
    conn: sqlite3.Connection, normalized: str, site: str,
    is_used: bool | None = None,
) -> int:
    sql = """SELECT COUNT(*) AS n FROM price_observations
             WHERE normalized_query = ? AND site = ? AND status = 'ok'"""
    args: list = [normalized, site]
    if is_used is not None:
        sql += " AND is_used = ?"
        args.append(int(is_used))
    row = conn.execute(sql, args).fetchone()
    return int(row["n"]) if row else 0


def historic_low_global(
    conn: sqlite3.Connection, normalized: str, is_used: bool,
) -> tuple[float, str, str] | None:
    """Cheapest price ever recorded across ANY site for this (game, variant),
    plus the site and date it came from. Returns None if no data."""
    row = conn.execute(
        """SELECT price_ron, site, scraped_at FROM price_observations
           WHERE normalized_query = ? AND is_used = ? AND status = 'ok'
             AND price_ron IS NOT NULL
           ORDER BY price_ron ASC, scraped_at ASC
           LIMIT 1""",
        (normalized, int(is_used)),
    ).fetchone()
    if row is None:
        return None
    return float(row["price_ron"]), str(row["site"]), str(row["scraped_at"])


def iter_history(
    conn: sqlite3.Connection, normalized: str
) -> Iterator[Observation]:
    rows = conn.execute(
        """SELECT normalized_query, site, matched_title, price_ron, url, availability,
                  is_used, status, strategy_used, scraped_at
           FROM price_observations
           WHERE normalized_query = ? AND status = 'ok'
           ORDER BY site, scraped_at""",
        (normalized,),
    ).fetchall()
    for r in rows:
        d = dict(r)
        d["is_used"] = bool(d["is_used"])
        yield Observation(**d)


def now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
