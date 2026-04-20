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

SCHEMA_VERSION = 1

_SCHEMA = """
CREATE TABLE IF NOT EXISTS favorites (
    id               INTEGER PRIMARY KEY,
    normalized_query TEXT UNIQUE NOT NULL,
    display_query    TEXT NOT NULL,
    added_at         TEXT NOT NULL DEFAULT (datetime('now'))
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
    scraped_at       TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_obs_query_site ON price_observations(normalized_query, site);
CREATE INDEX IF NOT EXISTS idx_obs_scraped_at ON price_observations(scraped_at);
"""


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


def record_observation(conn: sqlite3.Connection, obs: Observation) -> None:
    conn.execute(
        """INSERT INTO price_observations
           (normalized_query, site, matched_title, price_ron, url, availability,
            is_used, status, strategy_used, scraped_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            obs.normalized_query, obs.site, obs.matched_title, obs.price_ron,
            obs.url, obs.availability, int(obs.is_used), obs.status,
            obs.strategy_used, obs.scraped_at,
        ),
    )
    conn.commit()


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
) -> tuple[float, str] | None:
    """Cheapest price ever recorded across ANY site for this (game, variant),
    plus the site it came from. Returns None if no data."""
    row = conn.execute(
        """SELECT price_ron, site FROM price_observations
           WHERE normalized_query = ? AND is_used = ? AND status = 'ok'
             AND price_ron IS NOT NULL
           ORDER BY price_ron ASC, scraped_at ASC
           LIMIT 1""",
        (normalized, int(is_used)),
    ).fetchone()
    if row is None:
        return None
    return float(row["price_ron"]), str(row["site"])


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
