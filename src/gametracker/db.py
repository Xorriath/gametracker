"""SQLite storage for favorites and price observations."""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterator

DB_PATH = Path.home() / ".gametracker" / "db.sqlite"

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
    p = path or DB_PATH
    p.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(p, detect_types=sqlite3.PARSE_DECLTYPES)
    conn.row_factory = sqlite3.Row
    conn.executescript(_SCHEMA)
    conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
    return conn


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
    conn: sqlite3.Connection, normalized: str, site: str
) -> Observation | None:
    row = conn.execute(
        """SELECT normalized_query, site, matched_title, price_ron, url, availability,
                  is_used, status, strategy_used, scraped_at
           FROM price_observations
           WHERE normalized_query = ? AND site = ?
           ORDER BY scraped_at DESC LIMIT 1""",
        (normalized, site),
    ).fetchone()
    if row is None:
        return None
    d = dict(row)
    d["is_used"] = bool(d["is_used"])
    return Observation(**d)


def historic_low(
    conn: sqlite3.Connection, normalized: str, site: str
) -> float | None:
    row = conn.execute(
        """SELECT MIN(price_ron) AS lo FROM price_observations
           WHERE normalized_query = ? AND site = ? AND price_ron IS NOT NULL
             AND status = 'ok'""",
        (normalized, site),
    ).fetchone()
    return row["lo"] if row and row["lo"] is not None else None


def observation_count(
    conn: sqlite3.Connection, normalized: str, site: str
) -> int:
    row = conn.execute(
        """SELECT COUNT(*) AS n FROM price_observations
           WHERE normalized_query = ? AND site = ? AND status = 'ok'""",
        (normalized, site),
    ).fetchone()
    return int(row["n"]) if row else 0


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
