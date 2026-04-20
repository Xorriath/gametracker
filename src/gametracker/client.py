"""HTTP client with browser impersonation, per-domain pacing, and cookie persistence.

One SiteClient per site. Use as an async context manager.
"""
from __future__ import annotations

import asyncio
import json
import logging
import random
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from curl_cffi.requests import AsyncSession

log = logging.getLogger(__name__)

COOKIES_DIR = Path.home() / ".gametracker" / "cookies"
DEFAULT_IMPERSONATE = "firefox133"

DEFAULT_HEADERS = {
    "Accept-Language": "ro-RO,ro;q=0.9,en;q=0.8",
    "DNT": "1",
    "Upgrade-Insecure-Requests": "1",
}


class BlockedError(Exception):
    """Site returned 403 or a challenge page."""


class RateLimited(Exception):
    """429 response — rate limited."""


@dataclass
class SiteConfig:
    name: str
    cooldown: float = 5.0        # min-gap seconds between requests
    jitter: float = 4.0          # added uniform(0, jitter) on top of cooldown
    timeout: float = 20.0
    impersonate: str = DEFAULT_IMPERSONATE
    retries: int = 3
    extra_headers: dict[str, str] = field(default_factory=dict)


# Generous defaults inferred from our scout.
# altex sits behind Akamai Bot Manager so it gets the most defensive cooldown.
SITE_COOLDOWN_DEFAULTS: dict[str, float] = {
    "altex": 120.0,
    "emag": 15.0,
    "flanco": 10.0,
    "trendyol": 5.0,
    "ozone": 5.0,
    "buy2play": 5.0,
    "jocurinoi": 5.0,
}


def site_config(name: str, overrides: dict[str, float] | None = None) -> SiteConfig:
    cd = (overrides or {}).get(name, SITE_COOLDOWN_DEFAULTS.get(name, 5.0))
    return SiteConfig(name=name, cooldown=cd)


class SiteClient:
    """One HTTP client per site — pacing + cookie persistence."""

    def __init__(self, cfg: SiteConfig) -> None:
        self.cfg = cfg
        self._last_at: float = 0.0
        self._lock = asyncio.Lock()
        self._session: AsyncSession | None = None
        self._last_url: str | None = None

    @property
    def cookies_path(self) -> Path:
        return COOKIES_DIR / f"{self.cfg.name}.json"

    def _load_cookies(self, session: AsyncSession) -> None:
        p = self.cookies_path
        if not p.exists():
            return
        try:
            data = json.loads(p.read_text())
        except Exception:
            return
        for c in data.get("cookies", []):
            try:
                session.cookies.set(
                    c["name"], c["value"],
                    domain=c.get("domain", ""),
                    path=c.get("path", "/"),
                )
            except Exception:
                continue

    def _save_cookies(self, session: AsyncSession) -> None:
        p = self.cookies_path
        p.parent.mkdir(parents=True, exist_ok=True)
        cookies: list[dict[str, Any]] = []
        try:
            jar = getattr(session.cookies, "jar", None) or session.cookies
            for c in jar:
                cookies.append({
                    "name": c.name,
                    "value": c.value,
                    "domain": c.domain,
                    "path": c.path,
                    "expires": c.expires,
                })
        except Exception:
            pass
        try:
            p.write_text(json.dumps({"cookies": cookies}))
        except Exception:
            log.debug("failed to save cookies for %s", self.cfg.name)

    async def __aenter__(self) -> "SiteClient":
        self._session = AsyncSession(impersonate=self.cfg.impersonate)
        await self._session.__aenter__()
        self._load_cookies(self._session)
        return self

    async def __aexit__(self, *exc: Any) -> None:
        if self._session is not None:
            try:
                self._save_cookies(self._session)
            finally:
                await self._session.__aexit__(*exc)
            self._session = None

    async def _pace(self) -> None:
        now = time.monotonic()
        target = self._last_at + self.cfg.cooldown + random.uniform(0, self.cfg.jitter)
        wait = max(0.0, target - now)
        if wait > 0:
            log.debug("%s: pacing %.1fs", self.cfg.name, wait)
            await asyncio.sleep(wait)
        self._last_at = time.monotonic()

    async def get(
        self,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        referer: str | None = None,
        **kw: Any,
    ) -> Any:
        """GET with pacing + retries. Raises BlockedError on 403, RateLimited on 429."""
        assert self._session is not None, "use as async context manager"
        last_err: Exception | None = None
        for attempt in range(self.cfg.retries):
            async with self._lock:
                await self._pace()
                hdrs: dict[str, str] = {**DEFAULT_HEADERS, **self.cfg.extra_headers}
                ref = referer or self._last_url
                if ref:
                    hdrs["Referer"] = ref
                if headers:
                    hdrs.update(headers)
                try:
                    r = await self._session.get(
                        url, headers=hdrs, timeout=self.cfg.timeout, **kw
                    )
                except Exception as e:
                    last_err = e
                    log.debug("%s: network error on %s: %s", self.cfg.name, url, e)
                    if attempt + 1 == self.cfg.retries:
                        raise
                    await asyncio.sleep(2 ** attempt)
                    continue

            if r.status_code == 403:
                raise BlockedError(f"{self.cfg.name}: 403 on {url}")
            if r.status_code == 429:
                delay = _parse_retry_after(r.headers.get("Retry-After")) or 10.0
                if attempt + 1 == self.cfg.retries:
                    raise RateLimited(f"{self.cfg.name}: 429 on {url}")
                log.debug("%s: 429, retry after %.1fs", self.cfg.name, delay)
                await asyncio.sleep(delay)
                continue
            if 500 <= r.status_code < 600 and attempt + 1 < self.cfg.retries:
                await asyncio.sleep(2 ** attempt)
                continue

            self._last_url = url
            return r

        # Exhausted retries without success.
        raise last_err or RuntimeError("retries exhausted without a response")


def _parse_retry_after(val: str | None) -> float | None:
    if not val:
        return None
    try:
        return float(val)
    except ValueError:
        return None
