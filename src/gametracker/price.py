"""Parse Romanian price strings into floats."""
from __future__ import annotations

import html
import re

# Examples we must parse:
#   "369,90Lei"              (jocurinoi SERP — RO comma, no space)
#   "255,00 Lei" / "255 Lei" (eMAG, general)
#   "99.99"                  (Flanco microdata, altex JSON, buy2play API)
#   "412.22 RON"             (Trendyol ld+json)
#   "1.234,56 lei"           (defensive: big prices with RO thousands)
#   255<sup>,00</sup> Lei    (eMAG SERP split markup — decoded to "255,00 Lei")

_NUM_RE = re.compile(r"-?\d[\d\.,\s]*")


class PriceParseError(ValueError):
    pass


def _strip_tags(s: str) -> str:
    # Remove any HTML tags and decode entities — for eMAG's <sup>,00</sup> markup
    s = re.sub(r"<[^>]+>", "", s)
    s = html.unescape(s)
    return s


def parse_price(raw: str | float | int | None) -> float:
    """Parse a price string into a float (RON). Raises PriceParseError on failure."""
    if raw is None:
        raise PriceParseError("empty price")
    if isinstance(raw, (int, float)):
        return float(raw)
    s = _strip_tags(str(raw)).strip()
    if not s:
        raise PriceParseError("empty price")

    m = _NUM_RE.search(s)
    if not m:
        raise PriceParseError(f"no numeric token in {raw!r}")
    num = m.group(0).strip()
    # Remove internal whitespace (e.g. "1 234,56")
    num = re.sub(r"\s+", "", num)

    # Decide decimal separator.
    has_dot = "." in num
    has_comma = "," in num
    if has_dot and has_comma:
        # "1.234,56" → last separator is decimal
        if num.rfind(",") > num.rfind("."):
            num = num.replace(".", "").replace(",", ".")
        else:  # "1,234.56" (unlikely in RO but defensive)
            num = num.replace(",", "")
    elif has_comma:
        # If comma is followed by exactly 1-2 digits at end, it's decimal.
        # Otherwise (e.g. "1,234") it's thousands.
        tail = num.rsplit(",", 1)[-1]
        if 1 <= len(tail) <= 2 and tail.isdigit():
            num = num.replace(",", ".")
        else:
            num = num.replace(",", "")
    # else: only dot or only digits — nothing to do.

    try:
        val = float(num)
    except ValueError as e:
        raise PriceParseError(f"cannot parse {raw!r}: {e}") from e
    if val < 0:
        raise PriceParseError(f"negative price: {raw!r}")
    return round(val, 2)
