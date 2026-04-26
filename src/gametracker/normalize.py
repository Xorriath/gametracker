"""Query normalization for DB keys and matching."""
from __future__ import annotations

import re
import unicodedata

_PUNCT = re.compile(r"[^\w\s]", re.UNICODE)
_WS = re.compile(r"\s+")

# Symbols the storefronts sprinkle into titles (™ ® © ℠) that NFKD would
# decompose into letter sequences ("TM", "C", "SM"). That breaks token coverage
# (e.g. "Battlefield™" → "battlefieldtm") so we strip them BEFORE NFKD instead
# of letting compatibility decomposition fold them into the alphabet.
_TYPOGRAPHIC_NOISE = str.maketrans({
    "™": " ", "®": " ", "©": " ", "℠": " ",
})


def normalize_query(text: str) -> str:
    """Lowercase, strip accents and punctuation, collapse whitespace."""
    t = text.translate(_TYPOGRAPHIC_NOISE)
    t = unicodedata.normalize("NFKD", t)
    t = t.encode("ascii", "ignore").decode("ascii")
    t = t.lower()
    t = _PUNCT.sub(" ", t)
    t = _WS.sub(" ", t).strip()
    return t
