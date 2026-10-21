"""Resolve Korean relative/absolute date expressions against an article date."""

from __future__ import annotations

import re
from datetime import date, timedelta

_ISO = re.compile(r"(\d{4})-(\d{1,2})-(\d{1,2})")
_YMD = re.compile(r"(\d{4})년\s*(\d{1,2})월\s*(\d{1,2})일")
_MD = re.compile(r"(\d{1,2})월\s*(\d{1,2})일")
_D = re.compile(r"(?:지난|오는|이달)?\s*(\d{1,2})일")
_WORDS = {"오늘": 0, "어제": -1, "내일": 1, "그제": -2, "엊그제": -2}


def _safe(y: int, m: int, d: int) -> date | None:
    try:
        return date(y, m, d)
    except ValueError:
        return None


def resolve_date(text: str, base: date) -> date | None:
    t = (text or "").strip()
    if not t:
        return None
    if t in _WORDS:
        return base + timedelta(days=_WORDS[t])
    if m := _ISO.search(t):
        return _safe(int(m[1]), int(m[2]), int(m[3]))
    if m := _YMD.search(t):
        return _safe(int(m[1]), int(m[2]), int(m[3]))
    if m := _MD.search(t):
        return _safe(base.year, int(m[1]), int(m[2]))
    if m := _D.fullmatch(t):
        d = int(m[1])
        cand = _safe(base.year, base.month, d)
        if cand is None or cand > base:  # a day number later than today means last month
            prev = base.replace(day=1) - timedelta(days=1)
            cand = _safe(prev.year, prev.month, d)
        return cand
    return None
