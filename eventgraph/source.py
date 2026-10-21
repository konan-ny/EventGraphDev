"""Read-only access to the MySQL news corpus."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from urllib.parse import urlparse

from .config import Settings
from .db import mysql_connect

log = logging.getLogger(__name__)

_TAG = re.compile(r"<[^<>]{1,80}>")  # html tags and broadcast cues like <앵커>, <기자>
# tags that end a line (block tags, broadcast cues like <앵커>) vs inline ones (<b>, <a>)
_BREAKING = re.compile(r"^</?(br|p|div|li|h\d|tr)(?![a-z])|[가-힣]", re.IGNORECASE)
_SPACES = re.compile(r"[ \t\u3000]+")
_BLANK_LINES = re.compile(r"\n\s*\n+")
_HOST_PREFIX = re.compile(r"^(www|news|m|mnews)\.")


@dataclass
class Doc:
    source_table: str
    source_idx: int
    title: str
    body: str
    writedate: datetime
    url: str
    press: str


def clean_body(raw: str) -> str:
    s = _TAG.sub(lambda m: "\n" if _BREAKING.search(m.group(0)[1:]) else "", raw or "")
    s = _SPACES.sub(" ", s)
    s = "\n".join(line.strip() for line in s.splitlines())
    s = _BLANK_LINES.sub("\n", s)
    return s.strip()


def press_from_url(url2: str) -> str:
    host = urlparse(url2 or "").hostname or ""
    return _HOST_PREFIX.sub("", host)


def fetch_candidates(
    settings: Settings, date_from: date, date_to: date
) -> tuple[list[Doc], dict]:
    t = settings.db_table
    lo, hi = date_from.isoformat(), (date_to + timedelta(days=1)).isoformat()
    base = f"FROM `{t}` WHERE writedate >= %s AND writedate < %s"
    with mysql_connect(settings) as conn, conn.cursor() as cur:
        cur.execute(
            f"SELECT SUM(writedate <= '1900-01-01') bad_date, SUM(body IS NULL OR body = '') empty_body {base}",
            (lo, hi),
        )
        r = cur.fetchone()
        stats = {
            "excluded_bad_date": int(r["bad_date"] or 0),
            "excluded_empty_body": int(r["empty_body"] or 0),
        }
        cur.execute(
            f"SELECT idx, title, body, writedate, url, url2 {base} AND writedate > '1900-01-01' AND body IS NOT NULL AND body <> ''",
            (lo, hi),
        )
        docs = [
            Doc(
                t,
                int(row["idx"]),
                row["title"],
                clean_body(row["body"]),
                row["writedate"],
                row["url"] or "",
                press_from_url(row["url2"] or ""),
            )
            for row in cur.fetchall()
        ]
    stats["total"] = len(docs)
    log.info("fetch_candidates %s..%s: %s", lo, date_to, stats)
    return docs, stats
