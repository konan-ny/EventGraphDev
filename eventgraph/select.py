"""Stage A: pick the documents worth sending to the LLM."""

from __future__ import annotations

import logging
import re
from datetime import date

from .config import Settings
from .db import pg_connect
from .source import Doc, fetch_candidates

log = logging.getLogger(__name__)

_EXPAND_PROMPT = (
    "다음 사건과 관련된 한국어 뉴스 기사를 찾기 위한 검색 키워드 10~20개를 만들어라. "
    "사건 이름의 동의어·약칭, 핵심 인물·기관·국가·지역·사건 고유명사를 포함한다. "
    "JSON 문자열 배열만 출력하고 다른 텍스트는 쓰지 않는다.\n\n"
    "사건: {event}\n이미 알려진 키워드: {seeds}"
)


def expand_keywords(client, model: str, event_name: str, seeds: list[str]) -> list[str]:
    """Ask the LLM for related keywords; always returns event_name + seeds first."""
    base = _dedupe([event_name, *seeds])
    try:
        resp = client.messages.create(
            model=model,
            max_tokens=2048,
            output_config={"effort": "low"},
            messages=[
                {
                    "role": "user",
                    "content": _EXPAND_PROMPT.format(
                        event=event_name, seeds=", ".join(seeds) or "없음"
                    ),
                }
            ],
        )
        text = "".join(getattr(b, "text", "") for b in resp.content)
        # quoted strings only: a truncated array still yields its complete items
        extra = [k.strip() for k in re.findall(r'"([^"\n]+)"', text) if k.strip()]
    except Exception as e:  # noqa: BLE001 - degrade to seeds, never stop the run
        log.warning("expand_keywords failed (%s); using seeds only", e)
        extra = []
    return _dedupe(base + extra)


def _dedupe(items: list[str]) -> list[str]:
    seen: set[str] = set()
    out = []
    for k in items:
        if k and k not in seen:
            seen.add(k)
            out.append(k)
    return out


BODY_CAP = 5  # a keyword repeated 60 times in a long article counts as 5: length must not win


def score_document(doc: Doc, keywords: list[str], title_weight: float = 3.0) -> float:
    kws = [k for k in keywords if len(k) >= 2]  # one-char keywords ('美') match too much noise
    return sum(doc.title.count(k) * title_weight + min(doc.body.count(k), BODY_CAP) for k in kws)


def select_documents(
    settings: Settings,
    run_id: str,
    event_name: str,
    seeds: list[str],
    date_from: date,
    date_to: date,
    client,
) -> dict:
    docs, stats = fetch_candidates(settings, date_from, date_to)
    keywords = expand_keywords(client, settings.anthropic_model, event_name, seeds)
    scored = [(score_document(d, keywords), d) for d in docs]
    scored = sorted(((s, d) for s, d in scored if s > 0), key=lambda x: x[0], reverse=True)[
        : settings.max_docs
    ]
    with pg_connect(settings) as conn:
        with conn.cursor() as cur:
            cur.executemany(
                "insert into run_document(run_id, source_table, source_idx, title, url, press, writedate, score, rank)"
                " values (%s,%s,%s,%s,%s,%s,%s,%s,%s) on conflict do nothing",
                [
                    (
                        run_id,
                        d.source_table,
                        d.source_idx,
                        d.title,
                        d.url,
                        d.press,
                        d.writedate,
                        s,
                        i + 1,
                    )
                    for i, (s, d) in enumerate(scored)
                ],
            )
            cur.execute(
                "update run set expanded_keywords=%s, doc_selected=%s where id=%s",
                (keywords, len(scored), run_id),
            )
        conn.commit()
    result = {
        "candidates": len(docs),
        "selected": len(scored),
        "keywords": keywords,
        **stats,
    }
    log.info(
        "select_documents run=%s: %s",
        run_id,
        {k: v for k, v in result.items() if k != "keywords"},
    )
    return result
