"""Run orchestration: select -> extract -> batch, with status/counters on the run row."""

from __future__ import annotations

import logging
import re
from collections.abc import Callable
from datetime import UTC, date, datetime
from typing import Any

from .batch.job import run_batch
from .config import Settings
from .db import pg_connect
from .extract.client import extract_document
from .extract.graph import run_extraction
from .select import select_documents
from .source import fetch_by_ids

log = logging.getLogger(__name__)

STAGES = ("select", "extract", "batch")
ACTIVE = ("selecting", "extracting", "batching")
_SLUG = re.compile(r"[^0-9A-Za-z가-힣]")


def create_run(
    settings: Settings,
    event_name: str,
    date_from: date,
    date_to: date,
    seed_persons: list[str],
    seed_orgs: list[str],
    seed_countries: list[str],
    id_prefix: str = "",
) -> str:
    slug = _SLUG.sub("", event_name)[:8]
    stamp = datetime.now(tz=UTC).astimezone()
    run_id = f"{id_prefix}{stamp:%Y%m%d-%H%M%S}-{slug}"
    with pg_connect(settings) as conn:
        conn.execute(
            "insert into run(id, event_name, date_from, date_to, seed_persons, seed_orgs, seed_countries)"
            " values (%s,%s,%s,%s,%s,%s,%s)",
            (run_id, event_name, date_from, date_to, seed_persons, seed_orgs, seed_countries),
        )
        conn.commit()
    return run_id


def get_active_run(settings: Settings) -> str | None:
    with pg_connect(settings) as conn:
        row = conn.execute(
            "select id from run where status = any(%s) order by started_at desc limit 1",
            (list(ACTIVE),),
        ).fetchone()
    return row[0] if row else None


def _set(settings: Settings, run_id: str, **cols: Any) -> None:
    sets = ", ".join(f"{k}=%s" for k in cols)
    with pg_connect(settings) as conn:
        conn.execute(f"update run set {sets} where id=%s", (*cols.values(), run_id))
        conn.commit()


_RUN_COLS = ("event_name", "date_from", "date_to", "seed_persons", "seed_orgs", "seed_countries")


def _load_run(settings: Settings, run_id: str) -> dict:
    with pg_connect(settings) as conn:
        row = conn.execute(
            f"select {', '.join(_RUN_COLS)} from run where id=%s", (run_id,)
        ).fetchone()
    if row is None:
        raise KeyError(f"run {run_id} not found")
    return dict(zip(_RUN_COLS, row, strict=True))


def _client(settings: Settings, client: Any):
    if client is not None:
        return client
    import anthropic

    return anthropic.Anthropic(api_key=settings.anthropic_api_key or None)


def _selected_docs(settings: Settings, run_id: str) -> list[tuple[int, Any]]:
    with pg_connect(settings) as conn:
        rows = conn.execute(
            "select id, source_table, source_idx from run_document"
            " where run_id=%s and status='selected' order by rank",
            (run_id,),
        ).fetchall()
    bodies: dict[str, dict[int, Any]] = {}
    for t in {r[1] for r in rows}:
        bodies[t] = fetch_by_ids(settings, t, [r[2] for r in rows if r[1] == t])
    docs = [(r[0], bodies[r[1]][r[2]]) for r in rows if r[2] in bodies[r[1]]]
    log.info("extract stage: %d selected docs, %d bodies found", len(rows), len(docs))
    return docs


def run_pipeline(
    settings: Settings,
    run_id: str,
    stages: tuple[str, ...] = STAGES,
    client: Any = None,
    extract_fn: Callable = extract_document,
    progress_cb: Callable[[int, int], None] | None = None,
) -> dict:
    run = _load_run(settings, run_id)
    seeds = [*run["seed_persons"], *run["seed_orgs"], *run["seed_countries"]]
    results: dict[str, dict] = {}
    try:
        if "select" in stages:
            _set(settings, run_id, status="selecting")
            results["select"] = select_documents(
                settings,
                run_id,
                run["event_name"],
                seeds,
                run["date_from"],
                run["date_to"],
                _client(settings, client),
            )
        if "extract" in stages:
            real_llm = extract_fn is extract_document
            if real_llm and not settings.anthropic_api_key:
                raise RuntimeError("ANTHROPIC_API_KEY not set in config/.env")
            _set(settings, run_id, status="extracting")
            r = run_extraction(
                settings,
                run_id,
                _selected_docs(settings, run_id),
                run["event_name"],
                seeds,
                _client(settings, client) if real_llm else client,
                extract_fn,
                progress_cb,
            )
            results["extract"] = r
            with pg_connect(settings) as conn:
                counts = dict(
                    conn.execute(
                        "select status, count(*) from run_document where run_id=%s group by status",
                        (run_id,),
                    ).fetchall()
                )
                conn.execute(
                    "update run set doc_extracted=%s, doc_failed=%s,"
                    " tokens_in=tokens_in+%s, tokens_out=tokens_out+%s where id=%s",
                    (
                        counts.get("extracted", 0),
                        counts.get("failed", 0),
                        r["tokens_in"],
                        r["tokens_out"],
                        run_id,
                    ),
                )
                conn.commit()
        if "batch" in stages:
            _set(settings, run_id, status="batching")
            results["batch"] = run_batch(settings, run_id)
        _set(settings, run_id, status="done", error=None, finished_at=datetime.now(tz=UTC))
    except Exception as e:
        log.exception("run %s failed", run_id)
        _set(
            settings,
            run_id,
            status="failed",
            error=f"{type(e).__name__}: {e}"[:2000],
            finished_at=datetime.now(tz=UTC),
        )
        raise
    summary = {k: {kk: vv for kk, vv in v.items() if kk != "keywords"} for k, v in results.items()}
    log.info("run %s done: %s", run_id, summary)
    return results
