"""Stage B: LangGraph per-document extraction, run concurrently with asyncio."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import Callable
from typing import Any, TypedDict

import anthropic
import pyarrow as pa
import pyarrow.parquet as pq
from langgraph.graph import END, START, StateGraph

from ..config import Settings
from ..db import pg_connect
from ..source import Doc
from .client import ExtractionError, extract_document

log = logging.getLogger(__name__)
PARQUET_BATCH = 100
MAX_ATTEMPTS = 2  # one retry
RATE_LIMIT_BACKOFF = (1, 2, 4, 8)


class DocState(TypedDict, total=False):
    run_document_id: int
    doc: Doc
    extraction_json: str | None
    meta: dict
    error: str | None
    attempts: int


def _build_graph(extract_fn, client, model: str, event_name: str, seeds: list[str]):
    async def extract(state: DocState) -> DocState:
        attempts = state.get("attempts", 0) + 1
        try:
            x, meta = await _call_with_backoff(
                extract_fn, client, model, state["doc"], event_name, seeds
            )
            return {
                "extraction_json": x.model_dump_json(),
                "meta": meta,
                "error": None,
                "attempts": attempts,
            }
        except (ExtractionError, anthropic.APIError) as e:
            return {"extraction_json": None, "error": f"{e}", "attempts": attempts}

    def route(state: DocState) -> str:
        if state.get("extraction_json") is not None:
            return "write"
        return "extract" if state["attempts"] < MAX_ATTEMPTS else "failed"

    def write(state: DocState) -> DocState:
        return state

    def failed(state: DocState) -> DocState:
        log.warning(
            "doc %s failed after %d attempts: %s",
            state["run_document_id"],
            state["attempts"],
            state["error"],
        )
        return state

    g = StateGraph(DocState)
    g.add_node("extract", extract)
    g.add_node("write", write)
    g.add_node("failed", failed)
    g.add_edge(START, "extract")
    g.add_conditional_edges(
        "extract", route, {"extract": "extract", "write": "write", "failed": "failed"}
    )
    g.add_edge("write", END)
    g.add_edge("failed", END)
    return g.compile()


async def _call_with_backoff(extract_fn, client, model, doc, event_name, seeds):
    for i, delay in enumerate((*RATE_LIMIT_BACKOFF, None)):
        try:
            return await asyncio.to_thread(extract_fn, client, model, doc, event_name, seeds)
        except anthropic.RateLimitError:
            if delay is None:
                raise
            log.info("429 on doc %s, backoff %ss (%d)", doc.source_idx, delay, i + 1)
            await asyncio.sleep(delay)
    raise AssertionError("unreachable")


def run_extraction(
    settings: Settings,
    run_id: str,
    docs: list[tuple[int, Doc]],
    event_name: str,
    seeds: list[str],
    client: Any,
    extract_fn: Callable = extract_document,
    progress_cb: Callable[[int, int], None] | None = None,
) -> dict:
    out_dir = settings.data_dir / "runs" / run_id
    (out_dir / "extracted").mkdir(parents=True, exist_ok=True)
    graph = _build_graph(extract_fn, client, settings.anthropic_model, event_name, seeds)
    sem = asyncio.Semaphore(settings.llm_concurrency)
    done = 0

    async def one(rdid: int, doc: Doc) -> DocState:
        nonlocal done
        async with sem:
            state = await graph.ainvoke({"run_document_id": rdid, "doc": doc, "attempts": 0})
        done += 1
        if progress_cb:
            progress_cb(done, len(docs))
        return state

    async def main():
        return await asyncio.gather(*(one(r, d) for r, d in docs))

    states = asyncio.run(main())

    ok = [s for s in states if s.get("extraction_json") is not None]
    bad = [s for s in states if s.get("extraction_json") is None]
    stamp = str(time.time_ns())  # reruns must not overwrite earlier parts
    for i in range(0, len(ok), PARQUET_BATCH):
        chunk = ok[i : i + PARQUET_BATCH]
        table = pa.table(
            {
                "run_document_id": pa.array([s["run_document_id"] for s in chunk], pa.int64()),
                "extraction": pa.array([s["extraction_json"] for s in chunk], pa.string()),
                "tokens_in": pa.array([s["meta"]["tokens_in"] for s in chunk], pa.int64()),
                "tokens_out": pa.array([s["meta"]["tokens_out"] for s in chunk], pa.int64()),
                "elapsed_ms": pa.array([s["meta"]["elapsed_ms"] for s in chunk], pa.int64()),
            }
        )
        pq.write_table(
            table, out_dir / "extracted" / f"part-{stamp}-{i // PARQUET_BATCH:05d}.parquet"
        )
    with (out_dir / "failed.jsonl").open("w", encoding="utf-8") as f:
        for s in bad:
            f.write(
                json.dumps(
                    {"run_document_id": s["run_document_id"], "error": s.get("error")},
                    ensure_ascii=False,
                )
                + "\n"
            )

    with pg_connect(settings) as conn, conn.cursor() as cur:
        cur.executemany(
            "update run_document set status='extracted' where id=%s",
            [(s["run_document_id"],) for s in ok],
        )
        cur.executemany(
            "update run_document set status='failed' where id=%s",
            [(s["run_document_id"],) for s in bad],
        )
        conn.commit()

    result = {
        "extracted": len(ok),
        "failed": len(bad),
        "tokens_in": sum(s["meta"]["tokens_in"] for s in ok),
        "tokens_out": sum(s["meta"]["tokens_out"] for s in ok),
    }
    log.info("run_extraction run=%s: %s", run_id, result)
    if not ok:
        log.warning("run_extraction run=%s produced ZERO extractions", run_id)
    return result
