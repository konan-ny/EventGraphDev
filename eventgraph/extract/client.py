"""One document in, one validated Extraction out."""

from __future__ import annotations

import json
import logging
import time

from pydantic import ValidationError

from ..source import Doc
from .dates import resolve_date
from .models import OUTPUT_SCHEMA, Extraction
from .prompt import SYSTEM_PROMPT, build_user_message

log = logging.getLogger(__name__)
MAX_TOKENS = 8192


class ExtractionError(Exception):
    """Response could not be turned into an Extraction (bad JSON, truncated, refused)."""


def _json_slice(text: str) -> str:
    start, end = text.find("{"), text.rfind("}")
    if start < 0 or end <= start:
        raise ExtractionError("no JSON object in response")
    return text[start : end + 1]


def extract_document(
    client, model: str, doc: Doc, event_name: str, seeds: list[str]
) -> tuple[Extraction, dict]:
    t0 = time.perf_counter()
    resp = client.messages.create(
        model=model,
        max_tokens=MAX_TOKENS,
        system=SYSTEM_PROMPT,
        output_config={"effort": "low", "format": {"type": "json_schema", "schema": OUTPUT_SCHEMA}},
        messages=[{"role": "user", "content": build_user_message(doc, event_name, seeds)}],
    )
    meta = {
        "tokens_in": resp.usage.input_tokens,
        "tokens_out": resp.usage.output_tokens,
        "elapsed_ms": int((time.perf_counter() - t0) * 1000),
    }
    if resp.stop_reason != "end_turn":
        raise ExtractionError(f"stop_reason={resp.stop_reason}")
    text = "".join(getattr(b, "text", "") for b in resp.content if getattr(b, "type", "") == "text")
    try:
        data = json.loads(_json_slice(text))
    except ValueError as e:
        raise ExtractionError(f"json: {e}") from e
    _resolve_dates(data, doc.writedate.date())
    try:
        extraction = Extraction.model_validate(data)
    except ValidationError as e:
        raise ExtractionError(f"schema: {e.errors()[:3]}") from e
    return extraction, meta


def _resolve_dates(data: dict, base) -> None:
    """'지난 24일' / '6월 25일' style strings -> ISO using the article date; else untouched."""
    for r in data.get("relations") or []:
        if isinstance(r, dict) and isinstance(r.get("evidence_date"), str):
            r["evidence_date"] = str(resolve_date(r["evidence_date"], base) or r["evidence_date"])
    for ev in data.get("events") or []:
        if isinstance(ev, dict) and isinstance(ev.get("date"), str):
            ev["date"] = str(resolve_date(ev["date"], base) or ev["date"])
