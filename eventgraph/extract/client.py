"""One document in, one validated Extraction out."""

from __future__ import annotations

import logging
import time

from pydantic import ValidationError

from ..source import Doc
from .models import Extraction
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
        output_config={"effort": "low"},
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
        extraction = Extraction.model_validate_json(_json_slice(text))
    except ValidationError as e:
        raise ExtractionError(f"schema: {e.errors()[:3]}") from e
    except ValueError as e:  # json decode errors are ValueError subclasses
        raise ExtractionError(f"json: {e}") from e
    return extraction, meta
