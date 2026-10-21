from datetime import datetime
from types import SimpleNamespace

import pytest

from eventgraph.extract.client import ExtractionError, extract_document
from eventgraph.source import Doc

DOC = Doc("t", 1, "제목", "본문", datetime(2026, 6, 27, 10, 0), "http://u", "sbs.co.kr")


def _client(text, stop="end_turn"):
    class Messages:
        @staticmethod
        def create(**kw):
            return SimpleNamespace(
                content=[
                    SimpleNamespace(type="thinking", text=""),
                    SimpleNamespace(type="text", text=text),
                ],
                stop_reason=stop,
                usage=SimpleNamespace(input_tokens=100, output_tokens=20),
            )

    return SimpleNamespace(messages=Messages())


def test_extract_document_parses_fenced_json():
    text = '```json\n{"entities":[{"temp_id":"e1","type":"COUNTRY","name_ko":"이란"}],"relations":[],"events":[]}\n```'
    x, meta = extract_document(_client(text), "m", DOC, "ev", [])
    assert x.entities[0].name_ko == "이란"
    assert meta["tokens_in"] == 100 and meta["tokens_out"] == 20 and meta["elapsed_ms"] >= 0


def test_extract_document_raises_on_non_json():
    with pytest.raises(ExtractionError):
        extract_document(_client("not json"), "m", DOC, "ev", [])


def test_extract_document_raises_on_truncation():
    with pytest.raises(ExtractionError, match="max_tokens"):
        extract_document(_client('{"entities":[', stop="max_tokens"), "m", DOC, "ev", [])
