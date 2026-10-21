from dataclasses import replace
from datetime import date
from types import SimpleNamespace

import pytest

from eventgraph.extract.models import Extraction
from eventgraph.pipeline import create_run, get_active_run, run_pipeline


class FakeClient:
    """expand_keywords gets a JSON array; nothing else is called through the client."""

    class messages:  # mirrors anthropic client shape
        @staticmethod
        def create(**kw):
            return SimpleNamespace(content=[SimpleNamespace(text='["이란", "호르무즈"]')])


def fake_extract_with_one_entity(client, model, doc, event_name, seeds):
    x = Extraction.model_validate(
        {
            "entities": [
                {"temp_id": "e1", "type": "COUNTRY", "name_ko": "이란"},
                {"temp_id": "e2", "type": "PERSON", "name_ko": "도널드 트럼프"},
            ],
            "relations": [
                {
                    "src_temp_id": "e2",
                    "dst_temp_id": "e1",
                    "type": "CRITICIZES",
                    "evidence": doc.title,
                }
            ],
            "events": [],
        }
    )
    return x, {"tokens_in": 10, "tokens_out": 5, "elapsed_ms": 1}


def test_create_run_id_shape(settings, pg):
    rid = create_run(
        settings,
        "미국-이란 호르무즈 사태",
        date(2026, 6, 27),
        date(2026, 6, 27),
        ["트럼프"],
        [],
        ["이란"],
        id_prefix="test-",
    )
    assert rid.startswith("test-") and rid.endswith("미국이란호르무즈")
    row = pg.execute(
        "select status, seed_persons, seed_countries from run where id=%s", (rid,)
    ).fetchone()
    assert row == ("created", ["트럼프"], ["이란"])


@pytest.mark.integration
def test_run_pipeline_end_to_end_small(settings, pg, tmp_path):
    s = replace(settings, data_dir=tmp_path, max_docs=5)
    rid = create_run(
        s,
        "호르무즈",
        date(2026, 6, 27),
        date(2026, 6, 27),
        ["트럼프"],
        [],
        ["이란"],
        id_prefix="test-",
    )
    r = run_pipeline(s, rid, client=FakeClient(), extract_fn=fake_extract_with_one_entity)
    row = pg.execute(
        "select status, doc_selected, doc_extracted, doc_failed, tokens_in, finished_at from run where id=%s",
        (rid,),
    ).fetchone()
    assert row[:5] == ("done", 5, 5, 0, 50) and row[5] is not None
    assert pg.execute("select count(*) from entity where run_id=%s", (rid,)).fetchone()[0] == 2
    assert (
        r["select"]["selected"] == 5
        and r["extract"]["extracted"] == 5
        and r["batch"]["entities"] == 2
    )


def test_run_pipeline_marks_failed_on_exception(settings, pg, tmp_path):
    s = replace(settings, data_dir=tmp_path, max_docs=2)
    rid = create_run(
        s, "호르무즈", date(2026, 6, 27), date(2026, 6, 27), [], [], [], id_prefix="test-"
    )

    def boom(*a, **k):
        raise RuntimeError("kaboom")

    with pytest.raises(RuntimeError):
        run_pipeline(s, rid, stages=("select", "extract"), client=FakeClient(), extract_fn=boom)
    row = pg.execute("select status, error from run where id=%s", (rid,)).fetchone()
    assert row[0] == "failed" and "kaboom" in row[1]


def test_extract_refuses_without_api_key(settings, pg, tmp_path):
    s = replace(settings, data_dir=tmp_path, anthropic_api_key="")
    rid = create_run(
        s, "호르무즈", date(2026, 6, 27), date(2026, 6, 27), [], [], [], id_prefix="test-"
    )
    with pytest.raises(RuntimeError, match="ANTHROPIC_API_KEY"):
        run_pipeline(s, rid, stages=("extract",))
    assert pg.execute("select status from run where id=%s", (rid,)).fetchone()[0] == "failed"


def test_get_active_run(settings, pg):
    assert get_active_run(settings) is None
    rid = create_run(
        settings, "x", date(2026, 6, 27), date(2026, 6, 27), [], [], [], id_prefix="test-"
    )
    pg.execute("update run set status='extracting' where id=%s", (rid,))
    assert get_active_run(settings) == rid


@pytest.mark.integration
def test_run_fails_when_every_extraction_fails(settings, pg, tmp_path):
    from eventgraph.extract.client import ExtractionError

    s = replace(settings, data_dir=tmp_path, max_docs=3)
    rid = create_run(
        s, "호르무즈", date(2026, 6, 27), date(2026, 6, 27), [], [], [], id_prefix="test-"
    )

    def bad(*a, **k):
        raise ExtractionError("bad json")

    with pytest.raises(RuntimeError, match="0"):
        run_pipeline(s, rid, client=FakeClient(), extract_fn=bad)
    row = pg.execute("select status, doc_failed from run where id=%s", (rid,)).fetchone()
    assert row == ("failed", 3)
    assert pg.execute("select count(*) from entity where run_id=%s", (rid,)).fetchone()[0] == 0


@pytest.mark.integration
def test_run_reports_progress_on_run_row(settings, pg, tmp_path):
    seen = []

    def fake(client, model, doc, event_name, seeds):
        seen.append(
            pg.execute(
                "select doc_extracted from run where id like 'test-%%' order by started_at desc limit 1"
            ).fetchone()[0]
        )
        return Extraction(), {"tokens_in": 1, "tokens_out": 1, "elapsed_ms": 0}

    s = replace(settings, data_dir=tmp_path, max_docs=12, llm_concurrency=1)
    rid = create_run(
        s, "호르무즈", date(2026, 6, 27), date(2026, 6, 27), [], [], [], id_prefix="test-"
    )
    run_pipeline(s, rid, stages=("select", "extract"), client=FakeClient(), extract_fn=fake)
    assert max(seen) >= 10  # progress was written to the run row before extraction finished


def test_created_run_counts_as_active(settings, pg):
    rid = create_run(
        settings, "x", date(2026, 6, 27), date(2026, 6, 27), [], [], [], id_prefix="test-"
    )
    assert get_active_run(settings) == rid
