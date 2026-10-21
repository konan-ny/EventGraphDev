import json
from dataclasses import replace
from datetime import datetime

import pyarrow.parquet as pq
import pytest

from eventgraph.extract.client import ExtractionError
from eventgraph.extract.graph import run_extraction
from eventgraph.extract.models import Extraction
from eventgraph.source import Doc


@pytest.fixture
def sample_docs(pg):
    rid = "test-g1"
    pg.execute("delete from run_document where run_id=%s", (rid,))
    pg.execute("delete from run where id=%s", (rid,))
    pg.execute(
        "insert into run(id,event_name,date_from,date_to) values(%s,'ev','2026-06-01','2026-06-02')",
        (rid,),
    )
    out = []
    for i in range(1, 6):
        d = Doc("t", i, f"제목{i}", f"본문{i}", datetime(2026, 6, 1, 9, i), f"http://u/{i}", "p")
        rdid = pg.execute(
            "insert into run_document(run_id,source_table,source_idx,title,writedate,rank) values(%s,%s,%s,%s,%s,%s) returning id",
            (rid, d.source_table, d.source_idx, d.title, d.writedate, i),
        ).fetchone()[0]
        out.append((rdid, d))
    return rid, out


@pytest.mark.integration
def test_run_extraction_continues_past_failures(settings, pg, tmp_path, sample_docs):
    rid, docs = sample_docs
    calls = {"n": 0}

    def fake(client, model, doc, event_name, seeds):
        calls["n"] += 1
        if doc.source_idx == 3:
            raise ExtractionError("bad json")
        ent = {"temp_id": "e1", "type": "PERSON", "name_ko": f"인물{doc.source_idx}"}
        return Extraction.model_validate({"entities": [ent], "relations": [], "events": []}), {
            "tokens_in": 10,
            "tokens_out": 5,
            "elapsed_ms": 1,
        }

    s = replace(settings, data_dir=tmp_path, llm_concurrency=2)
    r = run_extraction(s, rid, docs, "ev", [], client=None, extract_fn=fake)
    assert r == {"extracted": 4, "failed": 1, "tokens_in": 40, "tokens_out": 20}
    assert calls["n"] == 6  # the failing doc is retried once
    table = pq.read_table(tmp_path / "runs" / rid / "extracted")
    assert table.num_rows == 4
    assert set(table.column_names) >= {
        "run_document_id",
        "extraction",
        "tokens_in",
        "tokens_out",
        "elapsed_ms",
    }
    row = table.to_pylist()[0]
    assert json.loads(row["extraction"])["entities"][0]["type"] == "PERSON"
    failed = (tmp_path / "runs" / rid / "failed.jsonl").read_text(encoding="utf-8").splitlines()
    assert len(failed) == 1 and json.loads(failed[0])["error"].startswith("bad json")
    st = dict(
        pg.execute(
            "select status, count(*) from run_document where run_id=%s group by status", (rid,)
        ).fetchall()
    )
    assert st == {"extracted": 4, "failed": 1}


@pytest.mark.integration
def test_run_extraction_reports_progress(settings, pg, tmp_path, sample_docs):
    rid, docs = sample_docs
    seen = []

    def fake(client, model, doc, event_name, seeds):
        return Extraction(), {"tokens_in": 1, "tokens_out": 1, "elapsed_ms": 0}

    run_extraction(
        replace(settings, data_dir=tmp_path),
        rid,
        docs,
        "ev",
        [],
        None,
        fake,
        progress_cb=lambda d, t: seen.append((d, t)),
    )
    assert seen[-1] == (5, 5) and len(seen) == 5
