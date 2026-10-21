import json
from dataclasses import replace
from datetime import date, datetime

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from eventgraph.batch.job import run_batch


def _ent(tid, typ, name, forms=(), aff=()):
    return {
        "temp_id": tid,
        "type": typ,
        "name_ko": name,
        "surface_forms": list(forms),
        "affiliations": [{"target_temp_id": t, "role": r} for t, r in aff],
    }


def _rel(src, dst, typ, ev, d=None, raw=""):
    return {
        "src_temp_id": src,
        "dst_temp_id": dst,
        "type": typ,
        "raw_label": raw,
        "evidence": ev,
        "evidence_date": d,
    }


def _evt(d, summary, parts, ev, loc=None):
    return {
        "date": d,
        "summary_ko": summary,
        "participants": parts,
        "location_temp_id": loc,
        "evidence": ev,
    }


DOCS = [
    # doc1: 트럼프(미국) CRITICIZES 이란, event 도하 협상 (트럼프, 아락치)
    {
        "entities": [
            _ent("e1", "PERSON", "도널드 트럼프", ["트럼프 대통령"], [("e2", "대통령")]),
            _ent("e2", "COUNTRY", "미국"),
            _ent("e3", "COUNTRY", "이란"),
            _ent("e4", "PERSON", "아바스 아락치", ["아락치"], [("e3", "외무장관")]),
        ],
        "relations": [_rel("e1", "e3", "CRITICIZES", "트럼프가 이란을 비난했다.", "2026-06-25")],
        "events": [
            _evt("2026-06-30", "미·이란 도하 협상", ["e1", "e4"], "도하에서 협상이 열렸다.")
        ],
    },
    # doc2: "트럼프 대통령" (merge target) same CRITICIZES without date, event 도하 회담 (same participants)
    {
        "entities": [
            _ent("t1", "PERSON", "트럼프 대통령", ["트럼프"], [("t2", "")]),
            _ent("t2", "COUNTRY", "미국"),
            _ent("t3", "COUNTRY", "이란"),
            _ent("t4", "PERSON", "아락치", []),
        ],
        "relations": [_rel("t1", "t3", "CRITICIZES", "트럼프 대통령은 이란을 강하게 비판했다.")],
        "events": [_evt("2026-06-30", "미·이란 도하 회담", ["t1", "t4"], "도하 회담이 열렸다.")],
    },
    # doc3: 아락치 MEMBER_OF 이란, OTHER(경고) 아락치 -> 미국
    {
        "entities": [
            _ent("a1", "PERSON", "아바스 아락치", [], [("a2", "외무장관")]),
            _ent("a2", "COUNTRY", "이란"),
            _ent("a3", "COUNTRY", "미국"),
        ],
        "relations": [
            _rel("a1", "a2", "MEMBER_OF", "아락치 이란 외무장관은"),
            _rel("a1", "a3", "OTHER", "아락치는 미국에 경고했다.", raw="경고"),
        ],
        "events": [],
    },
]


def _make_fixture(tmp_path, pg, run_id):
    pg.execute("delete from run_document where run_id=%s", (run_id,))
    pg.execute("delete from run where id=%s", (run_id,))
    pg.execute(
        "insert into run(id,event_name,date_from,date_to) values(%s,'ev','2026-06-24','2026-07-02')",
        (run_id,),
    )
    ids = []
    for i, d in enumerate(DOCS, start=1):
        rdid = pg.execute(
            "insert into run_document(run_id,source_table,source_idx,title,writedate,rank,status)"
            " values(%s,'t',%s,%s,%s,%s,'extracted') returning id",
            (run_id, i, f"제목{i}", datetime(2026, 6, 25 + i, 9, 0), i),
        ).fetchone()[0]
        ids.append(rdid)
    out = tmp_path / "runs" / run_id / "extracted"
    out.mkdir(parents=True)
    table = pa.table(
        {
            "run_document_id": pa.array(ids, pa.int64()),
            "extraction": pa.array([json.dumps(d, ensure_ascii=False) for d in DOCS], pa.string()),
            "tokens_in": pa.array([1, 1, 1], pa.int64()),
            "tokens_out": pa.array([1, 1, 1], pa.int64()),
            "elapsed_ms": pa.array([1, 1, 1], pa.int64()),
        }
    )
    pq.write_table(table, out / "part-00000.parquet")


@pytest.mark.integration
def test_run_batch_merges_and_is_idempotent(settings, pg, tmp_path):
    rid = "test-b1"
    _make_fixture(tmp_path, pg, rid)
    s = replace(settings, data_dir=tmp_path)
    r1 = run_batch(s, rid)
    r2 = run_batch(s, rid)
    assert r1 == r2
    assert r1["entities"] == 4 and r1["relations"] == 3 and r1["events"] == 1
    assert r1["evidence"] == 6  # relation evidence 4 + event evidence 2 (merged event keeps both)
    assert r1["other_relations"] == 1

    persons = {
        r[0]: r
        for r in pg.execute(
            "select e.name_ko, e.doc_count, e.surface_forms, e.degree from entity e"
            " join onto_class c on c.id=e.class_id where e.run_id=%s and c.name='PERSON'",
            (rid,),
        ).fetchall()
    }
    assert set(persons) == {"도널드 트럼프", "아바스 아락치"}  # most frequent full name wins
    assert persons["도널드 트럼프"][1] == 2 and "트럼프 대통령" in persons["도널드 트럼프"][2]
    assert persons["아바스 아락치"][1] == 3

    rel = pg.execute(
        "select r.doc_count, r.date_min, r.date_max from relation r"
        " join onto_relation_type t on t.id=r.type_id where r.run_id=%s and t.name='CRITICIZES'",
        (rid,),
    ).fetchone()
    assert rel == (2, date(2026, 6, 25), date(2026, 6, 27))  # evidence_date, else article date

    aff = pg.execute(
        "select t.name_ko, a.role, a.doc_count, a.is_primary from affiliation a"
        " join entity p on p.id=a.person_id join entity t on t.id=a.target_id"
        " where a.run_id=%s and p.name_ko='도널드 트럼프'",
        (rid,),
    ).fetchall()
    assert len(aff) == 1 and aff[0][0] == "미국" and aff[0][2] == 2 and aff[0][3] is True

    ev = pg.execute(
        "select e.date, e.doc_count, count(p.entity_id) from event e"
        " join event_participant p on p.event_id=e.id where e.run_id=%s group by e.id",
        (rid,),
    ).fetchall()
    assert ev == [(date(2026, 6, 30), 2, 2)]
    assert (
        pg.execute(
            "select count(*) from evidence where run_id=%s and event_id is not null", (rid,)
        ).fetchone()[0]
        == 2
    )
    assert (
        pg.execute(
            "select degree from entity where run_id=%s and name_ko='아바스 아락치'", (rid,)
        ).fetchone()[0]
        == 2
    )  # 이란, 미국
