from dataclasses import replace

import pytest
from fastapi.testclient import TestClient

from eventgraph import api as api_mod
from eventgraph.batch.job import run_batch
from tests.test_job import _make_fixture

RID = "test-b1"


@pytest.fixture(scope="module")
def client_with_fixture(settings, tmp_path_factory):
    """One batch load of the Task 8 fixture, shared by every API test in this module."""
    from eventgraph.db import pg_connect

    tmp = tmp_path_factory.mktemp("api")
    s = replace(settings, data_dir=tmp)
    conn = pg_connect(s)
    conn.autocommit = True
    _make_fixture(tmp, conn, RID)
    run_batch(s, RID)
    api_mod.SETTINGS = s
    yield TestClient(api_mod.app)
    conn.close()


@pytest.mark.integration
def test_graph_period_filter(client_with_fixture):
    c = client_with_fixture
    g = c.get(f"/runs/{RID}/graph").json()
    assert len(g["edges"]) == 3 and len(g["nodes"]) == 4
    trump = next(n for n in g["nodes"] if n["name"] == "도널드 트럼프")
    assert trump["type"] == "PERSON" and trump["affiliations"][0]["name"] == "미국"
    g2 = c.get(f"/runs/{RID}/graph", params={"from": "2026-06-01", "to": "2026-06-24"}).json()
    assert g2["edges"] == [] and g2["nodes"] == []
    g3 = c.get(f"/runs/{RID}/graph", params={"from": "2026-06-27", "to": "2026-06-27"}).json()
    assert {e["type"] for e in g3["edges"]} == {"CRITICIZES"}


@pytest.mark.integration
def test_events_and_entity_detail(client_with_fixture):
    c = client_with_fixture
    ev = c.get(f"/runs/{RID}/events").json()
    assert len(ev) == 1 and ev[0]["date"] == "2026-06-30" and len(ev[0]["participants"]) == 2
    assert (
        c.get(f"/runs/{RID}/events", params={"from": "2026-07-01", "to": "2026-07-02"}).json() == []
    )
    eid = ev[0]["participants"][0]["id"]
    d = c.get(f"/runs/{RID}/entities/{eid}").json()
    assert d["id"] == eid and d["events"][0]["id"] == ev[0]["id"] and "surface_forms" in d


@pytest.mark.integration
def test_evidence_includes_source(client_with_fixture):
    c = client_with_fixture
    g = c.get(f"/runs/{RID}/graph").json()
    rid = next(e["id"] for e in g["edges"] if e["type"] == "CRITICIZES")
    ev = c.get(f"/runs/{RID}/relations/{rid}/evidence").json()
    assert (
        len(ev) == 2
        and {"sentence", "evidence_date", "title", "press", "writedate", "url"} <= ev[0].keys()
    )


@pytest.mark.integration
def test_runs_listing_and_detail(client_with_fixture):
    c = client_with_fixture
    runs = c.get("/runs").json()
    assert any(r["id"] == RID for r in runs)
    d = c.get(f"/runs/{RID}").json()
    assert d["id"] == RID and "expanded_keywords" in d and "error" in d
    assert c.get("/runs/nope").status_code == 404


@pytest.mark.integration
def test_post_run_conflicts_when_active(client_with_fixture, pg):
    c = client_with_fixture
    pg.execute(
        "insert into run(id,event_name,date_from,date_to,status) values('test-active','x','2026-06-01','2026-06-02','extracting')"
    )
    body = {
        "event_name": "x",
        "date_from": "2026-06-27",
        "date_to": "2026-06-27",
        "seed_persons": [],
        "seed_orgs": [],
        "seed_countries": [],
    }
    assert c.post("/runs", json=body).status_code == 409
    pg.execute("delete from run where id='test-active'")


@pytest.mark.integration
def test_post_run_validates_dates(client_with_fixture):
    body = {"event_name": "x", "date_from": "2026-06-28", "date_to": "2026-06-27"}
    assert client_with_fixture.post("/runs", json=body).status_code == 422


@pytest.mark.integration
def test_index_served(client_with_fixture):
    r = client_with_fixture.get("/")
    assert r.status_code == 200 and "<html" in r.text.lower()
