"""FastAPI surface over the ontology tables + run control."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from datetime import date
from pathlib import Path

from fastapi import BackgroundTasks, FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from psycopg.rows import dict_row
from pydantic import BaseModel, Field, model_validator

from .config import Settings, load_settings
from .db import pg_connect
from .migrate import migrate
from .pipeline import ACTIVE, create_run, get_active_run, run_pipeline

log = logging.getLogger(__name__)


@asynccontextmanager
async def _lifespan(_: FastAPI):
    s = _settings()
    migrate(s)
    with pg_connect(s) as conn:  # a run left active by a crashed server would block every POST
        n = conn.execute(
            "update run set status='failed', error='interrupted by server restart',"
            " finished_at=now() where status = any(%s)",
            (list(ACTIVE),),
        ).rowcount
        conn.commit()
    if n:
        log.warning("marked %d interrupted run(s) as failed on startup", n)
    yield


app = FastAPI(title="EventGraph", lifespan=_lifespan)
STATIC = Path(__file__).with_name("static")
SETTINGS: Settings | None = None  # tests inject one; otherwise loaded from config/.env


def _settings() -> Settings:
    global SETTINGS
    if SETTINGS is None:
        SETTINGS = load_settings()
    return SETTINGS


def _q(sql: str, params: tuple = ()) -> list[dict]:
    with pg_connect(_settings()) as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, params)
        return cur.fetchall()


class RunRequest(BaseModel):
    event_name: str = Field(min_length=1)
    date_from: date
    date_to: date
    seed_persons: list[str] = Field(default_factory=list)
    seed_orgs: list[str] = Field(default_factory=list)
    seed_countries: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def _order(self) -> RunRequest:
        if self.date_to < self.date_from:
            raise ValueError("date_to must be >= date_from")
        return self


def _background_run(settings: Settings, run_id: str) -> None:
    try:
        run_pipeline(settings, run_id)
    except Exception:  # already recorded on the run row; keep the server alive
        log.exception("background run %s failed", run_id)


@app.get("/")
def index() -> FileResponse:
    return FileResponse(STATIC / "index.html")


@app.post("/runs", status_code=201)
def post_run(req: RunRequest, bg: BackgroundTasks) -> dict:
    s = _settings()
    with pg_connect(s) as conn:  # serialize check+insert so two requests cannot both pass
        conn.execute("select pg_advisory_xact_lock(7421)")
        active = get_active_run(s)
        if active:
            conn.rollback()
            raise HTTPException(409, f"run {active} is still in progress")
        rid = create_run(
            s,
            req.event_name,
            req.date_from,
            req.date_to,
            req.seed_persons,
            req.seed_orgs,
            req.seed_countries,
        )
        conn.commit()
    bg.add_task(_background_run, s, rid)
    return {"run_id": rid}


_RUN_LIST = (
    "id, event_name, date_from, date_to, status, doc_selected, doc_extracted, doc_failed,"
    " started_at, finished_at"
)


@app.get("/runs")
def list_runs() -> list[dict]:
    return _q(f"select {_RUN_LIST} from run order by started_at desc")


@app.get("/runs/{run_id}")
def get_run(run_id: str) -> dict:
    rows = _q(
        f"select {_RUN_LIST}, expanded_keywords, error, tokens_in, tokens_out,"
        " seed_persons, seed_orgs, seed_countries from run where id=%s",
        (run_id,),
    )
    if not rows:
        raise HTTPException(404, "run not found")
    return rows[0]


_FROM = Query(None, alias="from")
_TO = Query(None, alias="to")


def _edge_period(from_: date | None, to: date | None) -> tuple[str, tuple]:
    """Edge visible when at least one piece of evidence is dated inside [from, to]
    (evidence_date, else the article date) — spec 4.3, not range overlap."""
    if from_ is None and to is None:
        return "", ()
    cond = (
        " and exists (select 1 from evidence v join run_document d on d.id = v.run_document_id"
        " where v.relation_id = r.id"
        " and coalesce(v.evidence_date, cast(d.writedate as date)) between %s and %s)"
    )
    return cond, (from_ or date.min, to or date.max)


@app.get("/runs/{run_id}/graph")
def graph(run_id: str, from_: date | None = _FROM, to: date | None = _TO) -> dict:
    cond, params = _edge_period(from_, to)
    edges = _q(
        "select r.id, r.src_id as src, r.dst_id as dst, t.name as type, r.raw_label,"
        " r.doc_count, r.date_min, r.date_max"
        " from relation r join onto_relation_type t on t.id = r.type_id"
        f" where r.run_id = %s{cond} order by r.doc_count desc",
        (run_id, *params),
    )
    ids = {e["src"] for e in edges} | {e["dst"] for e in edges}
    if not ids:
        return {"nodes": [], "edges": []}
    affs = _q(
        "select a.person_id, a.target_id, a.role, a.is_primary, e.name_ko, c.name as type"
        " from affiliation a join entity e on e.id = a.target_id"
        " join onto_class c on c.id = e.class_id"
        " where a.run_id = %s and a.person_id = any(%s) order by a.doc_count desc",
        (run_id, list(ids)),
    )
    ids |= {a["target_id"] for a in affs}
    nodes = _q(
        "select e.id, c.name as type, e.name_ko as name, e.doc_count, e.degree"
        " from entity e join onto_class c on c.id = e.class_id where e.id = any(%s)",
        (list(ids),),
    )
    by_person: dict[int, list] = {}
    for a in affs:
        by_person.setdefault(a["person_id"], []).append(
            {"id": a["target_id"], "name": a["name_ko"], "type": a["type"], "role": a["role"]}
        )
    for n in nodes:
        n["affiliations"] = by_person.get(n["id"], [])
    return {"nodes": nodes, "edges": edges}


@app.get("/runs/{run_id}/events")
def events(run_id: str, from_: date | None = _FROM, to: date | None = _TO) -> list[dict]:
    cond, params = "", ()
    if from_ or to:
        cond, params = " and e.date between %s and %s", (from_ or date.min, to or date.max)
    rows = _q(
        "select e.id, e.date, e.summary_ko as summary, e.doc_count, l.name_ko as location"
        " from event e left join entity l on l.id = e.location_id"
        f" where e.run_id = %s{cond} order by e.date nulls last, e.doc_count desc",
        (run_id, *params),
    )
    if not rows:
        return []
    parts = _q(
        "select p.event_id, en.id, en.name_ko as name from event_participant p"
        " join entity en on en.id = p.entity_id where p.event_id = any(%s)",
        ([r["id"] for r in rows],),
    )
    by_event: dict[int, list] = {}
    for p in parts:
        by_event.setdefault(p["event_id"], []).append({"id": p["id"], "name": p["name"]})
    for r in rows:
        r["participants"] = by_event.get(r["id"], [])
    return rows


@app.get("/runs/{run_id}/relations/{rid}/evidence")
def relation_evidence(run_id: str, rid: int) -> list[dict]:
    return _q(
        "select v.sentence, v.evidence_date, d.title, d.press, d.writedate, d.url"
        " from evidence v join run_document d on d.id = v.run_document_id"
        " where v.run_id = %s and v.relation_id = %s order by d.writedate",
        (run_id, rid),
    )


@app.get("/runs/{run_id}/events/{eid}/evidence")
def event_evidence(run_id: str, eid: int) -> list[dict]:
    return _q(
        "select v.sentence, v.evidence_date, d.title, d.press, d.writedate, d.url"
        " from evidence v join run_document d on d.id = v.run_document_id"
        " where v.run_id = %s and v.event_id = %s order by d.writedate",
        (run_id, eid),
    )


@app.get("/runs/{run_id}/entities/{eid}")
def entity(run_id: str, eid: int) -> dict:
    rows = _q(
        "select e.id, c.name as type, e.name_ko as name, e.surface_forms, e.doc_count, e.degree"
        " from entity e join onto_class c on c.id = e.class_id where e.run_id = %s and e.id = %s",
        (run_id, eid),
    )
    if not rows:
        raise HTTPException(404, "entity not found")
    out = rows[0]
    out["affiliations"] = _q(
        "select t.id, t.name_ko as name, c.name as type, a.role, a.doc_count, a.is_primary"
        " from affiliation a join entity t on t.id = a.target_id"
        " join onto_class c on c.id = t.class_id where a.person_id = %s order by a.doc_count desc",
        (eid,),
    )
    out["events"] = _q(
        "select e.id, e.date, e.summary_ko as summary from event e"
        " join event_participant p on p.event_id = e.id where p.entity_id = %s"
        " order by e.date nulls last",
        (eid,),
    )
    return out
