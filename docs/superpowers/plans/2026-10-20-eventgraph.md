# EventGraph Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 한국어 뉴스 MySQL 코퍼스에서 사용자가 지정한 사건의 인물·기관·지역·관계·날짜 사건을 LLM으로 추출하고, Spark 배치로 병합해 PostgreSQL 온톨로지에 적재한 뒤, 로컬 웹에서 인물 관계도 + 연동 타임라인으로 보여주는 재사용 도구.

**Architecture:** 4단계 파이프라인. (A) SQL+Python 문서 선별 → (B) LangGraph로 감싼 Claude 구조화 추출(asyncio, 문서별 Parquet) → (C) PySpark 로컬 배치(정규화·병합·집계, JDBC로 PostgreSQL 적재) → (D) FastAPI + 단일 HTML(Cytoscape.js). 모든 산출물은 run_id 단위로 멱등.

**Tech Stack:** Python 3.11, uv, pymysql, psycopg 3, pydantic 2, anthropic SDK, langgraph, pyarrow, pyspark 3.5, FastAPI, Cytoscape.js(CDN), PostgreSQL 16(Docker), pytest.

**Spec:** `docs/superpowers/specs/2026-10-20-eventgraph-design.md`

## Global Constraints

- 원천 MySQL은 **읽기 전용**. 접속·테이블명은 `config/.env`의 DB_HOST/DB_USER/DB_PWD/DB_NAME/DB_TABLENAME.
- 모든 파일 I/O는 `encoding="utf-8"` 명시. 경로에 한글 포함 가능. Windows 11.
- 표시 이름은 한국어 표준명(인물: 직함 제외 성명, 기관: 공식 한국어 명칭, 국가: 한국어 국명). 원문 표기는 surface_forms 보존.
- 추출 대상 문서 상한 `MAX_DOCS` 기본 300, LLM 동시성 `LLM_CONCURRENCY` 기본 5, 모델 ID `ANTHROPIC_MODEL` 설정값(기본 `claude-haiku-5-5`, 사용자 결정).
- 개별 문서 실패는 run을 멈추지 않는다. `failed.jsonl`에 사유 기록, run 표에 doc_failed 집계.
- 단계별 건수(선별·추출 성공·실패·PG 적재 행 수)는 로그와 run 표에 반드시 남긴다. 빈 결과도 로그로 보이게.
- Spark는 `local[*]`, Parquet만 읽음. 세션 생성은 `eventgraph/batch/spark.py` 한 곳.
- 동시 run 1개. 실행 중 POST /runs → 409.
- git은 로컬 저장소만. push 전제 금지.
- 신규 의존성은 스택에 명시된 것만(pymysql, psycopg[binary], pydantic, anthropic, langgraph, pyarrow, pyspark, fastapi, uvicorn, python-dotenv, pytest, pytest-asyncio, httpx). 다른 패키지 추가 금지.

## Review Focus

1. **기사 본문에 `<앵커>`, `<기자>`, `<br>`가 섞여 있다** → clean_body는 HTML 태그만 제거하고 `<앵커>`·`<기자>` 같은 방송 지시문도 각괄호째 제거한다. Task 3 테스트에 포함.
2. **`writedate='0000-00-00'` 또는 빈 본문** → 선별 쿼리에서 제외되고 선별 건수 로그에 "제외 n건"이 남는다. Task 3 테스트에 포함.
3. **LLM이 relation type에 스키마 밖 라벨("비판")을 돌려준다** → OTHER로 강제 변환하고 raw_label에 보존, 문서는 실패 처리하지 않는다. Task 5 테스트에 포함.
4. **relation의 src/dst temp_id가 entities에 없다** → 해당 relation만 버리고 경고 로그, 문서는 성공 처리. Task 5 테스트에 포함.
5. **같은 run을 두 번 배치 적재** → PG 행 수가 두 배가 되지 않는다(run_id 선삭제). Task 8 테스트에 포함.

---

## File Structure

```
pyproject.toml                 uv 프로젝트, 의존성, pytest 설정
docker-compose.yml             postgres:16, 포트 5433, 볼륨
config/.env                    (기존) DB_* + 추가: ANTHROPIC_API_KEY, ANTHROPIC_MODEL, PG_DSN, MAX_DOCS, LLM_CONCURRENCY, DATA_DIR
eventgraph/
  config.py                    Settings 로딩 (.env → dataclass)
  db.py                        pg_connect(), mysql_connect()
  schema.sql                   PG DDL + onto_class/onto_relation_type 시드
  migrate.py                   schema.sql 실행
  source.py                    Doc, clean_body, press_from_url, fetch_candidates
  select.py                    expand_keywords, score_document, select_documents
  extract/models.py            pydantic: Entity, Relation, Event, Extraction, RELATION_TYPES
  extract/dates.py             resolve_date
  extract/prompt.py            SYSTEM_PROMPT, build_user_message
  extract/client.py            extract_document (anthropic 호출 + 검증)
  extract/graph.py             LangGraph: run_extraction (asyncio, parquet, failed.jsonl)
  batch/normalize.py           norm_key, TITLE_SUFFIXES
  batch/spark.py               get_spark()
  batch/job.py                 run_batch(run_id)
  pipeline.py                  create_run, run_pipeline(run_id, stages)
  cli.py                       python -m eventgraph ...
  api.py                       FastAPI 앱
  static/index.html            폼 + 관계도 + 타임라인 + 상세
tests/
  conftest.py                  settings fixture, pg fixture(테스트용 run_id 정리), 샘플 Doc
  test_source.py  test_select.py  test_dates.py  test_models.py  test_client.py
  test_graph.py   test_normalize.py  test_job.py  test_pipeline.py  test_api.py
docs/golden/hormuz/            골든 세트(선별 20건, 정답, 비교 결과)
scripts/golden_compare.py      추출 결과 vs 정답 비교
```

---

### Task 1: 프로젝트 골격·설정·PostgreSQL 컨테이너

**Files:**
- Create: `pyproject.toml`, `docker-compose.yml`, `eventgraph/__init__.py`, `eventgraph/config.py`, `eventgraph/db.py`, `tests/conftest.py`, `tests/test_config.py`, `.gitignore`
- Modify: `config/.env` (키 추가, 값은 사용자 입력 유지)

**Interfaces:**
- Produces: `Settings` dataclass(`db_host, db_user, db_pwd, db_name, db_table, anthropic_api_key, anthropic_model, pg_dsn, max_docs:int, llm_concurrency:int, data_dir:Path`), `load_settings(path: Path | None = None) -> Settings` (기본 경로 `config/.env`), `pg_connect(settings) -> psycopg.Connection`, `mysql_connect(settings) -> pymysql.Connection`(charset utf8mb4, DictCursor).

- [ ] **Step 1: git init, uv init, 의존성 추가**

```bash
git init
uv init --no-readme --python 3.11
uv add pymysql "psycopg[binary]" pydantic anthropic langgraph pyarrow pyspark fastapi uvicorn python-dotenv
uv add --dev pytest pytest-asyncio httpx
```
`.gitignore`: `.venv/ data/ config/.env __pycache__/ .omc/`

- [ ] **Step 2: docker-compose.yml** — postgres:16, 컨테이너명 `eventgraph-pg`, `5433:5432`, POSTGRES_USER/PASSWORD/DB 모두 `eventgraph`, named volume. `docker compose up -d` 후 `docker compose ps`에 running 확인.

- [ ] **Step 3: config/.env에 추가** (기존 DB_* 줄은 유지)
```
ANTHROPIC_API_KEY=
ANTHROPIC_MODEL=claude-haiku-5-5
PG_DSN=postgresql://eventgraph:eventgraph@localhost:5433/eventgraph
MAX_DOCS=300
LLM_CONCURRENCY=5
DATA_DIR=data
```
ANTHROPIC_API_KEY는 사용자가 채운다. 비어 있으면 Task 3 이후 LLM 호출 단계가 시작 시 명확한 오류로 중단되어야 한다.

- [ ] **Step 4: 실패 테스트** `tests/test_config.py`
```python
def test_load_settings_reads_env(tmp_path):
    p = tmp_path / ".env"
    p.write_text("DB_HOST=h\nDB_USER=u\nDB_PWD=p\nDB_NAME=n\nDB_TABLENAME=t\nPG_DSN=postgresql://x\nMAX_DOCS=12\nLLM_CONCURRENCY=2\nDATA_DIR=d\nANTHROPIC_MODEL=m\nANTHROPIC_API_KEY=k\n", encoding="utf-8")
    s = load_settings(p)
    assert s.db_table == "t" and s.max_docs == 12 and s.llm_concurrency == 2
    assert s.data_dir == Path("d") and s.anthropic_model == "m"

def test_load_settings_defaults(tmp_path):
    p = tmp_path / ".env"; p.write_text("DB_HOST=h\nDB_USER=u\nDB_PWD=p\nDB_NAME=n\nDB_TABLENAME=t\n", encoding="utf-8")
    s = load_settings(p)
    assert s.max_docs == 300 and s.llm_concurrency == 5 and s.anthropic_model == "claude-haiku-5-5"
```
- [ ] **Step 5: 실행해 실패 확인** `uv run pytest tests/test_config.py -v` → ImportError.
- [ ] **Step 6: 구현** `load_settings`(dotenv_values로 파싱, 값에 `#` 포함 비밀번호가 잘리지 않는지 확인: dotenv는 따옴표 없는 값의 ` #` 이후를 주석 처리하므로 `.env`의 DB_PWD는 따옴표로 감싸도록 사용자 안내 또는 직접 `=`로 split 파싱 — **직접 split 파싱을 택한다**), `pg_connect`, `mysql_connect`.
- [ ] **Step 7: 통과 확인** `uv run pytest tests/test_config.py -v` → PASS. 추가로 `uv run python -c "from eventgraph.config import load_settings; from eventgraph.db import *; s=load_settings(); mysql_connect(s).close(); pg_connect(s).close(); print('ok')"` → ok.
- [ ] **Step 8: conftest.py** — `settings` fixture(load_settings()), `pg` fixture(connection, 테스트 종료 시 `run_id`가 `test-`로 시작하는 행 삭제).
- [ ] **Step 9: Commit** `git add -A && git commit -m "chore: project skeleton, settings, postgres compose"`

---

### Task 2: PostgreSQL 온톨로지 스키마

**Files:**
- Create: `eventgraph/schema.sql`, `eventgraph/migrate.py`, `tests/test_migrate.py`

**Interfaces:**
- Produces: 표 `onto_class, onto_relation_type, run, run_document, entity, affiliation, relation, evidence, event, event_participant` (컬럼은 스펙 5(D) 그대로, 추가: `relation.date_min date, date_max date`; `run.id text PK`, 나머지 id는 `bigserial`; `run_document.id bigserial`, `(run_id, source_table, source_idx)` UNIQUE; `entity.surface_forms text[]`; `run.error text`). `migrate(settings) -> None` 멱등(`CREATE TABLE IF NOT EXISTS`, 시드는 `ON CONFLICT DO NOTHING`).
- 관계 유형 시드 16개: MEMBER_OF, LEADS, REPRESENTS, MEETS, CRITICIZES, SUPPORTS, NEGOTIATES_WITH, ORDERS, SUCCEEDS, ALLIES_WITH, SANCTIONS, ATTACKS, AGREES_WITH, PARTICIPATES, OCCURS_AT, OTHER. src_class/dst_class는 참고용 텍스트.
- `run.status` 값: `created | selecting | extracting | batching | done | failed`.

- [ ] **Step 1: 실패 테스트** `tests/test_migrate.py`
```python
def test_migrate_creates_tables_and_seed(settings, pg):
    migrate(settings); migrate(settings)  # 멱등
    cur = pg.execute("select count(*) from onto_relation_type"); assert cur.fetchone()[0] == 16
    cur = pg.execute("select to_regclass('relation'), to_regclass('evidence'), to_regclass('event_participant')")
    assert None not in cur.fetchone()
    cur = pg.execute("select column_name from information_schema.columns where table_name='relation'")
    assert {"date_min","date_max","raw_label"} <= {r[0] for r in cur.fetchall()}
```
- [ ] **Step 2: 실패 확인** `uv run pytest tests/test_migrate.py -v`
- [ ] **Step 3: schema.sql 작성, migrate.py 구현**(파일을 읽어 한 트랜잭션으로 실행, `python -m eventgraph.migrate`로도 실행 가능).
- [ ] **Step 4: 통과 확인**
- [ ] **Step 5: Commit** `feat: postgres ontology schema and migrate`

---

### Task 3: 원천 읽기·정제

**Files:**
- Create: `eventgraph/source.py`, `tests/test_source.py`

**Interfaces:**
- Produces: `@dataclass Doc(source_table:str, source_idx:int, title:str, body:str, writedate:datetime, url:str, press:str)`; `clean_body(raw:str) -> str`; `press_from_url(url2:str) -> str`(도메인 기반, 예 `news.sbs.co.kr`→`sbs.co.kr`, 빈 값→`""`); `fetch_candidates(settings, date_from:date, date_to:date) -> tuple[list[Doc], dict]` — 두 번째는 `{"total":n, "excluded_bad_date":n, "excluded_empty_body":n}`. 쿼리: `writedate >= date_from AND writedate < date_to + 1일 AND writedate > '1900-01-01' AND body IS NOT NULL AND body <> ''`. 반환 Doc의 body는 clean_body 적용 후.

- [ ] **Step 1: 실패 테스트**
```python
def test_clean_body_strips_html_and_broadcast_cues():
    raw = "<앵커>\n요즘 <b>불방망이</b>를<br/>휘두르는 <기자>\n배정훈 기자입니다."
    out = clean_body(raw)
    assert "<" not in out and "앵커" not in out and "기자>" not in out
    assert "불방망이를" in out and "배정훈 기자입니다." in out

def test_press_from_url():
    assert press_from_url("https://news.sbs.co.kr/news/endPage.do?x=1") == "sbs.co.kr"
    assert press_from_url("https://www.yna.co.kr/view/AKR1") == "yna.co.kr"
    assert press_from_url("") == ""

@pytest.mark.integration
def test_fetch_candidates_excludes_bad_rows(settings):
    docs, stats = fetch_candidates(settings, date(2026,6,24), date(2026,6,24))
    assert stats["total"] == len(docs) > 0
    assert all(d.writedate.year == 2026 and d.body for d in docs)
```
- [ ] **Step 2: 실패 확인**
- [ ] **Step 3: 구현.** `press_from_url`은 `urllib.parse.urlparse().hostname`에서 선행 `www.`·`news.`·`m.` 제거. clean_body: `<[^>]+>` 제거 후 연속 공백 정리.
- [ ] **Step 4: 통과 확인** `uv run pytest tests/test_source.py -v`(integration 포함, MySQL 접속)
- [ ] **Step 5: Commit** `feat: mysql source reader with body cleaning`

---

### Task 4: 키워드 확장·관련도 점수·문서 선별

**Files:**
- Create: `eventgraph/select.py`, `tests/test_select.py`

**Interfaces:**
- Consumes: `Doc`, `fetch_candidates`, `pg_connect`.
- Produces:
  - `expand_keywords(client, model:str, event_name:str, seeds:list[str]) -> list[str]` — Claude에 "사건 관련 한국어 키워드 10~20개를 JSON 배열로"를 요청, 응답을 `json.loads`, 실패 시 `[event_name] + seeds` 반환 + 경고 로그. `client`는 `anthropic.Anthropic` 또는 `.messages.create`를 가진 대체 객체.
  - `score_document(doc:Doc, keywords:list[str], title_weight:float=3.0) -> float` — 각 키워드의 title 등장 횟수×title_weight + body 등장 횟수 합.
  - `select_documents(settings, run_id:str, event_name:str, seeds:list[str], date_from, date_to, client) -> dict` — 후보 fetch → 확장 키워드 → 점수 → score>0 중 상위 `settings.max_docs` → `run_document`에 INSERT(status `selected`), `run.expanded_keywords`·`doc_selected` UPDATE. 반환 `{"candidates":n, "selected":n, "keywords":[...], **fetch_stats}`.

- [ ] **Step 1: 실패 테스트**
```python
def test_score_document_weights_title():
    d = Doc("t",1,"이란 호르무즈 봉쇄","이란이 호르무즈를 닫았다. 이란 외무부.",datetime(2026,6,25),"", "")
    assert score_document(d, ["이란","호르무즈"]) == 9  # title(이란1+호르무즈1)×3=6, body 이란2+호르무즈1=3
    assert score_document(d, ["젤렌스키"]) == 0

def test_expand_keywords_falls_back_on_bad_json():
    class Fake:  # messages.create returns non-JSON
        class messages:
            @staticmethod
            def create(**kw): return SimpleNamespace(content=[SimpleNamespace(text="not json")])
    assert expand_keywords(Fake(), "m", "사건", ["씨앗"]) == ["사건","씨앗"]

def test_expand_keywords_parses_array():
    ... create returns text='["이란","호르무즈"]' → == ["이란","호르무즈"]
```
- [ ] **Step 2: 실패 확인**
- [ ] **Step 3: 구현.** `expand_keywords` 프롬프트에 "JSON 배열만 출력" 명시, `max_tokens=512`.
- [ ] **Step 4: 통과 확인**
- [ ] **Step 5: 통합 확인(수동)** `uv run python -c "..."`로 run_id `test-sel` 생성 후 `select_documents(..., "미국-이란 호르무즈 사태", ["트럼프","이란"], date(2026,6,24), date(2026,7,2), client)` → selected == 300, keywords에 "호르무즈" 포함 확인. 끝나면 test- 행 삭제.
- [ ] **Step 6: Commit** `feat: keyword expansion and document selection`

---

### Task 5: 추출 스키마·날짜 해석·단일 문서 추출

**Files:**
- Create: `eventgraph/extract/__init__.py`, `eventgraph/extract/models.py`, `eventgraph/extract/dates.py`, `eventgraph/extract/prompt.py`, `eventgraph/extract/client.py`, `tests/test_models.py`, `tests/test_dates.py`, `tests/test_client.py`

**Interfaces:**
- Produces:
  - `RELATION_TYPES: frozenset[str]` (16개, Task 2와 동일). `ENTITY_TYPES = {"PERSON","ORG","LOCATION","COUNTRY"}`.
  - pydantic: `Affiliation(target_temp_id:str, role:str="")`, `Entity(temp_id, type, name_ko, surface_forms:list[str]=[], affiliations:list[Affiliation]=[])`, `Relation(src_temp_id, dst_temp_id, type:str, raw_label:str="", evidence:str, evidence_date:date|None=None)`, `Event(date:date|None, summary_ko, participants:list[str]=[], location_temp_id:str|None=None, evidence:str)`, `Extraction(entities, relations, events)`.
  - `Extraction.model_validator`: type∉RELATION_TYPES → `raw_label=원값, type="OTHER"`; temp_id 미존재 relation·participant 제거(제거 건수는 `Extraction.dropped:int` 필드에 기록); type∉ENTITY_TYPES인 entity 제거.
  - `resolve_date(text:str, base:date) -> date|None` — "오늘"→base, "어제"→base-1, "내일"→base+1, "지난 24일"/"24일"→base와 같은 달의 24일(단, 24일이 base 이후면 전달), "6월 25일"→base 연도, "2026년 6월 25일" 절대, 그 외 None.
  - `SYSTEM_PROMPT:str`(표준명 규칙·날짜 규칙·관계 유형 목록·JSON만 출력), `build_user_message(doc:Doc, event_name:str, seeds:list[str]) -> str`.
  - `extract_document(client, model:str, doc:Doc, event_name:str, seeds:list[str]) -> tuple[Extraction, dict]` — `client.messages.create(..., max_tokens=4096)`; 응답 텍스트에서 첫 `{`~마지막 `}`를 잘라 `Extraction.model_validate_json`; 둘째 반환은 `{"tokens_in","tokens_out","elapsed_ms"}`. JSON 파싱 실패 시 `ExtractionError` 발생(재시도는 Task 6).

- [ ] **Step 1: 실패 테스트** `test_dates.py`
```python
B = date(2026,6,27)
@pytest.mark.parametrize("t,exp", [("오늘",B),("어제",date(2026,6,26)),("지난 24일",date(2026,6,24)),("30일",date(2026,5,30)),("6월 25일",date(2026,6,25)),("2025년 1월 2일",date(2025,1,2)),("언젠가",None)])
def test_resolve_date(t,exp): assert resolve_date(t,B) == exp
```
`test_models.py`
```python
def test_unknown_relation_type_becomes_other():
    x = Extraction.model_validate({"entities":[{"temp_id":"e1","type":"PERSON","name_ko":"A"},{"temp_id":"e2","type":"COUNTRY","name_ko":"이란"}],
        "relations":[{"src_temp_id":"e1","dst_temp_id":"e2","type":"비판","evidence":"A가 이란을 비판했다"}],"events":[]})
    assert x.relations[0].type == "OTHER" and x.relations[0].raw_label == "비판"

def test_dangling_relation_dropped_not_fatal():
    x = Extraction.model_validate({"entities":[{"temp_id":"e1","type":"PERSON","name_ko":"A"}],
        "relations":[{"src_temp_id":"e1","dst_temp_id":"zz","type":"MEETS","evidence":"x"}],"events":[]})
    assert x.relations == [] and x.dropped == 1
```
`test_client.py`: Fake client가 ```json ... ``` 펜스로 감싼 유효 JSON을 돌려줘도 파싱되는지, `not json` 응답이면 `ExtractionError`인지.
- [ ] **Step 2: 실패 확인**
- [ ] **Step 3: 구현.** SYSTEM_PROMPT에 반드시 포함: (1) 사건 이름과 무관한 개체는 추출하지 않음, (2) 인물 표준명은 직함 제외, (3) 상대 날짜는 기사 발행일 기준 YYYY-MM-DD로, 불명이면 null, (4) relation.type은 목록 중 하나, 없으면 OTHER + raw_label, (5) evidence는 원문 문장 그대로, (6) JSON 외 출력 금지. few-shot 1개(짧은 가상 기사).
- [ ] **Step 4: 통과 확인** `uv run pytest tests/test_dates.py tests/test_models.py tests/test_client.py -v`
- [ ] **Step 5: 실 API 1건 확인(수동)** 호르무즈 기사 1건으로 `extract_document` 호출, entities에 COUNTRY "이란" 포함, tokens_in > 0 확인. 출력 JSON을 `docs/golden/hormuz/sample_extraction.json`에 저장.
- [ ] **Step 6: Commit** `feat: extraction schema, prompt, date resolution, single-doc extractor`

---

### Task 6: LangGraph 추출 파이프라인

**Files:**
- Create: `eventgraph/extract/graph.py`, `tests/test_graph.py`

**Interfaces:**
- Consumes: `extract_document`, `ExtractionError`, `Doc`.
- Produces: `run_extraction(settings, run_id:str, docs:list[tuple[int, Doc]], event_name:str, seeds:list[str], client, extract_fn=extract_document, progress_cb:Callable[[int,int],None]|None=None) -> dict` — `docs`의 int는 `run_document.id`. 그래프 노드: `load → extract → validate → write`; extract 실패(ExtractionError 또는 anthropic APIError) 시 1회 재시도 후 `failed`. 문서별 노드 그래프를 `asyncio.Semaphore(settings.llm_concurrency)`로 동시 실행, 429는 지수 백오프(1,2,4,8초, 최대 4회, 재시도 횟수와 별개). 출력 `data/runs/{run_id}/extracted/part-{i:05d}.parquet`(100건 단위 묶음, 컬럼: run_document_id:int64, extraction:string(JSON), tokens_in, tokens_out, elapsed_ms) 와 `data/runs/{run_id}/failed.jsonl`(`{"run_document_id", "error"}`). 반환 `{"extracted":n, "failed":n, "tokens_in":n, "tokens_out":n}`. `run_document.status`를 `extracted|failed`로 UPDATE.

- [ ] **Step 1: 실패 테스트**
```python
def test_run_extraction_continues_past_failures(settings, pg, tmp_path, sample_docs):  # sample_docs: 5건, run_document 선삽입
    calls = {"n":0}
    def fake(client, model, doc, event_name, seeds):
        calls["n"] += 1
        if doc.source_idx == 3: raise ExtractionError("bad json")
        return Extraction(entities=[],relations=[],events=[]), {"tokens_in":10,"tokens_out":5,"elapsed_ms":1}
    settings = replace(settings, data_dir=tmp_path)
    r = run_extraction(settings, "test-g1", sample_docs, "ev", [], client=None, extract_fn=fake)
    assert r["extracted"] == 4 and r["failed"] == 1 and r["tokens_in"] == 40
    assert calls["n"] == 6  # 실패 1건은 1회 재시도
    rows = pq.read_table(tmp_path/"runs/test-g1/extracted").num_rows; assert rows == 4
    assert len((tmp_path/"runs/test-g1/failed.jsonl").read_text(encoding="utf-8").splitlines()) == 1
```
- [ ] **Step 2: 실패 확인**
- [ ] **Step 3: 구현.** LangGraph `StateGraph` 상태는 `TypedDict(run_document_id, doc, extraction, meta, error, attempts)`. 그래프는 문서 1건을 처리하고, 바깥에서 `asyncio.gather`로 돌린다. Parquet 쓰기는 모두 끝난 뒤 100건씩 pyarrow로.
- [ ] **Step 4: 통과 확인**
- [ ] **Step 5: Commit** `feat: langgraph extraction pipeline with retry and parquet output`

---

### Task 7: 정규화 규칙

**Files:**
- Create: `eventgraph/batch/__init__.py`, `eventgraph/batch/normalize.py`, `tests/test_normalize.py`

**Interfaces:**
- Produces: `TITLE_SUFFIXES: tuple[str,...]`(대통령, 총리, 국무총리, 장관, 외무장관, 국방장관, 의원, 대표, 원내대표, 위원장, 사령관, 대변인, 총장, 지사, 시장, 회장, 사장, 감독, 선수, 기자, 씨), `norm_key(type:str, name_ko:str) -> str` — 괄호와 내용 제거 → 공백 제거 → PERSON이면 TITLE_SUFFIXES 중 가장 긴 접미 1개 제거(반복 1회) → 소문자. 결과가 비면 원래 이름 공백 제거본.

- [ ] **Step 1: 실패 테스트**
```python
@pytest.mark.parametrize("t,n,exp", [
 ("PERSON","도널드 트럼프","도널드트럼프"),("PERSON","트럼프 대통령","트럼프"),("PERSON","트럼프","트럼프"),
 ("PERSON","아바스 아락치 외무장관","아바스아락치"),("PERSON","김민석 국무총리","김민석"),
 ("ORG","이란 혁명수비대(IRGC)","이란혁명수비대"),("COUNTRY","미국","미국"),("PERSON","대통령","대통령")])
def test_norm_key(t,n,exp): assert norm_key(t,n) == exp
```
- [ ] **Step 2: 실패 확인** → **Step 3: 구현** → **Step 4: 통과 확인**
- [ ] **Step 5: Commit** `feat: entity normalization key`

---

### Task 8: Spark 배치 → PostgreSQL

**Files:**
- Create: `eventgraph/batch/spark.py`, `eventgraph/batch/job.py`, `tests/test_job.py`, `tests/fixtures/extracted_small/` (Parquet 1개, 3문서 — Step 1에서 생성)

**Interfaces:**
- Consumes: Parquet 포맷(Task 6), `norm_key`, PG 스키마(Task 2).
- Produces: `get_spark(app_name="eventgraph") -> SparkSession` — `local[*]`, `spark.jars.packages=org.postgresql:postgresql:42.7.4`, `spark.sql.session.timeZone=Asia/Seoul`, `spark.driver.memory=2g`. `run_batch(settings, run_id:str) -> dict` 반환 `{"entities":n,"relations":n,"events":n,"evidence":n,"other_relations":n}`.
- 배치 단계(스펙 5(C)): ① extraction JSON 파싱(`from_json`) → entities explode → `norm_key` UDF → `(run_id,type,norm_key)` 그룹: name_ko는 최빈값, surface_forms는 `array_distinct(flatten(collect_list))`, doc_count. ② affiliation: 문서별 `temp_id→entity_id` 매핑 후 (person, target, role) 그룹, doc_count, 최다를 is_primary. ③ relation: (type, src, dst) 그룹, doc_count, raw_label은 최빈값, date_min/max = evidence_date(없으면 run_document.writedate) 범위. evidence는 relation별 전부 보존. ④ event: date 동일 그룹 내에서 참여자 집합 자카드 ≥ 0.5면 병합(그룹당 건수가 작으므로 `groupBy(date).applyInPandas`로 그룹 내 O(n²) 허용 — `# ponytail: 날짜 그룹 내 O(n²), 수천 건/일 넘으면 MinHash`). ⑤ degree = 인물별 relation 상대 수. ⑥ 적재: psycopg로 `DELETE ... WHERE run_id=%s` 8개 표(의존 순서) → Spark JDBC `mode("append")`. id 매핑이 필요하므로 entity는 먼저 쓰고 다시 읽어(`jdbc read where run_id`) id를 조인한다.
- OTHER 관계 및 (type, src_class, dst_class) 조합 건수를 INFO 로그로 출력.

- [ ] **Step 1: fixture 생성** 스크립트가 아닌 테스트 내 `_make_fixture(tmp_path)`: 3문서. 문서1: 트럼프(PERSON, 소속 미국), 이란(COUNTRY), 관계 CRITICIZES 트럼프→이란 evidence_date 2026-06-25, 사건 "미·이란 도하 협상" 2026-06-30 참여 [트럼프, 아락치]. 문서2: "트럼프 대통령"(PERSON), 이란, 같은 CRITICIZES, 사건 "미·이란 도하 회담" 2026-06-30 참여 [트럼프, 아락치] (→ 병합 기대). 문서3: 아락치 → 이란 MEMBER_OF, 관계 type "OTHER" raw_label "경고". run_document 3행 선삽입(writedate 2026-06-26~28).
- [ ] **Step 2: 실패 테스트**
```python
@pytest.mark.integration
def test_run_batch_merges_and_is_idempotent(settings, pg, tmp_path):
    _make_fixture(tmp_path, pg, "test-b1")
    settings = replace(settings, data_dir=tmp_path)
    r1 = run_batch(settings, "test-b1"); r2 = run_batch(settings, "test-b1")
    assert r1 == r2
    names = {x[0] for x in pg.execute("select name_ko from entity where run_id='test-b1' and class_id=(select id from onto_class where name='PERSON')")}
    assert names == {"트럼프","아바스 아락치"} or names == {"도널드 트럼프","아바스 아락치"}  # 트럼프 1노드
    assert pg.execute("select count(*) from entity where run_id='test-b1'").fetchone()[0] == 3  # 트럼프, 아락치, 이란
    rel = pg.execute("select doc_count, date_min, date_max from relation r join onto_relation_type t on t.id=r.type_id where run_id='test-b1' and t.name='CRITICIZES'").fetchone()
    assert rel[0] == 2 and rel[1] == date(2026,6,25) and rel[2] == date(2026,6,27)
    assert pg.execute("select count(*) from event where run_id='test-b1'").fetchone()[0] == 1
    assert pg.execute("select count(*) from evidence where run_id='test-b1'").fetchone()[0] == 6  # 관계 evidence 4(CRITICIZES×2, MEMBER_OF, OTHER) + 사건 evidence 2(병합돼도 보존)
    assert r1["other_relations"] == 1
```
- [ ] **Step 3: 실패 확인** `uv run pytest tests/test_job.py -v -m integration` (첫 실행은 ivy로 JDBC jar 다운로드, 네트워크 필요)
- [ ] **Step 4: 구현** `spark.py`, `job.py`. 단계마다 `.count()`를 INFO 로그.
- [ ] **Step 5: 통과 확인.** 배치 소요 시간 로그로 확인(목표 1분 이내, 로컬 Spark 기동 포함).
- [ ] **Step 6: Commit** `feat: spark batch merge and postgres load`

---

### Task 9: 파이프라인 오케스트레이션·CLI

**Files:**
- Create: `eventgraph/pipeline.py`, `eventgraph/cli.py`, `eventgraph/__main__.py`, `tests/test_pipeline.py`

**Interfaces:**
- Consumes: `select_documents`, `run_extraction`, `run_batch`, `fetch_candidates`.
- Produces:
  - `create_run(settings, event_name, date_from, date_to, seed_persons:list[str], seed_orgs:list[str], seed_countries:list[str]) -> str` — `run.id = f"{YYYYMMDD-HHMMSS}-{slug}"`(slug: 사건 이름의 한글·영숫자만 8자), status `created`.
  - `run_pipeline(settings, run_id:str, stages:tuple[str,...]=("select","extract","batch"), client=None, extract_fn=extract_document) -> dict` — 각 단계 전 `run.status` 갱신, 단계 결과를 run 표(doc_selected, doc_extracted, doc_failed, tokens_in/out, finished_at)에 기록, 예외 시 status `failed` + `run.error` 컬럼(Task 2 스키마에 `error text` 추가)에 메시지. "extract" 단계는 `run_document.status='selected'`인 행만 대상(재실행 시 성공분 건너뜀). ANTHROPIC_API_KEY 비어 있으면 extract 시작 전 `RuntimeError("ANTHROPIC_API_KEY not set")`.
  - `get_active_run(settings) -> str|None` — status가 `selecting|extracting|batching`인 run.
  - CLI: `python -m eventgraph migrate`, `python -m eventgraph run --event "..." --from 2026-06-24 --to 2026-07-02 [--persons a,b] [--orgs ...] [--countries ...] [--stages select,extract,batch]`, `python -m eventgraph rerun <run_id> --stages batch`, `python -m eventgraph serve`(Task 10). 종료 시 단계별 건수를 표로 stdout.

- [ ] **Step 1: 실패 테스트** — fake extract_fn, 선별은 실 MySQL 1일치, `max_docs=5`:
```python
@pytest.mark.integration
def test_run_pipeline_end_to_end_small(settings, pg, tmp_path):
    settings = replace(settings, data_dir=tmp_path, max_docs=5)
    rid = create_run(settings, "test-호르무즈", date(2026,6,27), date(2026,6,27), ["트럼프"], [], ["이란"])
    r = run_pipeline(settings, rid, client=FakeClient(), extract_fn=fake_extract_with_one_entity)
    row = pg.execute("select status, doc_selected, doc_extracted, doc_failed from run where id=%s",(rid,)).fetchone()
    assert row[0] == "done" and row[1] == 5 and row[2] == 5 and row[3] == 0
    assert pg.execute("select count(*) from entity where run_id=%s",(rid,)).fetchone()[0] >= 1

def test_run_pipeline_marks_failed_on_exception(...): extract_fn이 항상 RuntimeError → status == "failed", error 비어있지 않음
def test_extract_refuses_without_api_key(...): anthropic_api_key="" → RuntimeError 메시지에 "ANTHROPIC_API_KEY"
```
(`run.id`가 `test-`로 시작해야 conftest 정리에 걸린다: create_run에 `id_prefix:str=""` 인자 추가.)
- [ ] **Step 2: 실패 확인** → **Step 3: 구현** → **Step 4: 통과 확인**
- [ ] **Step 5: 실 실행(수동, 비용 발생)** `uv run python -m eventgraph run --event "미국-이란 호르무즈 사태" --from 2026-06-24 --to 2026-07-02 --persons "도널드 트럼프" --countries "미국,이란"`. 확인: doc_extracted ≥ 280, 15분 이내, PG entity 중 PERSON에 "도널드 트럼프" 1노드. 소요 시간·토큰·비용을 `docs/golden/hormuz/run_notes.md`에 기록.
- [ ] **Step 6: Commit** `feat: pipeline orchestration and cli`

---

### Task 10: FastAPI

**Files:**
- Create: `eventgraph/api.py`, `tests/test_api.py`

**Interfaces:**
- Consumes: `create_run`, `run_pipeline`, `get_active_run`, PG 표.
- Produces(JSON):
  - `POST /runs` body `{event_name, date_from, date_to, seed_persons:[], seed_orgs:[], seed_countries:[]}` → 201 `{run_id}`; 활성 run 있으면 409. BackgroundTasks로 `run_pipeline`.
  - `GET /runs` → `[{id, event_name, date_from, date_to, status, doc_selected, doc_extracted, doc_failed, started_at, finished_at}]` 최신순.
  - `GET /runs/{id}` → 위 1건 + `expanded_keywords, error, tokens_in, tokens_out`.
  - `GET /runs/{id}/graph?from=YYYY-MM-DD&to=YYYY-MM-DD` → `{"nodes":[{id, type, name, doc_count, degree, affiliations:[{name,type,role}]}], "edges":[{id, src, dst, type, raw_label, doc_count, date_min, date_max}]}`. 기간 지정 시 `date_max >= from AND date_min <= to`인 간선과 그 양끝 노드만. 노드에 소속 기관·국가가 있으면 그 기관·국가 노드도 포함.
  - `GET /runs/{id}/events?from&to` → `[{id, date, summary, doc_count, participants:[{id,name}], location}]` 날짜순.
  - `GET /runs/{id}/relations/{rid}/evidence` → `[{sentence, evidence_date, title, press, writedate, url}]`.
  - `GET /runs/{id}/entities/{eid}` → `{id, type, name, surface_forms, affiliations, events:[{id,date,summary}]}`.
  - `GET /` → `static/index.html`.

- [ ] **Step 1: 실패 테스트** httpx `TestClient`; Task 8 fixture로 `test-b1` 적재 후:
```python
def test_graph_period_filter(client_with_fixture):
    g = client.get("/runs/test-b1/graph").json(); assert len(g["edges"]) == 3
    g2 = client.get("/runs/test-b1/graph", params={"from":"2026-06-01","to":"2026-06-24"}).json()
    assert g2["edges"] == []
def test_post_run_conflicts_when_active(...): run 하나 status 'extracting'로 삽입 → POST → 409
def test_evidence_includes_source(...): sentence, url, press 키 존재
```
- [ ] **Step 2: 실패 확인** → **Step 3: 구현**(psycopg row_factory=dict_row) → **Step 4: 통과 확인**
- [ ] **Step 5: Commit** `feat: fastapi endpoints`

---

### Task 11: 프런트 화면

**Files:**
- Create: `eventgraph/static/index.html`

**Interfaces:**
- Consumes: Task 10 API 전부.
- 화면: 상단 폼(사건 이름, 시작일·종료일 `<input type="date">`, 인물·기관·국가 텍스트 3개, 실행 버튼, 과거 run `<select>`), 실행 중이면 2초 폴링으로 상태 문구 `선별 N건 → 추출 n/N(실패 f) → 배치 → 완료`. 결과: 좌 70% Cytoscape(PERSON 원, ORG 사각, COUNTRY 육각, LOCATION 마름모; 노드 라벨 `이름\n소속1·소속2`; 간선 라벨 type(OTHER면 raw_label), 굵기 `1+log2(doc_count)`), 우 30% 타임라인 리스트 + 기간 슬라이더(`<input type="range">` 2개, 변경 시 graph·events 재조회), 하단 상세 패널(노드 클릭 → entities API, 간선 클릭 → evidence API를 제목·언론사·날짜·링크로 표시, 사건 클릭 → 참여 인물 노드 `highlight` 클래스). Cytoscape는 cdnjs, 레이아웃 `cose`. 빌드 도구 없음, 외부 스크립트는 Cytoscape 하나.

- [ ] **Step 1: 구현**
- [ ] **Step 2: 확인(수동)** `uv run python -m eventgraph serve` → http://localhost:8000 에서 Task 9 Step 5의 호르무즈 run 열기. 확인 항목: 트럼프 노드 아래 "미국" 표시, 간선 클릭 시 근거 문장과 기사 링크, 슬라이더를 6/30~7/1로 좁히면 간선 수 감소, 사건 클릭 시 참여자 강조. 스크린샷을 `docs/golden/hormuz/screen.png`로 저장.
- [ ] **Step 3: Commit** `feat: graph + timeline web ui`

---

### Task 12: 골든 세트와 품질 측정

**Files:**
- Create: `scripts/golden_compare.py`, `docs/golden/hormuz/top20.jsonl`, `docs/golden/hormuz/answers.jsonl`, `docs/golden/hormuz/result.md`

**Interfaces:**
- `top20.jsonl`: Task 9 실 run의 `run_document` rank 1~20(`run_document_id, title, url, writedate`).
- `answers.jsonl`: 문서별 `{run_document_id, persons:[표준명], orgs:[], countries:[], relations:[{src,dst,type}], events:[{date,summary}]}` — Claude 초안, 사용자 교정(사용자 결정).
- `golden_compare.py <run_id> <answers.jsonl>` → stdout·`result.md`: 개체 재현율(정답 인물 중 PG entity surface_forms/name_ko에 매칭된 비율), 관계 정확도((src,dst,type) 일치율, OTHER는 raw_label 무시), 병합 오류 목록(정답 1인물 ↔ PG 2노드 이상 = 쪼개짐, PG 1노드 ↔ 정답 2인물 = 잘못 합침).

- [ ] **Step 1: top20.jsonl 추출**(SQL → 파일)
- [ ] **Step 2: 20건 본문을 읽고 answers.jsonl 초안 작성**(Claude). 사용자에게 교정 요청.
- [ ] **Step 3: golden_compare.py 구현·실행**, result.md 기록. 쪼개짐이 있으면 원인(직함 사전 누락, 표기 변형)을 result.md에 적고, TITLE_SUFFIXES만 보강한 뒤 `rerun --stages batch`로 재측정(LLM 재호출 없음). 변경 전후 수치를 함께 기록.
- [ ] **Step 4: Commit** `test: hormuz golden set and comparison`

---

### Task 13: 두 번째 사건으로 재사용성 확인

- [ ] **Step 1: 실 실행** `--event "러시아-우크라이나 전쟁" --from 2026-06-24 --to 2026-07-06 --persons "볼로디미르 젤렌스키,블라디미르 푸틴" --countries "러시아,우크라이나"`
- [ ] **Step 2: 확인** 코드 변경 없이 완료(status done), 화면에서 젤렌스키·푸틴 노드와 크림반도(LOCATION) 존재. OTHER 관계 라벨 상위 10개를 `docs/golden/ukraine/run_notes.md`에 기록(온톨로지 확장 후보).
- [ ] **Step 3: Commit** `docs: second-event run notes`

---

## Self-Review 기록

- 스펙 커버리지: 목표 6항 → Task 4·9(폼 하나로 끝), 8·10·11(소속·근거·타임라인), 5·8(미입력 인물 추가, 한국어 표준명), 6(실패 계속). 비목표는 미구현 확인. 반목표 → 7·12(쪼개짐), 10·11(근거), 9(사건 입력값), 6·9(무음 실패: failed.jsonl·status failed·API 키 검사). 스펙 5(A)~(E), 6, 7 전부 대응 Task 있음. `run.error` 컬럼은 Task 9에서 필요해 Task 2 스키마에 포함하도록 명시.
- 타입 일관성: `Doc` 필드, `Extraction.dropped`, `run_document.status` 값(`selected|extracted|failed`), `run.status` 값, `norm_key(type, name_ko)`, `run_batch` 반환 키를 Task 간 동일하게 사용.
- Review Focus 5건은 각각 Task 3·3·5·5·8 테스트에 들어 있음.
- 비율: 스펙 150줄, 계획 330줄. 코드 블록은 테스트 단언 위주.
