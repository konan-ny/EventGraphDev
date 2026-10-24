# EventGraph

한국어 뉴스 코퍼스(MySQL)에서 **사건 이름·기간·주요 인물/기관/국가**를 입력받아, 관련 기사를 골라 LLM으로 인물·기관·지역·관계·날짜 사건을 뽑고, PySpark 배치로 병합해 PostgreSQL 온톨로지에 적재한 뒤, 웹 화면에서 **인물 관계도 + 연동 타임라인 + 근거 문장**으로 보여주는 도구입니다. 사건은 입력값이라 어떤 사건이든 같은 파이프라인이 돕니다.

- 현황 한 장: [BRIEF.md](BRIEF.md) · 결정 기록: [DECISIONS.md](DECISIONS.md)
- 설계: [docs/superpowers/specs/2026-10-20-eventgraph-design.md](docs/superpowers/specs/2026-10-20-eventgraph-design.md) · 구현 계획: [docs/superpowers/plans/2026-10-20-eventgraph.md](docs/superpowers/plans/2026-10-20-eventgraph.md)

## 흐름

```
[폼 입력] → (A) 문서 선별 → (B) LLM 추출 → Parquet → (C) Spark 배치 → PostgreSQL → (D) 웹 화면
             MySQL(읽기)     LangGraph+Claude            PySpark local[4]   온톨로지 표   FastAPI + Cytoscape.js
```

| 단계 | 모듈 | 하는 일 |
|---|---|---|
| A 선별 | `eventgraph/select.py`, `source.py` | 기간 필터 → LLM 키워드 확장 → 제목·본문 등장 횟수 점수 → 상위 `MAX_DOCS`건 |
| B 추출 | `eventgraph/extract/` | 문서별 구조화 출력(JSON 스키마)으로 개체·관계·사건 추출, 1회 재시도, 실패는 `failed.jsonl` |
| C 배치 | `eventgraph/batch/` | 이름 정규화·동일인 병합, 관계·사건 집계, 근거 보존, PostgreSQL 적재(run 단위 멱등) |
| D 화면 | `eventgraph/api.py`, `static/index.html` | 관계도(핵심/전체), 각주형 근거, 하단 사건 타임라인(기간 드래그) |

## 실행

사전 준비: Python 3.11, [uv](https://docs.astral.sh/uv/), Docker(PostgreSQL), Java 17+(Spark), MySQL 원천 접근 권한.

```bash
uv sync                                   # 의존성
docker compose up -d                      # PostgreSQL (localhost:5434)
uv run python -m eventgraph migrate       # 온톨로지 스키마·시드
uv run python -m eventgraph serve --port 8766   # http://127.0.0.1:8766
```

CLI로 한 번에 돌리기:

```bash
uv run python -m eventgraph run --event "미국-이란 호르무즈 사태" --from 2026-06-24 --to 2026-07-02 \
  --persons "도널드 트럼프" --countries "미국,이란"
uv run python -m eventgraph rerun <run_id> --stages batch      # LLM 재호출 없이 배치만 다시
```

## 설정 (`config/.env`)

| 키 | 뜻 |
|---|---|
| `DB_HOST` `DB_USER` `DB_PWD` `DB_NAME` `DB_TABLENAME` | 원천 MySQL(읽기 전용). 테이블은 `newsnaver`와 같은 포맷이면 교체 가능 |
| `ANTHROPIC_API_KEY` | Claude API 키 |
| `ANTHROPIC_MODEL` | 추출 모델. 기본 `claude-haiku-5-5` |
| `PG_DSN` | 온톨로지 PostgreSQL. 기본 `postgresql://eventgraph:eventgraph@localhost:5434/eventgraph` |
| `MAX_DOCS` | 사건당 추출 문서 상한. 기본 300 |
| `LLM_CONCURRENCY` | 동시 LLM 호출 수. 기본 5 |
| `DATA_DIR` | 산출물 루트. 기본 `data` |

## 폴더

```
config/.env            비밀정보·설정 (git 제외)
eventgraph/            코드 (select · extract/ · batch/ · pipeline · api · static/)
tests/                 pytest. integration 표시는 MySQL·PostgreSQL·Spark 필요
scripts/golden_compare.py   골든 세트 비교
docs/golden/<사건>/    실 실행 기록(run_notes.md), 골든 세트(top20·answers·result)
docs/design/           UI 시안·스크린샷
data/runs/<run_id>/    추출 Parquet·failed.jsonl (git 제외)
data/runs/log/         실행 로그 (git 제외)
lib/                   PostgreSQL JDBC jar (첫 실행 때 자동 다운로드, git 제외)
```

## 테스트

```bash
uv run pytest -q                      # 전체 (Spark 포함, 약 5분)
uv run pytest -q -m "not integration" # 외부 서비스 없이
```

## 품질 측정

골든 세트는 `docs/golden/hormuz/answers.jsonl`(20건). 규칙·임계값 변경은 이 20건의 재측정으로만 확정합니다.

```bash
uv run python scripts/golden_compare.py <run_id> docs/golden/hormuz/answers.jsonl --out docs/golden/hormuz/result.md
```

## 알려진 제약

- Windows 로컬 Spark: `local[4]`, JDBC jar를 driver classpath로, Parquet은 pyarrow로 읽음(winutils 불필요). 클러스터 전환 시 `eventgraph/batch/spark.py` 한 곳만 바꿉니다.
- 동시 실행은 run 1개. 서버 재시작 시 진행 중이던 run은 `failed`로 표시됩니다.
- 관계 유형 밖의 관계는 `OTHER`로 적재되고 원문 라벨이 `raw_label`에 남습니다(온톨로지 확장 재료).
