# EventGraph 설계 문서

작성일: 2026-10-20
상태: 승인(2026-10-20, 빌더 검토 반영)

## 1. 의도와 배경

정세 분석가가 특정 사건(예: 러시아-우크라이나 전쟁)을 이해하려면 "시간 순으로 무슨 일이 있었고, 누가 누구와 어떤 관계인가"를 한 화면에서 봐야 한다. 지금은 뉴스를 사람이 읽고 손으로 정리한다.

EventGraph는 이미 수집된 한국어 뉴스 코퍼스(MySQL)에서 사용자가 지정한 사건에 관련된 기사를 골라, LLM으로 인물·기관·지역·관계·날짜 사건을 추출하고, 이를 온톨로지 기반 그래프로 쌓아 **인물 관계도 + 연동 타임라인**으로 보여주는 재사용 도구다. 사건은 입력값이며, 어떤 사건이든 같은 파이프라인이 처리한다.

부차 목적: 실제 운영 시스템은 수백만 문서 규모가 예정되어 있어, 코퍼스 단위 배치를 PySpark로 구현해 Spark 활용 경험을 쌓는다.

## 2. 목표 / 비목표 / 반목표

**목표**
- 폼 입력(사건 이름, 기간, 주요 인물·기관·국가) 하나로 끝까지 돈다.
- 인물 관계도: 인물 노드에 소속 국가·기관이 표시되고, 간선에 관계 유형·근거 문장·출처 기사가 붙는다.
- 타임라인: 날짜가 붙은 사건 목록, 관계도와 기간 필터로 연동.
- 사용자가 입력하지 않은 인물·기관도 문서에서 발견되면 그래프에 추가된다.
- 모든 표시 이름은 한국어 표준명. 원문 표기는 별칭으로 보존.
- 개별 문서 실패가 전체 실행을 멈추지 않는다. 실패는 기록되고 나머지는 계속된다.

**비목표(이번 범위 밖)**
- 웹 검색·크롤링 등 원천 수집. 원천은 이미 MySQL에 있다.
- 다국어 원문 처리. 한국어 코퍼스만 대상.
- 수집 문서에 대한 자유 질의응답(RAG). 벡터 DB(Qdrant/Milvus)·LlamaIndex는 이 기능이 들어올 때 추가.
- 배포, 인증, 다중 사용자. 로컬 웹 서비스로 충분.
- Redis. 작업 큐나 캐시가 병목으로 실측될 때 추가.

**반목표(이렇게 되면 실패)**
- 같은 인물이 표기 차이("젤렌스키 대통령", "볼로디미르 젤렌스키")로 여러 노드로 쪼개져 관계도가 읽히지 않는다.
- 간선을 클릭해도 근거를 확인할 수 없어 분석가가 신뢰하지 못한다.
- 사건을 바꾸려면 코드를 고쳐야 한다.
- LLM이 조용히 실패해 그래프가 비어 있는데 성공으로 보고된다.

## 3. 제약

- 원천: MySQL 8.0 `portalnews.newsnaver` (읽기 전용). 접속 정보는 `config/.env`의 DB_HOST/DB_USER/DB_PWD/DB_NAME/DB_TABLENAME. 테이블명은 설정값이며 형제 테이블(newsnate, newsdaum, news)로 바꿔 끼울 수 있어야 한다.
- 원천 실측(2026-10-20): 30만 건, 2026-06~07. 필드 title, body(평균 700자, text), writedate, url, url2(언론사 원문 URL), section("세계;", "정치 사회;" 형태의 세미콜론 구분). `press`는 전부 공백이므로 언론사는 url2 도메인으로 복원. 불량 날짜('0000-00-00') 39건, 빈 본문 173건, HTML 태그 섞인 본문 약 5,300건 존재.
- LLM: Anthropic API. 모델은 Claude Haiku 5.5(`claude-haiku-5-5`, 사용자 결정), 모델 ID는 설정값으로 교체 가능.
- 스택: Python 3.11, PostgreSQL(온톨로지 저장소), PySpark(로컬 모드, 코퍼스 배치), LangGraph(추출 파이프라인 오케스트레이션), FastAPI + 단일 HTML(Cytoscape.js). Windows 11 로컬 개발. 한글 경로 포함, UTF-8 명시 필수.
- 비용 상한: 사건 1건 실행당 추출 대상 문서 수는 설정값(기본 300건)으로 제한한다.

## 4. 사용자 흐름

1. 브라우저에서 폼을 연다. 입력: 사건 이름(필수), 시작일·종료일(YYYY-MM-DD, 필수), 주요 인물·기관·국가/지역(각각 쉼표 구분, 선택). 소스 테이블은 설정값이므로 폼에 없다.
2. "실행"을 누르면 실행(run)이 생성되고, 진행 상태(선별 N건 → 추출 n/N → 배치 → 완료)가 화면에 보인다. 실패 문서 수도 함께 보인다.
3. 완료 후 결과 화면: 왼쪽 관계도, 오른쪽 타임라인, 아래 상세 패널.
   - 관계도: 인물은 원, 기관·국가는 다른 모양. 인물 노드 라벨 아래 소속 국가·기관 표시. 간선 굵기는 근거 문서 수.
   - 타임라인: 날짜 순 사건 목록. 기간 슬라이더를 좁히면 그 기간의 사건과, 근거 날짜가 기간 안에 드는 간선·그 양끝 인물만 관계도에 남는다. 간선의 근거 날짜는 evidence_date, 없으면 출처 기사 writedate를 쓴다. 필터는 서버(graph API의 기간 파라미터)에서 처리한다. [Claude 판단]
   - 사건(EVENT)은 관계도 노드로 그리지 않는다. 타임라인 항목이며, 클릭하면 참여 인물이 관계도에서 강조된다. [Claude 판단]
   - 노드 클릭: 별칭, 소속, 관련 사건 목록. 간선 클릭: 관계 유형, 근거 문장 목록, 각 문장의 출처 기사 제목·언론사·날짜·URL.
4. 과거 실행 목록에서 이전 결과를 다시 열 수 있다.

## 5. 아키텍처

```
[폼 입력] → (A) 문서 선별 → (B) LLM 추출 → Parquet → (C) Spark 배치 → PostgreSQL → (D) 웹 화면
                MySQL(읽기)      LangGraph                 PySpark(local)      온톨로지 표    FastAPI+HTML
```

### (A) 문서 선별
- 기간으로 writedate 필터(불량 날짜·빈 본문 제외).
- 본문 정제: HTML 태그 제거. 기자 서명·앵커 지시문 등 상용구는 1차에서 제거하지 않는다(LLM이 걸러낸다).
- 관련도 점수: 사건 이름과 사용자 입력 인물·기관·국가 키워드가 title/body에 등장하는 횟수의 가중합(title 가중치 높음). 사건 이름은 LLM에게 동의어·관련 키워드 10~20개를 한 번 생성시켜 확장한다(예: "러시아-우크라이나 전쟁" → 우크라이나, 러시아, 젤렌스키, 푸틴, 크림, 돈바스...). 확장 결과는 run에 기록되고 사용자가 확인할 수 있다.
- 상위 N건(설정, 기본 300)만 추출 대상. 선별 결과(점수·순위)는 run_document 표에 저장해 재현 가능하게 한다.
- 이 단계는 Spark가 아니라 SQL + Python으로 한다. 30만 건 규모에서 Spark는 과잉이고, 수백만 규모가 되면 이 단계만 Spark 잡으로 옮긴다(인터페이스: 입력 run_id, 출력 run_document).

### (B) LLM 추출 (LangGraph)
노드: 문서 적재 → 추출 호출 → 스키마 검증 → Parquet 기록 → (실패 시) 재시도 1회 후 실패 기록.
- 추출 입력: 기사 제목·본문·발행일·사건 이름·사용자 씨앗 목록(힌트).
- 추출 출력(구조화, JSON 스키마 강제):
  - entities: [{temp_id, type(PERSON|ORG|LOCATION|COUNTRY), name_ko(표준명), surface_forms[], affiliations:[{target_temp_id, role}]}]
  - relations: [{src_temp_id, dst_temp_id, type(온톨로지 관계 유형 중 하나 또는 OTHER), raw_label, evidence(원문 문장), evidence_date(문장에 날짜가 있으면)}]
  - events: [{date(YYYY-MM-DD), summary_ko, participants[temp_id], location_temp_id, evidence}]
- 표준명 규칙: 인물은 직함 제외 성명(예: "볼로디미르 젤렌스키"), 기관은 공식 한국어 명칭, 국가는 한국어 국명. 원문 표기는 surface_forms에 모두 넣는다.
- 날짜 해석: "지난 24일", "오늘" 같은 상대 표현은 기사 writedate 기준으로 절대 날짜로 바꾼다. 해석 불가면 null.
- 병렬도: asyncio 세마포어(설정, 기본 5). 레이트리밋 응답은 지수 백오프.
- 출력: `data/runs/{run_id}/extracted/*.parquet` (문서별 1행, entities/relations/events는 중첩 컬럼). 실패 문서는 `failed.jsonl`에 사유와 함께.
- 관측: 문서별 입력 토큰·출력 토큰·소요 시간 기록. run 종료 시 합계를 run 표에 저장.

### (C) Spark 배치 (PySpark local)
입력: run의 Parquet. 출력: PostgreSQL 온톨로지 표.
1. **개체 정규화·병합**: 모든 문서의 entities를 펼친 뒤 (type, 정규화 키)로 그룹. 정규화 키 = name_ko에서 공백·직함 접미("대통령", "장관", "총리", "의원", "대표" 등 사전)·괄호 제거. 같은 그룹의 surface_forms는 합집합. 1차에서는 규칙 기반 병합만 한다. LLM 기반 동일인 판정은 반목표(쪼개짐)가 실측될 때 추가.
2. **소속 집계**: 인물→기관/국가 소속은 문서 수로 집계하고 최다 소속을 대표 소속으로.
3. **관계 집계**: (src, dst, type)으로 그룹, 근거 문장·출처 문서 목록 수집, 문서 수가 간선 가중치.
4. **사건 정리**: 날짜 동일 + 참여자 집합 자카드 ≥ 0.5이면 하나로 묶고 근거를 모은다.
5. **그래프 통계**: 인물별 연결 수(degree), 근거 문서 수. 중심성은 degree만(GraphFrames 미사용).
6. JDBC로 PostgreSQL에 run_id 단위로 쓴다(해당 run의 기존 행 삭제 후 삽입, 멱등).
- Spark는 로컬 모드(`local[*]`). Java 17 이상(로컬은 21 설치됨). Spark 세션 생성 코드는 한 곳에 두어 나중에 클러스터 마스터 URL만 바꿔 쓴다.
- 1차에서 Spark는 MySQL을 직접 읽지 않는다. (A)에서 Python이 읽어 Parquet로 넘기므로 Spark는 Parquet만 본다. 수백만 규모 전환 시 (A)를 Spark JDBC 읽기로 바꾸는 것이 업그레이드 경로다.

### (D) 저장소: PostgreSQL 온톨로지
"클래스·관계유형 정의"와 "인스턴스"를 분리한다.

정의 표(고정 데이터, 마이그레이션으로 적재):
- `onto_class(id, name)`: PERSON, ORG, LOCATION, COUNTRY, EVENT
- `onto_relation_type(id, name, src_class, dst_class, description)`: 초기 세트
  - 인물→기관/국가: MEMBER_OF(소속), LEADS(수장), REPRESENTS(대표)
  - 인물↔인물: MEETS(회담·접촉), CRITICIZES(비난·비판), SUPPORTS(지지), NEGOTIATES_WITH(협상), ORDERS(지시·명령), SUCCEEDS(후임)
  - 기관/국가↔기관/국가: ALLIES_WITH(협력·동맹), SANCTIONS(제재), ATTACKS(공격), AGREES_WITH(합의)
  - 사건→개체: PARTICIPATES(참여), OCCURS_AT(장소)
  - OTHER: 스키마 밖 관계. 적재하되 LLM이 준 자유 라벨을 raw_label에 보존하고 로그에 카운트. 온톨로지 확장 재료.
  - src_class/dst_class는 **참고용**이며 강제하지 않는다. "젤렌스키(인물)가 러시아(국가)를 비난"처럼 교차 클래스 관계는 그대로 해당 유형으로 적재한다. (유형, src_class, dst_class) 조합별 건수는 배치 로그에 남겨 온톨로지 정비 재료로 쓴다. [Claude 판단]
  - EVENT 클래스와 PARTICIPATES/OCCURS_AT은 정의 표에만 있고, 인스턴스는 relation 표가 아니라 event/event_participant 표에 저장한다. [Claude 판단]

인스턴스 표(모두 run_id 포함):
- `run(id, event_name, date_from, date_to, seed_persons, seed_orgs, seed_countries, expanded_keywords, status, doc_selected, doc_extracted, doc_failed, tokens_in, tokens_out, started_at, finished_at)`
- `run_document(id, run_id, source_table, source_idx, title, url, press, writedate, score, rank, status)`
- `entity(id, run_id, class_id, name_ko, norm_key, surface_forms[], doc_count, degree)`
- `affiliation(run_id, person_id, target_id, role, doc_count, is_primary)`
- `relation(id, run_id, type_id, src_id, dst_id, raw_label, doc_count, date_min, date_max)` — date_min/max는 근거 날짜 범위(기간 필터용)
- `evidence(id, run_id, relation_id NULL, event_id NULL, run_document_id, sentence, evidence_date)` — evidence_date가 null이면 조회 시 출처 기사 writedate로 대체
- `event(id, run_id, date, summary_ko, location_id, doc_count)`
- `event_participant(event_id, entity_id)`

### (E) 웹
- FastAPI. 엔드포인트: POST /runs(실행 생성·백그라운드 시작), GET /runs, GET /runs/{id}(상태), GET /runs/{id}/graph(노드·간선 JSON, 선택 기간 파라미터), GET /runs/{id}/events, GET /runs/{id}/relations/{rid}/evidence, GET /runs/{id}/entities/{eid}.
- 프런트: 단일 HTML + Cytoscape.js(CDN). 빌드 도구 없음. 타임라인은 단순 리스트 + 기간 슬라이더(range input 2개).
- 백그라운드 실행은 1차에서 FastAPI BackgroundTasks. 동시에 run 하나만 허용(실행 중이면 409). 큐가 필요해지면 그때 Redis.

## 6. 품질 확인 방법

- 골든 세트: 첫 검증 사건은 **미국-이란 호르무즈 사태**(2026-06~07 코퍼스에 제목 기준 약 3,200건, 트럼프·이란 외무부·혁명수비대·영국·프랑스·카타르·한국 선박 등 인물·국가·기관 혼재). 선별 상위 20건의 정답(인물·관계·사건)은 Claude가 초안을 작성하고 사용자가 교정한다(사용자 결정). 두 번째 검증 사건은 러시아-우크라이나 전쟁(약 1,500건)으로, 사건을 바꿔도 같은 파이프라인이 도는지 확인한다. 추출 결과와 비교해 개체 재현율, 관계 정확도, 병합 오류(쪼개짐·잘못 합침) 수를 기록한다. 임계값(관련도 컷, 병합 규칙) 변경은 이 20건 결과로만 확정한다.
- 각 단계는 산출물을 남기고(run_document, Parquet, failed.jsonl, PostgreSQL 행 수) 단계별 건수를 로그·run 표에 기록해 "돌았는데 비어 있음"을 즉시 볼 수 있게 한다.
- 비기능: 300건 추출이 로컬에서 15분 이내, Spark 배치 1분 이내를 목표. 초과하면 병렬도·모델을 조정.

## 7. 빌더에게 맡기는 것

- 프로젝트 레이아웃, 패키지 관리(uv/pip), 테스트 구성.
- 추출 프롬프트의 구체 문안과 few-shot 예시. 단, 출력 스키마는 5(B)를 따른다.
- 직함 사전의 초기 목록, 관련도 점수의 가중치 수치.
- Cytoscape 레이아웃·색상.
- PostgreSQL 마이그레이션 방식(SQL 파일 직접 실행으로 충분).
- 로그 포맷.

## 8. 결정 사항 요약(누가 정했나)

- 사용자 결정: 재사용 도구, 폼 입력 항목, 한국어 코퍼스 대상, 결과 한국어, 스택(Python/PostgreSQL/Spark/LangGraph), 온톨로지를 RDB로, Spark 사용(수백만 규모 대비·학습), 원천은 별도 인입, 로컬 웹.
- Claude 판단(사용자 확인 받음): Qdrant/Milvus·LlamaIndex·Redis 보류, Spark를 코퍼스 배치에만 사용하고 LLM 호출은 Python 비동기, 원천 MySQL은 읽기 전용이고 온톨로지는 PostgreSQL에 별도 저장.
- Claude 판단(이 문서에서 처음 제시): 관련도 점수 방식과 상위 N건 컷, LLM 키워드 확장 1회, 규칙 기반 병합 우선, 초기 관계 유형 세트, 동시 run 1개 제한, BackgroundTasks 사용, 사건 묶기 규칙(날짜 동일 + 자카드 0.5).
- Claude 판단(빌더 검토 후 추가): 기간 필터는 간선 근거 날짜 기준·서버 처리, 사건은 노드가 아님, 관계 유형의 클래스 제약은 참고용(교차 클래스 허용). Java는 설치된 21을 쓴다(Spark 3.5 지원). PostgreSQL은 Docker로 띄운다.
