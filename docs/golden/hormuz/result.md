# 호르무즈 골든 세트 결과 (Task 12)

- 대상: run `20261021-102144-미국이란호르무즈` 선별 상위 20건(`top20.jsonl`), 정답 `answers.jsonl`(Claude 초안, **사용자 교정 전**)
- 정답 기준: 기사 앞부분 2,000자에 등장한 인물·기관·국가 전부. 추출기는 "사건과 관련된 개체만" 뽑도록 지시되어 있어 재현율 분모가 과대함(좌담 패널, 매체명, 연구소 등). 교정 시 "사건 관련 개체만"으로 줄이면 재현율은 올라간다.
- 비교 스크립트: `scripts/golden_compare.py <run_id> answers.jsonl --out result.md`

## 변경 전후 (병합 규칙 보강, LLM 재호출 없이 `rerun --stages batch`)

| 지표 | 변경 전 | 변경 후 |
|---|---|---|
| PERSON 재현율 | 63% | 58% |
| ORG 재현율 | 26% | 26% |
| COUNTRY 재현율 | 52% | 52% |
| 관계 정확도 | 26/48 = 54% | 24/48 = 50% |
| 쪼개짐 | 6 | 0 |
| 잘못 합침 | 2 | 0 |

변경 내용(규칙 기반, `eventgraph/batch/job.py canonical_keys`):
1. 단일 토큰 성(姓)은 그 성으로 끝나는 긴 이름이 **정규화 후 하나로 수렴**할 때 병합 (갈리바프 → 모하메드 바게르 갈리바프). 다중 토큰 이름끼리는 성이 같아도 병합하지 않음 (알리 하메네이 ≠ 모즈타바 하메네이).
2. 음역 변형: 성이 같고(또는 접미) 이름 토큰이 편집거리 1 이내면 병합 (마르코/마코 루비오, 재러드/제러드 쿠슈너, 압바스/세예드 아바스 아라그치, 알 부사이디/알부사이디).
3. 비교 스크립트의 이름 매칭을 엄격하게 바꿈(별칭은 완전 일치만) → 변경 후 PERSON 재현율 수치가 3건 내려간 것은 규칙 변화가 아니라 **측정 기준 변화**.

남은 쪼개짐: `에스마일 바게이` / `이스마엘 바게이`(편집거리 2)는 규칙으로 안 잡힘. 변경 전 6건 → 후 0건(측정 기준 기준).

리뷰 후 수정(직함 토큰 제거·결정적 대표 이름·자기 간선 제거) 적용 뒤 `rerun --stages batch` 재측정: 수치 동일(쪼개짐 0, 잘못 합침 0, PERSON 58%). 이 코퍼스에서는 '트럼프 대통령'이 별칭 규칙으로 이미 병합되어 있었음.

## 관찰 및 다음 단계
- **관계 유형 커버리지**: 549 관계 중 447(81%)가 OTHER. raw_label 상위(배치 로그): 보도·반박·경고·제안·통항 제한 등. 온톨로지에 REPORTS/WARNS/REJECTS/PROPOSES 계열 추가 검토.
- **선별 노이즈**: 관련도 점수가 등장 횟수 합이라 긴 기사·'미국'·'트럼프' 다빈출 기사가 상위에 옴. 상위 20건 중 3건(rank 4·9·19: 미국 건국 250주년, 외신 모음, AI 증시)은 사건과 무관. 길이 정규화 또는 키워드별 상한이 필요 — 재선별은 LLM 재호출 비용이 들어 이번엔 보류.
- **ORG 병합**: 규칙이 PERSON에만 적용되어 "혁명수비대/이란 혁명수비대/이슬람혁명수비대/이란 이슬람혁명수비대"가 분리. ORG용 별칭 사전 또는 포함 관계 병합 필요.
- **effort=low 재현율**: 주변 개체(중재국 카타르·오만·파키스탄 등)를 자주 놓침. 사용자 교정 후 effort=medium 1회 재추출로 비교할 가치가 있음(≈$5).

---

# 골든 세트 비교 — run `20261021-102144-미국이란호르무즈`

## 개체 재현율 (문서 단위, 정답 기준)

| 클래스 | 정답 | 적중 | 재현율 |
|---|---|---|---|
| PERSON | 57 | 33 | 58% |
| ORG | 58 | 15 | 26% |
| COUNTRY | 104 | 54 | 52% |

**관계 정확도**: 24/48 = 50%

## 병합 품질 (PostgreSQL entity, PERSON)

- 쪼개짐(정답 1인 → 노드 2개 이상): 0건
- 잘못 합침(노드 1개 ← 정답 2인 이상): 0건

## 놓친 개체

- doc 378 PERSON `장지향`
- doc 378 PERSON `성일광`
- doc 378 PERSON `유달승`
- doc 378 ORG `아산정책연구원`
- doc 378 ORG `서강대 유로메나연구소`
- doc 378 ORG `경향신문`
- doc 378 COUNTRY `레바논`
- doc 378 COUNTRY `스위스`
- doc 378 COUNTRY `한국`
- doc 379 ORG `상하이협력기구`
- doc 379 ORG `탈레반`
- doc 379 ORG `하마스`
- doc 379 COUNTRY `아프가니스탄`
- doc 379 COUNTRY `사우디아라비아`
- doc 379 COUNTRY `이스라엘`
- doc 379 COUNTRY `시리아`
- doc 380 PERSON `김열수`
- doc 380 PERSON `성일광`
- doc 380 ORG `한국군사문제연구원`
- doc 380 ORG `서강대 유로메나연구소`
- doc 380 COUNTRY `카타르`
- doc 380 COUNTRY `오만`
- doc 380 COUNTRY `파키스탄`
- doc 380 COUNTRY `스위스`
- doc 381 ORG `NATO`
- doc 381 ORG `IMF`
- doc 381 ORG `세계은행`
- doc 381 COUNTRY `한국`
- doc 381 COUNTRY `캐나다`
- doc 381 COUNTRY `인도`
- doc 381 COUNTRY `벨기에`
- doc 382 ORG `이란군`
- doc 382 COUNTRY `스위스`
- doc 382 COUNTRY `일본`
- doc 382 COUNTRY `오만`
- doc 382 COUNTRY `한국`
- doc 383 ORG `미군 중부사령부`
- doc 384 PERSON `모하마드 바게리`
- doc 384 PERSON `호세인 살라미`
- doc 384 PERSON `알리 하메네이`
- doc 384 PERSON `알리 라리자니`
- doc 384 ORG `모사드`
- doc 384 ORG `최고국가안보회의`
- doc 384 COUNTRY `이라크`
- doc 384 COUNTRY `한국`
- doc 384 COUNTRY `네덜란드`
- doc 385 PERSON `호세인 살라미`
- doc 385 PERSON `아미르 알리 하지자데`
- doc 385 PERSON `아바스 아라그치`
- doc 385 PERSON `왕이`
- doc 385 ORG `뉴욕타임스`
- doc 385 ORG `미국 중앙정보국`
- doc 385 COUNTRY `이스라엘`
- doc 386 ORG `월스트리트저널`
- doc 386 COUNTRY `카타르`
- doc 386 COUNTRY `오만`
- doc 386 COUNTRY `중국`
- doc 387 PERSON `김태준`
- doc 387 ORG `한반도안보문제연구소`
- doc 387 COUNTRY `중국`
- doc 387 COUNTRY `러시아`
- doc 388 ORG `CNN`
- doc 388 ORG `AP통신`
- doc 388 COUNTRY `쿠웨이트`
- doc 388 COUNTRY `바레인`
- doc 388 COUNTRY `파나마`
- doc 389 PERSON `하마드 빈 이사 알할리파`
- doc 389 PERSON `마르코 루비오`
- doc 389 PERSON `팀 쿡`
- doc 389 PERSON `사티아 나델라`
- doc 389 ORG `미 해군 5함대`
- doc 389 ORG `애플`
- doc 389 ORG `마이크로소프트`
- doc 389 COUNTRY `아랍에미리트`
- doc 389 COUNTRY `쿠웨이트`
- doc 389 COUNTRY `중국`
- doc 390 PERSON `아바스 아라그치`
- doc 390 PERSON `바드르 빈 하마드 알부사이디`
- doc 390 ORG `월스트리트저널`
- doc 390 ORG `미국 재무부`
- doc 390 COUNTRY `오만`
- doc 391 ORG `이란 외무부`
- doc 391 ORG `미 5함대`
- doc 391 COUNTRY `쿠웨이트`
- doc 391 COUNTRY `바레인`
- doc 391 COUNTRY `레바논`
- doc 391 COUNTRY `스위스`
- doc 392 PERSON `모하마드 바게르 갈리바프`
- doc 392 ORG `이슬람협력기구`
- doc 392 ORG `IRNA`
- doc 392 ORG `IRIB`
- doc 392 ORG `헤즈볼라`
- doc 392 COUNTRY `파키스탄`
- doc 392 COUNTRY `아제르바이잔`
- doc 392 COUNTRY `오만`
- doc 392 COUNTRY `이스라엘`
- doc 392 COUNTRY `레바논`
- doc 393 ORG `프레스TV`
- doc 393 ORG `쿠웨이트 군`
- doc 393 ORG `로이터통신`
- doc 393 ORG `CNN`
- doc 393 COUNTRY `오만`
- doc 394 PERSON `김태준`
- doc 394 ORG `한반도안보문제연구소`
- doc 394 ORG `헤즈볼라`
- doc 394 COUNTRY `레바논`
- doc 394 COUNTRY `이스라엘`
- doc 394 COUNTRY `바레인`
- doc 394 COUNTRY `오만`
- doc 395 PERSON `백승훈`
- doc 395 PERSON `이인철`
- doc 395 ORG `한국외대 중동연구소`
- doc 395 ORG `참조은경제연구소`
- doc 395 COUNTRY `레바논`
- doc 396 ORG `로이터통신`
- doc 396 ORG `입소스`
- doc 397 ORG `미들이스트아이`

## 놓친 관계

- doc 378 미국 -AGREES_WITH-> 이란
- doc 379 왕이 -MEETS-> 아바스 아라그치
- doc 379 시진핑 -MEETS-> 마수드 페제시키안
- doc 379 중국 -ALLIES_WITH-> 파키스탄
- doc 379 미국 -ATTACKS-> 이란
- doc 382 도널드 트럼프 -AGREES_WITH-> 마수드 페제시키안
- doc 382 미국 -SANCTIONS-> 이란
- doc 384 이스라엘 -ATTACKS-> 이란
- doc 385 아바스 아라그치 -MEETS-> 왕이
- doc 385 중국 -SUPPORTS-> 이란
- doc 386 미국 -AGREES_WITH-> 이란
- doc 386 미국 -ATTACKS-> 이란
- doc 388 미국 -ATTACKS-> 이란
- doc 388 도널드 트럼프 -ORDERS-> 미 중부사령부
- doc 389 마르코 루비오 -MEETS-> 하마드 빈 이사 알할리파
- doc 389 이란 이슬람혁명수비대 -ATTACKS-> 바레인
- doc 390 모하마드 바게르 갈리바프 -MEETS-> 바드르 빈 하마드 알부사이디
- doc 390 아바스 아라그치 -MEETS-> 바드르 빈 하마드 알부사이디
- doc 391 이란 -ATTACKS-> 미국
- doc 392 모하마드 바게르 갈리바프 -CRITICIZES-> 미국
- doc 394 미국 -ATTACKS-> 이란
- doc 394 이스라엘 -ATTACKS-> 헤즈볼라
- doc 395 미국 -AGREES_WITH-> 이란
- doc 397 이란 -NEGOTIATES_WITH-> 오만
