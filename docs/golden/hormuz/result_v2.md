# 골든 세트 비교 — run `20261021-102144-미국이란호르무즈`

## 개체 재현율 (문서 단위, 정답 기준)

| 클래스 | 정답 | 적중 | 재현율 |
|---|---|---|---|
| PERSON | 44 | 31 | 70% |
| ORG | 26 | 15 | 58% |
| COUNTRY | 83 | 51 | 61% |

**관계 정확도**: 24/47 = 51%

## 병합 품질 (PostgreSQL entity, PERSON)

- 쪼개짐(정답 1인 → 노드 2개 이상): 0건
- 잘못 합침(노드 1개 ← 정답 2인 이상): 0건

## 놓친 개체

- doc 378 COUNTRY `레바논`
- doc 378 COUNTRY `스위스`
- doc 380 COUNTRY `카타르`
- doc 380 COUNTRY `오만`
- doc 380 COUNTRY `파키스탄`
- doc 380 COUNTRY `스위스`
- doc 382 ORG `이란군`
- doc 382 COUNTRY `스위스`
- doc 382 COUNTRY `오만`
- doc 382 COUNTRY `한국`
- doc 383 ORG `미군 중부사령부`
- doc 384 PERSON `모하마드 바게리`
- doc 384 PERSON `호세인 살라미`
- doc 384 PERSON `알리 하메네이`
- doc 384 PERSON `알리 라리자니`
- doc 384 ORG `최고국가안보회의`
- doc 385 PERSON `호세인 살라미`
- doc 385 PERSON `아미르 알리 하지자데`
- doc 385 PERSON `아바스 아라그치`
- doc 385 PERSON `왕이`
- doc 385 COUNTRY `이스라엘`
- doc 386 COUNTRY `카타르`
- doc 386 COUNTRY `오만`
- doc 388 COUNTRY `쿠웨이트`
- doc 388 COUNTRY `바레인`
- doc 389 PERSON `하마드 빈 이사 알할리파`
- doc 389 PERSON `마르코 루비오`
- doc 389 ORG `미 해군 5함대`
- doc 389 COUNTRY `아랍에미리트`
- doc 389 COUNTRY `쿠웨이트`
- doc 390 PERSON `아바스 아라그치`
- doc 390 PERSON `바드르 빈 하마드 알부사이디`
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
- doc 392 ORG `헤즈볼라`
- doc 392 COUNTRY `파키스탄`
- doc 392 COUNTRY `아제르바이잔`
- doc 392 COUNTRY `오만`
- doc 392 COUNTRY `이스라엘`
- doc 392 COUNTRY `레바논`
- doc 393 ORG `쿠웨이트 군`
- doc 393 COUNTRY `오만`
- doc 394 ORG `헤즈볼라`
- doc 394 COUNTRY `레바논`
- doc 394 COUNTRY `이스라엘`
- doc 394 COUNTRY `바레인`
- doc 394 COUNTRY `오만`
- doc 395 COUNTRY `레바논`

## 놓친 관계

- doc 378 미국 -AGREES_WITH-> 이란
- doc 379 왕이 -MEETS-> 아바스 아라그치
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
