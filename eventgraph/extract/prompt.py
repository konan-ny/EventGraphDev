"""Prompt for per-document extraction."""

from __future__ import annotations

from ..source import Doc
from .models import RELATION_TYPES

# Korean gloss per type: the model picks by meaning, the gloss is what it reads
_GLOSS = {
    "MEMBER_OF": "소속",
    "LEADS": "수장·지휘",
    "REPRESENTS": "대표·대변",
    "MEETS": "회담·면담·접촉",
    "CRITICIZES": "비난·비판·규탄",
    "SUPPORTS": "지지·지원",
    "NEGOTIATES_WITH": "협상·교섭",
    "ORDERS": "지시·명령",
    "SUCCEEDS": "후임·승계",
    "ALLIES_WITH": "협력·동맹·공조",
    "SANCTIONS": "제재·봉쇄·면제 조치",
    "ATTACKS": "공격·공습·타격",
    "AGREES_WITH": "합의·서명·체결",
    "WARNS": "경고·위협",
    "REJECTS": "반박·거부·항의",
    "PRESSURES": "압박·요구·촉구",
    "MEDIATES": "중재·주선",
    "REPORTS": "보도·발표·주장",
}
assert set(_GLOSS) == RELATION_TYPES - {"OTHER", "PARTICIPATES", "OCCURS_AT"}
_TYPES = ", ".join(f"{t}({g})" for t, g in _GLOSS.items())

SYSTEM_PROMPT = f"""너는 한국어 뉴스 기사에서 사건 지식그래프를 추출하는 분석가다. 결과는 JSON 하나만 출력한다.

규칙
1. 주어진 사건과 관련된 개체·관계·사건을 추출한다. 사건과 무관한 인물·기관(기사 작성자, 전문가 패널, 매체명, 무관한 기업)은 넣지 않는다.
   단, 기사에 언급된 국가·지역·국제기구·정부기관은 행위 주체가 아니어도(장소·중재국·피해국·협상 장소) 모두 넣는다. 놓치지 않는 것이 더 중요하다.
2. 개체 type은 PERSON, ORG, LOCATION, COUNTRY 중 하나. name_ko는 반드시 한글 표준명이며 영문 표기는 쓰지 않는다:
   인물은 직함을 뺀 성명(예: "볼로디미르 젤렌스키", "블라디미르 푸틴", "도널드 트럼프"), 기관은 공식 한국어 명칭, 국가는 한국어 국명.
   기사에 나온 표기(영문, 약칭, 직함 포함형)는 모두 surface_forms에 넣는다.
3. 인물의 소속(국가·기관)은 affiliations에 넣는다. target_temp_id는 같은 응답의 entity temp_id여야 한다.
4. relation.type은 다음 중 하나: {_TYPES}. 어디에도 맞지 않으면 "OTHER"로 두고 raw_label에 한국어 관계명을 적는다.
   src/dst temp_id는 모두 entities에 있어야 한다. evidence는 관계를 보여주는 기사 원문 문장 그대로.
   evidence_date는 그 문장이 가리키는 날짜가 분명할 때만 YYYY-MM-DD, 아니면 null.
5. events는 날짜가 붙은 구체적 일(회담, 공격, 발표, 제재 등). date는 기사 발행일 기준으로 "지난 24일", "오늘" 같은 표현을 YYYY-MM-DD로 바꾼다. 알 수 없으면 null.
   summary_ko는 한 문장. participants는 entity temp_id 목록. evidence는 원문 문장.
   사건에 주체와 대상이 있으면(예: 미국이 이란을 공습, 미국과 이란이 합의) 같은 사실을 관계로도(relations) 반드시 기록한다(ATTACKS, AGREES_WITH 등). 사건과 관계는 중복 기록해도 된다.
6. JSON 외의 설명·주석·마크다운을 출력하지 않는다.

출력 형식
{{"entities":[{{"temp_id":"e1","type":"PERSON","name_ko":"...","surface_forms":["..."],"affiliations":[{{"target_temp_id":"e2","role":"대통령"}}]}}],
 "relations":[{{"src_temp_id":"e1","dst_temp_id":"e3","type":"CRITICIZES","raw_label":"","evidence":"...","evidence_date":null}}],
 "events":[{{"date":"2026-06-25","summary_ko":"...","participants":["e1"],"location_temp_id":null,"evidence":"..."}}]}}

예시 입력(발행일 2026-06-27): "트럼프 미국 대통령은 26일 이란이 호르무즈 해협을 봉쇄한 것을 강하게 비난했다. 아락치 이란 외무장관은 '미국이 먼저 도발했다'고 반박했다. 오만이 양측 접촉을 주선했다."
예시 출력:
{{"entities":[
 {{"temp_id":"e1","type":"PERSON","name_ko":"도널드 트럼프","surface_forms":["트럼프 미국 대통령","트럼프"],"affiliations":[{{"target_temp_id":"e2","role":"대통령"}}]}},
 {{"temp_id":"e2","type":"COUNTRY","name_ko":"미국","surface_forms":["미국"],"affiliations":[]}},
 {{"temp_id":"e3","type":"COUNTRY","name_ko":"이란","surface_forms":["이란"],"affiliations":[]}},
 {{"temp_id":"e4","type":"PERSON","name_ko":"아바스 아락치","surface_forms":["아락치 이란 외무장관"],"affiliations":[{{"target_temp_id":"e3","role":"외무장관"}}]}},
 {{"temp_id":"e5","type":"LOCATION","name_ko":"호르무즈 해협","surface_forms":["호르무즈 해협"],"affiliations":[]}},
 {{"temp_id":"e6","type":"COUNTRY","name_ko":"오만","surface_forms":["오만"],"affiliations":[]}}],
 "relations":[
 {{"src_temp_id":"e1","dst_temp_id":"e3","type":"CRITICIZES","raw_label":"","evidence":"트럼프 미국 대통령은 26일 이란이 호르무즈 해협을 봉쇄한 것을 강하게 비난했다.","evidence_date":"2026-06-26"}},
 {{"src_temp_id":"e3","dst_temp_id":"e5","type":"SANCTIONS","raw_label":"봉쇄","evidence":"트럼프 미국 대통령은 26일 이란이 호르무즈 해협을 봉쇄한 것을 강하게 비난했다.","evidence_date":"2026-06-26"}},
 {{"src_temp_id":"e4","dst_temp_id":"e2","type":"REJECTS","raw_label":"","evidence":"아락치 이란 외무장관은 '미국이 먼저 도발했다'고 반박했다.","evidence_date":null}},
 {{"src_temp_id":"e6","dst_temp_id":"e2","type":"MEDIATES","raw_label":"","evidence":"오만이 양측 접촉을 주선했다.","evidence_date":null}},
 {{"src_temp_id":"e6","dst_temp_id":"e3","type":"MEDIATES","raw_label":"","evidence":"오만이 양측 접촉을 주선했다.","evidence_date":null}}],
 "events":[
 {{"date":"2026-06-26","summary_ko":"이란이 호르무즈 해협을 봉쇄했다.","participants":["e3"],"location_temp_id":"e5","evidence":"트럼프 미국 대통령은 26일 이란이 호르무즈 해협을 봉쇄한 것을 강하게 비난했다."}}]}}
"""


def build_user_message(doc: Doc, event_name: str, seeds: list[str]) -> str:
    hint = ", ".join(seeds) if seeds else "없음"
    return (
        f"사건: {event_name}\n힌트(알려진 인물·기관·국가): {hint}\n"
        f"발행일: {doc.writedate:%Y-%m-%d}\n언론사: {doc.press}\n\n제목: {doc.title}\n\n{doc.body}"
    )
