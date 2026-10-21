"""Structured extraction schema. Validation repairs instead of rejecting a document."""

from __future__ import annotations

import datetime as dt
import logging

from pydantic import BaseModel, Field, model_validator

log = logging.getLogger(__name__)

ENTITY_TYPES = frozenset({"PERSON", "ORG", "LOCATION", "COUNTRY"})
RELATION_TYPES = frozenset(
    {
        "MEMBER_OF",
        "LEADS",
        "REPRESENTS",
        "MEETS",
        "CRITICIZES",
        "SUPPORTS",
        "NEGOTIATES_WITH",
        "ORDERS",
        "SUCCEEDS",
        "ALLIES_WITH",
        "SANCTIONS",
        "ATTACKS",
        "AGREES_WITH",
        "PARTICIPATES",
        "OCCURS_AT",
        "OTHER",
    }
)


class Affiliation(BaseModel):
    target_temp_id: str
    role: str = ""


class Entity(BaseModel):
    temp_id: str
    type: str
    name_ko: str
    surface_forms: list[str] = Field(default_factory=list)
    affiliations: list[Affiliation] = Field(default_factory=list)


class Relation(BaseModel):
    src_temp_id: str
    dst_temp_id: str
    type: str
    raw_label: str = ""
    evidence: str
    evidence_date: dt.date | None = None


class Event(BaseModel):
    date: dt.date | None = None  # field name shadows the type, hence dt.date
    summary_ko: str
    participants: list[str] = Field(default_factory=list)
    location_temp_id: str | None = None
    evidence: str


class Extraction(BaseModel):
    entities: list[Entity] = Field(default_factory=list)
    relations: list[Relation] = Field(default_factory=list)
    events: list[Event] = Field(default_factory=list)
    dropped: int = 0  # relations/participants removed during repair

    @model_validator(mode="after")
    def _repair(self) -> Extraction:
        bad_type = [e.temp_id for e in self.entities if e.type not in ENTITY_TYPES]
        if bad_type:
            log.warning("dropping entities with unknown type: %s", bad_type)
            self.entities = [e for e in self.entities if e.type in ENTITY_TYPES]
        ids = {e.temp_id for e in self.entities}
        for e in self.entities:
            e.affiliations = [a for a in e.affiliations if a.target_temp_id in ids]
        kept = []
        for r in self.relations:
            if r.src_temp_id not in ids or r.dst_temp_id not in ids:
                self.dropped += 1
                continue
            if r.type not in RELATION_TYPES:
                r.raw_label = r.raw_label or r.type
                r.type = "OTHER"
            kept.append(r)
        self.relations = kept
        for ev in self.events:
            n = len(ev.participants)
            ev.participants = [p for p in ev.participants if p in ids]
            self.dropped += n - len(ev.participants)
            if ev.location_temp_id not in ids:
                ev.location_temp_id = None
        if self.dropped:
            log.warning("extraction repair dropped %d dangling references", self.dropped)
        return self


def _obj(props: dict, required: list[str] | None = None) -> dict:
    return {
        "type": "object",
        "properties": props,
        "required": required or list(props),
        "additionalProperties": False,
    }


_STR = {"type": "string"}
_NSTR = {"type": ["string", "null"]}
_STRS = {"type": "array", "items": _STR}

# Strict JSON schema for output_config.format: every key required, no extras.
OUTPUT_SCHEMA = _obj(
    {
        "entities": {
            "type": "array",
            "items": _obj(
                {
                    "temp_id": _STR,
                    "type": {"type": "string", "enum": sorted(ENTITY_TYPES)},
                    "name_ko": _STR,
                    "surface_forms": _STRS,
                    "affiliations": {
                        "type": "array",
                        "items": _obj({"target_temp_id": _STR, "role": _STR}),
                    },
                }
            ),
        },
        "relations": {
            "type": "array",
            "items": _obj(
                {
                    "src_temp_id": _STR,
                    "dst_temp_id": _STR,
                    "type": _STR,
                    "raw_label": _STR,
                    "evidence": _STR,
                    "evidence_date": _NSTR,
                }
            ),
        },
        "events": {
            "type": "array",
            "items": _obj(
                {
                    "date": _NSTR,
                    "summary_ko": _STR,
                    "participants": _STRS,
                    "location_temp_id": _NSTR,
                    "evidence": _STR,
                }
            ),
        },
    }
)
