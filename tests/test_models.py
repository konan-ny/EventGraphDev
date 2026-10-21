from eventgraph.extract.models import RELATION_TYPES, Extraction


def _ents():
    return [
        {"temp_id": "e1", "type": "PERSON", "name_ko": "A"},
        {"temp_id": "e2", "type": "COUNTRY", "name_ko": "이란"},
    ]


def test_relation_types_match_schema_seed():
    assert len(RELATION_TYPES) == 16 and "OTHER" in RELATION_TYPES


def test_unknown_relation_type_becomes_other():
    x = Extraction.model_validate(
        {
            "entities": _ents(),
            "relations": [
                {
                    "src_temp_id": "e1",
                    "dst_temp_id": "e2",
                    "type": "비판",
                    "evidence": "A가 이란을 비판했다",
                }
            ],
            "events": [],
        }
    )
    assert x.relations[0].type == "OTHER" and x.relations[0].raw_label == "비판"


def test_dangling_relation_dropped_not_fatal():
    x = Extraction.model_validate(
        {
            "entities": [{"temp_id": "e1", "type": "PERSON", "name_ko": "A"}],
            "relations": [
                {"src_temp_id": "e1", "dst_temp_id": "zz", "type": "MEETS", "evidence": "x"}
            ],
            "events": [
                {"date": None, "summary_ko": "s", "participants": ["e1", "zz"], "evidence": "x"}
            ],
        }
    )
    assert x.relations == [] and x.dropped == 2  # 1 relation + 1 participant
    assert x.events[0].participants == ["e1"]


def test_unknown_entity_type_dropped_with_its_relations():
    x = Extraction.model_validate(
        {
            "entities": [
                {"temp_id": "e1", "type": "PERSON", "name_ko": "A"},
                {"temp_id": "e9", "type": "THING", "name_ko": "x"},
            ],
            "relations": [
                {"src_temp_id": "e1", "dst_temp_id": "e9", "type": "MEETS", "evidence": "x"}
            ],
            "events": [],
        }
    )
    assert [e.temp_id for e in x.entities] == ["e1"] and x.relations == [] and x.dropped == 1
