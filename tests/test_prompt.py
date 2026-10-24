from eventgraph.extract.models import RELATION_TYPES
from eventgraph.extract.prompt import SYSTEM_PROMPT


def test_prompt_lists_every_relation_type_with_korean_gloss():
    for t in RELATION_TYPES - {"OTHER", "PARTICIPATES", "OCCURS_AT"}:
        assert f"{t}(" in SYSTEM_PROMPT, t  # e.g. WARNS(경고·위협)


def test_prompt_states_user_rules():
    assert (
        "언급된 국가" in SYSTEM_PROMPT and "놓치지" in SYSTEM_PROMPT
    )  # D-22-1: include mentioned countries
    assert "관계로도" in SYSTEM_PROMPT  # D-22-2: event subject/object also as a relation
    assert "한글" in SYSTEM_PROMPT and "영문" in SYSTEM_PROMPT  # Korean standard names only
