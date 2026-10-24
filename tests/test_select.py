from datetime import datetime
from types import SimpleNamespace

from eventgraph.select import expand_keywords, score_document
from eventgraph.source import Doc


def _fake_client(text: str):
    class Messages:
        @staticmethod
        def create(**kw):
            return SimpleNamespace(content=[SimpleNamespace(text=text)])

    return SimpleNamespace(messages=Messages())


def test_score_document_weights_title():
    d = Doc(
        "t",
        1,
        "이란 호르무즈 봉쇄",
        "이란이 호르무즈를 닫았다. 이란 외무부.",
        datetime(2026, 6, 25),
        "",
        "",
    )
    assert score_document(d, ["이란", "호르무즈"]) == 9  # title (1+1)*3=6, body 이란2+호르무즈1=3
    assert score_document(d, ["젤렌스키"]) == 0


def test_expand_keywords_falls_back_on_bad_json():
    assert expand_keywords(_fake_client("not json"), "m", "사건", ["씨앗"]) == [
        "사건",
        "씨앗",
    ]


def test_expand_keywords_parses_array_and_dedupes():
    out = expand_keywords(_fake_client('["이란", "호르무즈", "이란"]'), "m", "사건", ["이란"])
    assert out == ["사건", "이란", "호르무즈"]


def test_expand_keywords_survives_truncated_json():
    out = expand_keywords(
        _fake_client('```json\n["호르무즈 해협","호르무즈 봉쇄","호르무'), "m", "사건", []
    )
    assert out == ["사건", "호르무즈 해협", "호르무즈 봉쇄"]


def test_score_caps_repeated_body_hits_and_ignores_one_char_keywords():
    long_doc = Doc("t", 1, "경제 전망", "미국 " * 60, datetime(2026, 6, 25), "", "")
    short_doc = Doc(
        "t",
        2,
        "호르무즈 봉쇄",
        "이란이 호르무즈를 닫았다. 미국이 반발했다.",
        datetime(2026, 6, 25),
        "",
        "",
    )
    kws = ["미국", "이란", "호르무즈", "美"]
    assert score_document(long_doc, kws) == 5  # 60 hits of one keyword count as the cap (5)
    assert score_document(short_doc, kws) > score_document(long_doc, kws)
    assert (
        score_document(Doc("t", 3, "美 제재", "美 美 美", datetime(2026, 6, 25), "", ""), kws) == 0
    )
