from datetime import date

import pytest

from eventgraph.source import clean_body, fetch_candidates, press_from_url


def test_clean_body_strips_html_and_broadcast_cues():
    raw = "<앵커>\n요즘 <b>불방망이</b>를<br/>휘두르는 <기자>\n배정훈 기자입니다."
    out = clean_body(raw)
    assert "<" not in out and "앵커" not in out and "기자>" not in out
    assert "불방망이를" in out and "배정훈 기자입니다." in out


def test_clean_body_collapses_whitespace():
    assert clean_body("가  나\n\n\n다   ") == "가 나\n다"


def test_press_from_url():
    assert press_from_url("https://news.sbs.co.kr/news/endPage.do?x=1") == "sbs.co.kr"
    assert press_from_url("https://www.yna.co.kr/view/AKR1") == "yna.co.kr"
    assert press_from_url("http://m.khan.co.kr/a") == "khan.co.kr"
    assert press_from_url("") == ""


@pytest.mark.integration
def test_fetch_candidates_excludes_bad_rows(settings):
    docs, stats = fetch_candidates(settings, date(2026, 6, 24), date(2026, 6, 24))
    assert stats["total"] == len(docs) > 0
    assert {"excluded_bad_date", "excluded_empty_body"} <= stats.keys()
    assert all(d.writedate.year == 2026 and d.body and d.press for d in docs[:50])
    assert all(d.writedate.date() == date(2026, 6, 24) for d in docs)
