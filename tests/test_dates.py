from datetime import date

import pytest

from eventgraph.extract.dates import resolve_date

B = date(2026, 6, 27)


@pytest.mark.parametrize(
    "t,exp",
    [
        ("오늘", B),
        ("어제", date(2026, 6, 26)),
        ("내일", date(2026, 6, 28)),
        ("지난 24일", date(2026, 6, 24)),
        ("24일", date(2026, 6, 24)),
        ("30일", date(2026, 5, 30)),
        ("6월 25일", date(2026, 6, 25)),
        ("2025년 1월 2일", date(2025, 1, 2)),
        ("2026-06-25", date(2026, 6, 25)),
        ("언젠가", None),
        ("", None),
    ],
)
def test_resolve_date(t, exp):
    assert resolve_date(t, B) == exp
