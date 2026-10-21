import pytest

from eventgraph.batch.normalize import norm_key


@pytest.mark.parametrize(
    "t,n,exp",
    [
        ("PERSON", "도널드 트럼프", "도널드트럼프"),
        ("PERSON", "트럼프 대통령", "트럼프"),
        ("PERSON", "트럼프", "트럼프"),
        ("PERSON", "아바스 아락치 외무장관", "아바스아락치"),
        ("PERSON", "김민석 국무총리", "김민석"),
        ("PERSON", "Donald Trump", "donaldtrump"),
        ("ORG", "이란 혁명수비대(IRGC)", "이란혁명수비대"),
        ("ORG", "미국 국무장관", "미국국무장관"),  # suffix stripping is PERSON-only
        ("COUNTRY", "미국", "미국"),
        ("PERSON", "대통령", "대통령"),  # never strip to empty
    ],
)
def test_norm_key(t, n, exp):
    assert norm_key(t, n) == exp
