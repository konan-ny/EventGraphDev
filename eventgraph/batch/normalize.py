"""Entity normalization key used to merge surface-form variants. Pure python (Spark UDF)."""

from __future__ import annotations

import re

# longest first so "국무총리" wins over "총리"
TITLE_SUFFIXES: tuple[str, ...] = tuple(
    sorted(
        {
            "대통령",
            "국무총리",
            "총리",
            "부총리",
            "장관",
            "외무장관",
            "외교장관",
            "국방장관",
            "차관",
            "의원",
            "대표",
            "원내대표",
            "위원장",
            "사령관",
            "대변인",
            "총장",
            "지사",
            "시장",
            "회장",
            "사장",
            "감독",
            "선수",
            "기자",
            "씨",
            "총재",
            "장군",
            "대사",
            "총통",
            "주석",
            "서기장",
            "왕세자",
            "국왕",
        },
        key=len,
        reverse=True,
    )
)

_PAREN = re.compile(r"\([^)]*\)|（[^）]*）|\[[^\]]*\]")
_SPACE = re.compile(r"\s+")


def name_tokens(name_ko: str) -> list[str]:
    """Whitespace tokens minus title tokens ('트럼프 대통령' -> ['트럼프']); never empty."""
    toks = (name_ko or "").split()
    kept = [t for t in toks if t not in TITLE_SUFFIXES]
    return kept or toks


def norm_key(type: str, name_ko: str) -> str:
    base = _SPACE.sub("", _PAREN.sub("", name_ko or "")).lower()
    if type != "PERSON":
        return base
    for suf in TITLE_SUFFIXES:
        if base.endswith(suf) and len(base) > len(suf):
            return base[: -len(suf)]
    return base
