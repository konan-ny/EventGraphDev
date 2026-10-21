from eventgraph.batch.job import canonical_keys


def _row(typ, name, aliases=()):
    from eventgraph.batch.normalize import name_tokens, norm_key

    return (typ, norm_key(typ, name), [norm_key(typ, a) for a in aliases], name_tokens(name))


def test_single_token_surname_merges_into_unique_full_name():
    canon = canonical_keys([_row("PERSON", "도널드 트럼프"), _row("PERSON", "트럼프")])
    assert canon[("PERSON", "트럼프")] == "도널드트럼프"


def test_two_token_names_sharing_surname_stay_separate():
    canon = canonical_keys([_row("PERSON", "알리 하메네이"), _row("PERSON", "모즈타바 하메네이")])
    assert canon[("PERSON", "알리하메네이")] == "알리하메네이"
    assert canon[("PERSON", "모즈타바하메네이")] == "모즈타바하메네이"


def test_transliteration_variant_first_name_merges():
    canon = canonical_keys([_row("PERSON", "마르코 루비오"), _row("PERSON", "마코 루비오")])
    assert len({canon[("PERSON", "마르코루비오")], canon[("PERSON", "마코루비오")]}) == 1
    canon = canonical_keys([_row("PERSON", "재러드 쿠슈너"), _row("PERSON", "제러드 쿠슈너")])
    assert len({canon[("PERSON", "재러드쿠슈너")], canon[("PERSON", "제러드쿠슈너")]}) == 1


def test_spaced_surname_variant_merges():
    canon = canonical_keys(
        [_row("PERSON", "바드르 빈 하마드 알 부사이디"), _row("PERSON", "바드르 알부사이디")]
    )
    assert len(set(canon.values())) == 1


def test_alias_match_merges():
    canon = canonical_keys(
        [_row("PERSON", "도널드 트럼프", ["트럼프 대통령"]), _row("PERSON", "트럼프 대통령")]
    )
    assert canon[("PERSON", "트럼프")] == "도널드트럼프"


def test_org_untouched():
    canon = canonical_keys([_row("ORG", "혁명수비대"), _row("ORG", "이란 혁명수비대")])
    assert canon[("ORG", "혁명수비대")] == "혁명수비대"


def test_single_token_merges_when_longer_spellings_resolve_to_one():
    canon = canonical_keys(
        [
            _row("PERSON", "모하마드 바게르 갈리바프"),
            _row("PERSON", "모하메드 바게르 갈리바프"),
            _row("PERSON", "갈리바프"),
        ]
    )
    assert len(set(canon.values())) == 1


def test_honorific_prefixed_variant_merges():
    canon = canonical_keys(
        [_row("PERSON", "세예드 아바스 아라그치"), _row("PERSON", "압바스 아라그치")]
    )
    assert len(set(canon.values())) == 1


def test_title_suffixed_mention_merges_into_full_name():
    canon = canonical_keys([_row("PERSON", "도널드 트럼프"), _row("PERSON", "트럼프 대통령")])
    assert canon[("PERSON", "트럼프")] == "도널드트럼프"
