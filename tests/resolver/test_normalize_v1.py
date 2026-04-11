"""tests/resolver/test_normalize_v1.py"""
from resolver.normalize import conservative_normalize, aggressive_normalize, token_metadata


def test_conservative_strips_periods():
    assert conservative_normalize("I.B.M.") == "IBM"


def test_conservative_ampersand():
    assert conservative_normalize("AT&T Inc.") == "AT AND T INC"


def test_conservative_unicode():
    result = conservative_normalize("Lockhéed Martin")
    assert result == "LOCKHEED MARTIN"


def test_aggressive_removes_corp_suffix():
    assert aggressive_normalize("Raytheon Technologies Corporation") == "RAYTHEON TECHNOLOGIES"


def test_aggressive_removes_the():
    assert aggressive_normalize("The Boeing Company") == "BOEING"


def test_aggressive_token_sorted():
    a = aggressive_normalize("Technologies Raytheon")
    b = aggressive_normalize("Raytheon Technologies")
    assert a == b


def test_aggressive_single_meaningful_word():
    result = aggressive_normalize("Holdings LLC")
    assert result is not None and len(result) > 0


def test_token_metadata_bigrams():
    meta = token_metadata("Science Applications International")
    assert meta["token_count"] >= 2
    assert len(meta["bigrams"]) >= 1


def test_token_metadata_empty():
    meta = token_metadata(None)
    assert meta["tokens"] == []
    assert meta["bigrams"] == []
    assert meta["token_count"] == 0
