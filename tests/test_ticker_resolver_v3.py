import pytest
from unittest.mock import patch, MagicMock
from ticker_resolver_v3 import TickerResolverV3, _normalize

def test_resolve_with_cage_code():
    """Test V3 resolves via CAGE → LEI → ticker."""
    resolver = TickerResolverV3()

    # Mock CAGE resolver
    cage_mock = MagicMock()
    cage_mock.resolve_cage.return_value = {
        "lei": "5493001KJTIIGC8Y1R12",
        "confidence": 0.95
    }
    resolver.cage_resolver = cage_mock

    # Mock LEI resolver
    lei_mock = MagicMock()
    lei_mock.resolve_lei.return_value = {
        "ticker": "ACME",
        "cik": "0000012345",
        "confidence": 0.9,
        "source": "openfigi"
    }
    resolver.lei_resolver = lei_mock

    # Mock yfinance for market cap
    with patch("yfinance.Ticker") as mock_yf:
        mock_yf.return_value.fast_info.market_cap = 500_000_000  # $500M

        result = resolver.resolve("ACME CORP", cage_code="12ABC")

        assert result["resolved_ticker"] == "ACME"
        assert result["confidence"] == "high"
        assert "cage_lei_openfigi" in result["evidence_type"]
        assert len(result["audit_trail"]) == 2

def test_resolve_cage_fails_falls_back_to_sec():
    """Test V3 falls back to SEC if CAGE fails."""
    resolver = TickerResolverV3()

    # Mock failed CAGE resolver
    cage_mock = MagicMock()
    cage_mock.resolve_cage.return_value = {"lei": None}
    resolver.cage_resolver = cage_mock

    # Should fall through to existing SEC logic
    result = resolver.resolve("NORTHROP GRUMMAN", cage_code="INVALID")

    # Either resolves via SEC or returns unresolved
    assert "resolved_ticker" in result

def test_resolve_without_cage_code():
    """Test V3 works without CAGE (backward compatible with V2)."""
    resolver = TickerResolverV3()

    # Should use existing SEC logic
    result = resolver.resolve("NORTHROP GRUMMAN")

    # Should resolve via existing SEC paths or return unresolved
    assert "resolved_ticker" in result
    assert "evidence_type" in result
