import pytest
from unittest.mock import patch
from lei_resolver import LeiResolver, is_valid_lei

def test_lei_validation():
    """Test LEI format validation (20 alphanumeric)."""
    assert is_valid_lei("5493001KJTIIGC8Y1R12") == True
    assert is_valid_lei("5493001KJTIIGC8Y1R1") == False  # 19 chars
    assert is_valid_lei("5493001KJTIIGC8Y1R122") == False  # 21 chars
    assert is_valid_lei("") == False
    assert is_valid_lei(None) == False

def test_lei_resolver_openfigi_success():
    """Test LeiResolver with valid OpenFIGI response."""
    resolver = LeiResolver()

    openfigi_response = [
        {
            "data": [
                {
                    "figi": "BBG000B9XRY4",
                    "name": "ACME CORP",
                    "ticker": "ACME",
                    "exchCode": "US"
                }
            ]
        }
    ]

    with patch("requests.post") as mock_post:
        mock_post.return_value.status_code = 200
        mock_post.return_value.json.return_value = openfigi_response

        result = resolver.resolve_lei("5493001KJTIIGC8Y1R12")

        assert result["ticker"] == "ACME"
        assert result["confidence"] >= 0.85
        assert result["source"] == "openfigi"

def test_lei_resolver_invalid_lei():
    """Test LeiResolver with invalid LEI."""
    resolver = LeiResolver()

    result = resolver.resolve_lei("INVALID_LEI")

    assert result["ticker"] is None
    assert result["confidence"] == 0
    assert result["rejection_reason"] == "invalid_lei_format"

def test_lei_resolver_no_ticker_found():
    """Test LeiResolver when OpenFIGI finds no ticker."""
    resolver = LeiResolver()

    with patch("requests.post") as mock_post:
        mock_post.return_value.status_code = 200
        mock_post.return_value.json.return_value = [{"data": []}]  # No results

        result = resolver.resolve_lei("9999999999999999ZZZ9")

        assert result["ticker"] is None
        assert result["confidence"] == 0
        assert "not_found" in result["rejection_reason"]
