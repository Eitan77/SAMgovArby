import pytest
from unittest.mock import patch, MagicMock
from cage_resolver import CageResolver, is_valid_cage_code

def test_cage_validation():
    """Test CAGE code format validation."""
    assert is_valid_cage_code("12345") == True
    assert is_valid_cage_code("ABCDE") == True
    assert is_valid_cage_code("123") == False  # Too short
    assert is_valid_cage_code("123456") == False  # Too long
    assert is_valid_cage_code("") == False  # Empty
    assert is_valid_cage_code(None) == False  # None

def test_cage_resolver_valid_response():
    """Test CageResolver with valid GLEIF response."""
    resolver = CageResolver()

    mock_response = {
        "lei_records": [
            {
                "lei": "5493001KJTIIGC8Y1R12",
                "entity": {"registered_as": "ACME CORP"},
                "legalForm": {"code": "SM"}
            }
        ]
    }

    with patch("requests.get") as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = mock_response

        result = resolver.resolve_cage("12ABC")

        assert result["lei"] == "5493001KJTIIGC8Y1R12"
        assert result["confidence"] >= 0.8
        assert result["source"] == "gleif"

def test_cage_resolver_invalid_cage():
    """Test CageResolver with invalid CAGE code."""
    resolver = CageResolver()

    result = resolver.resolve_cage("INVALID")

    assert result["lei"] is None
    assert result["confidence"] == 0
    assert result["rejection_reason"] == "invalid_cage_format"

def test_cage_resolver_api_error():
    """Test CageResolver when GLEIF API fails."""
    resolver = CageResolver()

    with patch("requests.get") as mock_get:
        mock_get.side_effect = Exception("API Error")

        result = resolver.resolve_cage("99ZZZ")

        assert result["lei"] is None
        assert result["confidence"] == 0
        assert "api_error" in result["rejection_reason"]
