import pytest
from src.fetch import fetch_breweries

def test_fetch_returns_list():
    data = fetch_breweries()
    assert isinstance(data, list)
    if data:
        assert isinstance(data[0], dict)
        assert 'id' in data[0]