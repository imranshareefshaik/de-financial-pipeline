from unittest.mock import Mock, patch
import pytest
import requests
from src.ingestion.api_client import APIClient


@patch("src.ingestion.api_client.requests.get")
def test_get_market_data_success(mock_get):
    mock_response = Mock()
    mock_response.json.return_value = [{"id": "bitcoin", "current_price": 50000}]
    mock_get.return_value = mock_response

    client = APIClient()
    data = client.get_market_data()
    assert data == [{"id": "bitcoin", "current_price": 50000}]


@patch("src.ingestion.api_client.requests.get")
def test_get_market_data_http_error(mock_get):
    mock_response = Mock()
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("503 Service Unavailable")
    mock_get.return_value = mock_response

    with pytest.raises(RuntimeError, match="Failed to fetch data"):
        APIClient().get_market_data("AAPL")