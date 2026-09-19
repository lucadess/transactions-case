from itertools import count
from unittest.mock import MagicMock, patch

import pytest
import requests

from utils.api_client import APIClient


def _mock_response(status_code: int, payload: dict = None) -> MagicMock:
    response = MagicMock(spec=requests.Response)
    response.status_code = status_code
    response.json.return_value = payload or {}
    if status_code >= 400:
        response.raise_for_status.side_effect = requests.HTTPError(f"{status_code} error")
    else:
        response.raise_for_status.side_effect = None
    return response


@patch("utils.api_client.api_client.time.sleep", return_value=None)
@patch("utils.api_client.api_client.requests.get")
def test_fetch_page_returns_parsed_json(mock_get, mock_sleep):
    payload = {"items": [{"id": 1}], "limit": 1000, "offset": 0, "total": 1}
    mock_get.return_value = _mock_response(200, payload)

    client = APIClient(base_url="https://api.example.com", api_key="secret", max_requests_per_minute=600)
    result = client.fetch_page("/v1/customers", limit=1000, offset=0)

    assert result == payload
    mock_get.assert_called_once_with(
        "https://api.example.com/v1/customers",
        params={"limit": 1000, "offset": 0},
        headers={"X-API-Key": "secret"},
    )


@patch("utils.api_client.api_client.time.monotonic", side_effect=count(0, 100))
@patch("utils.api_client.api_client.time.sleep", return_value=None)
@patch("utils.api_client.api_client.requests.get")
def test_retries_on_429_then_succeeds(mock_get, mock_sleep, mock_monotonic):
    payload = {"items": [], "limit": 1000, "offset": 0, "total": 0}
    mock_get.side_effect = [_mock_response(429), _mock_response(200, payload)]

    client = APIClient(base_url="https://api.example.com", api_key="secret", max_requests_per_minute=600)
    result = client.fetch_page("/v1/customers", limit=1000, offset=0)

    assert result == payload
    assert mock_get.call_count == 2
    mock_sleep.assert_called_once_with(1.0)


@patch("utils.api_client.api_client.time.sleep", return_value=None)
@patch("utils.api_client.api_client.requests.get")
def test_retries_on_500_then_succeeds(mock_get, mock_sleep):
    payload = {"items": [], "limit": 1000, "offset": 0, "total": 0}
    mock_get.side_effect = [_mock_response(503), _mock_response(200, payload)]

    client = APIClient(base_url="https://api.example.com", api_key="secret", max_requests_per_minute=600)
    result = client.fetch_page("/v1/customers", limit=1000, offset=0)

    assert result == payload
    assert mock_get.call_count == 2


@patch("utils.api_client.api_client.time.monotonic", side_effect=count(0, 100))
@patch("utils.api_client.api_client.time.sleep", return_value=None)
@patch("utils.api_client.api_client.requests.get")
def test_exponential_backoff_between_retries(mock_get, mock_sleep, mock_monotonic):
    mock_get.side_effect = [_mock_response(429), _mock_response(429), _mock_response(429), _mock_response(429)]

    client = APIClient(base_url="https://api.example.com", api_key="secret", max_requests_per_minute=600)
    with pytest.raises(requests.HTTPError):
        client.fetch_page("/v1/customers", limit=1000, offset=0)

    sleep_calls = [call.args[0] for call in mock_sleep.call_args_list]
    assert sleep_calls == [1.0, 2.0, 4.0]


@patch("utils.api_client.api_client.time.sleep", return_value=None)
@patch("utils.api_client.api_client.requests.get")
def test_gives_up_after_max_retries(mock_get, mock_sleep):
    mock_get.side_effect = [
        _mock_response(500),
        _mock_response(500),
        _mock_response(500),
        _mock_response(500),
    ]

    client = APIClient(base_url="https://api.example.com", api_key="secret", max_requests_per_minute=600)
    with pytest.raises(requests.HTTPError):
        client.fetch_page("/v1/customers", limit=1000, offset=0)

    assert mock_get.call_count == 4


@patch("utils.api_client.api_client.time.sleep", return_value=None)
@patch("utils.api_client.api_client.requests.get")
def test_non_retryable_error_raises_immediately(mock_get, mock_sleep):
    mock_get.return_value = _mock_response(404)

    client = APIClient(base_url="https://api.example.com", api_key="secret", max_requests_per_minute=600)
    with pytest.raises(requests.HTTPError):
        client.fetch_page("/v1/customers", limit=1000, offset=0)

    assert mock_get.call_count == 1
    mock_sleep.assert_not_called()


@patch("utils.api_client.api_client.time.monotonic")
@patch("utils.api_client.api_client.time.sleep", return_value=None)
@patch("utils.api_client.api_client.requests.get")
def test_throttles_between_requests(mock_get, mock_sleep, mock_monotonic):
    payload = {"items": [], "limit": 1000, "offset": 0, "total": 0}
    mock_get.return_value = _mock_response(200, payload)

    # First call starts at t=0, second call happens instantly at t=0 too,
    # so the client should sleep for the full min interval before it fires.
    mock_monotonic.side_effect = [0.0, 0.0, 0.0]

    client = APIClient(base_url="https://api.example.com", api_key="secret", max_requests_per_minute=60)
    client.fetch_page("/v1/customers", limit=1000, offset=0)
    client.fetch_page("/v1/customers", limit=1000, offset=1000)

    mock_sleep.assert_called_once_with(1.0)
