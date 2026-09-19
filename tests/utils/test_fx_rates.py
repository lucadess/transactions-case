from unittest.mock import MagicMock, patch

import pytest
import requests

from gold.fx_rates import fetch_eur_rates


def _mock_response(status_code: int, payload: dict = None) -> MagicMock:
    response = MagicMock(spec=requests.Response)
    response.status_code = status_code
    response.json.return_value = payload or {}
    if status_code >= 400:
        response.raise_for_status.side_effect = requests.HTTPError(f"{status_code} error")
    else:
        response.raise_for_status.side_effect = None
    return response


@patch("gold.fx_rates.requests.get")
def test_fetch_eur_rates_inverts_eur_to_x_into_x_to_eur(mock_get):
    mock_get.return_value = _mock_response(200, {"base": "EUR", "rates": {"USD": 2.0, "GBP": 4.0}})

    rates = fetch_eur_rates()

    assert rates["USD"] == 0.5
    assert rates["GBP"] == 0.25
    mock_get.assert_called_once_with("https://api.frankfurter.app/latest", params={"from": "EUR"})


@patch("gold.fx_rates.requests.get")
def test_fetch_eur_rates_includes_eur_as_one(mock_get):
    mock_get.return_value = _mock_response(200, {"base": "EUR", "rates": {"USD": 2.0}})

    rates = fetch_eur_rates()

    assert rates["EUR"] == 1.0


@patch("gold.fx_rates.requests.get")
def test_fetch_eur_rates_raises_on_error_response(mock_get):
    mock_get.return_value = _mock_response(500)

    with pytest.raises(requests.HTTPError):
        fetch_eur_rates()
