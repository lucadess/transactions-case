import requests

FRANKFURTER_URL = "https://api.frankfurter.app/latest"


def fetch_eur_rates() -> dict:
    response = requests.get(FRANKFURTER_URL, params={"from": "EUR"})
    response.raise_for_status()

    rates = {currency: 1 / rate for currency, rate in response.json()["rates"].items()}
    rates["EUR"] = 1.0
    return rates
