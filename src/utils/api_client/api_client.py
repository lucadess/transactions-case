import logging
import time
from typing import Optional

import requests

logger = logging.getLogger(__name__)


class APIClient:
    def __init__(self, base_url: str, api_key: str, max_requests_per_minute: int):
        self.base_url = base_url
        self.api_key = api_key
        self.max_requests_per_minute = max_requests_per_minute
        self._min_interval = 60.0 / max_requests_per_minute
        self._last_request_at: Optional[float] = None

    def fetch_page(self, endpoint: str, limit: int, offset: int) -> dict:
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        params = {"limit": limit, "offset": offset}
        headers = {"X-API-Key": self.api_key}

        max_retries = 3
        backoff_seconds = 1.0

        for attempt in range(max_retries + 1):
            self._throttle()
            response = requests.get(url, params=params, headers=headers)

            is_retryable = response.status_code == 429 or response.status_code >= 500
            if is_retryable and attempt < max_retries:
                logger.warning(
                    "Retryable status %s from %s (attempt %d/%d), backing off %.1fs",
                    response.status_code, url, attempt + 1, max_retries + 1, backoff_seconds,
                )
                time.sleep(backoff_seconds)
                backoff_seconds *= 2
                continue

            response.raise_for_status()
            return response.json()

    def _throttle(self) -> None:
        if self._last_request_at is not None:
            elapsed = time.monotonic() - self._last_request_at
            remaining = self._min_interval - elapsed
            if remaining > 0:
                time.sleep(remaining)
        self._last_request_at = time.monotonic()
