"""
WHOOP REST API client.

Wraps the developer endpoints we consume:
  - GET /v1/cycle
  - GET /v1/recovery
  - GET /v1/activity/sleep
  - GET /v1/activity/workout
  - GET /v1/user/measurement/body

Handles bearer auth, pagination via ``nextToken``, and 429 rate-limit Retry-After.
Network failures are retried with exponential backoff.
"""

from __future__ import annotations

import os
import sys
import time
from typing import Iterator, Optional

import requests

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from config import settings  # noqa: E402
from data_generators.whoop_api.auth import get_access_token  # noqa: E402
from logger import get_logger  # noqa: E402
from utils.retry import retry  # noqa: E402

log = get_logger(__name__)


class WhoopApiError(RuntimeError):
    pass


class WhoopClient:
    def __init__(self, base_url: Optional[str] = None, page_size: int = 25):
        self.base_url = base_url or settings.whoop_api_base_url
        self.page_size = page_size

    def _headers(self) -> dict:
        return {
            "Authorization": f"Bearer {get_access_token()}",
            "Accept": "application/json",
        }

    @retry(max_retries=3, backoff_factor=2.0, exceptions=(requests.RequestException,))
    def _get(self, path: str, params: dict) -> dict:
        url = f"{self.base_url}{path}"
        response = requests.get(url, headers=self._headers(), params=params, timeout=30)
        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", "60"))
            log.warning(
                "WHOOP rate-limited; sleeping",
                extra={"extra_data": {"path": path, "retry_after_s": retry_after}},
            )
            time.sleep(retry_after)
            response = requests.get(url, headers=self._headers(), params=params, timeout=30)
        if response.status_code == 401:
            # Token may have expired between auth.get_access_token() and the call.
            response = requests.get(url, headers=self._headers(), params=params, timeout=30)
        response.raise_for_status()
        return response.json()

    def _paginate(self, path: str, start: str, end: str) -> Iterator[dict]:
        next_token: Optional[str] = None
        page = 0
        while True:
            params = {"start": start, "end": end, "limit": self.page_size}
            if next_token:
                params["nextToken"] = next_token
            payload = self._get(path, params)
            records = payload.get("records", [])
            for record in records:
                yield record
            next_token = payload.get("next_token") or payload.get("nextToken")
            page += 1
            if not next_token or not records:
                break

    # ── Endpoints (WHOOP API v2 — cycle/recovery/sleep/workout migrated 2025+) ─
    def list_cycles(self, start: str, end: str) -> Iterator[dict]:
        return self._paginate("/v2/cycle", start=start, end=end)

    def list_recovery(self, start: str, end: str) -> Iterator[dict]:
        return self._paginate("/v2/recovery", start=start, end=end)

    def list_sleep(self, start: str, end: str) -> Iterator[dict]:
        return self._paginate("/v2/activity/sleep", start=start, end=end)

    def list_workouts(self, start: str, end: str) -> Iterator[dict]:
        return self._paginate("/v2/activity/workout", start=start, end=end)

    @retry(max_retries=3, backoff_factor=2.0, exceptions=(requests.RequestException,))
    def get_body_measurement(self) -> dict:
        # User profile / body measurement endpoints stayed on v1.
        url = f"{self.base_url}/v1/user/measurement/body"
        response = requests.get(url, headers=self._headers(), timeout=30)
        response.raise_for_status()
        return response.json()
