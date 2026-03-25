from __future__ import annotations

from dataclasses import dataclass
from typing import Any
import json
import os
import time
from urllib import error, request


class EastmoneyError(RuntimeError):
    """Raised when Eastmoney returns an error or an unexpected payload."""


@dataclass(slots=True)
class EastmoneyClient:
    api_key: str
    timeout: int = 25
    max_retries: int = 3
    retry_delay: float = 1.5

    @classmethod
    def from_env(cls) -> "EastmoneyClient":
        api_key = os.getenv("EASTMONEY_APIKEY")
        if not api_key:
            raise EastmoneyError(
                "Missing EASTMONEY_APIKEY. Please set the environment variable before running the strategy."
            )
        return cls(api_key=api_key)

    def search_news(self, query: str) -> dict[str, Any]:
        payload = self._post(
            "https://mkapi2.dfcfs.com/finskillshub/api/claw/news-search",
            {"query": query},
        )
        return (((payload.get("data") or {}).get("data") or {}).get("llmSearchResponse")) or {}

    def query_data(self, tool_query: str) -> dict[str, Any]:
        payload = self._post(
            "https://mkapi2.dfcfs.com/finskillshub/api/claw/query",
            {"toolQuery": tool_query},
        )
        return (((payload.get("data") or {}).get("data") or {}).get("searchDataResultDTO")) or {}

    def _post(self, url: str, body: dict[str, Any]) -> dict[str, Any]:
        last_payload: dict[str, Any] | None = None
        for attempt in range(self.max_retries + 1):
            data = json.dumps(body).encode("utf-8")
            req = request.Request(
                url=url,
                data=data,
                method="POST",
                headers={
                    "Content-Type": "application/json",
                    "apikey": self.api_key,
                },
            )
            try:
                with request.urlopen(req, timeout=self.timeout) as resp:
                    payload = json.loads(resp.read().decode("utf-8"))
            except error.HTTPError as exc:
                detail = exc.read().decode("utf-8", errors="replace")
                raise EastmoneyError(f"Eastmoney HTTP {exc.code}: {detail}") from exc
            except error.URLError as exc:
                raise EastmoneyError(f"Eastmoney request failed: {exc}") from exc
            except TimeoutError as exc:
                if attempt >= self.max_retries:
                    raise EastmoneyError(f"Eastmoney request timed out: {exc}") from exc
                time.sleep(self.retry_delay * (attempt + 1))
                continue

            if payload.get("status") == 0:
                return payload

            last_payload = payload
            if payload.get("status") != 112 or attempt >= self.max_retries:
                break
            time.sleep(self.retry_delay * (attempt + 1))

        raise EastmoneyError(str(last_payload))
