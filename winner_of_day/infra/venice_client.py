from __future__ import annotations

import asyncio

import httpx


class VeniceClient:
    def __init__(
        self,
        *,
        base_url: str,
        api_key: str | None,
        timeout_seconds: float = 90.0,
        retry_attempts: int = 2,
        retry_backoff_seconds: float = 1.0,
    ):
        self.base_url = base_url if base_url.endswith("/") else base_url + "/"
        self.api_key = api_key
        self.timeout_seconds = float(timeout_seconds or 90.0)
        self.retry_attempts = int(retry_attempts or 0)
        self.retry_backoff_seconds = float(retry_backoff_seconds or 0.0)
        self._client: httpx.AsyncClient | None = None

    async def start(self) -> None:
        if self._client is None:
            timeout = httpx.Timeout(self.timeout_seconds, connect=min(10.0, self.timeout_seconds))
            self._client = httpx.AsyncClient(base_url=self.base_url, timeout=timeout)

    async def stop(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    def reconfigure(
        self,
        *,
        base_url: str | None = None,
        api_key: str | None = None,
        timeout_seconds: float | None = None,
        retry_attempts: int | None = None,
        retry_backoff_seconds: float | None = None,
    ) -> None:
        if base_url:
            self.base_url = base_url if base_url.endswith("/") else base_url + "/"
        if api_key is not None:
            self.api_key = api_key
        if timeout_seconds is not None:
            self.timeout_seconds = float(timeout_seconds)
        if retry_attempts is not None:
            self.retry_attempts = int(retry_attempts)
        if retry_backoff_seconds is not None:
            self.retry_backoff_seconds = float(retry_backoff_seconds)

    def _auth_headers(self) -> dict[str, str]:
        if not self.api_key:
            return {}
        return {"Authorization": f"Bearer {self.api_key}"}

    async def request(self, method: str, path: str, **kwargs) -> httpx.Response:
        if self._client is None:
            await self.start()
        assert self._client is not None

        headers = kwargs.pop("headers", {})
        request_headers = {**self._auth_headers(), **headers}

        attempts = max(1, self.retry_attempts + 1)
        backoff = self.retry_backoff_seconds
        last_exc: Exception | None = None

        for attempt in range(attempts):
            try:
                response = await self._client.request(method, path, headers=request_headers, **kwargs)
                if response.status_code in (408, 429, 500, 502, 503, 504) and attempt + 1 < attempts:
                    delay = backoff * (2**attempt) if backoff > 0 else 0
                    if delay > 0:
                        await asyncio.sleep(delay)
                    continue
                if response.status_code >= 400:
                    message = response.text.strip()
                    if len(message) > 500:
                        message = message[:500] + "..."
                    raise RuntimeError(f"HTTP {response.status_code}: {message}")
                return response
            except httpx.RequestError as e:
                last_exc = e
                if attempt + 1 >= attempts:
                    raise
                delay = backoff * (2**attempt) if backoff > 0 else 0
                if delay > 0:
                    await asyncio.sleep(delay)

        if last_exc is not None:
            raise last_exc
        raise RuntimeError("Venice request failed")
