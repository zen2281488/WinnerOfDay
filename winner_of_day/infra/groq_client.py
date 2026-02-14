from __future__ import annotations

try:
    from groq import AsyncGroq
except ImportError:  # pragma: no cover
    AsyncGroq = None


class GroqClientManager:
    def __init__(self, api_key: str | None = None):
        self.api_key = api_key
        self._client = AsyncGroq(api_key=api_key) if (api_key and AsyncGroq) else None

    @property
    def client(self):
        return self._client

    def reconfigure(self, api_key: str | None) -> None:
        self.api_key = api_key
        self._client = AsyncGroq(api_key=api_key) if (api_key and AsyncGroq) else None
