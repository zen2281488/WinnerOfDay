import httpx
import pytest

from winner_of_day.infra.venice_client import VeniceClient


class FakeResponse:
    def __init__(self, status_code: int, text: str = ""):
        self.status_code = status_code
        self.text = text


class FakeClient:
    def __init__(self, sequence):
        self.sequence = list(sequence)
        self.calls = 0

    async def request(self, method, path, headers=None, **kwargs):
        item = self.sequence[self.calls]
        self.calls += 1
        if isinstance(item, Exception):
            raise item
        return item


@pytest.mark.asyncio
async def test_venice_client_retries_on_5xx_then_succeeds():
    client = VeniceClient(base_url="https://api.venice.ai/api/v1/", api_key="x", retry_attempts=2, retry_backoff_seconds=0)
    fake = FakeClient([FakeResponse(500, "boom"), FakeResponse(200, "ok")])
    client._client = fake

    response = await client.request("POST", "chat/completions", json={"x": 1})

    assert response.status_code == 200
    assert fake.calls == 2


@pytest.mark.asyncio
async def test_venice_client_retries_on_request_error_then_succeeds():
    client = VeniceClient(base_url="https://api.venice.ai/api/v1/", api_key="x", retry_attempts=2, retry_backoff_seconds=0)
    req = httpx.Request("POST", "https://api.venice.ai/api/v1/chat/completions")
    fake = FakeClient([httpx.RequestError("net", request=req), FakeResponse(200, "ok")])
    client._client = fake

    response = await client.request("POST", "chat/completions", json={"x": 1})

    assert response.status_code == 200
    assert fake.calls == 2
