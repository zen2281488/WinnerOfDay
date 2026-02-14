import asyncio

import pytest

from winner_of_day.services.game_service import GameService


@pytest.mark.asyncio
async def test_game_locking_allows_single_runner_per_peer():
    service = GameService()
    calls: list[str] = []

    async def long_runner():
        calls.append("started")
        await asyncio.sleep(0.05)
        calls.append("done")

    first = asyncio.create_task(service.run_with_peer_lock(100, long_runner))
    await asyncio.sleep(0.01)
    second = await service.run_with_peer_lock(100, long_runner)
    first_result = await first

    assert first_result is True
    assert second is False
    assert calls == ["started", "done"]
