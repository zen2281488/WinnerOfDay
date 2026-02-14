import pytest

from winner_of_day.services.context_service import MessageDeduplicator


@pytest.mark.asyncio
async def test_message_dedup_works_for_same_key():
    dedup = MessageDeduplicator(max_size=10)
    assert await dedup.try_mark(2000000001, 15) is True
    assert await dedup.try_mark(2000000001, 15) is False

    await dedup.release(2000000001, 15)
    assert await dedup.try_mark(2000000001, 15) is True
