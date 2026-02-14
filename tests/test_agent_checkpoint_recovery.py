import pytest

from winner_of_day.agent.checkpoint import AgentCheckpoint


@pytest.mark.asyncio
async def test_checkpoint_file_is_reusable(tmp_path):
    db_path = tmp_path / "agent_checkpoint.sqlite3"

    first = AgentCheckpoint(str(db_path))
    saver = await first.start()
    assert saver is not None
    await first.stop()

    second = AgentCheckpoint(str(db_path))
    saver_again = await second.start()
    assert saver_again is not None
    await second.stop()

    assert db_path.exists()
