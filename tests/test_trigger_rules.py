from types import SimpleNamespace

import pytest

pytest.importorskip("vkbottle")

from winner_of_day.core.rules import (
    ChatbotTriggerRule,
    EqualsRule,
    StartswithRule,
    configure_bot_group_id_provider,
    is_chatbot_trigger_message,
    strip_bot_mention,
)


@pytest.fixture(autouse=True)
def setup_group_id():
    configure_bot_group_id_provider(lambda: 123)


@pytest.mark.asyncio
async def test_rules_basic_checks():
    startswith = StartswithRule("/кто")
    equals = EqualsRule("/кто")
    msg = SimpleNamespace(text="/кто", from_id=1, peer_id=2, reply_message=None)
    assert await startswith.check(msg) is True
    assert await equals.check(msg) is True


@pytest.mark.asyncio
async def test_strip_bot_mention_and_trigger():
    text = "[club123|bot] привет"
    assert strip_bot_mention(text) == "привет"

    msg = SimpleNamespace(
        text="[club123|bot] привет",
        from_id=10,
        peer_id=2000000001,
        reply_message=None,
    )
    assert is_chatbot_trigger_message(msg, admin_user_id=None) is True
    trigger_rule = ChatbotTriggerRule(admin_user_id=None)
    assert await trigger_rule.check(msg) is True


@pytest.mark.asyncio
async def test_trigger_ignores_commands_with_mention():
    msg = SimpleNamespace(
        text="[club123|bot] /кто",
        from_id=10,
        peer_id=2000000001,
        reply_message=None,
    )
    assert is_chatbot_trigger_message(msg, admin_user_id=None) is False
