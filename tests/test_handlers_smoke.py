import pytest

from winner_of_day.app_context import AppContext
from winner_of_day.handlers.register import register_handlers


def test_register_handlers_smoke():
    class DummyBot:
        pass

    ctx = AppContext(settings={}, state={})
    register_handlers(DummyBot(), ctx)


def test_create_app_smoke():
    pytest.importorskip("aiosqlite")
    pytest.importorskip("vkbottle")
    from winner_of_day.main import create_app

    app = create_app()
    assert isinstance(app, AppContext)
    assert app.bot is not None
    assert "agent_runtime" in app.services
