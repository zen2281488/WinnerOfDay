from __future__ import annotations

from winner_of_day.app_context import AppContext
from winner_of_day.config.settings import SettingsService
from winner_of_day.handlers import register_handlers
from winner_of_day.repositories import (
    DialogsRepo,
    GameRepo,
    MemoryRepo,
    MessagesRepo,
    ModerationRepo,
    ProfilesRepo,
    ScheduleRepo,
    SettingsRepo,
    SummaryRepo,
)
from winner_of_day.services import (
    AccessService,
    ChatbotService,
    ContextService,
    GameService,
    GuardService,
    LlmService,
    MaintenanceService,
    MemoryService,
    ReactionsService,
    SettingsRuntimeService,
    SummaryService,
)
from winner_of_day.state import RuntimeState


def _load_legacy_bot():
    from . import legacy_bot

    return legacy_bot


def _build_repositories() -> dict[str, object]:
    return {
        "messages": MessagesRepo(),
        "dialogs": DialogsRepo(),
        "settings": SettingsRepo(),
        "profiles": ProfilesRepo(),
        "summary": SummaryRepo(),
        "memory": MemoryRepo(),
        "game": GameRepo(),
        "schedule": ScheduleRepo(),
        "moderation": ModerationRepo(),
    }


def _build_services(settings_service: SettingsService, settings) -> dict[str, object]:
    return {
        "settings": SettingsRuntimeService(settings_service, settings),
        "access": AccessService(),
        "llm": LlmService(),
        "guard": GuardService(),
        "context": ContextService(),
        "summary": SummaryService(),
        "memory": MemoryService(),
        "game": GameService(),
        "reactions": ReactionsService(),
        "chatbot": ChatbotService(),
        "maintenance": MaintenanceService(),
    }


def create_app() -> AppContext:
    legacy_bot = _load_legacy_bot()
    settings_service = SettingsService()
    settings = settings_service.load_from_env()
    state = RuntimeState()
    repos = _build_repositories()
    services = _build_services(settings_service, settings)
    settings_service.bind_persist(getattr(legacy_bot, "set_bot_setting", None))
    ctx = AppContext(settings=settings, state=state, repos=repos, services=services, bot=legacy_bot.bot)
    register_handlers(legacy_bot.bot, ctx)
    if hasattr(legacy_bot, "set_app_context"):
        legacy_bot.set_app_context(ctx)
    return ctx


def run() -> None:
    legacy_bot = _load_legacy_bot()
    create_app()
    legacy_bot.run()
