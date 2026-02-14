from __future__ import annotations

from . import admin_handlers, chatbot_handlers, game_handlers, logger_handler, reaction_handlers, settings_handlers


def register_handlers(bot, ctx) -> None:
    admin_handlers.register(bot, ctx)
    game_handlers.register(bot, ctx)
    settings_handlers.register(bot, ctx)
    chatbot_handlers.register(bot, ctx)
    reaction_handlers.register(bot, ctx)
    logger_handler.register(bot, ctx)
