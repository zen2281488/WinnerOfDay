from __future__ import annotations

from winner_of_day.config.settings import AppSettings, SettingsService


class SettingsRuntimeService:
    def __init__(self, settings_service: SettingsService, settings: AppSettings):
        self.settings_service = settings_service
        self.settings = settings

    def apply_db_overrides(self, overrides: dict[str, str]) -> None:
        self.settings_service.apply_db_overrides(self.settings, overrides)

    def set_runtime(self, key: str, value: str) -> None:
        self.settings_service.set_runtime(self.settings, key, value)

    async def persist(self, key: str, value: str) -> None:
        await self.settings_service.persist(key, value)
