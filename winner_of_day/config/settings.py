from __future__ import annotations

import os
from dataclasses import dataclass, field


@dataclass(slots=True)
class AppSettings:
    env: dict[str, str] = field(default_factory=dict)
    runtime_overrides: dict[str, str] = field(default_factory=dict)

    def get(self, key: str, default: str | None = None) -> str | None:
        if key in self.runtime_overrides:
            return self.runtime_overrides[key]
        return self.env.get(key, default)

    def as_dict(self) -> dict[str, str]:
        merged = dict(self.env)
        merged.update(self.runtime_overrides)
        return merged


class SettingsService:
    """Runtime settings facade with DB override support."""

    def __init__(self):
        self._persist_fn = None

    def bind_persist(self, persist_fn):
        self._persist_fn = persist_fn

    def load_from_env(self) -> AppSettings:
        return AppSettings(env={k: str(v) for k, v in os.environ.items() if v is not None})

    def apply_db_overrides(self, settings: AppSettings, overrides: dict[str, str]) -> AppSettings:
        for key, value in (overrides or {}).items():
            if key:
                settings.runtime_overrides[str(key)] = "" if value is None else str(value)
        return settings

    def set_runtime(self, settings: AppSettings, key: str, value: str) -> None:
        settings.runtime_overrides[str(key)] = "" if value is None else str(value)

    async def persist(self, key: str, value: str) -> None:
        if self._persist_fn is None:
            return
        await self._persist_fn(str(key), "" if value is None else str(value))
