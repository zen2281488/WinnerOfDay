from winner_of_day.config.settings import SettingsService


def test_settings_load_and_runtime_override(monkeypatch):
    monkeypatch.setenv("CHAT_MAX_TOKENS", "300")
    service = SettingsService()
    settings = service.load_from_env()

    assert settings.get("CHAT_MAX_TOKENS") == "300"

    service.set_runtime(settings, "CHAT_MAX_TOKENS", "700")
    assert settings.get("CHAT_MAX_TOKENS") == "700"


def test_apply_db_overrides(monkeypatch):
    monkeypatch.setenv("LLM_PROVIDER", "groq")
    service = SettingsService()
    settings = service.load_from_env()
    service.apply_db_overrides(settings, {"LLM_PROVIDER": "venice", "CHAT_MAX_TOKENS": "500"})

    assert settings.get("LLM_PROVIDER") == "venice"
    assert settings.get("CHAT_MAX_TOKENS") == "500"
