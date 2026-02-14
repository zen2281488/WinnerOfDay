from __future__ import annotations

import re


WEB_SEARCH_FRESHNESS_HINTS_RE = re.compile(
    r"(?i)\b("
    r"today|now|latest|recent|current|up[- ]?to[- ]?date|breaking|"
    r"price|rates?|exchange[- ]?rate|stock|market|weather|forecast|news|"
    r"release(?:d)?|version|patch|changelog|roadmap|schedule|result|score|"
    r"сегодня|сейчас|актуал|последн|свеж|новост|курс|цена|погода|"
    r"релиз|верси|патч|обновл|расписан|результат|счет|кто сейчас"
    r")\b"
)
WEB_SEARCH_EXPLICIT_HINTS_RE = re.compile(
    r"(?i)\b("
    r"check (?:the )?(?:internet|web)|search (?:the )?(?:internet|web)|google|look up|verify online|"
    r"проверь в (?:интернете|сети)|найди в (?:интернете|сети)|загугли|поищи в интернете|"
    r"проверь актуальн|сверь с источниками|проверь онлайн"
    r")\b"
)
WEB_SEARCH_SOURCES_HINTS_RE = re.compile(
    r"(?i)\b("
    r"source|sources|citation|citations|proof|references?|links?|url|urls?|"
    r"источник|источники|ссылка|ссылки|пруф|пруфы|докажи|подтверди ссылкой"
    r")\b"
)


class LlmService:
    @staticmethod
    def analyze_web_search_hints(user_text: str) -> tuple[bool, bool, bool]:
        text = str(user_text or "").strip()
        if not text:
            return False, False, False
        explicit_web_request = bool(WEB_SEARCH_EXPLICIT_HINTS_RE.search(text))
        freshness_needed = bool(WEB_SEARCH_FRESHNESS_HINTS_RE.search(text))
        sources_requested = bool(WEB_SEARCH_SOURCES_HINTS_RE.search(text))
        return explicit_web_request, freshness_needed, sources_requested

    @staticmethod
    def decide_chat_web_search(
        *,
        provider: str,
        enabled: bool,
        mode: str,
        user_text: str,
    ) -> tuple[bool, bool, str]:
        if provider != "venice":
            return False, False, "provider_not_venice"
        if not enabled:
            return False, False, "off"

        normalized_mode = str(mode or "smart").strip().lower()
        if normalized_mode == "off":
            return False, False, "off"

        explicit_web_request, freshness_needed, sources_requested = LlmService.analyze_web_search_hints(user_text)
        if normalized_mode == "always":
            return True, sources_requested, "smart_hit"
        if normalized_mode == "explicit":
            should_enable = explicit_web_request or sources_requested
            return should_enable, sources_requested, ("explicit" if should_enable else "off")
        if normalized_mode == "smart":
            should_enable = explicit_web_request or freshness_needed or sources_requested
            if explicit_web_request:
                return should_enable, sources_requested, "explicit"
            return should_enable, sources_requested, ("smart_hit" if should_enable else "off")
        return False, sources_requested, "off"
