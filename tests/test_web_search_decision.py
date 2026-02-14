from winner_of_day.services.llm_service import LlmService


def test_web_search_disabled_for_non_venice_provider():
    enabled, sources, reason = LlmService.decide_chat_web_search(
        provider="groq",
        enabled=True,
        mode="smart",
        user_text="latest news",
    )
    assert enabled is False
    assert sources is False
    assert reason == "provider_not_venice"


def test_web_search_smart_mode_detects_freshness_and_sources():
    enabled, sources, reason = LlmService.decide_chat_web_search(
        provider="venice",
        enabled=True,
        mode="smart",
        user_text="дай latest новости и ссылки на источники",
    )
    assert enabled is True
    assert sources is True
    assert reason in {"smart_hit", "explicit"}


def test_web_search_explicit_mode_requires_trigger():
    enabled, _, reason = LlmService.decide_chat_web_search(
        provider="venice",
        enabled=True,
        mode="explicit",
        user_text="просто поболтаем",
    )
    assert enabled is False
    assert reason == "off"


def test_web_search_parameters_force_enable_and_news_source():
    params = LlmService.build_web_search_parameters(
        enabled=True,
        sources_requested=False,
        explicit_web_request=True,
        freshness_needed=True,
        source="auto",
        query_generation="auto",
        enable_scraping=False,
        default_citations=False,
    )
    assert params["enable_web_search"] is True
    assert params["search_source"] == "news"
    assert params["enable_search_query_generation"] is True
    assert params["enable_web_citations"] is True
