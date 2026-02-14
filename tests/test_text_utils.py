from winner_of_day.core.text import (
    merge_continuation_text,
    split_text_for_sending,
    strip_reasoning_leak,
    trim_text,
    trim_text_middle,
    trim_text_tail,
)


def test_trim_helpers():
    assert trim_text("  abc  ", 10) == "abc"
    assert trim_text("abcdef", 3) == "abc"
    assert trim_text_tail("abcdef", 3) == "def"
    assert trim_text_middle("abcdefghij", 9) == "ab ... ij"


def test_split_text_for_sending():
    text = "A " * 400
    parts = split_text_for_sending(text, max_chars=120, max_parts=4)
    assert 1 <= len(parts) <= 4
    assert all(len(part) <= 120 for part in parts)


def test_strip_reasoning_leak():
    leaked = "Analyze the user's input\n\nFinal answer: Готово"
    assert strip_reasoning_leak(leaked) == "Готово"


def test_merge_continuation_text_overlap():
    base = "Это начало и середина"
    extra = " и середина и конец"
    merged = merge_continuation_text(base, extra)
    assert "конец" in merged
