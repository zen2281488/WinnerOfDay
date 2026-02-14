from winner_of_day.config.env import (
    normalize_prompt,
    read_bool_env,
    read_float_env,
    read_int_env,
    read_int_list_env,
    read_str_list_env,
)


def test_read_bool_env(monkeypatch):
    monkeypatch.setenv("BOOL_FLAG", "true")
    assert read_bool_env("BOOL_FLAG", default=False) is True
    monkeypatch.setenv("BOOL_FLAG", "off")
    assert read_bool_env("BOOL_FLAG", default=True) is False
    monkeypatch.delenv("BOOL_FLAG", raising=False)
    assert read_bool_env("BOOL_FLAG", default=True) is True


def test_read_int_env(monkeypatch):
    monkeypatch.setenv("INT_FLAG", "12")
    assert read_int_env("INT_FLAG", default=3) == 12
    monkeypatch.setenv("INT_FLAG", "1")
    assert read_int_env("INT_FLAG", default=3, min_value=5) == 5
    monkeypatch.setenv("INT_FLAG", "oops")
    assert read_int_env("INT_FLAG", default=3) == 3


def test_read_float_env(monkeypatch):
    monkeypatch.setenv("FLOAT_FLAG", "2.5")
    assert read_float_env("FLOAT_FLAG", default=0.5) == 2.5
    monkeypatch.setenv("FLOAT_FLAG", "oops")
    assert read_float_env("FLOAT_FLAG", default=0.5) == 0.5


def test_read_list_env(monkeypatch):
    monkeypatch.setenv("INT_LIST", "1, 2, bad, 3")
    assert read_int_list_env("INT_LIST") == [1, 2, 3]
    monkeypatch.setenv("STR_LIST", "a, b, , c")
    assert read_str_list_env("STR_LIST") == ["a", "b", "c"]


def test_normalize_prompt():
    assert normalize_prompt("line1\\nline2") == "line1\nline2"
