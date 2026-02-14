from __future__ import annotations

import logging
import os

TRUE_VALUES = {"1", "true", "yes", "on"}


def read_bool_env(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return value.strip().lower() in TRUE_VALUES


def read_int_env(name: str, default: int | None = None, min_value: int | None = None) -> int | None:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    try:
        number = int(value)
    except ValueError:
        logging.getLogger("winner_of_day_bot").warning("%s is not a valid integer", name)
        return default
    if min_value is not None and number < min_value:
        return min_value
    return number


def read_float_env(name: str, default: float | None = None) -> float | None:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    try:
        return float(value)
    except ValueError:
        logging.getLogger("winner_of_day_bot").warning("%s is not a valid float", name)
        return default


def read_int_list_env(name: str) -> list[int]:
    value = os.getenv(name)
    if not value:
        return []
    parts = [part.strip() for part in value.split(",") if part.strip()]
    result: list[int] = []
    for part in parts:
        try:
            result.append(int(part))
        except ValueError:
            logging.getLogger("winner_of_day_bot").warning("%s has invalid integer: %s", name, part)
    return result


def read_str_list_env(name: str) -> list[str]:
    value = os.getenv(name)
    if not value:
        return []
    parts = [part.strip() for part in value.split(",") if part.strip()]
    return [part for part in parts if part]


def normalize_prompt(value: str) -> str:
    if not value:
        return ""
    return value.replace("\\r\\n", "\n").replace("\\n", "\n")
