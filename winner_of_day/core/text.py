"""Core text helpers extracted from legacy bot."""

from __future__ import annotations

import re


def normalize_spaces(value: str) -> str:
    return " ".join((value or "").strip().split())


def trim_text(text: str, max_chars: int) -> str:
    if not text:
        return ""
    cleaned = text.strip()
    if max_chars <= 0:
        return cleaned
    if len(cleaned) > max_chars:
        return cleaned[:max_chars].rstrip()
    return cleaned


def trim_text_tail(text: str, max_chars: int) -> str:
    if not text:
        return ""
    cleaned = text.strip()
    if max_chars <= 0:
        return cleaned
    if len(cleaned) > max_chars:
        return cleaned[-max_chars:].lstrip()
    return cleaned


def trim_text_middle(text: str, max_chars: int, *, sep: str = " ... ") -> str:
    if not text:
        return ""
    cleaned = text.strip()
    if max_chars <= 0:
        return cleaned
    if len(cleaned) <= max_chars:
        return cleaned
    if max_chars <= len(sep) + 2:
        return cleaned[:max_chars].rstrip()
    head = (max_chars - len(sep)) // 2
    tail = max_chars - len(sep) - head
    return f"{cleaned[:head].rstrip()}{sep}{cleaned[-tail:].lstrip()}"


def strip_reasoning_leak(text: str) -> str:
    cleaned = (text or "").strip()
    if not cleaned:
        return ""

    lowered = cleaned.casefold()
    markers = (
        "analyze the user's input",
        "determine the response strategy",
        "internal monologue",
        "constraint checklist",
        "confidence score",
        "chain-of-thought",
        "(@draft)",
    )
    if not any(marker in lowered for marker in markers):
        return cleaned

    cut_patterns = (
        r"\(\s*@?draft\s*\)",
        r"\*\*\s*draft\b",
        r"\bdraft\b\s*:",
        r"\bfinal\s+answer\b\s*:",
        r"\bfinal\b\s*:",
        r"\banswer\b\s*:",
        r"\bответ\b\s*:",
    )
    last_match = None
    combined = re.compile(r"(?is)" + "|".join(cut_patterns))
    for match in combined.finditer(cleaned):
        last_match = match
    if last_match is not None:
        candidate = cleaned[last_match.end() :].strip()
    else:
        candidate = ""

    if not candidate:
        paragraphs = [part.strip() for part in re.split(r"\n{2,}", cleaned) if part.strip()]
        if paragraphs:
            candidate = paragraphs[-1]

    candidate = (candidate or "").strip()
    if not candidate:
        return cleaned

    candidate = re.sub(r"^\s*@?id\d+\s*\([^\)]*\)\s*", "", candidate, flags=re.IGNORECASE)
    candidate = re.sub(r"^\s*\d+\s*[\)\.:]\s*", "", candidate)
    candidate = candidate.strip(" \n\r\t:-*")
    return candidate if candidate else cleaned


def split_text_for_sending(
    text: str,
    *,
    max_chars: int,
    max_parts: int,
    tail_note: str | None = "\n\n(ответ слишком длинный; увеличь `/лимит` или попроси продолжение)",
) -> list[str]:
    cleaned = (text or "").strip()
    if not cleaned:
        return []
    if max_chars <= 0:
        return [cleaned]
    max_parts = max(1, int(max_parts or 1))

    parts: list[str] = []
    remaining = cleaned
    breakers: list[tuple[str, int, int]] = [
        ("\n\n", 0, 0),
        ("\n", 0, 1),
        (". ", 1, 2),
        ("! ", 1, 2),
        ("? ", 1, 2),
        ("… ", 1, 2),
        ("... ", 3, 2),
        ("; ", 1, 3),
        (": ", 1, 3),
        (", ", 1, 4),
        (" ", 0, 5),
    ]

    while remaining and len(parts) < max_parts:
        if len(remaining) <= max_chars:
            parts.append(remaining)
            remaining = ""
            break

        window = remaining[: max_chars + 1]
        candidates: list[tuple[int, int]] = []
        for sep, add, strength in breakers:
            idx = window.rfind(sep)
            if idx > 0:
                candidates.append((idx + add, strength))

        split_idx = 0
        if candidates:
            threshold = max(1, int(max_chars * 0.75))
            chosen: tuple[int, int] | None = None
            for strength in sorted({strength for _, strength in candidates}):
                best_pos = max((pos for pos, s in candidates if s == strength), default=0)
                if best_pos >= threshold:
                    chosen = (best_pos, strength)
                    break
            if chosen is None:
                chosen = max(candidates, key=lambda item: item[0])
            split_idx = int(chosen[0] or 0)
        if split_idx <= 0:
            split_idx = max_chars

        chunk = remaining[:split_idx].rstrip()
        if chunk:
            parts.append(chunk)
        remaining = remaining[split_idx:].lstrip()

    if remaining:
        note = tail_note or ""
        if note:
            if parts:
                if len(parts[-1]) + len(note) <= max_chars:
                    parts[-1] = (parts[-1].rstrip() + note).strip()
                else:
                    limit = max(1, max_chars - len(note))
                    shortened_parts = split_text_for_sending(parts[-1], max_chars=limit, max_parts=1, tail_note="")
                    shortened = shortened_parts[0] if shortened_parts else trim_text(parts[-1], limit)
                    parts[-1] = (shortened.rstrip() + note).strip()
            else:
                parts.append(trim_text(remaining, max_chars))

    return parts


def merge_continuation_text(base_text: str, extra_text: str) -> str:
    base = str(base_text or "").strip()
    extra = str(extra_text or "").strip()
    if not base:
        return extra
    if not extra:
        return base

    max_overlap = min(len(base), len(extra), 220)
    for overlap in range(max_overlap, 19, -1):
        if base[-overlap:].casefold() == extra[:overlap].casefold():
            merged = (base + extra[overlap:]).strip()
            return merged if merged else base

    if base.casefold().endswith(extra.casefold()):
        return base
    separator = "" if re.search(r"[\s\n]$", base) else "\n"
    return (base + separator + extra).strip()
