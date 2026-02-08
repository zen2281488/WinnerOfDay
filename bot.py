import asyncio
import datetime
import json
import logging
import os
import random
import re
import sys
from collections import Counter

import aiosqlite
import httpx
from vkbottle.bot import Bot, Message
from vkbottle.dispatch.rules import ABCRule  # Для создания своего правила

LOG_FORMAT = "%(asctime)s | %(levelname)s | %(name)s:%(lineno)d | %(message)s"
logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)
log = logging.getLogger("winner_of_day_bot")

try:
    from groq import AsyncGroq
except ImportError:
    AsyncGroq = None

# ================= НАСТРОЙКИ =================
VK_TOKEN = os.getenv("VK_TOKEN")
LLM_PROVIDER = os.getenv("LLM_PROVIDER", "").strip().lower()

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
        log.warning("%s is not a valid integer", name)
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
        log.warning("%s is not a valid float", name)
        return default

def read_int_list_env(name: str):
    value = os.getenv(name)
    if not value:
        return []
    parts = [part.strip() for part in value.split(",") if part.strip()]
    result = []
    for part in parts:
        try:
            result.append(int(part))
        except ValueError:
            log.warning("%s has invalid integer: %s", name, part)
    return result

ADMIN_USER_ID = read_int_env("ADMIN_USER_ID")
ALLOWED_PEER_IDS = read_int_list_env("ALLOWED_PEER_ID")
if not ALLOWED_PEER_IDS:
    ALLOWED_PEER_IDS = None

CHATBOT_ENABLED = read_bool_env("CHATBOT_ENABLED", default=True)

CHAT_HISTORY_LIMIT = read_int_env("CHAT_HISTORY_LIMIT", default=6, min_value=0)
CHAT_MESSAGE_MAX_CHARS = read_int_env("CHAT_MESSAGE_MAX_CHARS", default=300, min_value=0)
LLM_MAX_TOKENS = read_int_env("LLM_MAX_TOKENS", default=800, min_value=1)
CHAT_MAX_TOKENS = read_int_env("CHAT_MAX_TOKENS", default=300, min_value=1)
CHAT_RESPONSE_MAX_CHARS = read_int_env("CHAT_RESPONSE_MAX_CHARS", default=600, min_value=0)

BOT_REPLY_FULL_LIMIT = read_int_env("CHAT_BOT_FULL_LIMIT", default=2, min_value=0)
BOT_REPLY_SHORT_LIMIT = read_int_env("CHAT_BOT_SHORT_LIMIT", default=2, min_value=0)
BOT_REPLY_FULL_MAX_CHARS = read_int_env("CHAT_BOT_FULL_MAX_CHARS", default=800, min_value=0)
BOT_REPLY_SHORT_MAX_CHARS = read_int_env("CHAT_BOT_SHORT_MAX_CHARS", default=160, min_value=0)

GROQ_API_KEY = os.getenv("GROQ_API_KEY")
GROQ_MODEL = os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile")
GROQ_TEMPERATURE = read_float_env("GROQ_TEMPERATURE", default=0.9)
if GROQ_TEMPERATURE is None:
    GROQ_TEMPERATURE = 0.9

VENICE_API_KEY = os.getenv("VENICE_API_KEY")
VENICE_MODEL = os.getenv("VENICE_MODEL", "llama-3.3-70b")
VENICE_BASE_URL = os.getenv("VENICE_BASE_URL", "https://api.venice.ai/api/v1/")
if not VENICE_BASE_URL.endswith("/"):
    VENICE_BASE_URL += "/"

VENICE_TEMPERATURE = read_float_env("VENICE_TEMPERATURE", default=0.9)
if VENICE_TEMPERATURE is None:
    VENICE_TEMPERATURE = 0.9

VENICE_TIMEOUT = read_float_env("VENICE_TIMEOUT", default=30.0)
if VENICE_TIMEOUT is None:
    VENICE_TIMEOUT = 30.0

VENICE_INCLUDE_SYSTEM_PROMPT = read_bool_env("VENICE_INCLUDE_SYSTEM_PROMPT", default=False)

if not LLM_PROVIDER:
    if VENICE_API_KEY and not GROQ_API_KEY:
        LLM_PROVIDER = "venice"
    else:
        LLM_PROVIDER = "groq"

CHAT_LLM_PROVIDER = os.getenv("CHAT_LLM_PROVIDER", "").strip().lower()
if not CHAT_LLM_PROVIDER:
    CHAT_LLM_PROVIDER = LLM_PROVIDER

CHAT_GROQ_MODEL = os.getenv("CHAT_GROQ_MODEL", GROQ_MODEL)
CHAT_GROQ_TEMPERATURE = read_float_env("CHAT_GROQ_TEMPERATURE", default=GROQ_TEMPERATURE)
if CHAT_GROQ_TEMPERATURE is None:
    CHAT_GROQ_TEMPERATURE = GROQ_TEMPERATURE

CHAT_VENICE_MODEL = os.getenv("CHAT_VENICE_MODEL", VENICE_MODEL)
CHAT_VENICE_TEMPERATURE = read_float_env("CHAT_VENICE_TEMPERATURE", default=VENICE_TEMPERATURE)
if CHAT_VENICE_TEMPERATURE is None:
    CHAT_VENICE_TEMPERATURE = VENICE_TEMPERATURE

BUILD_DATE = os.getenv("BUILD_DATE", "unknown")
BUILD_SHA = os.getenv("BUILD_SHA", "")
BOT_GROUP_ID = None
USER_NAME_CACHE: dict[int, str] = {}

if not VK_TOKEN:
    log.error("VK_TOKEN is missing")
    sys.exit(1)

if LLM_PROVIDER not in ("groq", "venice"):
    log.error("LLM_PROVIDER must be groq or venice")
    sys.exit(1)

if CHAT_LLM_PROVIDER not in ("groq", "venice"):
    log.error("CHAT_LLM_PROVIDER must be groq or venice")
    sys.exit(1)

if LLM_PROVIDER == "groq":
    if not GROQ_API_KEY:
        log.error("GROQ_API_KEY is missing while LLM_PROVIDER=groq")
        sys.exit(1)
    if AsyncGroq is None:
        log.error("groq package is not installed but LLM_PROVIDER=groq")
        sys.exit(1)
else:
    if not VENICE_API_KEY:
        log.error("VENICE_API_KEY is missing while LLM_PROVIDER=venice")
        sys.exit(1)

if CHATBOT_ENABLED:
    if CHAT_LLM_PROVIDER == "groq":
        if not GROQ_API_KEY:
            log.error("GROQ_API_KEY is missing while CHAT_LLM_PROVIDER=groq")
            sys.exit(1)
        if AsyncGroq is None:
            log.error("groq package is not installed but CHAT_LLM_PROVIDER=groq")
            sys.exit(1)
    else:
        if not VENICE_API_KEY:
            log.error("VENICE_API_KEY is missing while CHAT_LLM_PROVIDER=venice")
            sys.exit(1)

# === Команды ===
GAME_TITLE = os.getenv("GAME_TITLE", "Пидор дня")
LEADERBOARD_TITLE = os.getenv("LEADERBOARD_TITLE", "📊 Пидерборд")
CMD_RUN = "/кто"
CMD_RESET = "/сброс"
CMD_TIME_SET = "/время"
CMD_TIME_RESET = "/сброс_времени"
CMD_SETTINGS = "/настройки"
CMD_SET_MODEL = "/установить_модель"
CMD_SET_KEY = "/установить_ключ"
CMD_SET_TEMPERATURE = "/установить_температуру"
CMD_SET_PROVIDER = "/провайдер"
CMD_LIST_MODELS = "/список_моделей"
CMD_PROMPT = "/промт"
CMD_LEADERBOARD = "/лидерборд"
CMD_LEADERBOARD_TIMER_SET = "/таймер_лидерборда"
CMD_LEADERBOARD_TIMER_RESET = "/сброс_таймера_лидерборда"

DB_NAME = os.getenv("DB_PATH", "chat_history.db")
MSK_TZ = datetime.timezone(datetime.timedelta(hours=3))

def format_build_date(value: str) -> str:
    if not value or value == "unknown":
        return "неизвестно"
    try:
        normalized = value.strip()
        if normalized.endswith("Z"):
            normalized = normalized[:-1] + "+00:00"
        dt = datetime.datetime.fromisoformat(normalized)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.timezone.utc)
        dt = dt.astimezone(MSK_TZ)
        return dt.strftime("%d.%m.%y в %H:%M")
    except Exception:
        return value

# Свои правила (case-insensitive)
class StartswithRule(ABCRule[Message]):
    def __init__(self, prefix: str):
        self.prefix = prefix.lower()

    async def check(self, event: Message) -> bool:
        text = (event.text or "").strip().lower()
        return text.startswith(self.prefix)

class EqualsRule(ABCRule[Message]):
    def __init__(self, text: str):
        self.text = text.lower()

    async def check(self, event: Message) -> bool:
        return (event.text or "").strip().lower() == self.text


def is_chatbot_trigger_message(message: Message) -> bool:
    text = message.text
    if not text:
        return False
    if text.lstrip().startswith("/"):
        return False
    reply_from_id = extract_reply_from_id(message)
    is_reply_to_bot = bool(BOT_GROUP_ID and reply_from_id == -BOT_GROUP_ID)
    is_admin_dm = bool(
        ADMIN_USER_ID
        and message.from_id == ADMIN_USER_ID
        and message.peer_id == message.from_id
    )
    is_mention = has_bot_mention(text)
    return is_admin_dm or is_mention or is_reply_to_bot


class ChatbotTriggerRule(ABCRule[Message]):
    async def check(self, event: Message) -> bool:
        return is_chatbot_trigger_message(event)

# Промпт
def normalize_prompt(value: str) -> str:
    if not value:
        return ""
    return value.replace("\\r\\n", "\n").replace("\\n", "\n")

def strip_command(text: str, command: str) -> str:
    if not text:
        return ""
    trimmed = text.strip()
    if trimmed.lower().startswith(command.lower()):
        return trimmed[len(command):].strip()
    return trimmed

def parse_llm_scope(value: str) -> str | None:
    if not value:
        return None
    normalized = value.strip().lower()
    if normalized in ("chat", "чат"):
        return "chat"
    if normalized in ("game", "игра"):
        return "game"
    return None

SYSTEM_PROMPT = (
    "Формат ответа — строго валидный JSON, только объект и только двойные кавычки. "
    "Пример: {\"user_id\": 123, \"reason\": \"...\"}\n"
    "Никакого текста вне JSON.\n"
)
CHAT_SYSTEM_PROMPT = normalize_prompt(
    os.getenv(
        "CHAT_SYSTEM_PROMPT",
        "Ты чат-бот сообщества VK. Отвечай по-русски, по делу и без JSON."
    )
)
USER_PROMPT_TEMPLATE = normalize_prompt(os.getenv("USER_PROMPT_TEMPLATE"))

if not USER_PROMPT_TEMPLATE:
    log.error("Missing USER_PROMPT_TEMPLATE in environment")
    sys.exit(1)

def render_user_prompt(context_text: str) -> str:
    prompt = USER_PROMPT_TEMPLATE.replace("{{GAME_TITLE}}", GAME_TITLE)
    if "{{CHAT_LOG}}" in prompt:
        prompt = prompt.replace("{{CHAT_LOG}}", context_text)
    else:
        prompt = f"{prompt}\n\n{context_text}"
    return prompt

def has_bot_mention(text: str) -> bool:
    if not text or not BOT_GROUP_ID:
        return False
    group_id = str(BOT_GROUP_ID)
    lowered = text.lower()
    if f"@club{group_id}" in lowered or f"@public{group_id}" in lowered:
        return True
    return re.search(rf"\[(club|public){group_id}\|", lowered) is not None

def strip_bot_mention(text: str) -> str:
    if not text or not BOT_GROUP_ID:
        return text
    group_id = str(BOT_GROUP_ID)
    cleaned = re.sub(rf"\[(club|public){group_id}\|[^\]]+\]", "", text, flags=re.IGNORECASE)
    cleaned = re.sub(rf"@(?:club|public){group_id}\b", "", cleaned, flags=re.IGNORECASE)
    return cleaned.strip()

def trim_text(text: str, max_chars: int) -> str:
    if not text:
        return ""
    cleaned = text.strip()
    if max_chars <= 0:
        return cleaned
    if len(cleaned) > max_chars:
        return cleaned[:max_chars].rstrip()
    return cleaned

def trim_chat_text(text: str) -> str:
    return trim_text(text, CHAT_MESSAGE_MAX_CHARS)

def normalize_max_tokens(value: int, fallback: int) -> int:
    if value is None:
        return fallback
    if value < 1:
        return 1
    return value

def extract_reply_text(message: Message) -> str:
    reply_message = getattr(message, "reply_message", None)
    if not reply_message:
        return ""
    reply_text = getattr(reply_message, "text", None)
    if reply_text is None and isinstance(reply_message, dict):
        reply_text = reply_message.get("text")
    return str(reply_text) if reply_text else ""

def extract_reply_from_id(message: Message):
    reply_message = getattr(message, "reply_message", None)
    if not reply_message:
        return None
    reply_from_id = getattr(reply_message, "from_id", None)
    if reply_from_id is None and isinstance(reply_message, dict):
        reply_from_id = reply_message.get("from_id")
    return reply_from_id

async def build_chat_history(peer_id: int, user_id: int) -> list:
    history = []
    bot_limit = BOT_REPLY_FULL_LIMIT + BOT_REPLY_SHORT_LIMIT
    if CHAT_HISTORY_LIMIT <= 0 and bot_limit <= 0:
        return history

    user_rows = []
    bot_rows = []
    async with aiosqlite.connect(DB_NAME) as db:
        if CHAT_HISTORY_LIMIT > 0:
            cursor = await db.execute(
                """
                SELECT id, text, timestamp
                FROM bot_dialogs
                WHERE peer_id = ? AND user_id = ? AND role = 'user'
                ORDER BY timestamp DESC, id DESC
                LIMIT ?
                """,
                (peer_id, user_id, CHAT_HISTORY_LIMIT),
            )
            user_rows = await cursor.fetchall()
        if bot_limit > 0:
            cursor = await db.execute(
                """
                SELECT id, text, timestamp
                FROM bot_dialogs
                WHERE peer_id = ? AND user_id = ? AND role = 'assistant'
                ORDER BY timestamp DESC, id DESC
                LIMIT ?
                """,
                (peer_id, user_id, bot_limit),
            )
            bot_rows = await cursor.fetchall()

    entries = []
    for entry_id, text, ts in user_rows:
        content = trim_chat_text(text)
        if content:
            entries.append((ts, entry_id, "user", content))

    for idx, (entry_id, text, ts) in enumerate(bot_rows):
        if idx < BOT_REPLY_FULL_LIMIT:
            content = trim_text(text, BOT_REPLY_FULL_MAX_CHARS)
        else:
            content = trim_text(text, BOT_REPLY_SHORT_MAX_CHARS)
        if content:
            entries.append((ts, entry_id, "assistant", content))

    entries.sort(key=lambda item: (item[0], item[1]))
    for _, _, role, content in entries:
        history.append({"role": role, "content": content})
    return history

def extract_group_id(group_response):
    if not group_response:
        return None
    if isinstance(group_response, list):
        first = group_response[0] if group_response else None
        return getattr(first, "id", None) if first else None
    direct_id = getattr(group_response, "id", None)
    if direct_id:
        return direct_id
    groups = getattr(group_response, "groups", None)
    if groups:
        first = groups[0]
        return getattr(first, "id", None)
    response = getattr(group_response, "response", None)
    if response:
        groups = getattr(response, "groups", None)
        if groups:
            first = groups[0]
            return getattr(first, "id", None)
    return None

def is_message_allowed(message: Message) -> bool:
    if ALLOWED_PEER_IDS is None:
        return True
    if message.peer_id in ALLOWED_PEER_IDS:
        return True
    if ADMIN_USER_ID and message.from_id == ADMIN_USER_ID and message.peer_id == message.from_id:
        return True
    return False

def format_allowed_peers() -> str:
    if not ALLOWED_PEER_IDS:
        return "не заданы"
    return ", ".join(str(peer_id) for peer_id in ALLOWED_PEER_IDS)

async def ensure_message_allowed(message: Message, action_label: str | None = None) -> bool:
    if is_message_allowed(message):
        return True
    action_text = f" к {action_label}" if action_label else ""
    admin_hint = " Если вы администратор, напишите боту в ЛС." if ADMIN_USER_ID else ""
    await send_reply(
        message,
        f"⛔ Доступ{action_text} ограничен. Разрешены чаты: {format_allowed_peers()}.{admin_hint}"
    )
    log.info(
        "Access denied peer_id=%s user_id=%s action=%s",
        message.peer_id,
        message.from_id,
        action_label or "unknown"
    )
    return False

async def ensure_command_allowed(message: Message, command: str) -> bool:
    asyncio.create_task(store_message(message))
    return await ensure_message_allowed(message, action_label=f"команде `{command}`")

def get_reply_to_id(message: Message):
    if getattr(message, "is_unavailable", False):
        return None
    reply_to = getattr(message, "conversation_message_id", None)
    if isinstance(reply_to, int) and reply_to > 0:
        return reply_to
    return None

async def send_reply(message: Message, text: str, **kwargs):
    reply_to = get_reply_to_id(message)
    if reply_to:
        kwargs.setdefault("reply_to", reply_to)
    try:
        await message.answer(text, **kwargs)
    except Exception as e:
        error_text = str(e).lower()
        if reply_to and ("reply_to" in error_text or "forwarded message not found" in error_text):
            try:
                log.warning("send_reply failed with reply_to, retrying without reply_to: %s", e)
                kwargs.pop("reply_to", None)
                await message.answer(text, **kwargs)
                return
            except Exception as inner:
                log.exception("send_reply fallback failed: %s", inner)
                return
        log.exception("send_reply failed: %s", e)

def get_conversation_message_id(message: Message) -> int | None:
    value = getattr(message, "conversation_message_id", None)
    if isinstance(value, int) and value > 0:
        return value
    return None

async def store_message(message: Message):
    try:
        if not is_message_allowed(message):
            return
        if message.from_id is None or message.from_id <= 0:
            return
        text = getattr(message, "text", None)
        if text is None:
            return
        username = USER_NAME_CACHE.get(message.from_id)
        if not username:
            try:
                user_info = await message.get_user()
                username = f"{user_info.first_name} {user_info.last_name}"
            except Exception as e:
                log.debug("Failed to resolve username user_id=%s: %s", message.from_id, e)
                username = "Unknown"
            USER_NAME_CACHE[message.from_id] = username
        conversation_message_id = get_conversation_message_id(message)
        async with aiosqlite.connect(DB_NAME) as db:
            await db.execute(
                "INSERT OR IGNORE INTO messages (user_id, peer_id, text, timestamp, username, conversation_message_id) VALUES (?, ?, ?, ?, ?, ?)",
                (message.from_id, message.peer_id, text, message.date, username, conversation_message_id),
            )
            await db.commit()
    except Exception as e:
        log.exception("Failed to store message peer_id=%s user_id=%s: %s", message.peer_id, message.from_id, e)


bot = Bot(token=VK_TOKEN)
groq_client = AsyncGroq(api_key=GROQ_API_KEY) if GROQ_API_KEY and AsyncGroq else None

def build_venice_headers() -> dict:
    return {"Authorization": f"Bearer {VENICE_API_KEY}"}

async def venice_request(method: str, path: str, **kwargs) -> httpx.Response:
    headers = kwargs.pop("headers", {})
    request_headers = {**build_venice_headers(), **headers}
    timeout = httpx.Timeout(VENICE_TIMEOUT)
    async with httpx.AsyncClient(base_url=VENICE_BASE_URL, timeout=timeout) as client:
        response = await client.request(method, path, headers=request_headers, **kwargs)
    if response.status_code >= 400:
        message = response.text.strip()
        if len(message) > 500:
            message = message[:500] + "..."
        raise RuntimeError(f"HTTP {response.status_code}: {message}")
    return response

# ================= БАЗА ДАННЫХ =================
async def init_db():
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("CREATE TABLE IF NOT EXISTS messages (user_id INTEGER, peer_id INTEGER, text TEXT, timestamp INTEGER, username TEXT)")
        cursor = await db.execute("PRAGMA table_info(messages)")
        columns = {row[1] for row in await cursor.fetchall()}
        if "conversation_message_id" not in columns:
            await db.execute("ALTER TABLE messages ADD COLUMN conversation_message_id INTEGER")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_messages_peer_time ON messages (peer_id, timestamp)")
        await db.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_messages_peer_conversation_id ON messages (peer_id, conversation_message_id)"
        )
        await db.execute("CREATE TABLE IF NOT EXISTS bot_dialogs (id INTEGER PRIMARY KEY AUTOINCREMENT, peer_id INTEGER, user_id INTEGER, role TEXT, text TEXT, timestamp INTEGER)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_bot_dialogs_peer_user_time ON bot_dialogs (peer_id, user_id, timestamp)")
        await db.execute("CREATE TABLE IF NOT EXISTS daily_game (peer_id INTEGER, date TEXT, winner_id INTEGER, reason TEXT, PRIMARY KEY (peer_id, date))")
        await db.execute("CREATE TABLE IF NOT EXISTS last_winner (peer_id INTEGER PRIMARY KEY, winner_id INTEGER, timestamp INTEGER)")
        await db.execute("CREATE TABLE IF NOT EXISTS leaderboard_schedule (peer_id INTEGER PRIMARY KEY, day INTEGER, time TEXT, last_run_month TEXT)")
        await db.execute("CREATE TABLE IF NOT EXISTS schedules (peer_id INTEGER PRIMARY KEY, time TEXT)")
        await db.commit()

# ================= LLM ЗАПРОСЫ =================
def get_llm_settings(target: str) -> tuple[str, str, float, str, float]:
    if target == "chat":
        return (
            CHAT_LLM_PROVIDER,
            CHAT_GROQ_MODEL,
            CHAT_GROQ_TEMPERATURE,
            CHAT_VENICE_MODEL,
            CHAT_VENICE_TEMPERATURE,
        )
    return (LLM_PROVIDER, GROQ_MODEL, GROQ_TEMPERATURE, VENICE_MODEL, VENICE_TEMPERATURE)

async def fetch_llm_messages(messages: list, max_tokens: int = None, *, target: str = "game") -> str:
    provider, groq_model, groq_temperature, venice_model, venice_temperature = get_llm_settings(target)
    max_tokens = normalize_max_tokens(max_tokens, LLM_MAX_TOKENS)
    if provider == "venice":
        log.debug(
            "Sending request to Venice. Target=%s Model=%s Temp=%s",
            target,
            venice_model,
            venice_temperature,
        )
        payload = {
            "model": venice_model,
            "messages": messages,
            "temperature": venice_temperature,
            "max_tokens": max_tokens,
            "venice_parameters": {
                "include_venice_system_prompt": VENICE_INCLUDE_SYSTEM_PROMPT,
            },
        }
        response = await venice_request("POST", "chat/completions", json=payload)
        response_data = response.json()
        content = (
            (response_data.get("choices") or [{}])[0]
            .get("message", {})
            .get("content")
        )
        if not content:
            raise ValueError("Empty content in Venice response")
        return content

    if provider != "groq":
        raise ValueError(f"Unsupported LLM provider: {provider}")
    if not groq_client:
        raise RuntimeError("Groq client is not initialized")
    log.debug(
        "Sending request to Groq. Target=%s Model=%s Temp=%s",
        target,
        groq_model,
        groq_temperature,
    )
    completion = await groq_client.chat.completions.create(
        model=groq_model,
        messages=messages,
        temperature=groq_temperature,
        max_tokens=max_tokens,
    )
    content = completion.choices[0].message.content
    if not content:
        raise ValueError("Empty content in Groq response")
    return content

async def fetch_llm_content(system_prompt: str, user_prompt: str, *, target: str = "game") -> str:
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]
    return await fetch_llm_messages(messages, target=target)


async def choose_winner_via_llm(chat_log: list, excluded_user_id=None) -> dict:
    context_lines = []
    available_ids = set()
    alias_map = {}
    alias_names = {}
    alias_to_user_id = {}
    alias_order = []
    alias_counter = 0

    def get_alias(uid: int, safe_name: str) -> str:
        nonlocal alias_counter
        if uid not in alias_map:
            alias_counter += 1
            alias = f"U{alias_counter}"
            alias_map[uid] = alias
            alias_names[alias] = safe_name
            alias_to_user_id[alias] = uid
            alias_order.append(alias)
        return alias_map[uid]
    
    for uid, text, name in chat_log:
        if excluded_user_id is not None and uid == excluded_user_id:
            continue
        if len(text.strip()) < 3:
            continue
        safe_name = name if name else "Unknown"
        alias = get_alias(uid, safe_name)
        context_lines.append(f"{alias}: {text}")
        available_ids.add(uid)

    if not context_lines:
        return {"user_id": 0, "reason": "Все молчат. Скучные натуралы."}

    alias_parts = [
        f"{alias}={alias_to_user_id[alias]}|{alias_names[alias]}"
        for alias in alias_order
    ]
    alias_map_line = "USERS: " + "; ".join(alias_parts)
    context_text = f"{alias_map_line}\n" + "\n".join(context_lines)

    user_prompt = render_user_prompt(context_text)

    try:
        content = await fetch_llm_content(SYSTEM_PROMPT, user_prompt)
        
        try:
            result = json.loads(content)
        except json.JSONDecodeError:
            if "{" in content and "}" in content:
                start = content.find("{")
                end = content.rfind("}") + 1
                json_str = content[start:end]
                result = json.loads(json_str)
            else:
                raise
        
        if not isinstance(result, dict):
            raise ValueError("Result is not a dictionary")
            
        user_id_raw = result.get("user_id", 0)
        user_id = None
        if isinstance(user_id_raw, str):
            raw = user_id_raw.strip()
            if raw:
                alias_key = raw.upper()
                if alias_key in alias_to_user_id:
                    user_id = alias_to_user_id[alias_key]
                elif raw.isdigit():
                    user_id = int(raw)
        elif isinstance(user_id_raw, (int, float)):
            user_id = int(user_id_raw)

        if user_id not in available_ids:
            result['user_id'] = random.choice(list(available_ids))
        else:
            result['user_id'] = user_id
            
        return result

    except Exception as e:
        log.exception("LLM API error (%s): %s", LLM_PROVIDER, e)
    
    # Fallback
    log.warning("Using fallback selection after LLM failure")
    if available_ids:
        user_counts = Counter([uid for uid, _, _ in chat_log if uid in available_ids])
        if user_counts:
            most_active = max(user_counts.items(), key=lambda x: x[1])[0]
            fallback_reasons = [
                f"Настрочил {user_counts[most_active]} сообщений и нихуя умного. Поздравляю, ты душный.",
                f"За {user_counts[most_active]} сообщений спама. ИИ сломался от твоей тупости, поэтому победа твоя.",
                "ИИ отказался работать с таким контингентом, поэтому ты пидор просто по факту существования."
            ]
            return {"user_id": most_active, "reason": random.choice(fallback_reasons)}
    
    return {"user_id": 0, "reason": "Чат мертв, и вы все мертвы внутри."}

# ================= ИГРОВАЯ ЛОГИКА =================
async def run_game_logic(peer_id: int, reset_if_exists: bool = False):
    """
    reset_if_exists=True: Если игра запускается таймером, мы удаляем старый результат и выбираем заново.
    reset_if_exists=False: (По умолчанию) Если играем вручную, бот скажет 'Уже выбрали'.
    """
    if ALLOWED_PEER_IDS is not None and peer_id not in ALLOWED_PEER_IDS:
        log.info("Game logic skipped for peer_id=%s (not in allowed list)", peer_id)
        return
    log.debug("Game logic start peer_id=%s reset_if_exists=%s", peer_id, reset_if_exists)
    today = datetime.datetime.now(MSK_TZ).date().isoformat()
    last_winner_id = None
    exclude_user_id = None
    
    async def send_msg(text):
        try:
            await bot.api.messages.send(peer_id=peer_id, message=text, random_id=0)
        except Exception as e:
            log.warning("Failed to send message to peer_id=%s: %s", peer_id, e)

    async with aiosqlite.connect(DB_NAME) as db:
        # ЛОГИКА АВТО-СБРОСА
        if reset_if_exists:
            # Если это авто-запуск, сначала удаляем старую запись
            await db.execute("DELETE FROM daily_game WHERE peer_id = ? AND date = ?", (peer_id, today))
            await db.commit()

        # Проверяем, есть ли победитель (если сбросили выше, то тут уже ничего не найдет)
        cursor = await db.execute("SELECT winner_id, reason FROM daily_game WHERE peer_id = ? AND date = ?", (peer_id, today))
        result = await cursor.fetchone()

        if result:
            winner_id, reason = result
            try:
                user_info = await bot.api.users.get(user_ids=[winner_id])
                name = f"{user_info[0].first_name} {user_info[0].last_name}"
            except Exception as e:
                log.warning("Failed to resolve winner name peer_id=%s user_id=%s: %s", peer_id, winner_id, e)
                name = "Unknown"
            await send_msg(f"Уже определили!\n{GAME_TITLE}: [id{winner_id}|{name}]\n\n📝 {reason}\n\n(Чтобы сбросить: {CMD_RESET})")
            return

        # Сбор сообщений
        cursor = await db.execute(
            "SELECT winner_id FROM last_winner WHERE peer_id = ? LIMIT 1",
            (peer_id,)
        )
        row = await cursor.fetchone()
        if row:
            last_winner_id = row[0]
        else:
            cursor = await db.execute(
                "SELECT winner_id FROM daily_game WHERE peer_id = ? ORDER BY date DESC LIMIT 1",
                (peer_id,)
            )
            row = await cursor.fetchone()
            if row:
                last_winner_id = row[0]

        now_msk = datetime.datetime.now(MSK_TZ)
        day_start = now_msk.replace(hour=0, minute=0, second=0, microsecond=0)
        day_end = day_start + datetime.timedelta(days=1)
        start_ts = int(day_start.timestamp())
        end_ts = int(day_end.timestamp())

        cursor = await db.execute("""
            SELECT user_id, text, username 
            FROM messages 
            WHERE peer_id = ? 
            AND timestamp >= ? AND timestamp < ?
            AND LENGTH(TRIM(text)) > 2
            ORDER BY timestamp DESC 
            LIMIT 200
        """, (peer_id, start_ts, end_ts))
        rows = await cursor.fetchall()
        log.debug("Collected %s messages for peer_id=%s (today)", len(rows), peer_id)

        soft_min_messages = 50
        if len(rows) < soft_min_messages:
            remaining = soft_min_messages - len(rows)
            before_count = len(rows)
            cursor = await db.execute("""
                SELECT user_id, text, username 
                FROM messages 
                WHERE peer_id = ? 
                AND timestamp < ?
                AND LENGTH(TRIM(text)) > 2
                ORDER BY timestamp DESC 
                LIMIT ?
            """, (peer_id, start_ts, remaining))
            rows.extend(await cursor.fetchall())
            log.debug(
                "Soft-min fill for peer_id=%s: added=%s total=%s",
                peer_id,
                len(rows) - before_count,
                len(rows),
            )

        if len(rows) < 3:
            log.info("Not enough messages for peer_id=%s: %s", peer_id, len(rows))
            await send_msg("Мало сообщений. Пишите больше, чтобы я мог выбрать худшего.")
            return

        chat_log = list(reversed(rows))
        candidate_ids = {uid for uid, text, _ in chat_log if len(text.strip()) >= 3}
        if last_winner_id is not None and last_winner_id in candidate_ids and len(candidate_ids) > 1:
            exclude_user_id = last_winner_id
            log.debug("Excluding last winner user_id=%s for peer_id=%s", exclude_user_id, peer_id)

    log.info(
        "Selecting winner peer_id=%s messages=%s excluded_user_id=%s",
        peer_id,
        len(chat_log),
        exclude_user_id,
    )
    await send_msg(f"🎲 Изучаю {len(chat_log)} сообщений... Кто же сегодня опозорится?")
    
    try:
        decision = await choose_winner_via_llm(chat_log, excluded_user_id=exclude_user_id)
        winner_id = decision['user_id']
        reason = decision.get('reason', 'Нет причины')
        
        if winner_id == 0:
            await send_msg("Ошибка выбора. Попробуйте позже.")
            return

    except Exception as e:
        log.exception("Error in game logic for peer_id=%s: %s", peer_id, e)
        await send_msg("Ошибка при выборе победителя.")
        return

    try:
        user_data = await bot.api.users.get(user_ids=[winner_id])
        winner_name = f"{user_data[0].first_name} {user_data[0].last_name}"
    except Exception as e:
        log.warning("Failed to resolve winner name peer_id=%s user_id=%s: %s", peer_id, winner_id, e)
        winner_name = "Жертва"
    log.info("Winner selected peer_id=%s user_id=%s", peer_id, winner_id)

    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "INSERT INTO daily_game (peer_id, date, winner_id, reason) VALUES (?, ?, ?, ?)", 
            (peer_id, today, winner_id, reason)
        )
        await db.execute(
            "INSERT OR REPLACE INTO last_winner (peer_id, winner_id, timestamp) VALUES (?, ?, ?)",
            (peer_id, winner_id, int(datetime.datetime.now(MSK_TZ).timestamp()))
        )
        await db.commit()

    await send_msg(
        f"🏳 {GAME_TITLE.upper()} ВЫБРАН!\n"
        f"Победитель (сегодня): [id{winner_id}|{winner_name}]\n\n"
        f"📝 Причина:\n{reason}"
    )
# ================= УТИЛИТЫ =================
# ================= ЛОГИКА: ЛИДЕРБОРД =================
def last_day_of_month(year: int, month: int) -> int:
    if month == 12:
        next_month = datetime.date(year + 1, 1, 1)
    else:
        next_month = datetime.date(year, month + 1, 1)
    return (next_month - datetime.timedelta(days=1)).day

async def build_leaderboard_text(peer_id: int) -> str:
    today = datetime.datetime.now(MSK_TZ).date()
    month_start = today.replace(day=1)
    if today.month == 12:
        next_month = datetime.date(today.year + 1, 1, 1)
    else:
        next_month = datetime.date(today.year, today.month + 1, 1)

    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute(
            """
            SELECT winner_id, COUNT(*) as wins
            FROM daily_game
            WHERE peer_id = ? AND date >= ? AND date < ?
            GROUP BY winner_id
            ORDER BY wins DESC, winner_id ASC
            """,
            (peer_id, month_start.isoformat(), next_month.isoformat())
        )
        month_rows = await cursor.fetchall()

        cursor = await db.execute(
            """
            SELECT winner_id, COUNT(*) as wins
            FROM daily_game
            WHERE peer_id = ?
            GROUP BY winner_id
            ORDER BY wins DESC, winner_id ASC
            """,
            (peer_id,)
        )
        all_rows = await cursor.fetchall()

    user_ids = list({uid for uid, _ in (month_rows + all_rows)})
    name_map = {}
    if user_ids:
        try:
            for i in range(0, len(user_ids), 1000):
                chunk = user_ids[i:i + 1000]
                users = await bot.api.users.get(user_ids=chunk)
                name_map.update({u.id: f"{u.first_name} {u.last_name}" for u in users})
        except Exception as e:
            log.exception("Failed to fetch leaderboard user names: %s", e)
            name_map = {}

    def format_rows(rows):
        if not rows:
            return "Нет данных."
        medals = {1: "🥇", 2: "🥈", 3: "🥉"}
        lines = []
        for idx, (uid, wins) in enumerate(rows, start=1):
            name = name_map.get(uid, f"id{uid}")
            medal = medals.get(idx)
            prefix = f"{idx}. {medal}" if medal else f"{idx}."
            lines.append(f"{prefix} [id{uid}|{name}] — ×{wins}")
        return "\n".join(lines)

    month_label = today.strftime("%m.%Y")
    return (
        f"{LEADERBOARD_TITLE}\n\n"
        f"🗓 За {month_label}:\n{format_rows(month_rows)}\n\n"
        f"🏆 За все время:\n{format_rows(all_rows)}"
    )

async def post_leaderboard(peer_id: int, month_key: str):
    if ALLOWED_PEER_IDS is not None and peer_id not in ALLOWED_PEER_IDS:
        log.info("Leaderboard skipped for peer_id=%s (not in allowed list)", peer_id)
        return
    try:
        text = await build_leaderboard_text(peer_id)
        await bot.api.messages.send(peer_id=peer_id, message=text, random_id=0)
        log.info("Leaderboard posted peer_id=%s month=%s", peer_id, month_key)
    except Exception as e:
        log.exception("Failed to send leaderboard to peer_id=%s: %s", peer_id, e)
        return
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "UPDATE leaderboard_schedule SET last_run_month = ? WHERE peer_id = ?",
            (month_key, peer_id)
        )
        await db.commit()

async def scheduler_loop():
    log.info("Scheduler started")
    while True:
        try:
            now = datetime.datetime.now(MSK_TZ)
            now_time = now.strftime("%H:%M")
            month_key = now.strftime("%Y-%m")
            last_day = last_day_of_month(now.year, now.month)
            async with aiosqlite.connect(DB_NAME) as db:
                if ALLOWED_PEER_IDS is not None:
                    placeholders = ", ".join(["?"] * len(ALLOWED_PEER_IDS))
                    cursor = await db.execute(
                        f"SELECT peer_id FROM schedules WHERE time = ? AND peer_id IN ({placeholders})",
                        (now_time, *ALLOWED_PEER_IDS)
                    )
                else:
                    cursor = await db.execute("SELECT peer_id FROM schedules WHERE time = ?", (now_time,))
                rows = await cursor.fetchall()
                if rows:
                    log.debug("Triggering scheduled games for time %s: %s chats", now_time, len(rows))
                    for (peer_id,) in rows:
                        asyncio.create_task(run_game_logic(peer_id, reset_if_exists=True))
                if ALLOWED_PEER_IDS is not None:
                    placeholders = ", ".join(["?"] * len(ALLOWED_PEER_IDS))
                    cursor = await db.execute(
                        f"SELECT peer_id, day, time, last_run_month FROM leaderboard_schedule WHERE time = ? AND peer_id IN ({placeholders})",
                        (now_time, *ALLOWED_PEER_IDS)
                    )
                else:
                    cursor = await db.execute(
                        "SELECT peer_id, day, time, last_run_month FROM leaderboard_schedule WHERE time = ?",
                        (now_time,)
                    )
                lb_rows = await cursor.fetchall()
                if lb_rows:
                    for peer_id, day, _, last_run_month in lb_rows:
                        try:
                            day_int = int(day)
                        except (TypeError, ValueError):
                            continue
                        effective_day = min(day_int, last_day)
                        if now.day != effective_day:
                            continue
                        if last_run_month == month_key:
                            continue
                        log.debug(
                            "Triggering leaderboard for peer_id=%s month=%s day=%s",
                            peer_id,
                            month_key,
                            effective_day,
                        )
                        asyncio.create_task(post_leaderboard(peer_id, month_key))
            await asyncio.sleep(60)
        except Exception as e:
            log.exception("Error in scheduler: %s", e)
            await asyncio.sleep(60)

# ================= МЕНЮ НАСТРОЕК =================

@bot.on.message(EqualsRule(CMD_SETTINGS))
async def show_settings(message: Message):
    if not await ensure_command_allowed(message, CMD_SETTINGS):
        return
    log.debug("Settings requested peer_id=%s user_id=%s", message.peer_id, message.from_id)
    if LLM_PROVIDER == "groq":
        game_provider_label = "Groq"
        game_key_short = GROQ_API_KEY[:5] + "..." if GROQ_API_KEY else "не задан"
        game_model = GROQ_MODEL
        game_temperature = GROQ_TEMPERATURE
    else:
        game_provider_label = "Venice"
        game_key_short = VENICE_API_KEY[:5] + "..." if VENICE_API_KEY else "не задан"
        game_model = VENICE_MODEL
        game_temperature = VENICE_TEMPERATURE

    if CHAT_LLM_PROVIDER == "groq":
        chat_provider_label = "Groq"
        chat_key_short = GROQ_API_KEY[:5] + "..." if GROQ_API_KEY else "не задан"
        chat_model = CHAT_GROQ_MODEL
        chat_temperature = CHAT_GROQ_TEMPERATURE
    else:
        chat_provider_label = "Venice"
        chat_key_short = VENICE_API_KEY[:5] + "..." if VENICE_API_KEY else "не задан"
        chat_model = CHAT_VENICE_MODEL
        chat_temperature = CHAT_VENICE_TEMPERATURE
    if ALLOWED_PEER_IDS is None:
        access_line = "без ограничений"
    else:
        if len(ALLOWED_PEER_IDS) == 1:
            peers_label = f"чат {ALLOWED_PEER_IDS[0]}"
        else:
            peers_label = "чаты " + ", ".join(str(pid) for pid in ALLOWED_PEER_IDS)
        if ADMIN_USER_ID:
            access_line = f"{peers_label}, ЛС admin {ADMIN_USER_ID}"
        else:
            access_line = f"{peers_label}, ЛС admin не настроены"
    chatbot_status = "включен" if CHATBOT_ENABLED else "выключен"
    schedule_time = None
    leaderboard_day = None
    leaderboard_time = None
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute("SELECT time FROM schedules WHERE peer_id = ?", (message.peer_id,))
        row = await cursor.fetchone()
        if row:
            schedule_time = row[0]
        cursor = await db.execute("SELECT day, time FROM leaderboard_schedule WHERE peer_id = ?", (message.peer_id,))
        row = await cursor.fetchone()
        if row:
            leaderboard_day, leaderboard_time = row
    if schedule_time:
        schedule_line = f"Таймер (МСК): `{schedule_time}`\n"
    else:
        schedule_line = "Таймер (МСК): не установлен\n"
    if leaderboard_day is not None and leaderboard_time:
        leaderboard_line = f"Лидерборд (МСК): `{int(leaderboard_day):02d}-{leaderboard_time.replace(':','-')}`\n"
    else:
        leaderboard_line = "Лидерборд (МСК): не установлен\n"
    text = (
        f"🎛 **Настройки бота**\n\n"
        f"🎮 **Игра LLM:** `{game_provider_label}`\n"
        f"• модель: `{game_model}`\n"
        f"• температура: `{game_temperature}`\n"
        f"• ключ: `{game_key_short}`\n\n"
        f"💬 **Чатбот LLM:** `{chat_provider_label}`\n"
        f"• модель: `{chat_model}`\n"
        f"• температура: `{chat_temperature}`\n"
        f"• ключ: `{chat_key_short}`\n\n"
        f"📦 **Провайдеры:** `groq`, `venice`\n"
        f"🔒 **Доступ:** {access_line}\n"
        f"🧭 **Peer ID:** `{message.peer_id}`\n"
        f"💬 **Чатбот:** `{chatbot_status}`\n"
        f"Последнее обновление: {format_build_date(BUILD_DATE)}\n"
        f"{schedule_line}\n"
        f"{leaderboard_line}\n"
        f"**⚙ Команды:**\n"
        f"• `{CMD_SET_PROVIDER} [chat|game] groq|venice` - Выбрать провайдера\n"
        f"• `{CMD_SET_MODEL} [chat|game] <провайдер> <id>` - Сменить модель\n"
        f"• `{CMD_SET_KEY} <провайдер> <ключ>` - Новый API ключ\n"
        f"• `{CMD_SET_TEMPERATURE} [chat|game] <0.0-2.0>` - Установить температуру\n"
        f"• `{CMD_LIST_MODELS} <провайдер>` - Список моделей (Live)\n\n"
        f"• `{CMD_PROMPT}` или `{CMD_PROMPT} <текст>` - Показать/обновить user prompt\n\n"
        f"**🎮 Игра:**\n"
        f"• `{CMD_RUN}` - Найти пидора дня\n"
        f"• `{CMD_RESET}` - Сброс результата сегодня\n"
        f"• `{CMD_LEADERBOARD}` - Лидерборд месяца и все время\n"
        f"• `{CMD_TIME_SET} 14:00` - Установить авто-поиск (МСК)\n"
        f"• `{CMD_TIME_RESET}` - Удалить таймер\n"
        f"• `{CMD_LEADERBOARD_TIMER_SET} 05-18-30` - Таймер лидерборда (МСК)\n"
        f"• `{CMD_LEADERBOARD_TIMER_RESET}` - Сброс таймера лидерборда"
    )
    await send_reply(message, text)
@bot.on.message(StartswithRule(CMD_LIST_MODELS))
async def list_models_handler(message: Message):
    if not await ensure_command_allowed(message, CMD_LIST_MODELS):
        return
    args = strip_command(message.text, CMD_LIST_MODELS).lower()
    if not args:
        await send_reply(message, f"❌ Укажи провайдера: groq или venice.\nПример: `{CMD_LIST_MODELS} groq`")
        return
    provider = args
    if provider not in ("groq", "venice"):
        await send_reply(message, "❌ Неверный провайдер. Используй: groq или venice.")
        return
    log.info("List models request peer_id=%s user_id=%s provider=%s", message.peer_id, message.from_id, provider)
    if provider == "groq":
        await send_reply(message, "🔄 Связываюсь с API Groq...")
        try:
            if not GROQ_API_KEY:
                raise RuntimeError("Не найден GROQ_API_KEY")
            if AsyncGroq is None:
                raise RuntimeError("Пакет groq не установлен")
            client = groq_client or AsyncGroq(api_key=GROQ_API_KEY)
            models_response = await client.models.list()
            active_models = sorted([m.id for m in models_response.data], key=lambda x: (not x.startswith("llama"), x))

            if not active_models:
                await send_reply(message, "❌ Список моделей пуст (возможно проблема с ключом).")
                return

            models_text = "\n".join([f"• `{m}`" for m in active_models[:20]])
            example_model = active_models[0] if active_models else "model_id"

            await send_reply(message, 
                f"📦 **Доступные модели (Live API):**\n\n{models_text}\n\n"
                f"Чтобы выбрать модель, отправь ID в формате:\n"
                f"{CMD_SET_MODEL} groq {example_model}"
            )
        except Exception as e:
            log.exception("Groq models list failed: %s", e)
            await send_reply(message, f"❌ Ошибка API:\n{e}")
        return

    await send_reply(message, "🔄 Связываюсь с API Venice...")
    try:
        if not VENICE_API_KEY:
            raise RuntimeError("Не найден VENICE_API_KEY")
        response = await venice_request("GET", "models")
        models_response = response.json()
        model_ids = sorted({m.get("id") for m in models_response.get("data", []) if m.get("id")})

        if not model_ids:
            await send_reply(message, "❌ Список моделей пуст (возможно проблема с ключом).")
            return

        models_text = "\n".join([f"• `{m}`" for m in model_ids[:20]])
        example_model = model_ids[0] if model_ids else "model_id"

        await send_reply(message, 
            f"📦 **Доступные модели (Live API):**\n\n{models_text}\n\n"
            f"Чтобы выбрать модель, отправь ID в формате:\n"
            f"{CMD_SET_MODEL} venice {example_model}"
        )
    except Exception as e:
        log.exception("Venice models list failed: %s", e)
        await send_reply(message, f"❌ Ошибка API:\n{e}")

# ================= USER PROMPT =================

@bot.on.message(StartswithRule(CMD_PROMPT))
async def prompt_handler(message: Message):
    if not await ensure_command_allowed(message, CMD_PROMPT):
        return
    args = strip_command(message.text, CMD_PROMPT)
    global USER_PROMPT_TEMPLATE
    if not args:
        log.info("Prompt requested peer_id=%s user_id=%s", message.peer_id, message.from_id)
        if USER_PROMPT_TEMPLATE:
            await send_reply(message, f"Текущий USER_PROMPT_TEMPLATE:\n{USER_PROMPT_TEMPLATE}")
        else:
            await send_reply(message, "USER_PROMPT_TEMPLATE не задан.")
        return
    updated = normalize_prompt(args)
    if not updated:
        await send_reply(message, "Промпт пустой.")
        return
    USER_PROMPT_TEMPLATE = updated
    os.environ["USER_PROMPT_TEMPLATE"] = updated
    log.info(
        "Prompt updated peer_id=%s user_id=%s length=%s",
        message.peer_id,
        message.from_id,
        len(updated),
    )
    await send_reply(message, "✅ USER_PROMPT_TEMPLATE обновлен (в памяти).")

# Лидерборд по текущему чату
@bot.on.message(EqualsRule(CMD_LEADERBOARD))
async def leaderboard_handler(message: Message):
    if not await ensure_command_allowed(message, CMD_LEADERBOARD):
        return
    log.info("Leaderboard requested peer_id=%s user_id=%s", message.peer_id, message.from_id)
    text = await build_leaderboard_text(message.peer_id)
    await send_reply(message, text)

@bot.on.message(StartswithRule(CMD_SET_MODEL))
async def set_model_handler(message: Message):
    if not await ensure_command_allowed(message, CMD_SET_MODEL):
        return
    global GROQ_MODEL, VENICE_MODEL, CHAT_GROQ_MODEL, CHAT_VENICE_MODEL
    args = strip_command(message.text, CMD_SET_MODEL)
    if not args:
        await send_reply(message, f"❌ Укажи провайдера и модель!\nПример: `{CMD_SET_MODEL} groq llama-3.3-70b-versatile`")
        return
    parts = args.split(maxsplit=2)
    scope = parse_llm_scope(parts[0]) if parts else None
    if scope:
        if len(parts) < 3:
            await send_reply(
                message,
                f"❌ Укажи зону (chat|game), провайдера и модель!\nПример: `{CMD_SET_MODEL} chat venice openai-gpt-oss-120b`",
            )
            return
        provider, model_id = parts[1].lower(), parts[2].strip()
    else:
        scope = "game"
        if len(parts) < 2:
            await send_reply(message, f"❌ Укажи провайдера и модель!\nПример: `{CMD_SET_MODEL} venice venice-uncensored`")
            return
        provider, model_id = parts[0].lower(), parts[1].strip()
    if provider not in ("groq", "venice"):
        await send_reply(message, "❌ Неверный провайдер. Доступно: groq или venice.")
        return
    if provider == "groq":
        if scope == "chat":
            CHAT_GROQ_MODEL = model_id
            os.environ["CHAT_GROQ_MODEL"] = model_id
            log.info(
                "Chat Groq model updated peer_id=%s user_id=%s model=%s",
                message.peer_id,
                message.from_id,
                CHAT_GROQ_MODEL,
            )
            await send_reply(message, f"✅ Модель Groq (чатбот) изменена на: `{CHAT_GROQ_MODEL}`")
            return
        GROQ_MODEL = model_id
        os.environ["GROQ_MODEL"] = model_id
        log.info(
            "Game Groq model updated peer_id=%s user_id=%s model=%s",
            message.peer_id,
            message.from_id,
            GROQ_MODEL,
        )
        await send_reply(message, f"✅ Модель Groq (игра) изменена на: `{GROQ_MODEL}`")
        return
    if scope == "chat":
        CHAT_VENICE_MODEL = model_id
        os.environ["CHAT_VENICE_MODEL"] = model_id
        log.info(
            "Chat Venice model updated peer_id=%s user_id=%s model=%s",
            message.peer_id,
            message.from_id,
            CHAT_VENICE_MODEL,
        )
        await send_reply(message, f"✅ Модель Venice (чатбот) изменена на: `{CHAT_VENICE_MODEL}`")
        return
    VENICE_MODEL = model_id
    os.environ["VENICE_MODEL"] = model_id
    log.info(
        "Game Venice model updated peer_id=%s user_id=%s model=%s",
        message.peer_id,
        message.from_id,
        VENICE_MODEL,
    )
    await send_reply(message, f"✅ Модель Venice (игра) изменена на: `{VENICE_MODEL}`")

@bot.on.message(StartswithRule(CMD_SET_PROVIDER))
async def set_provider_handler(message: Message):
    if not await ensure_command_allowed(message, CMD_SET_PROVIDER):
        return
    global LLM_PROVIDER, CHAT_LLM_PROVIDER, groq_client
    args = strip_command(message.text, CMD_SET_PROVIDER)
    if not args:
        await send_reply(message, f"❌ Укажи провайдера!\nПример: `{CMD_SET_PROVIDER} groq` или `{CMD_SET_PROVIDER} chat venice`")
        return
    parts = args.split()
    scope = parse_llm_scope(parts[0]) if parts else None
    if scope:
        if len(parts) < 2:
            await send_reply(message, f"❌ Укажи провайдера!\nПример: `{CMD_SET_PROVIDER} {scope} groq`")
            return
        provider = parts[1].lower()
    else:
        scope = "game"
        provider = parts[0].lower()
    if provider not in ("groq", "venice"):
        await send_reply(message, "❌ Неверный провайдер. Доступно: groq или venice.")
        return
    if provider == "groq":
        if not GROQ_API_KEY:
            await send_reply(message, "❌ Не найден GROQ_API_KEY. Сначала задай ключ.")
            return
        if AsyncGroq is None:
            await send_reply(message, "❌ Пакет groq не установлен.")
            return
        if not groq_client:
            groq_client = AsyncGroq(api_key=GROQ_API_KEY)
    else:
        if not VENICE_API_KEY:
            await send_reply(message, "❌ Не найден VENICE_API_KEY. Сначала задай ключ.")
            return
    if scope == "chat":
        CHAT_LLM_PROVIDER = provider
        os.environ["CHAT_LLM_PROVIDER"] = provider
        log.info(
            "Chat provider updated peer_id=%s user_id=%s provider=%s",
            message.peer_id,
            message.from_id,
            CHAT_LLM_PROVIDER,
        )
        await send_reply(message, f"✅ Провайдер чатбота изменен на: `{CHAT_LLM_PROVIDER}`")
        return
    LLM_PROVIDER = provider
    os.environ["LLM_PROVIDER"] = provider
    log.info(
        "Game provider updated peer_id=%s user_id=%s provider=%s",
        message.peer_id,
        message.from_id,
        LLM_PROVIDER,
    )
    await send_reply(message, f"✅ Провайдер игры изменен на: `{LLM_PROVIDER}`")

@bot.on.message(StartswithRule(CMD_SET_KEY))
async def set_key_handler(message: Message):
    if not await ensure_command_allowed(message, CMD_SET_KEY):
        return
    global GROQ_API_KEY, VENICE_API_KEY, groq_client
    args = strip_command(message.text, CMD_SET_KEY)
    if not args:
        await send_reply(message, f"❌ Укажи провайдера и ключ!\nПример: `{CMD_SET_KEY} groq gsk_***`")
        return
    parts = args.split(maxsplit=1)
    if len(parts) < 2:
        await send_reply(message, f"❌ Укажи провайдера и ключ!\nПример: `{CMD_SET_KEY} venice vnk_***`")
        return
    provider, key = parts[0].lower(), parts[1].strip()
    if provider not in ("groq", "venice"):
        await send_reply(message, "❌ Неверный провайдер. Доступно: groq или venice.")
        return
    if provider == "groq":
        if AsyncGroq is None:
            await send_reply(message, "❌ Пакет groq не установлен.")
            return
        GROQ_API_KEY = key
        os.environ["GROQ_API_KEY"] = key
        log.info(
            "Groq API key updated peer_id=%s user_id=%s length=%s",
            message.peer_id,
            message.from_id,
            len(key),
        )
        groq_client = AsyncGroq(api_key=GROQ_API_KEY)
        await send_reply(message, "✅ API ключ Groq сохранен.")
        return
    VENICE_API_KEY = key
    os.environ["VENICE_API_KEY"] = key
    log.info(
        "Venice API key updated peer_id=%s user_id=%s length=%s",
        message.peer_id,
        message.from_id,
        len(key),
    )
    await send_reply(message, "✅ API ключ Venice сохранен.")

# ================= НАСТРОЙКИ ТЕМПЕРАТУРЫ =================

@bot.on.message(StartswithRule(CMD_SET_TEMPERATURE))
async def set_temperature_handler(message: Message):
    if not await ensure_command_allowed(message, CMD_SET_TEMPERATURE):
        return
    global GROQ_TEMPERATURE, VENICE_TEMPERATURE, CHAT_GROQ_TEMPERATURE, CHAT_VENICE_TEMPERATURE
    args = strip_command(message.text, CMD_SET_TEMPERATURE)
    if not args:
        await send_reply(message, f"❌ Укажи температуру!\nПример: `{CMD_SET_TEMPERATURE} 0.9` или `{CMD_SET_TEMPERATURE} chat 0.7`")
        return
    parts = args.split(maxsplit=1)
    scope = parse_llm_scope(parts[0]) if parts else None
    if scope:
        if len(parts) < 2:
            await send_reply(message, f"❌ Укажи температуру!\nПример: `{CMD_SET_TEMPERATURE} {scope} 0.9`")
            return
        value_raw = parts[1]
    else:
        scope = "game"
        value_raw = args
    try:
        value = float(value_raw.replace(",", "."))
    except ValueError:
        await send_reply(message, "❌ Неверный формат температуры. Укажи число, например 0.7")
        return
    if value < 0 or value > 2:
        await send_reply(message, "❌ Температура должна быть в диапазоне 0.0-2.0")
        return
    if scope == "chat":
        if CHAT_LLM_PROVIDER == "groq":
            CHAT_GROQ_TEMPERATURE = value
            os.environ["CHAT_GROQ_TEMPERATURE"] = str(value)
            log.info(
                "Chat Groq temperature updated peer_id=%s user_id=%s value=%s",
                message.peer_id,
                message.from_id,
                CHAT_GROQ_TEMPERATURE,
            )
            await send_reply(message, f"✅ Температура Groq (чатбот) установлена: `{CHAT_GROQ_TEMPERATURE}`")
            return
        CHAT_VENICE_TEMPERATURE = value
        os.environ["CHAT_VENICE_TEMPERATURE"] = str(value)
        log.info(
            "Chat Venice temperature updated peer_id=%s user_id=%s value=%s",
            message.peer_id,
            message.from_id,
            CHAT_VENICE_TEMPERATURE,
        )
        await send_reply(message, f"✅ Температура Venice (чатбот) установлена: `{CHAT_VENICE_TEMPERATURE}`")
        return

    if LLM_PROVIDER == "groq":
        GROQ_TEMPERATURE = value
        os.environ["GROQ_TEMPERATURE"] = str(value)
        log.info(
            "Groq temperature updated peer_id=%s user_id=%s value=%s",
            message.peer_id,
            message.from_id,
            GROQ_TEMPERATURE,
        )
        await send_reply(message, f"✅ Температура Groq (игра) установлена: `{GROQ_TEMPERATURE}`")
        return
    VENICE_TEMPERATURE = value
    os.environ["VENICE_TEMPERATURE"] = str(value)
    log.info(
        "Venice temperature updated peer_id=%s user_id=%s value=%s",
        message.peer_id,
        message.from_id,
        VENICE_TEMPERATURE,
    )
    await send_reply(message, f"✅ Температура Venice (игра) установлена: `{VENICE_TEMPERATURE}`")

@bot.on.message(EqualsRule(CMD_RESET))
async def reset_daily_game(message: Message):
    if not await ensure_command_allowed(message, CMD_RESET):
        return
    peer_id = message.peer_id
    today = datetime.datetime.now(MSK_TZ).date().isoformat()
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("DELETE FROM daily_game WHERE peer_id = ? AND date = ?", (peer_id, today))
        await db.commit()
    log.info("Daily game reset peer_id=%s user_id=%s date=%s", peer_id, message.from_id, today)
    await send_reply(message, f"✅ Результат сброшен! Можно начинать заново.\nКоманда {CMD_RUN} снова выберет пидора дня.")

@bot.on.message(EqualsRule(CMD_RUN))
async def trigger_game(message: Message):
    if not await ensure_command_allowed(message, CMD_RUN):
        return
    log.info("Manual game trigger peer_id=%s user_id=%s", message.peer_id, message.from_id)
    await run_game_logic(message.peer_id)

@bot.on.message(StartswithRule(CMD_TIME_SET))
async def set_schedule(message: Message):
    if not await ensure_command_allowed(message, CMD_TIME_SET):
        return
    try:
        args = strip_command(message.text, CMD_TIME_SET)
        datetime.datetime.strptime(args, "%H:%M")
        async with aiosqlite.connect(DB_NAME) as db:
            await db.execute(
                "INSERT OR REPLACE INTO schedules (peer_id, time) VALUES (?, ?)", 
                (message.peer_id, args)
            )
            await db.commit()
        log.info("Schedule set peer_id=%s user_id=%s time=%s", message.peer_id, message.from_id, args)
        await send_reply(message, f"✅ Таймер установлен! Поиск пидора будет в {args}. (МСК)")
    except ValueError:
        await send_reply(message, f"❌ Неверный формат времени! Используй: `{CMD_TIME_SET} 14:00` (МСК)")
    except Exception as e:
        log.exception("Schedule set failed peer_id=%s user_id=%s: %s", message.peer_id, message.from_id, e)
        await send_reply(message, f"❌ Ошибка: {e}")

@bot.on.message(EqualsRule(CMD_TIME_RESET))
async def unset_schedule(message: Message):
    if not await ensure_command_allowed(message, CMD_TIME_RESET):
        return
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("DELETE FROM schedules WHERE peer_id = ?", (message.peer_id,))
        await db.commit()
    log.info("Schedule reset peer_id=%s user_id=%s", message.peer_id, message.from_id)
    await send_reply(message, "✅ Таймер сброшен.")

@bot.on.message(StartswithRule(CMD_LEADERBOARD_TIMER_SET))
async def set_leaderboard_timer(message: Message):
    if not await ensure_command_allowed(message, CMD_LEADERBOARD_TIMER_SET):
        return
    args = strip_command(message.text, CMD_LEADERBOARD_TIMER_SET)
    match = re.match(r"^(\d{1,2})-(\d{1,2})-(\d{1,2})$", args)
    if not match:
        await send_reply(message, f"❌ Неверный формат! Используй: `{CMD_LEADERBOARD_TIMER_SET} 05-18-30` (МСК)")
        return
    day = int(match.group(1))
    hour = int(match.group(2))
    minute = int(match.group(3))
    if day < 1 or day > 31 or hour < 0 or hour > 23 or minute < 0 or minute > 59:
        await send_reply(message, "❌ Неверная дата/время. Формат: ДД-ЧЧ-ММ (МСК)")
        return
    time_str = f"{hour:02d}:{minute:02d}"
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "INSERT OR REPLACE INTO leaderboard_schedule (peer_id, day, time, last_run_month) VALUES (?, ?, ?, NULL)",
            (message.peer_id, day, time_str)
        )
        await db.commit()
    log.info(
        "Leaderboard timer set peer_id=%s user_id=%s day=%s time=%s",
        message.peer_id,
        message.from_id,
        day,
        time_str,
    )
    await send_reply(message, f"✅ Таймер лидерборда установлен: `{day:02d}-{hour:02d}-{minute:02d}` (МСК)")

@bot.on.message(EqualsRule(CMD_LEADERBOARD_TIMER_RESET))
async def reset_leaderboard_timer(message: Message):
    if not await ensure_command_allowed(message, CMD_LEADERBOARD_TIMER_RESET):
        return
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("DELETE FROM leaderboard_schedule WHERE peer_id = ?", (message.peer_id,))
        await db.commit()
    log.info("Leaderboard timer reset peer_id=%s user_id=%s", message.peer_id, message.from_id)
    await send_reply(message, "✅ Таймер лидерборда сброшен.")

@bot.on.message(ChatbotTriggerRule())
async def mention_reply_handler(message: Message):
    if not message.text:
        return
    asyncio.create_task(store_message(message))
    text = message.text
    is_admin_dm = bool(
        ADMIN_USER_ID
        and message.from_id == ADMIN_USER_ID
        and message.peer_id == message.from_id
    )
    if not await ensure_message_allowed(message, action_label="чатботу"):
        return
    if not CHATBOT_ENABLED:
        await send_reply(message, "💤 Чатбот отключен администратором.")
        log.info("Chatbot disabled peer_id=%s user_id=%s", message.peer_id, message.from_id)
        return
    cleaned = text if is_admin_dm else strip_bot_mention(text)
    if not cleaned:
        await send_reply(message, "Напиши сообщение после упоминания.")
        return
    if cleaned.lstrip().startswith("/"):
        return
    try:
        cleaned_for_llm = trim_chat_text(cleaned)
        if not cleaned_for_llm:
            await send_reply(message, "Напиши сообщение после упоминания.")
            return
        reply_text = extract_reply_text(message)
        if reply_text:
            reply_text = trim_chat_text(reply_text)
            if reply_text:
                cleaned_for_llm = f"Контекст реплая: {reply_text}\n\n{cleaned_for_llm}"
        history_messages = await build_chat_history(message.peer_id, message.from_id)
        history_user = sum(1 for item in history_messages if item["role"] == "user")
        history_bot = len(history_messages) - history_user
        log.debug(
            "Chatbot context peer_id=%s user_id=%s history_user=%s history_bot=%s",
            message.peer_id,
            message.from_id,
            history_user,
            history_bot,
        )

        chat_messages = [{"role": "system", "content": CHAT_SYSTEM_PROMPT}]
        chat_messages.extend(history_messages)
        chat_messages.append({"role": "user", "content": cleaned_for_llm})
        response_text = await fetch_llm_messages(chat_messages, max_tokens=CHAT_MAX_TOKENS, target="chat")
        response_text = trim_text(response_text, CHAT_RESPONSE_MAX_CHARS)
        if not response_text:
            await send_reply(message, "❌ Ответ получился пустым. Попробуй позже.")
            return
        log.debug(
            "Chatbot response peer_id=%s user_id=%s chars=%s",
            message.peer_id,
            message.from_id,
            len(response_text),
        )
        await send_reply(message, response_text)
        response_for_store = trim_text(response_text, BOT_REPLY_FULL_MAX_CHARS)
        async with aiosqlite.connect(DB_NAME) as db:
            await db.execute(
                "INSERT INTO bot_dialogs (peer_id, user_id, role, text, timestamp) VALUES (?, ?, ?, ?, ?)",
                (message.peer_id, message.from_id, "user", trim_chat_text(cleaned), message.date),
            )
            if response_for_store:
                now_ts = int(datetime.datetime.now(MSK_TZ).timestamp())
                await db.execute(
                    "INSERT INTO bot_dialogs (peer_id, user_id, role, text, timestamp) VALUES (?, ?, ?, ?, ?)",
                    (message.peer_id, message.from_id, "assistant", response_for_store, now_ts),
                )
            await db.commit()
    except Exception as e:
        log.exception("Mention reply failed: %s", e)
        await send_reply(message, "❌ Ошибка ответа. Попробуй позже.")

@bot.on.message()
async def logger(message: Message):
    if not message.text:
        return
    await store_message(message)

async def start_background_tasks():
    await init_db()
    global BOT_GROUP_ID
    try:
        group_response = await bot.api.groups.get_by_id()
        BOT_GROUP_ID = extract_group_id(group_response)
        if not BOT_GROUP_ID:
            log.warning("Failed to detect BOT_GROUP_ID from API response")
        else:
            log.info("Detected BOT_GROUP_ID=%s", BOT_GROUP_ID)
    except Exception as e:
        log.exception("Failed to load group id: %s", e)
    asyncio.create_task(scheduler_loop())

if __name__ == "__main__":
    log.info("Starting %s bot...", GAME_TITLE)
    allowed_peers_label = "all" if ALLOWED_PEER_IDS is None else format_allowed_peers()
    log.info(
        "Config game_provider=%s chat_provider=%s allowed_peers=%s chatbot_enabled=%s",
        LLM_PROVIDER,
        CHAT_LLM_PROVIDER,
        allowed_peers_label,
        CHATBOT_ENABLED,
    )
    bot.loop_wrapper.on_startup.append(start_background_tasks())
    bot.run_forever()


