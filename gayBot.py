import os
import sys
import json
import random
import datetime
import asyncio
import aiosqlite
import traceback
import re
from collections import Counter
from vkbottle.bot import Bot, Message
from vkbottle.dispatch.rules import ABCRule # Для создания своего правила
import logging
import httpx
try:
    from groq import AsyncGroq
except ImportError:
    AsyncGroq = None

# ================= НАСТРОЙКИ =================
VK_TOKEN = os.getenv("VK_TOKEN")
LLM_PROVIDER = os.getenv("LLM_PROVIDER", "").strip().lower()

GROQ_API_KEY = os.getenv("GROQ_API_KEY")
GROQ_MODEL = os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile")
try:
    GROQ_TEMPERATURE = float(os.getenv("GROQ_TEMPERATURE", "0.9"))
except ValueError:
    GROQ_TEMPERATURE = 0.9

VENICE_API_KEY = os.getenv("VENICE_API_KEY")
VENICE_MODEL = os.getenv("VENICE_MODEL", "llama-3.3-70b")
VENICE_BASE_URL = os.getenv("VENICE_BASE_URL", "https://api.venice.ai/api/v1/")
if not VENICE_BASE_URL.endswith("/"):
    VENICE_BASE_URL += "/"

try:
    VENICE_TEMPERATURE = float(os.getenv("VENICE_TEMPERATURE", "0.9"))
except ValueError:
    VENICE_TEMPERATURE = 0.9

try:
    VENICE_TIMEOUT = float(os.getenv("VENICE_TIMEOUT", "30"))
except ValueError:
    VENICE_TIMEOUT = 30.0

VENICE_INCLUDE_SYSTEM_PROMPT = os.getenv("VENICE_INCLUDE_SYSTEM_PROMPT", "false").strip().lower() in (
    "1",
    "true",
    "yes",
)

if not LLM_PROVIDER:
    if VENICE_API_KEY and not GROQ_API_KEY:
        LLM_PROVIDER = "venice"
    else:
        LLM_PROVIDER = "groq"

BUILD_DATE = os.getenv("BUILD_DATE", "unknown")
BUILD_SHA = os.getenv("BUILD_SHA", "")

if not VK_TOKEN:
    print("❌ ОШИБКА: Не найден VK_TOKEN!")
    sys.exit(1)

if LLM_PROVIDER not in ("groq", "venice"):
    print("❌ ОШИБКА: LLM_PROVIDER должен быть groq или venice!")
    sys.exit(1)

if LLM_PROVIDER == "groq":
    if not GROQ_API_KEY:
        print("❌ ОШИБКА: Не найден GROQ_API_KEY при выбранном провайдере groq!")
        sys.exit(1)
    if AsyncGroq is None:
        print("❌ ОШИБКА: Пакет groq не установлен, но выбран провайдер groq!")
        sys.exit(1)
else:
    if not VENICE_API_KEY:
        print("❌ ОШИБКА: Не найден VENICE_API_KEY при выбранном провайдере venice!")
        sys.exit(1)

# === КОМАНДЫ ===
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

# 🔥 КЛАСС ПРАВИЛА (Чтобы работало startswith) 🔥
class StartswithRule(ABCRule[Message]):
    def __init__(self, prefix: str):
        self.prefix = prefix

    async def check(self, event: Message) -> bool:
        return event.text.startswith(self.prefix)

# 🔥 ПРОМПТ 🔥
def normalize_prompt(value: str) -> str:
    if not value:
        return ""
    return value.replace("\\r\\n", "\n").replace("\\n", "\n")

SYSTEM_PROMPT = (
    "Формат ответа — строго валидный JSON, только объект и только двойные кавычки. "
    "Пример: {\"user_id\": 123, \"reason\": \"...\"}\n"
    "Никакого текста вне JSON.\n"
)
USER_PROMPT_TEMPLATE = normalize_prompt(os.getenv("USER_PROMPT_TEMPLATE"))

if not USER_PROMPT_TEMPLATE:
    print("ERROR: Missing USER_PROMPT_TEMPLATE in environment")
    sys.exit(1)

def render_user_prompt(context_text: str) -> str:
    prompt = USER_PROMPT_TEMPLATE.replace("{{GAME_TITLE}}", GAME_TITLE)
    if "{{CHAT_LOG}}" in prompt:
        prompt = prompt.replace("{{CHAT_LOG}}", context_text)
    else:
        prompt = f"{prompt}\n\n{context_text}"
    return prompt


bot = Bot(token=VK_TOKEN)
groq_client = AsyncGroq(api_key=GROQ_API_KEY) if LLM_PROVIDER == "groq" and AsyncGroq else None

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
        await db.execute("CREATE TABLE IF NOT EXISTS daily_game (peer_id INTEGER, date TEXT, winner_id INTEGER, reason TEXT, PRIMARY KEY (peer_id, date))")
        await db.execute("CREATE TABLE IF NOT EXISTS last_winner (peer_id INTEGER PRIMARY KEY, winner_id INTEGER, timestamp INTEGER)")
        await db.execute("CREATE TABLE IF NOT EXISTS leaderboard_schedule (peer_id INTEGER PRIMARY KEY, day INTEGER, time TEXT, last_run_month TEXT)")
        await db.execute("CREATE TABLE IF NOT EXISTS schedules (peer_id INTEGER PRIMARY KEY, time TEXT)")
        await db.commit()

# ================= LLM ЛОГИКА =================
async def fetch_llm_content(system_prompt: str, user_prompt: str) -> str:
    if LLM_PROVIDER == "venice":
        print(f"DEBUG: Sending request to Venice. Model: {VENICE_MODEL}, Temp: {VENICE_TEMPERATURE}")
        payload = {
            "model": VENICE_MODEL,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": VENICE_TEMPERATURE,
            "max_tokens": 800,
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

    if not groq_client:
        raise RuntimeError("Groq client is not initialized")
    print(f"DEBUG: Sending request to Groq. Model: {GROQ_MODEL}, Temp: {GROQ_TEMPERATURE}")
    completion = await groq_client.chat.completions.create(
        model=GROQ_MODEL,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        temperature=GROQ_TEMPERATURE,
        max_tokens=800,
    )
    content = completion.choices[0].message.content
    if not content:
        raise ValueError("Empty content in Groq response")
    return content


async def choose_winner_via_llm(chat_log: list, excluded_user_id=None) -> dict:
    context_lines = []
    available_ids = set()
    
    for uid, text, name in chat_log:
        if excluded_user_id is not None and uid == excluded_user_id:
            continue
        if len(text.strip()) < 3:
            continue
        safe_name = name if name else "Unknown"
        context_lines.append(f"[{uid}] {safe_name}: {text}")
        available_ids.add(uid)

    if not context_lines:
        return {"user_id": 0, "reason": "Все молчат. Скучные натуралы."}

    context_text = "\n".join(context_lines)

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
            
        user_id = int(result.get('user_id', 0))
        if user_id not in available_ids:
            result['user_id'] = random.choice(list(available_ids))
        else:
            result['user_id'] = user_id
            
        return result

    except Exception as e:
        print(f"ERROR: LLM API error ({LLM_PROVIDER}): {type(e).__name__}: {e}")
        traceback.print_exc()
    
    # Fallback
    print("DEBUG: Using fallback selection")
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
    today = datetime.datetime.now(MSK_TZ).date().isoformat()
    last_winner_id = None
    exclude_user_id = None
    
    async def send_msg(text):
        try:
            await bot.api.messages.send(peer_id=peer_id, message=text, random_id=0)
        except Exception as e:
            print(f"ERROR sending message to {peer_id}: {e}")

    async with aiosqlite.connect(DB_NAME) as db:
        # 🔥 ЛОГИКА АВТО-СБРОСА 🔥
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
            except:
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

        cursor = await db.execute("""
            SELECT user_id, text, username 
            FROM messages 
            WHERE peer_id = ? 
            AND LENGTH(TRIM(text)) > 2
            ORDER BY timestamp DESC 
            LIMIT 200
        """, (peer_id,))
        rows = await cursor.fetchall()
        
        if len(rows) < 3:
            await send_msg("Мало сообщений. Пишите больше, чтобы я мог выбрать худшего.")
            return

        chat_log = list(reversed(rows))
        candidate_ids = {uid for uid, text, _ in chat_log if len(text.strip()) >= 3}
        if last_winner_id is not None and last_winner_id in candidate_ids and len(candidate_ids) > 1:
            exclude_user_id = last_winner_id

    await send_msg(f"🎲 Изучаю {len(chat_log)} сообщений... Кто же сегодня опозорится?")
    
    try:
        decision = await choose_winner_via_llm(chat_log, excluded_user_id=exclude_user_id)
        winner_id = decision['user_id']
        reason = decision.get('reason', 'Нет причины')
        
        if winner_id == 0:
            await send_msg("Ошибка выбора. Попробуйте позже.")
            return

    except Exception as e:
        print(f"ERROR in game logic: {e}")
        await send_msg("Ошибка при выборе победителя.")
        return

    try:
        user_data = await bot.api.users.get(user_ids=[winner_id])
        winner_name = f"{user_data[0].first_name} {user_data[0].last_name}"
    except:
        winner_name = "Жертва"

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
        f"👑 {GAME_TITLE.upper()} НАЙДЕН!\n"
        f"Поздравляем (нет): [id{winner_id}|{winner_name}]\n\n"
        f"💬 Вердикт:\n{reason}"
    )
# ================= ПЛАНИРОВЩИК =================
# ================= Лидерборд: утилиты =================
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
        except Exception:
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
    try:
        text = await build_leaderboard_text(peer_id)
        await bot.api.messages.send(peer_id=peer_id, message=text, random_id=0)
    except Exception as e:
        print(f"ERROR sending leaderboard to {peer_id}: {e}")
        return
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "UPDATE leaderboard_schedule SET last_run_month = ? WHERE peer_id = ?",
            (month_key, peer_id)
        )
        await db.commit()

async def scheduler_loop():
    print("⏰ Scheduler started...")
    while True:
        try:
            now = datetime.datetime.now(MSK_TZ)
            now_time = now.strftime("%H:%M")
            month_key = now.strftime("%Y-%m")
            last_day = last_day_of_month(now.year, now.month)
            async with aiosqlite.connect(DB_NAME) as db:
                cursor = await db.execute("SELECT peer_id FROM schedules WHERE time = ?", (now_time,))
                rows = await cursor.fetchall()
                if rows:
                    print(f"⏰ Triggering scheduled games for time {now_time}: {len(rows)} chats")
                    for (peer_id,) in rows:
                        asyncio.create_task(run_game_logic(peer_id))
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
                        asyncio.create_task(post_leaderboard(peer_id, month_key))
            await asyncio.sleep(60)
        except Exception as e:
            print(f"ERROR in scheduler: {e}")
            await asyncio.sleep(60)

# ================= МЕНЮ НАСТРОЕК =================

@bot.on.message(text=CMD_SETTINGS)
async def show_settings(message: Message):
    provider_label = "Groq" if LLM_PROVIDER == "groq" else "Venice"
    if LLM_PROVIDER == "groq":
        key_short = GROQ_API_KEY[:5] + "..." if GROQ_API_KEY else "???"
        active_model = GROQ_MODEL
        active_temperature = GROQ_TEMPERATURE
    else:
        key_short = VENICE_API_KEY[:5] + "..." if VENICE_API_KEY else "???"
        active_model = VENICE_MODEL
        active_temperature = VENICE_TEMPERATURE
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
        f"🎛 **Настройки игры**\n\n"
        f"🤖 **Провайдер:** `{provider_label}`\n"
        f"📦 **Доступные провайдеры:** `groq`, `venice`\n"
        f"🎯 **Модель:** `{active_model}`\n"
        f"🔑 **Ключ:** `{key_short}`\n"
        f"🌡 **Температура:** `{active_temperature}`\n"
        f"Последнее обновление: {format_build_date(BUILD_DATE)}\n"
        f"{schedule_line}\n"
        f"{leaderboard_line}\n"
        f"**⚙ Команды:**\n"
        f"• `{CMD_SET_PROVIDER} groq|venice` - Выбрать провайдера\n"
        f"• `{CMD_SET_MODEL} <id>` - Сменить модель\n"
        f"• `{CMD_SET_KEY} <провайдер> <ключ>` - Новый API ключ\n"
        f"• `{CMD_SET_TEMPERATURE} <0.0-2.0>` - Установить температуру\n"
        f"• `{CMD_LIST_MODELS} <провайдер>` - Список моделей (Live)\n\n"
        f"**🎮 Игра:**\n"
        f"• `{CMD_RUN}` - Найти пидора дня\n"
        f"• `{CMD_RESET}` - Сброс результата сегодня\n"
        f"• `{CMD_LEADERBOARD}` - Лидерборд месяца и все время\n"
        f"• `{CMD_TIME_SET} 14:00` - Установить авто-поиск (МСК)\n"
        f"• `{CMD_TIME_RESET}` - Удалить таймер\n"
        f"• `{CMD_LEADERBOARD_TIMER_SET} 05-18-30` - Таймер лидерборда (МСК)\n"
        f"• `{CMD_LEADERBOARD_TIMER_RESET}` - Сброс таймера лидерборда"
    )
    await message.answer(text)
@bot.on.message(text=CMD_LIST_MODELS)
async def list_models_handler(message: Message):
    args = message.text.replace(CMD_LIST_MODELS, "").strip().lower()
    if not args:
        await message.answer(f"❌ Укажи провайдера: groq или venice.\nПример: `{CMD_LIST_MODELS} groq`")
        return
    provider = args
    if provider not in ("groq", "venice"):
        await message.answer("❌ Неверный провайдер. Используй: groq или venice.")
        return
    if provider == "groq":
        await message.answer("🔄 Связываюсь с API Groq...")
        try:
            if not GROQ_API_KEY:
                raise RuntimeError("Не найден GROQ_API_KEY")
            if AsyncGroq is None:
                raise RuntimeError("Пакет groq не установлен")
            client = groq_client or AsyncGroq(api_key=GROQ_API_KEY)
            models_response = await client.models.list()
            active_models = sorted([m.id for m in models_response.data], key=lambda x: (not x.startswith("llama"), x))

            if not active_models:
                await message.answer("❌ Список моделей пуст (возможно проблема с ключом).")
                return

            models_text = "\n".join([f"• `{m}`" for m in active_models[:20]])
            example_model = active_models[0] if active_models else "ваша_модель"

            await message.answer(
                f"📜 **Доступные модели (Live API):**\n\n{models_text}\n\n"
                f"Чтобы применить, скопируй ID и напиши:\n"
                f"{CMD_SET_MODEL} {example_model}"
            )
        except Exception as e:
            await message.answer(f"❌ Ошибка API:\n{e}")
        return

    await message.answer("🔄 Связываюсь с API Venice...")
    try:
        if not VENICE_API_KEY:
            raise RuntimeError("Не найден VENICE_API_KEY")
        response = await venice_request("GET", "models")
        models_response = response.json()
        model_ids = sorted({m.get("id") for m in models_response.get("data", []) if m.get("id")})

        if not model_ids:
            await message.answer("❌ Список моделей пуст (возможно проблема с ключом).")
            return

        models_text = "\n".join([f"• `{m}`" for m in model_ids[:20]])
        example_model = model_ids[0] if model_ids else "ваша_модель"

        await message.answer(
            f"📜 **Доступные модели (Live API):**\n\n{models_text}\n\n"
            f"Чтобы применить, скопируй ID и напиши:\n"
            f"{CMD_SET_MODEL} {example_model}"
        )
    except Exception as e:
        await message.answer(f"❌ Ошибка API:\n{e}")

# ОБРАБОТЧИКИ С НОВЫМ ПРАВИЛОМ
@bot.on.message(text=CMD_LEADERBOARD)
async def leaderboard_handler(message: Message):
    text = await build_leaderboard_text(message.peer_id)
    await message.answer(text)

@bot.on.message(StartswithRule(CMD_SET_MODEL))
async def set_model_handler(message: Message):
    global GROQ_MODEL, VENICE_MODEL
    args = message.text.replace(CMD_SET_MODEL, "").strip()
    if not args:
        await message.answer(f"❌ Укажите модель!\nПример: `{CMD_SET_MODEL} llama-3.3-70b`")
        return
    if LLM_PROVIDER == "groq":
        GROQ_MODEL = args
        os.environ["GROQ_MODEL"] = args
        await message.answer(f"✅ Модель изменена на: `{GROQ_MODEL}`")
        return
    VENICE_MODEL = args
    os.environ["VENICE_MODEL"] = args
    await message.answer(f"✅ Модель изменена на: `{VENICE_MODEL}`")

@bot.on.message(StartswithRule(CMD_SET_PROVIDER))
async def set_provider_handler(message: Message):
    global LLM_PROVIDER, groq_client
    args = message.text.replace(CMD_SET_PROVIDER, "").strip().lower()
    if not args:
        await message.answer(f"Укажи провайдера!\nПример: `{CMD_SET_PROVIDER} groq`")
        return
    if args not in ("groq", "venice"):
        await message.answer("Неверный провайдер. Используй: groq или venice.")
        return
    if args == "groq":
        if not GROQ_API_KEY:
            await message.answer("❌ Не найден GROQ_API_KEY. Сначала укажи ключ.")
            return
        if AsyncGroq is None:
            await message.answer("❌ Пакет groq не установлен.")
            return
        groq_client = AsyncGroq(api_key=GROQ_API_KEY)
    else:
        if not VENICE_API_KEY:
            await message.answer("❌ Не найден VENICE_API_KEY. Сначала укажи ключ.")
            return
        groq_client = None
    LLM_PROVIDER = args
    os.environ["LLM_PROVIDER"] = args
    await message.answer(f"✅ Провайдер изменен на: `{LLM_PROVIDER}`")

@bot.on.message(StartswithRule(CMD_SET_KEY))
async def set_key_handler(message: Message):
    global GROQ_API_KEY, VENICE_API_KEY, groq_client
    args = message.text.replace(CMD_SET_KEY, "").strip()
    if not args:
        await message.answer(f"❌ Укажите провайдера и ключ!\nПример: `{CMD_SET_KEY} groq gsk_***`")
        return
    parts = args.split(maxsplit=1)
    if len(parts) < 2:
        await message.answer(f"❌ Укажите провайдера и ключ!\nПример: `{CMD_SET_KEY} venice vnk_***`")
        return
    provider, key = parts[0].lower(), parts[1].strip()
    if provider not in ("groq", "venice"):
        await message.answer("❌ Неверный провайдер. Используй: groq или venice.")
        return
    if provider == "groq":
        if AsyncGroq is None:
            await message.answer("❌ Пакет groq не установлен.")
            return
        GROQ_API_KEY = key
        os.environ["GROQ_API_KEY"] = key
        if LLM_PROVIDER == "groq":
            groq_client = AsyncGroq(api_key=GROQ_API_KEY)
            await message.answer("✅ API ключ Groq обновлен. Клиент перезапущен.")
        else:
            await message.answer("✅ API ключ Groq обновлен.")
        return
    VENICE_API_KEY = key
    os.environ["VENICE_API_KEY"] = key
    await message.answer("✅ API ключ Venice обновлен.")

# ================= ОБЫЧНЫЕ КОМАНДЫ =================

@bot.on.message(StartswithRule(CMD_SET_TEMPERATURE))
async def set_temperature_handler(message: Message):
    global GROQ_TEMPERATURE, VENICE_TEMPERATURE
    args = message.text.replace(CMD_SET_TEMPERATURE, "").strip()
    if not args:
        await message.answer(f"Укажи температуру!\nПример: `{CMD_SET_TEMPERATURE} 0.9`")
        return
    try:
        value = float(args.replace(",", "."))
    except ValueError:
        await message.answer("Неверное значение температуры. Используй число, например 0.7")
        return
    if value < 0 or value > 2:
        await message.answer("Температура должна быть в диапазоне 0.0-2.0")
        return
    if LLM_PROVIDER == "groq":
        GROQ_TEMPERATURE = value
        os.environ["GROQ_TEMPERATURE"] = str(value)
        await message.answer(f"Температура установлена: `{GROQ_TEMPERATURE}`")
        return
    VENICE_TEMPERATURE = value
    os.environ["VENICE_TEMPERATURE"] = str(value)
    await message.answer(f"Температура установлена: `{VENICE_TEMPERATURE}`")

@bot.on.message(text=CMD_RESET)
async def reset_daily_game(message: Message):
    peer_id = message.peer_id
    today = datetime.datetime.now(MSK_TZ).date().isoformat()
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("DELETE FROM daily_game WHERE peer_id = ? AND date = ?", (peer_id, today))
        await db.commit()
    await message.answer("🔄 Результаты аннулированы! Память стерта.\nПишите /кто чтобы выбрать нового пидора.")

@bot.on.message(text=CMD_RUN)
async def trigger_game(message: Message):
    await run_game_logic(message.peer_id)

@bot.on.message(StartswithRule(CMD_TIME_SET))
async def set_schedule(message: Message):
    try:
        args = message.text.replace(CMD_TIME_SET, "").strip()
        datetime.datetime.strptime(args, "%H:%M")
        async with aiosqlite.connect(DB_NAME) as db:
            await db.execute(
                "INSERT OR REPLACE INTO schedules (peer_id, time) VALUES (?, ?)", 
                (message.peer_id, args)
            )
            await db.commit()
        await message.answer(f"⏰ Таймер установлен! Буду искать жертву в {args}. (МСК)")
    except ValueError:
        await message.answer("❌ Неверный формат! Используйте: /время 14:00 (МСК)")
    except Exception as e:
        await message.answer(f"Ошибка: {e}")

@bot.on.message(text=CMD_TIME_RESET)
async def unset_schedule(message: Message):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("DELETE FROM schedules WHERE peer_id = ?", (message.peer_id,))
        await db.commit()
    await message.answer("🔕 Таймер удален.")

@bot.on.message(StartswithRule(CMD_LEADERBOARD_TIMER_SET))
async def set_leaderboard_timer(message: Message):
    args = message.text.replace(CMD_LEADERBOARD_TIMER_SET, "").strip()
    match = re.match(r"^(\d{1,2})-(\d{1,2})-(\d{1,2})$", args)
    if not match:
        await message.answer(f"❌ Неверный формат! Используйте: `{CMD_LEADERBOARD_TIMER_SET} 05-18-30` (МСК)")
        return
    day = int(match.group(1))
    hour = int(match.group(2))
    minute = int(match.group(3))
    if day < 1 or day > 31 or hour < 0 or hour > 23 or minute < 0 or minute > 59:
        await message.answer("❌ Неверные значения. Формат: ДД-ЧЧ-ММ (МСК)")
        return
    time_str = f"{hour:02d}:{minute:02d}"
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "INSERT OR REPLACE INTO leaderboard_schedule (peer_id, day, time, last_run_month) VALUES (?, ?, ?, NULL)",
            (message.peer_id, day, time_str)
        )
        await db.commit()
    await message.answer(f"✅ Таймер лидерборда установлен: `{day:02d}-{hour:02d}-{minute:02d}` (МСК)")

@bot.on.message(text=CMD_LEADERBOARD_TIMER_RESET)
async def reset_leaderboard_timer(message: Message):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("DELETE FROM leaderboard_schedule WHERE peer_id = ?", (message.peer_id,))
        await db.commit()
    await message.answer("✅ Таймер лидерборда сброшен.")

@bot.on.message()
async def logger(message: Message):
    if message.text and not message.text.startswith("/"):
        try:
            user_info = await message.get_user()
            username = f"{user_info.first_name} {user_info.last_name}"
        except:
            username = "Unknown"
        async with aiosqlite.connect(DB_NAME) as db:
            await db.execute(
                "INSERT INTO messages (user_id, peer_id, text, timestamp, username) VALUES (?, ?, ?, ?, ?)",
                (message.from_id, message.peer_id, message.text, message.date, username)
            )
            await db.commit()

async def start_background_tasks():
    await init_db()
    asyncio.create_task(scheduler_loop())

if __name__ == "__main__":
    print(f"🚀 Starting {GAME_TITLE} bot...")
    logging.basicConfig(level=logging.DEBUG)
    bot.loop_wrapper.on_startup.append(start_background_tasks())
    bot.run_forever()
