<p align="center">
  <img alt="Winner of Day Bot" src="https://img.shields.io/static/v1.svg?label=Winner%20of%20Day%20Bot&message=VK%20LLM%20Bot&color=111827&labelColor=111827&style=for-the-badge&logo=vk&logoColor=white" style="width: 100%; height: auto;">
</p>

![License](https://img.shields.io/badge/License-MIT-green)
![Python](https://img.shields.io/badge/Python-3.11-blue)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED)
![VK API](https://img.shields.io/badge/VK-Bot-4C75A3)
![LLM](https://img.shields.io/badge/LLM-Groq%20%7C%20Venice-333333)

VK-бот сообщества: выбирает «победителя дня» по истории чата, ведет лидерборд и отвечает как чатбот через LLM.

## Возможности
- выбор «победителя дня» с причиной и хранением результата в SQLite
- лидерборд за месяц и за все время
- Groq и Venice, смена провайдера/модели/температуры на лету
- авто-запуск игры и таймер лидерборда (МСК)
- чатбот по упоминанию и в ЛС администратора
- Docker Compose запуск

## Быстрый старт (Docker)

1) Создай `.env` (см. шаблон ниже)
2) Запусти контейнер:

```bash
docker compose up -d --build
```

3) Логи:

```bash
docker compose logs -f vk-bot
```

## Конфигурация (.env)

### Минимум для запуска
```
VK_TOKEN=vk1.a.***
USER_PROMPT_TEMPLATE=Лог чата:\n{{CHAT_LOG}}\n\nКто из них победитель дня? Ответ — строго JSON.
```

### LLM провайдеры
```
LLM_PROVIDER=groq|venice
LLM_MAX_TOKENS=800

GROQ_API_KEY=gsk_***
GROQ_MODEL=llama-3.3-70b-versatile
GROQ_TEMPERATURE=0.9

VENICE_API_KEY=vnk_***
VENICE_MODEL=venice-uncensored
VENICE_BASE_URL=https://api.venice.ai/api/v1/
VENICE_TEMPERATURE=0.9
VENICE_TIMEOUT=90
VENICE_STRIP_THINKING_RESPONSE=true
VENICE_DISABLE_THINKING=false
# Override for chat only (optional)
CHAT_VENICE_STRIP_THINKING_RESPONSE=
CHAT_VENICE_DISABLE_THINKING=
# Retry on transient network/5xx/429 (optional)
VENICE_RETRY_ATTEMPTS=2
VENICE_RETRY_BACKOFF_SECONDS=1.0
# reasoning effort: low|medium|high (optional)
VENICE_REASONING_EFFORT=
# Override for chat only (optional)
CHAT_VENICE_REASONING_EFFORT=
```

### Чатбот
```
CHATBOT_ENABLED=true
CHAT_SYSTEM_PROMPT=Отвечай кратко и по-русски.
CHAT_FINAL_ONLY_PROMPT=
CHAT_MAX_TOKENS=300
CHAT_RESPONSE_MAX_CHARS=600
CHAT_MESSAGE_MAX_CHARS=300
CHAT_HISTORY_LIMIT=6
CHAT_BOT_FULL_LIMIT=2
CHAT_BOT_SHORT_LIMIT=2
CHAT_BOT_FULL_MAX_CHARS=800
CHAT_BOT_SHORT_MAX_CHARS=160
```

### Доступ
```
ALLOWED_PEER_ID=2000000001,2000000002
ADMIN_USER_ID=123456789
```

### Игра и данные
```
GAME_TITLE=Лидер дня
LEADERBOARD_TITLE=📊 Лидерборд
DB_PATH=./data/chat_history.db
```

## Команды
Команды нечувствительны к регистру.

Игра:
- `/кто` — найти победителя дня
- `/сброс` — сброс результата сегодня
- `/лидерборд` — лидерборд месяца и все время
- `/время 14:00` — установить авто-запуск (МСК)
- `/сброс_времени` — удалить таймер
- `/таймер_лидерборда 05-18-30` — таймер лидерборда (МСК)
- `/сброс_таймера_лидерборда` — сброс таймера лидерборда

Настройки LLM:
- `/провайдер groq|venice`
- `/список_моделей <провайдер>`
- `/установить_модель <провайдер> <id>`
- `/установить_ключ <провайдер> <ключ>`
- `/установить_температуру <0.0-2.0>`

Промпт:
- `/промт` — показать текущий USER_PROMPT_TEMPLATE
- `/промт <текст>` — обновить USER_PROMPT_TEMPLATE (в памяти)

## Эксплуатация

Обновление и перезапуск:
```bash
git pull
docker compose up -d --build
```

## Данные и приватность
- SQLite хранится в `./data/chat_history.db`.
- Секреты хранятся в `.env`, не коммить в репозиторий.

## Примечания
- Главный файл запуска в контейнере: `bot.py`.
- Логи идут в STDOUT и подходят для `docker compose logs`.

## Лицензия
MIT, см. `LICENSE`.
