from __future__ import annotations

import asyncio


class DummySettings:
    def __init__(self, data: dict[str, str] | None = None):
        self.data = dict(data or {})

    def get(self, key: str, default=None):
        return self.data.get(key, default)


class DummyState:
    def __init__(self):
        self.last_bot_message_ts_by_peer: dict[int, int] = {}
        self.messages_since_bot_by_peer: dict[int, int] = {}
        self.agent_locks: dict[int, asyncio.Lock] = {}


class DummyApi:
    def __init__(self):
        self.calls: list[tuple[str, dict]] = []

    async def request(self, method: str, payload: dict):
        self.calls.append((method, dict(payload)))
        if method == "messages.send":
            return 101
        if method == "messages.sendReaction":
            return 1
        return 0


class DummyBot:
    def __init__(self):
        self.api = DummyApi()


class DummyContext:
    def __init__(self, *, settings: dict[str, str] | None = None):
        self.settings = DummySettings(settings)
        self.state = DummyState()
        self.repos = {}
        self.services = {}
        self.bot = DummyBot()


class DummyVeniceResponse:
    def __init__(self, payload: dict):
        self._payload = payload

    def json(self):
        return self._payload


class DummyVeniceClient:
    def __init__(self, payload: dict | Exception):
        self.payload = payload
        self.calls = 0

    async def request(self, method: str, path: str, **kwargs):
        self.calls += 1
        if isinstance(self.payload, Exception):
            raise self.payload
        return DummyVeniceResponse(self.payload)


class DummyLegacyBot:
    DB_NAME = ":memory:"
    CHATBOT_AGENT_SYSTEM_PROMPT = "agent system"
    CHAT_VENICE_MODEL = "venice-test-model"
    CHAT_VENICE_TEMPERATURE = 0.3
    VENICE_INCLUDE_SYSTEM_PROMPT = True

    def __init__(self, venice_payload: dict | Exception | None = None):
        self.venice_client = DummyVeniceClient(
            venice_payload
            if venice_payload is not None
            else {
                "choices": [
                    {
                        "message": {
                            "content": (
                                '{"action":"none","text":"","reply_to_cmid":0,'
                                '"target_cmid":0,"reaction_id":0,"reason":"ok"}'
                            )
                        }
                    }
                ]
            }
        )

    async def build_chat_summary_prompt(self, peer_id: int):
        return f"summary:{peer_id}"

    async def build_user_memory_prompt(self, peer_id: int, user_id: int):
        return f"memory:{peer_id}:{user_id}"

    async def build_peer_chat_context_messages(
        self,
        peer_id: int,
        limit: int,
        max_chars: int,
        line_max_chars: int,
        skip_commands: bool,
        include_reply: bool,
        exclude_conversation_message_id: int,
        scope: str,
    ):
        return [{"role": "user", "content": f"ctx:{peer_id}:{limit}:{scope}"}]

    def mark_bot_activity(self, peer_id: int):
        return None


class DummyMessage:
    def __init__(self, *, peer_id: int = 2000000001, from_id: int = 123, text: str = "hello", cmid: int = 42):
        self.peer_id = peer_id
        self.from_id = from_id
        self.text = text
        self.conversation_message_id = cmid
