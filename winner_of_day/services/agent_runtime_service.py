from __future__ import annotations

import asyncio
import datetime
import json
import logging
import os
import random
import re
from typing import Any

import aiosqlite

from winner_of_day.agent import AgentActionResult, AgentDecision, build_agent_graph, build_initial_state
from winner_of_day.agent.checkpoint import AgentCheckpoint
from winner_of_day.agent.models import AgentState
from winner_of_day.agent.tools.vk_tools import VkTools

TRUE_VALUES = {"1", "true", "yes", "on"}
JSON_OBJECT_RE = re.compile(r"\{.*\}", flags=re.DOTALL)
AGENT_RESPONSE_FORMAT = {
    "type": "json_schema",
    "json_schema": {
        "name": "autonomous_vk_action",
        "schema": {
            "type": "object",
            "properties": {
                "action": {"type": "string", "enum": ["none", "send_message", "react"]},
                "text": {"type": "string"},
                "reply_to_cmid": {"type": "integer"},
                "target_cmid": {"type": "integer"},
                "reaction_id": {"type": "integer"},
                "reason": {"type": "string"},
            },
            "required": ["action", "text", "reply_to_cmid", "target_cmid", "reaction_id", "reason"],
            "additionalProperties": False,
        },
        "strict": True,
    },
}


def _coerce_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(default)


def _coerce_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _coerce_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if not text:
        return default
    return text in TRUE_VALUES


def _extract_text_from_llm_content(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        text = value.strip()
        return text or None
    if isinstance(value, list):
        parts: list[str] = []
        for item in value:
            if isinstance(item, str):
                chunk = item.strip()
                if chunk:
                    parts.append(chunk)
                continue
            if isinstance(item, dict):
                text_value = item.get("text") or item.get("output_text")
                if isinstance(text_value, str) and text_value.strip():
                    parts.append(text_value.strip())
        if not parts:
            return None
        return "\n".join(parts).strip() or None
    if isinstance(value, dict):
        return _extract_text_from_llm_content(value.get("text") or value.get("output_text") or value.get("content"))
    return None


def _extract_text_from_venice_response(response_data: dict[str, Any]) -> str | None:
    choices = response_data.get("choices")
    if isinstance(choices, list):
        for choice in choices:
            if not isinstance(choice, dict):
                continue
            message = choice.get("message")
            if isinstance(message, dict):
                text = _extract_text_from_llm_content(message.get("content"))
                if text:
                    return text
            text = _extract_text_from_llm_content(choice.get("text") or choice.get("output_text"))
            if text:
                return text
    return _extract_text_from_llm_content(response_data.get("text") or response_data.get("output_text"))


def _parse_json_object(raw_text: str) -> dict[str, Any] | None:
    text = str(raw_text or "").strip()
    if not text:
        return None
    if text.startswith("```"):
        text = text.strip("`")
        if "\n" in text:
            text = text.split("\n", 1)[1]
    text = text.strip()
    if text.endswith("```"):
        text = text[:-3].strip()
    try:
        parsed = json.loads(text)
        if isinstance(parsed, dict):
            return parsed
    except json.JSONDecodeError:
        pass
    match = JSON_OBJECT_RE.search(text)
    if match is None:
        return None
    snippet = match.group(0).strip()
    try:
        parsed = json.loads(snippet)
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else None


class AgentRuntimeService:
    def __init__(self, ctx, legacy_bot_module):
        self.ctx = ctx
        self.legacy_bot = legacy_bot_module
        self.log = logging.getLogger("winner_of_day_bot")
        self._checkpoint: AgentCheckpoint | None = None
        self._graph = None
        self._vk_tools: VkTools | None = None

    def _settings_get(self, key: str, default: str | None = None) -> str | None:
        settings = getattr(self.ctx, "settings", None)
        if settings is not None and hasattr(settings, "get"):
            value = settings.get(key, default)
        else:
            value = os.getenv(key, default)
        if value is None:
            return default
        text = str(value)
        return text if text != "" else default

    def _agent_enabled(self) -> bool:
        return _coerce_bool(self._settings_get("CHATBOT_AGENT_ENABLED", "false"), default=False)

    def _agent_engine(self) -> str:
        return str(self._settings_get("CHATBOT_AGENT_ENGINE", "legacy") or "legacy").strip().lower()

    def _agent_mode(self) -> str:
        mode = str(self._settings_get("CHATBOT_AGENT_MODE", "active") or "active").strip().lower()
        return mode if mode in {"active", "shadow"} else "active"

    def _agent_probability(self) -> float:
        return max(0.0, min(1.0, _coerce_float(self._settings_get("CHATBOT_AGENT_PROBABILITY", "0.35"), 0.35)))

    def _agent_cooldown_seconds(self) -> int:
        return max(0, _coerce_int(self._settings_get("CHATBOT_AGENT_COOLDOWN_SECONDS", "120"), 120))

    def _agent_min_messages_since_bot(self) -> int:
        return max(0, _coerce_int(self._settings_get("CHATBOT_AGENT_MIN_MESSAGES_SINCE_BOT", "4"), 4))

    def _agent_context_limit(self) -> int:
        return max(1, _coerce_int(self._settings_get("CHATBOT_AGENT_CONTEXT_LIMIT", "14"), 14))

    def _agent_max_tokens(self) -> int:
        return max(64, _coerce_int(self._settings_get("CHATBOT_AGENT_MAX_TOKENS", "260"), 260))

    def _agent_max_chars(self) -> int:
        return max(0, _coerce_int(self._settings_get("CHATBOT_AGENT_MAX_CHARS", "260"), 260))

    def _allow_reactions(self) -> bool:
        return _coerce_bool(self._settings_get("CHATBOT_AGENT_ALLOW_REACTIONS", "true"), default=True)

    def _allow_thread_reply(self) -> bool:
        return _coerce_bool(self._settings_get("CHATBOT_AGENT_ALLOW_THREAD_REPLY", "true"), default=True)

    def _checkpoint_path(self) -> str:
        return str(self._settings_get("CHATBOT_AGENT_CHECKPOINT_DB_PATH", "data/langgraph_agent.sqlite3"))

    def _agent_system_prompt(self) -> str:
        prompt = self._settings_get("CHATBOT_AGENT_SYSTEM_PROMPT", None)
        if prompt:
            return str(prompt)
        fallback = getattr(self.legacy_bot, "CHATBOT_AGENT_SYSTEM_PROMPT", "") or ""
        return str(fallback).strip()

    def _venice_model(self) -> str:
        for key in ("CHAT_VENICE_MODEL", "OPS_VENICE_MODEL", "VENICE_MODEL"):
            value = self._settings_get(key, None)
            if value:
                return value
        for attr in ("CHAT_VENICE_MODEL", "OPS_VENICE_MODEL", "VENICE_MODEL"):
            value = getattr(self.legacy_bot, attr, "")
            if value:
                return str(value)
        return "venice-uncensored"

    def _venice_temperature(self) -> float:
        for key in ("CHAT_VENICE_TEMPERATURE", "OPS_VENICE_TEMPERATURE", "VENICE_TEMPERATURE"):
            value = self._settings_get(key, None)
            if value is not None:
                return _coerce_float(value, 0.4)
        for attr in ("CHAT_VENICE_TEMPERATURE", "OPS_VENICE_TEMPERATURE", "VENICE_TEMPERATURE"):
            value = getattr(self.legacy_bot, attr, None)
            if value is not None:
                return _coerce_float(value, 0.4)
        return 0.4

    def _venice_include_system_prompt(self) -> bool:
        if hasattr(self.legacy_bot, "VENICE_INCLUDE_SYSTEM_PROMPT"):
            return bool(getattr(self.legacy_bot, "VENICE_INCLUDE_SYSTEM_PROMPT"))
        return True

    def _now_ts(self) -> int:
        return int(datetime.datetime.now(datetime.timezone.utc).timestamp())

    def _get_state_map(self, name: str) -> dict[int, Any]:
        state = getattr(self.ctx, "state", None)
        mapping = getattr(state, name, None)
        if isinstance(mapping, dict):
            return mapping
        return {}

    def _get_peer_lock(self, peer_id: int) -> asyncio.Lock:
        state = getattr(self.ctx, "state", None)
        if state is None:
            return asyncio.Lock()
        locks = getattr(state, "agent_locks", None)
        if not isinstance(locks, dict):
            locks = {}
            setattr(state, "agent_locks", locks)
        lock = locks.get(int(peer_id))
        if not isinstance(lock, asyncio.Lock):
            lock = asyncio.Lock()
            locks[int(peer_id)] = lock
        return lock

    def _is_message_eligible(self, message) -> bool:
        text = str(getattr(message, "text", "") or "").strip()
        if len(text) < 3:
            return False
        if text.startswith("/"):
            return False
        peer_id = _coerce_int(getattr(message, "peer_id", 0), 0)
        actor_id = _coerce_int(getattr(message, "from_id", 0), 0)
        if peer_id <= 0 or actor_id <= 0:
            return False
        if peer_id == actor_id:
            return False
        return True

    def _mark_bot_activity(self, peer_id: int) -> None:
        marker = getattr(self.legacy_bot, "mark_bot_activity", None)
        if callable(marker):
            try:
                marker(int(peer_id))
                return
            except Exception:
                pass
        last_map = self._get_state_map("last_bot_message_ts_by_peer")
        since_map = self._get_state_map("messages_since_bot_by_peer")
        now_ts = self._now_ts()
        last_map[int(peer_id)] = int(now_ts)
        since_map[int(peer_id)] = 0

    async def start(self) -> None:
        if not self._agent_enabled() or self._agent_engine() != "langgraph":
            return
        if self._graph is not None:
            return
        self._vk_tools = VkTools(self.ctx.bot)
        checkpointer = None
        try:
            self._checkpoint = AgentCheckpoint(self._checkpoint_path())
            checkpointer = await self._checkpoint.start()
        except Exception as e:
            self.log.exception("Agent checkpoint init failed: %s", e)
            self._checkpoint = None
            checkpointer = None
        self._graph = build_agent_graph(self, checkpointer=checkpointer)
        self.log.info(
            "Agent runtime initialized engine=%s mode=%s checkpoint=%s",
            self._agent_engine(),
            self._agent_mode(),
            self._checkpoint_path(),
        )

    async def stop(self) -> None:
        if self._checkpoint is not None:
            try:
                await self._checkpoint.stop()
            except Exception as e:
                self.log.debug("Agent checkpoint stop failed: %s", e)
        self._checkpoint = None
        self._graph = None

    async def handle_incoming_message(self, message) -> bool:
        if not self._agent_enabled() or self._agent_engine() != "langgraph":
            return False
        if not self._is_message_eligible(message):
            return False
        if self._graph is None:
            await self.start()
        if self._graph is None:
            return False

        peer_id = _coerce_int(getattr(message, "peer_id", 0), 0)
        since_map = self._get_state_map("messages_since_bot_by_peer")
        since_map[peer_id] = int(since_map.get(peer_id, 0) or 0) + 1

        lock = self._get_peer_lock(peer_id)
        async with lock:
            now_ts = self._now_ts()
            cooldown = self._agent_cooldown_seconds()
            min_messages = self._agent_min_messages_since_bot()
            probability = self._agent_probability()

            last_bot_ts = _coerce_int(self._get_state_map("last_bot_message_ts_by_peer").get(peer_id, 0), 0)
            if cooldown > 0 and now_ts - last_bot_ts < cooldown:
                return False
            if _coerce_int(since_map.get(peer_id, 0), 0) < min_messages:
                return False
            if probability <= 0:
                return False
            if probability < 1 and random.random() > probability:
                return False

            initial_state = build_initial_state(message)
            config = {"configurable": {"thread_id": str(peer_id)}}
            try:
                result_state = await self._graph.ainvoke(initial_state, config=config)
            except Exception as e:
                self.log.exception("Agent graph failed peer_id=%s user_id=%s: %s", peer_id, initial_state.get("actor_id"), e)
                return False
            action_result = AgentActionResult.from_value(dict(result_state or {}).get("action_result"))
            return bool(action_result.executed)

    async def observe(self, state: AgentState) -> AgentState:
        peer_id = int(state.get("peer_id") or 0)
        actor_id = int(state.get("actor_id") or 0)
        text = str(state.get("text") or "")
        context: dict[str, Any] = {
            "latest_user_text": text,
            "messages_since_bot": _coerce_int(self._get_state_map("messages_since_bot_by_peer").get(peer_id, 0), 0),
            "last_bot_message_ts": _coerce_int(self._get_state_map("last_bot_message_ts_by_peer").get(peer_id, 0), 0),
            "cooldown_seconds": self._agent_cooldown_seconds(),
        }
        try:
            summary_builder = getattr(self.legacy_bot, "build_chat_summary_prompt", None)
            if callable(summary_builder):
                summary_prompt = await summary_builder(peer_id)
                if summary_prompt:
                    context["summary_prompt"] = str(summary_prompt)
        except Exception as e:
            self.log.debug("Agent observe summary failed peer_id=%s: %s", peer_id, e)
        try:
            memory_builder = getattr(self.legacy_bot, "build_user_memory_prompt", None)
            if callable(memory_builder) and actor_id > 0:
                user_memory_prompt = await memory_builder(peer_id, actor_id)
                if user_memory_prompt:
                    context["user_memory_prompt"] = str(user_memory_prompt)
        except Exception as e:
            self.log.debug("Agent observe memory failed peer_id=%s user_id=%s: %s", peer_id, actor_id, e)
        try:
            context_builder = getattr(self.legacy_bot, "build_peer_chat_context_messages", None)
            if callable(context_builder):
                peer_turns = await context_builder(
                    peer_id,
                    limit=self._agent_context_limit(),
                    max_chars=2500,
                    line_max_chars=220,
                    skip_commands=True,
                    include_reply=True,
                    exclude_conversation_message_id=int(state.get("cmid") or 0),
                    scope="agent",
                )
                if peer_turns:
                    context["peer_turns"] = peer_turns
        except Exception as e:
            self.log.debug("Agent observe context failed peer_id=%s: %s", peer_id, e)
        return {"context": context}

    async def _venice_request_text(self, payload: dict[str, Any]) -> str:
        client = getattr(self.legacy_bot, "venice_client", None)
        if client is None:
            raise RuntimeError("venice_client_not_initialized")
        response = await client.request("POST", "chat/completions", json=payload)
        data = response.json()
        text = _extract_text_from_venice_response(data)
        if not text:
            raise ValueError("empty content in venice response")
        return str(text).strip()

    def _build_decide_payload(self, messages: list[dict[str, str]]) -> dict[str, Any]:
        return {
            "model": self._venice_model(),
            "messages": messages,
            "max_tokens": self._agent_max_tokens(),
            "temperature": self._venice_temperature(),
            "response_format": AGENT_RESPONSE_FORMAT,
            "venice_parameters": {
                "include_venice_system_prompt": self._venice_include_system_prompt(),
                "strip_thinking_response": True,
                "disable_thinking": True,
            },
        }

    def _sanitize_decision(self, decision: AgentDecision, state: AgentState) -> AgentDecision:
        max_chars = self._agent_max_chars()
        if max_chars > 0 and len(decision.text) > max_chars:
            decision.text = decision.text[:max_chars].strip()
        if decision.action == "send_message":
            if not decision.text:
                return AgentDecision(action="none", reason="empty_send_message")
            if not self._allow_thread_reply():
                decision.reply_to_cmid = 0
            return decision
        if decision.action == "react":
            if not self._allow_reactions():
                return AgentDecision(action="none", reason="reactions_disabled")
            if not (1 <= int(decision.reaction_id or 0) <= 16):
                return AgentDecision(action="none", reason="reaction_id_out_of_range")
            if int(decision.target_cmid or 0) <= 0:
                decision.target_cmid = int(state.get("cmid") or 0)
            if int(decision.target_cmid or 0) <= 0:
                return AgentDecision(action="none", reason="missing_target_cmid")
            return decision
        return AgentDecision(action="none", reason=decision.reason or "no_action")

    async def decide(self, state: AgentState) -> AgentState:
        prompt = self._agent_system_prompt()
        messages: list[dict[str, str]] = [{"role": "system", "content": prompt}]
        context = dict(state.get("context") or {})
        summary_prompt = str(context.get("summary_prompt") or "").strip()
        user_memory_prompt = str(context.get("user_memory_prompt") or "").strip()
        if summary_prompt:
            messages.append({"role": "system", "content": summary_prompt})
        if user_memory_prompt:
            messages.append({"role": "system", "content": user_memory_prompt})
        peer_turns = context.get("peer_turns")
        if isinstance(peer_turns, list):
            messages.extend(peer_turns[-self._agent_context_limit() :])
        user_payload = {
            "peer_id": int(state.get("peer_id") or 0),
            "actor_id": int(state.get("actor_id") or 0),
            "cmid": int(state.get("cmid") or 0),
            "text": str(state.get("text") or ""),
        }
        messages.append(
            {
                "role": "user",
                "content": f"Current message JSON:\n{json.dumps(user_payload, ensure_ascii=False, separators=(',', ':'))}",
            }
        )
        payload = self._build_decide_payload(messages)
        try:
            raw = await self._venice_request_text(payload)
        except Exception:
            fallback_payload = dict(payload)
            fallback_payload.pop("response_format", None)
            try:
                raw = await self._venice_request_text(fallback_payload)
            except Exception as e:
                self.log.exception(
                    "Agent decide failed peer_id=%s user_id=%s: %s",
                    state.get("peer_id"),
                    state.get("actor_id"),
                    e,
                )
                return {"decision": AgentDecision(action="none", reason="decide_error").to_dict(), "error": str(e)}
        parsed = _parse_json_object(raw)
        decision = AgentDecision.from_value(parsed)
        decision = self._sanitize_decision(decision, state)
        return {"decision": decision.to_dict()}

    async def act(self, state: AgentState) -> AgentState:
        decision = AgentDecision.from_value(state.get("decision"))
        if decision.action == "none":
            return {"action_result": AgentActionResult(executed=False, vk_method="none", error=decision.reason).to_dict()}
        if self._agent_mode() == "shadow":
            self.log.info(
                "Agent shadow decision peer_id=%s user_id=%s action=%s reason=%s",
                state.get("peer_id"),
                state.get("actor_id"),
                decision.action,
                decision.reason,
            )
            return {"action_result": AgentActionResult(executed=False, vk_method="shadow").to_dict()}
        if self._vk_tools is None:
            self._vk_tools = VkTools(self.ctx.bot)

        peer_id = int(state.get("peer_id") or 0)
        try:
            if decision.action == "send_message":
                reply_to = int(decision.reply_to_cmid or 0) if self._allow_thread_reply() else 0
                response_id = await self._vk_tools.send_message(peer_id=peer_id, text=decision.text, reply_to_cmid=reply_to)
                self._mark_bot_activity(peer_id)
                return {
                    "action_result": AgentActionResult(
                        executed=True,
                        vk_method="messages.send",
                        vk_response_id=int(response_id or 0),
                    ).to_dict()
                }
            if decision.action == "react":
                target_cmid = int(decision.target_cmid or state.get("cmid") or 0)
                if target_cmid <= 0:
                    return {
                        "action_result": AgentActionResult(
                            executed=False,
                            vk_method="messages.sendReaction",
                            error="missing_target_cmid",
                        ).to_dict()
                    }
                response_id = await self._vk_tools.send_reaction(
                    peer_id=peer_id,
                    cmid=target_cmid,
                    reaction_id=int(decision.reaction_id or 0),
                )
                self._mark_bot_activity(peer_id)
                return {
                    "action_result": AgentActionResult(
                        executed=True,
                        vk_method="messages.sendReaction",
                        vk_response_id=int(response_id or 1),
                    ).to_dict()
                }
        except Exception as e:
            self.log.exception(
                "Agent action failed peer_id=%s user_id=%s action=%s: %s",
                state.get("peer_id"),
                state.get("actor_id"),
                decision.action,
                e,
            )
            return {
                "action_result": AgentActionResult(
                    executed=False,
                    vk_method=decision.action,
                    error=str(e),
                ).to_dict()
            }
        return {"action_result": AgentActionResult(executed=False, vk_method="none").to_dict()}

    async def _store_action_artifact(self, state: AgentState, decision: AgentDecision, result: AgentActionResult) -> None:
        if not result.executed:
            return
        if decision.action != "send_message":
            return
        text = str(decision.text or "").strip()
        if not text:
            return
        db_name = str(getattr(self.legacy_bot, "DB_NAME", "chat_history.db"))
        timestamp = self._now_ts()
        try:
            async with aiosqlite.connect(db_name) as db:
                await db.execute(
                    "INSERT INTO bot_dialogs (peer_id, user_id, role, text, timestamp) VALUES (?, ?, ?, ?, ?)",
                    (
                        int(state.get("peer_id") or 0),
                        int(state.get("actor_id") or 0),
                        "assistant",
                        text,
                        int(timestamp),
                    ),
                )
                await db.commit()
        except Exception as e:
            self.log.debug(
                "Agent artifact persist failed peer_id=%s user_id=%s: %s",
                state.get("peer_id"),
                state.get("actor_id"),
                e,
            )

    async def record(self, state: AgentState) -> AgentState:
        decision = AgentDecision.from_value(state.get("decision"))
        action_result = AgentActionResult.from_value(state.get("action_result"))
        await self._store_action_artifact(state, decision, action_result)
        self.log.debug(
            "Agent record peer_id=%s user_id=%s action=%s executed=%s method=%s error=%s",
            state.get("peer_id"),
            state.get("actor_id"),
            decision.action,
            int(bool(action_result.executed)),
            action_result.vk_method,
            action_result.error,
        )
        return {}
