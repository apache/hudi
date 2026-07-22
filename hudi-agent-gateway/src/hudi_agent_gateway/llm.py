# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""LLM provider factory and provider-aware model discovery.

Construction never touches the network, so the gateway always starts;
connectivity is checked lazily and reported through ``/ready``. Model
discovery (``list_models``) asks the configured provider what it offers, so
the UI's model picker always reflects reality instead of a hardcoded list.
"""

from __future__ import annotations

import logging
import time

import httpx
from langchain_core.language_models.chat_models import BaseChatModel
from pydantic import SecretStr

from hudi_agent_gateway.config import GatewaySettings

logger = logging.getLogger("hudi_agent_gateway.llm")


def build_chat_model(settings: GatewaySettings, model: str | None = None) -> BaseChatModel:
    provider = settings.llm_provider
    model_name = model or settings.llm_model
    if provider == "anthropic":
        from langchain_anthropic import ChatAnthropic

        return ChatAnthropic(  # type: ignore[call-arg]  # `model` is an init alias
            model=model_name,
            api_key=SecretStr(settings.anthropic_api_key),
            timeout=settings.llm_timeout_seconds,
        )
    if provider == "openai":
        from langchain_openai import ChatOpenAI

        return ChatOpenAI(
            model=model_name,
            api_key=SecretStr(settings.openai_api_key),
            timeout=settings.llm_timeout_seconds,
        )
    if provider == "ollama":
        from langchain_ollama import ChatOllama

        # ChatOllama has no `timeout` field; the timeout must reach the
        # underlying httpx client, or a stalled Ollama hangs /v1/chat forever.
        return ChatOllama(
            model=model_name,
            base_url=settings.ollama_base_url,
            client_kwargs={"timeout": settings.llm_timeout_seconds},
        )
    if provider == "openai-compatible":
        from langchain_openai import ChatOpenAI

        return ChatOpenAI(
            model=model_name,
            base_url=settings.openai_base_url,
            api_key=SecretStr(settings.openai_api_key or "unused"),
            timeout=settings.llm_timeout_seconds,
        )
    raise ValueError(f"unsupported provider: {provider}")  # unreachable; config validates


async def list_models(settings: GatewaySettings, timeout: float = 5.0) -> list[str]:
    """The models offered by the configured provider, default model first.

    Falls back to just the configured default when the provider cannot be
    listed -- the picker degrades to a single entry, chat keeps working.
    """
    try:
        models = await _list_models_from_provider(settings, timeout)
    except Exception as e:  # noqa: BLE001 - discovery must never break the API
        logger.warning("model listing failed for provider %s: %s", settings.llm_provider, e)
        models = []
    default = settings.llm_model
    ordered = [default] + sorted(m for m in models if m != default)
    return ordered


async def _list_models_from_provider(settings: GatewaySettings, timeout: float) -> list[str]:
    provider = settings.llm_provider
    async with httpx.AsyncClient(timeout=timeout) as client:
        if provider == "anthropic":
            resp = await client.get(
                "https://api.anthropic.com/v1/models",
                headers={
                    "x-api-key": settings.anthropic_api_key,
                    "anthropic-version": "2023-06-01",
                },
                params={"limit": 100},
            )
            resp.raise_for_status()
            return [m["id"] for m in resp.json().get("data", [])]
        if provider == "openai":
            resp = await client.get(
                "https://api.openai.com/v1/models",
                headers={"Authorization": f"Bearer {settings.openai_api_key}"},
            )
            resp.raise_for_status()
            ids = [m["id"] for m in resp.json().get("data", [])]
            # Keep chat-capable families; drop embeddings/audio/image models.
            return [
                i
                for i in ids
                if (i.startswith("gpt-") or i.startswith("o"))
                and not any(x in i for x in ("embedding", "audio", "tts", "image", "dall-e"))
            ]
        if provider == "ollama":
            resp = await client.get(settings.ollama_base_url.rstrip("/") + "/api/tags")
            resp.raise_for_status()
            return [m["name"] for m in resp.json().get("models", [])]
        if provider == "openai-compatible":
            headers = {}
            if settings.openai_api_key:
                headers["Authorization"] = f"Bearer {settings.openai_api_key}"
            resp = await client.get(
                settings.openai_base_url.rstrip("/") + "/models", headers=headers
            )
            resp.raise_for_status()
            return [m["id"] for m in resp.json().get("data", [])]
    return []


class LLMReadiness:
    """Cheap, cached reachability check feeding ``/ready``.

    For local endpoints (ollama / openai-compatible) this probes the base URL;
    for hosted providers a configured API key is treated as ready -- a live
    call would cost money and rate limit.
    """

    def __init__(self, settings: GatewaySettings, cache_seconds: float = 10.0) -> None:
        self._settings = settings
        self._cache_seconds = cache_seconds
        self._cached_at = 0.0
        self._cached: tuple[bool, str] = (False, "not checked yet")

    async def check(self) -> tuple[bool, str]:
        now = time.monotonic()
        if now - self._cached_at < self._cache_seconds:
            return self._cached
        self._cached = await self._probe()
        self._cached_at = now
        return self._cached

    async def _probe(self) -> tuple[bool, str]:
        s = self._settings
        if s.llm_provider == "anthropic":
            return True, "api key configured"
        if s.llm_provider == "openai":
            return True, "api key configured"
        url = s.ollama_base_url if s.llm_provider == "ollama" else s.openai_base_url
        probe = url.rstrip("/") + ("/api/tags" if s.llm_provider == "ollama" else "/models")
        try:
            async with httpx.AsyncClient(timeout=3.0) as client:
                resp = await client.get(probe)
            if resp.status_code < 500:
                return True, f"reachable ({url})"
            return False, f"endpoint returned {resp.status_code} ({url})"
        except httpx.HTTPError as e:
            return False, f"unreachable ({url}): {type(e).__name__}"
