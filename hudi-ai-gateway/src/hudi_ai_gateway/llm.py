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

"""LLM provider factory.

Construction never touches the network, so the gateway always starts;
connectivity is checked lazily and reported through ``/ready``.
"""

from __future__ import annotations

import time

import httpx
from langchain_core.language_models.chat_models import BaseChatModel
from pydantic import SecretStr

from hudi_ai_gateway.config import GatewaySettings


def build_chat_model(settings: GatewaySettings) -> BaseChatModel:
    provider = settings.llm_provider
    if provider == "anthropic":
        from langchain_anthropic import ChatAnthropic

        return ChatAnthropic(  # type: ignore[call-arg]  # `model` is an init alias
            model=settings.llm_model,
            api_key=SecretStr(settings.anthropic_api_key),
            timeout=settings.llm_timeout_seconds,
        )
    if provider == "openai":
        from langchain_openai import ChatOpenAI

        return ChatOpenAI(
            model=settings.llm_model,
            api_key=SecretStr(settings.openai_api_key),
            timeout=settings.llm_timeout_seconds,
        )
    if provider == "ollama":
        from langchain_ollama import ChatOllama

        return ChatOllama(model=settings.llm_model, base_url=settings.ollama_base_url)
    if provider == "openai-compatible":
        from langchain_openai import ChatOpenAI

        return ChatOpenAI(
            model=settings.llm_model,
            base_url=settings.openai_base_url,
            api_key=SecretStr(settings.openai_api_key or "unused"),
            timeout=settings.llm_timeout_seconds,
        )
    raise ValueError(f"unsupported provider: {provider}")  # unreachable; config validates


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
