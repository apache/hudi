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

from __future__ import annotations

import pytest
from langchain_anthropic import ChatAnthropic
from langchain_ollama import ChatOllama
from langchain_openai import ChatOpenAI

from hudi_ai_gateway.config import GatewaySettings
from hudi_ai_gateway.llm import LLMReadiness, build_chat_model


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for var in ("ANTHROPIC_API_KEY", "OPENAI_API_KEY"):
        monkeypatch.delenv(var, raising=False)


def test_anthropic_factory() -> None:
    s = GatewaySettings(
        llm_provider="anthropic", llm_model="claude-sonnet-5", anthropic_api_key="sk-x"
    )
    model = build_chat_model(s)
    assert isinstance(model, ChatAnthropic)


def test_openai_factory() -> None:
    s = GatewaySettings(llm_provider="openai", llm_model="gpt-4o", openai_api_key="sk-x")
    assert isinstance(build_chat_model(s), ChatOpenAI)


def test_ollama_factory_plumbs_base_url() -> None:
    s = GatewaySettings(
        llm_provider="ollama", llm_model="qwen3:8b", ollama_base_url="http://host:11434"
    )
    model = build_chat_model(s)
    assert isinstance(model, ChatOllama)
    assert model.base_url == "http://host:11434"


def test_openai_compatible_factory_plumbs_base_url() -> None:
    s = GatewaySettings(
        llm_provider="openai-compatible",
        llm_model="my-vllm-model",
        openai_base_url="http://vllm:8000/v1",
    )
    model = build_chat_model(s)
    assert isinstance(model, ChatOpenAI)
    assert str(model.openai_api_base) == "http://vllm:8000/v1"


async def test_hosted_provider_ready_without_network() -> None:
    s = GatewaySettings(llm_provider="anthropic", anthropic_api_key="sk-x")
    ok, detail = await LLMReadiness(s).check()
    assert ok and "key" in detail


async def test_local_provider_unreachable_reports_not_ready() -> None:
    s = GatewaySettings(llm_provider="ollama", ollama_base_url="http://127.0.0.1:1")
    ok, detail = await LLMReadiness(s).check()
    assert not ok and "unreachable" in detail
