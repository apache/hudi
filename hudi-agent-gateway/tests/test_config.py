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
from pydantic import ValidationError

from hudi_agent_gateway.config import GatewaySettings


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for var in ("ANTHROPIC_API_KEY", "OPENAI_API_KEY"):
        monkeypatch.delenv(var, raising=False)


def test_defaults() -> None:
    s = GatewaySettings()
    assert s.llm_provider == "ollama"
    assert s.trino_catalog == "hudi"
    assert s.sql_row_cap == 200
    assert s.port == 8000


def test_env_prefix(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GATEWAY_SQL_ROW_CAP", "42")
    monkeypatch.setenv("GATEWAY_TRINO_HOST", "example")
    s = GatewaySettings()
    assert s.sql_row_cap == 42
    assert s.trino_host == "example"


def test_invalid_provider_rejected() -> None:
    with pytest.raises(ValidationError):
        GatewaySettings(llm_provider="bedrock")  # type: ignore[arg-type]


def test_anthropic_requires_key() -> None:
    with pytest.raises(ValidationError, match="ANTHROPIC_API_KEY"):
        GatewaySettings(llm_provider="anthropic")


def test_openai_requires_key() -> None:
    with pytest.raises(ValidationError, match="OPENAI_API_KEY"):
        GatewaySettings(llm_provider="openai")


def test_openai_compatible_requires_base_url() -> None:
    with pytest.raises(ValidationError, match="GATEWAY_OPENAI_BASE_URL"):
        GatewaySettings(llm_provider="openai-compatible")


def test_unprefixed_api_key_alias(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
    s = GatewaySettings(llm_provider="anthropic")
    assert s.anthropic_api_key == "sk-test"


def test_prefixed_api_key_alias(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GATEWAY_OPENAI_API_KEY", "sk-test2")
    s = GatewaySettings(llm_provider="openai")
    assert s.openai_api_key == "sk-test2"
