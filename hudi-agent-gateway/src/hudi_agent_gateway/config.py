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

"""Gateway configuration.

Every knob is an environment variable prefixed ``GATEWAY_`` (except the
provider API keys, which use their standard unprefixed names). Configuration
errors fail fast at startup; connectivity problems do not -- they surface
through ``/ready`` instead.
"""

from __future__ import annotations

from typing import Literal
from urllib.parse import urlparse

from pydantic import AliasChoices, Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

LLMProvider = Literal["anthropic", "openai", "ollama", "openai-compatible"]


class GatewaySettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="GATEWAY_", extra="ignore", populate_by_name=True)

    # --- server ---
    host: str = "0.0.0.0"
    port: int = 8000
    log_level: str = "INFO"

    # --- LLM ---
    llm_provider: LLMProvider = "ollama"
    llm_model: str = "qwen3:4b-instruct"
    llm_timeout_seconds: float = 120.0
    # Standard unprefixed key names, also accepted with the GATEWAY_ prefix.
    anthropic_api_key: str = Field(
        default="",
        validation_alias=AliasChoices("ANTHROPIC_API_KEY", "GATEWAY_ANTHROPIC_API_KEY"),
    )
    openai_api_key: str = Field(
        default="",
        validation_alias=AliasChoices("OPENAI_API_KEY", "GATEWAY_OPENAI_API_KEY"),
    )
    ollama_base_url: str = "http://localhost:11434"
    openai_base_url: str = ""  # required for provider=openai-compatible (vLLM etc.)

    # --- lakehouse ---
    sql_backend: Literal["trino", "spark-connect"] = "trino"
    spark_connect_uri: str = "sc://hudi-spark-connect.hudi-lakehouse.svc:15002"
    spark_connect_catalog: str = "spark_catalog"
    spark_connect_schema: str = "default"

    # --- Trino ---
    trino_host: str = "hudi-trino.hudi-lakehouse.svc"
    trino_port: int = 8080
    trino_user: str = "hudi-agent-gateway"
    trino_catalog: str = "hudi"
    trino_schema: str = "default"

    # --- guardrails / shaping ---
    sql_row_cap: int = Field(default=200, gt=0)
    sql_timeout_seconds: float = Field(default=120.0, gt=0)
    tool_result_max_bytes: int = Field(default=50_000, ge=80)

    # --- agent / sessions ---
    agent_max_iterations: int = 25
    chat_timeout_seconds: float = Field(default=300.0, gt=0)
    session_ttl_seconds: float = 3600.0
    max_sessions: int = 1000
    max_messages_per_session: int = 40
    system_prompt_extra: str = ""

    # --- surfaces ---
    mcp_enabled: bool = True

    @property
    def sql_dialect(self) -> str:
        return "spark" if self.sql_backend == "spark-connect" else "trino"

    @property
    def lakehouse_catalog(self) -> str:
        return (
            self.spark_connect_catalog
            if self.sql_backend == "spark-connect"
            else self.trino_catalog
        )

    @property
    def lakehouse_schema(self) -> str:
        return (
            self.spark_connect_schema if self.sql_backend == "spark-connect" else self.trino_schema
        )

    @model_validator(mode="after")
    def _validate_spark_connect(self) -> GatewaySettings:
        if self.sql_backend == "spark-connect":
            try:
                # PySpark parses semicolon parameters with an HTTP URI parser.
                uri = urlparse("http" + self.spark_connect_uri[2:])
                port = uri.port
            except ValueError as e:
                raise ValueError(
                    "GATEWAY_SPARK_CONNECT_URI requires a valid URI with a port from 1 to 65535"
                ) from e
            if port is not None and not 1 <= port <= 65535:
                raise ValueError("GATEWAY_SPARK_CONNECT_URI port must be from 1 to 65535")
            if (
                not self.spark_connect_uri.startswith("sc://")
                or not uri.hostname
                or uri.username is not None
                or uri.password is not None
                or uri.path not in ("", "/")
                or uri.query
                or uri.fragment
                or (uri.params and any(len(p.split("=")) != 2 for p in uri.params.split(";")))
            ):
                raise ValueError(
                    "GATEWAY_SPARK_CONNECT_URI requires sc://host:port "
                    "with optional /;key=value parameters"
                )
        return self

    @model_validator(mode="after")
    def _validate_provider_requirements(self) -> GatewaySettings:
        if self.llm_provider == "anthropic" and not self.anthropic_api_key:
            raise ValueError("GATEWAY_LLM_PROVIDER=anthropic requires ANTHROPIC_API_KEY")
        if self.llm_provider == "openai" and not self.openai_api_key:
            raise ValueError("GATEWAY_LLM_PROVIDER=openai requires OPENAI_API_KEY")
        if self.llm_provider == "openai-compatible" and not self.openai_base_url:
            raise ValueError(
                "GATEWAY_LLM_PROVIDER=openai-compatible requires GATEWAY_OPENAI_BASE_URL"
            )
        return self
