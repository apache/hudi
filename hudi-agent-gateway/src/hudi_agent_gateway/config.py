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

from pydantic import AliasChoices, Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

LLMProvider = Literal["anthropic", "openai", "ollama", "openai-compatible"]
LakehouseEngine = Literal["trino", "spark"]
SparkAuth = Literal["none", "nosasl", "ldap"]


class GatewaySettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="GATEWAY_", extra="ignore", populate_by_name=True
    )

    # --- server ---
    host: str = "0.0.0.0"
    port: int = 8000
    log_level: str = "INFO"

    # --- LLM ---
    llm_provider: LLMProvider = "ollama"
    llm_model: str = "qwen3:8b"
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

    # --- lakehouse engine ---
    # Which SQL engine serves the lakehouse tools. "trino" (default) queries a
    # Trino coordinator with the Hudi connector; "spark" queries a Spark
    # Thrift Server (HiveServer2 protocol), which additionally reads MOR
    # snapshots and Hudi 1.x tables that carry unstructured columns.
    engine: LakehouseEngine = "trino"

    # --- Trino backend ---
    trino_host: str = "hudi-trino.hudi-lakehouse.svc"
    trino_port: int = 8080
    trino_user: str = "hudi-agent-gateway"
    trino_catalog: str = "hudi"
    trino_schema: str = "default"

    # --- Spark Thrift Server backend ---
    spark_host: str = "localhost"
    spark_port: int = 10000
    spark_user: str = "hudi-agent-gateway"
    spark_database: str = "default"
    # HiveServer2 auth mode: "none" (SASL PLAIN, the Spark Thrift default),
    # "nosasl" (hive.server2.authentication=NOSASL), or "ldap" (user/password).
    spark_auth: SparkAuth = "none"
    spark_password: str = ""
    # Connection pooling: HiveServer2 sessions are expensive to open, so the
    # client keeps up to `spark_max_connections` live sessions (also the query
    # concurrency bound) and recycles them past the idle/lifetime limits.
    spark_max_connections: int = Field(default=8, ge=1, le=64)
    spark_pool_max_idle_seconds: float = Field(default=300.0, gt=0)
    spark_pool_max_lifetime_seconds: float = Field(default=1800.0, gt=0)

    # --- guardrails / shaping ---
    sql_row_cap: int = 200
    sql_timeout_seconds: float = 120.0
    tool_result_max_bytes: int = 50_000

    # --- agent / sessions ---
    agent_max_iterations: int = 25
    session_ttl_seconds: float = 3600.0
    max_sessions: int = 1000
    max_messages_per_session: int = 40
    system_prompt_extra: str = ""

    # --- surfaces ---
    mcp_enabled: bool = True

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
        if self.engine == "spark" and self.spark_auth == "ldap" and not self.spark_password:
            raise ValueError("GATEWAY_SPARK_AUTH=ldap requires GATEWAY_SPARK_PASSWORD")
        return self
