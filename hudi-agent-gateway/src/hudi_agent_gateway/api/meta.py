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

"""Operational endpoints: /health, /ready, /v1/tools, /v1/info."""

from __future__ import annotations

import time
from urllib.parse import urlparse

from fastapi import APIRouter, Request, Response

from hudi_agent_gateway import __version__
from hudi_agent_gateway.api.models import (
    DependencyStatus,
    InfoResponse,
    ModelsResponse,
    ReadyResponse,
)
from hudi_agent_gateway.llm import list_models

router = APIRouter()


@router.get("/health")
async def health() -> dict[str, str]:
    """Liveness: the process is up and serving."""
    return {"status": "ok", "version": __version__}


@router.get("/ready", response_model=ReadyResponse)
async def ready(request: Request, response: Response) -> ReadyResponse:
    """Readiness: configuration is valid AND dependencies are reachable."""
    state = request.app.state
    checks: dict[str, DependencyStatus] = {}

    lakehouse_ok = await state.lakehouse_client.ping()
    backend = state.settings.sql_backend
    checks[backend] = DependencyStatus(
        ok=lakehouse_ok,
        detail="reachable" if lakehouse_ok else
        f"cannot query {backend}; check connection settings, client driver, and server logs",
    )

    llm_ok, llm_detail = await state.llm_readiness.check()
    checks["llm"] = DependencyStatus(ok=llm_ok, detail=llm_detail)

    all_ok = all(c.ok for c in checks.values())
    if not all_ok:
        response.status_code = 503
    return ReadyResponse(ready=all_ok, checks=checks)


@router.get("/v1/tools")
async def list_tools(request: Request) -> dict[str, list[dict[str, object]]]:
    return {"tools": request.app.state.registry.listing()}


@router.get("/v1/models", response_model=ModelsResponse)
async def models(request: Request) -> ModelsResponse:
    """Models offered by the configured provider (live-discovered, 60s cache)."""
    state = request.app.state
    cache = getattr(state, "models_cache", None)
    if cache is None or time.monotonic() - cache[0] > 60.0:
        listed = await list_models(state.settings)
        cache = (time.monotonic(), listed)
        state.models_cache = cache
    return ModelsResponse(
        provider=state.settings.llm_provider,
        default=state.settings.llm_model,
        models=cache[1],
    )


@router.get("/v1/info", response_model=InfoResponse, response_model_by_alias=True)
async def info(request: Request) -> InfoResponse:
    s = request.app.state.settings
    lakehouse_url = f"http://{s.trino_host}:{s.trino_port}"
    if s.sql_backend == "spark-connect":
        uri = urlparse("http" + s.spark_connect_uri[2:])
        host = uri.hostname or ""
        if ":" in host:
            host = f"[{host}]"
        lakehouse_url = f"sc://{host}:{uri.port or 15002}"
    return InfoResponse(
        name="hudi-agent-gateway",
        version=__version__,
        provider=s.llm_provider,
        model=s.llm_model,
        catalog=s.lakehouse_catalog,
        schema=s.lakehouse_schema,
        sql_backend=s.sql_backend,
        lakehouse_url=lakehouse_url,
        trino_url=f"http://{s.trino_host}:{s.trino_port}" if s.sql_backend == "trino" else "",
        trino_ui_url=(
            f"http://{s.trino_host}:{s.trino_port}/ui/" if s.sql_backend == "trino" else ""
        ),
        mcp_enabled=s.mcp_enabled,
    )
