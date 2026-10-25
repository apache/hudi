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

    trino_ok = await state.trino_client.ping()
    checks["trino"] = DependencyStatus(
        ok=trino_ok,
        detail="reachable" if trino_ok else
        f"cannot reach trino at {state.settings.trino_host}:{state.settings.trino_port}",
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
    return InfoResponse(
        name="hudi-agent-gateway",
        version=__version__,
        provider=s.llm_provider,
        model=s.llm_model,
        catalog=s.trino_catalog,
        schema=s.trino_schema,
        trino_url=f"http://{s.trino_host}:{s.trino_port}",
        trino_ui_url=f"http://{s.trino_host}:{s.trino_port}/ui/",
        mcp_enabled=s.mcp_enabled,
    )
