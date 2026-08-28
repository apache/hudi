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

"""Application factory: one process, three surfaces (HTTP API, MCP, chat UI)."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from importlib import resources

from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles

from hudi_agent_gateway import __version__
from hudi_agent_gateway.agent import AgentCache
from hudi_agent_gateway.api import chat, meta
from hudi_agent_gateway.config import GatewaySettings
from hudi_agent_gateway.llm import LLMReadiness
from hudi_agent_gateway.log import log_event, setup_logging
from hudi_agent_gateway.mcp_server import build_mcp
from hudi_agent_gateway.sessions import SessionStore
from hudi_agent_gateway.tools import build_registry
from hudi_agent_gateway.tools.trino_client import TrinoClient

logger = logging.getLogger("hudi_agent_gateway.app")


def create_app(settings: GatewaySettings | None = None) -> FastAPI:
    s = settings or GatewaySettings()
    setup_logging(s.log_level)

    trino_client = TrinoClient(s)
    registry = build_registry(s, trino_client)
    # NOTE: the MCP ASGI sub-app has its own lifespan that MUST run inside the
    # outer app's lifespan, or FastMCP's streamable-HTTP session manager never
    # starts and /mcp requests hang.
    mcp_app = build_mcp(registry).http_app(path="/", stateless_http=True) if s.mcp_enabled else None

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        app.state.settings = s
        app.state.trino_client = trino_client
        app.state.registry = registry
        app.state.llm_readiness = LLMReadiness(s)
        app.state.sessions = SessionStore(s)
        app.state.agents = AgentCache(registry, app.state.sessions.checkpointer, s)
        sweeper = asyncio.create_task(app.state.sessions.sweep_loop())
        log_event(
            logger,
            "gateway_started",
            version=__version__,
            provider=s.llm_provider,
            model=s.llm_model,
            tools=len(registry),
            mcp_enabled=s.mcp_enabled,
        )
        try:
            if mcp_app is not None:
                async with mcp_app.lifespan(app):
                    yield
            else:
                yield
        finally:
            sweeper.cancel()

    app = FastAPI(title="hudi-agent-gateway", version=__version__, lifespan=lifespan)
    app.include_router(meta.router)
    app.include_router(chat.router)

    if mcp_app is not None:
        app.mount("/mcp", mcp_app)

    ui_dir = resources.files("hudi_agent_gateway") / "ui"
    app.mount("/ui", StaticFiles(directory=str(ui_dir), html=True), name="ui")

    @app.get("/", include_in_schema=False)
    async def root() -> RedirectResponse:
        return RedirectResponse(url="/ui/")

    return app
