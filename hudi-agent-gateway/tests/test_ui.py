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

"""The chat UI: served from the package, self-contained (no external URLs)."""

from __future__ import annotations

import re

import httpx
import pytest

from hudi_agent_gateway.app import create_app


@pytest.fixture()
async def client(settings):
    app = create_app(settings.model_copy(update={"mcp_enabled": False}))
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as c, app.router.lifespan_context(app):
        yield c


async def test_root_redirects_to_ui(client) -> None:
    resp = await client.get("/")
    assert resp.status_code == 307
    assert resp.headers["location"] == "/ui/"


async def test_ui_serves_index(client) -> None:
    resp = await client.get("/ui/")
    assert resp.status_code == 200
    assert "text/html" in resp.headers["content-type"]
    assert "Hudi Agent Gateway" in resp.text


@pytest.mark.parametrize(
    ("asset", "content_type"),
    [("app.js", "text/javascript"), ("markdown.js", "text/javascript"), ("style.css", "text/css")],
)
async def test_ui_assets_served_with_content_types(client, asset: str, content_type: str) -> None:
    resp = await client.get(f"/ui/{asset}")
    assert resp.status_code == 200
    assert content_type in resp.headers["content-type"]


async def test_ui_is_self_contained(client) -> None:
    """License/air-gap regression: no third-party origins in the UI.

    Sole exception: the Hudi logo image is hotlinked from hudi.apache.org
    (like the root README) instead of vendoring a binary; offline it degrades
    to alt text. Code (scripts/styles) must stay strictly local.
    """
    index = (await client.get("/ui/")).text
    refs = re.findall(r'(?:src|href)="([^"]+)"', index)
    for ref in refs:
        if ref.startswith("https://hudi.apache.org/"):
            continue
        assert not ref.startswith(("http://", "https://", "//")), f"external ref: {ref}"
    for asset in ("app.js", "markdown.js", "style.css"):
        body = (await client.get(f"/ui/{asset}")).text
        assert "https://cdn" not in body
        assert "@import" not in body
