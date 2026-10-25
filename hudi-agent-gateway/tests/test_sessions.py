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

import time

from hudi_agent_gateway.config import GatewaySettings
from hudi_agent_gateway.sessions import SessionStore


def _store(**overrides: object) -> SessionStore:
    settings = GatewaySettings(
        llm_provider="ollama",
        **overrides,  # type: ignore[arg-type]
    )
    return SessionStore(settings)


def test_touch_tracks_sessions() -> None:
    store = _store()
    store.touch("a")
    store.touch("b")
    assert store.active_sessions == 2


def test_ttl_sweep_evicts_idle_sessions(monkeypatch) -> None:
    store = _store(session_ttl_seconds=10.0)
    store.touch("old")
    # Age the session by shifting the recorded time back.
    store._last_access["old"] = time.monotonic() - 60.0
    store.touch("fresh")
    evicted = store.sweep()
    assert evicted == 1
    assert store.active_sessions == 1
    assert "fresh" in store._last_access


def test_lru_eviction_beyond_max_sessions() -> None:
    store = _store(max_sessions=3)
    for sid in ("a", "b", "c"):
        store.touch(sid)
    store.touch("d")  # exceeds the cap; "a" is oldest
    assert store.active_sessions == 3
    assert "a" not in store._last_access


def test_last_access_refresh_prevents_eviction() -> None:
    store = _store(max_sessions=3)
    for sid in ("a", "b", "c"):
        store.touch(sid)
    store.touch("a")  # refresh: now "b" is oldest
    store.touch("d")
    assert "a" in store._last_access
    assert "b" not in store._last_access
