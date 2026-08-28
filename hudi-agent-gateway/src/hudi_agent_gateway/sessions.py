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

"""Bounded in-memory session store.

Sessions map to LangGraph checkpointer threads (``thread_id = session_id``).
The store bounds memory growth two ways: an idle TTL and an LRU cap on the
number of live sessions, both enforced by a background sweeper.

Process-local by design => run a SINGLE replica. The seam for horizontal
scale is swapping the checkpointer for ``langgraph-checkpoint-postgres``
without touching callers.
"""

from __future__ import annotations

import asyncio
import logging
import time

from langgraph.checkpoint.memory import InMemorySaver

from hudi_agent_gateway.config import GatewaySettings
from hudi_agent_gateway.log import log_event

logger = logging.getLogger("hudi_agent_gateway.sessions")


class SessionStore:
    def __init__(self, settings: GatewaySettings) -> None:
        self.checkpointer = InMemorySaver()
        self._ttl = settings.session_ttl_seconds
        self._max_sessions = settings.max_sessions
        self._last_access: dict[str, float] = {}

    def touch(self, session_id: str) -> None:
        self._last_access[session_id] = time.monotonic()
        self._evict_lru()

    @property
    def active_sessions(self) -> int:
        return len(self._last_access)

    def _delete(self, session_id: str, reason: str) -> None:
        self._last_access.pop(session_id, None)
        try:
            self.checkpointer.delete_thread(session_id)
        except Exception:  # noqa: BLE001 - eviction must never take the server down
            logger.warning("failed to delete thread %s", session_id, exc_info=True)
        log_event(logger, "session_evicted", evicted_session_id=session_id, reason=reason)

    def _evict_lru(self) -> None:
        while len(self._last_access) > self._max_sessions:
            oldest = min(self._last_access, key=self._last_access.__getitem__)
            self._delete(oldest, "lru")

    def sweep(self) -> int:
        """Evict idle sessions; returns the number evicted."""
        now = time.monotonic()
        expired = [sid for sid, at in self._last_access.items() if now - at > self._ttl]
        for sid in expired:
            self._delete(sid, "ttl")
        return len(expired)

    async def sweep_loop(self, interval_seconds: float = 60.0) -> None:
        while True:
            await asyncio.sleep(interval_seconds)
            try:
                self.sweep()
            except Exception:  # noqa: BLE001
                logger.warning("session sweep failed", exc_info=True)
