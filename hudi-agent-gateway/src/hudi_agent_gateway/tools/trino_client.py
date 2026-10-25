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

"""Async wrapper over the (synchronous) Trino DB-API client."""

from __future__ import annotations

import asyncio
import contextlib
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any

import trino

from hudi_agent_gateway.config import GatewaySettings


class TrinoQueryError(Exception):
    """A query failed on the Trino side; message is safe to surface to the model."""


class TrinoTimeoutError(TrinoQueryError):
    pass


@dataclass
class QueryResult:
    columns: list[str]
    rows: list[list[Any]]
    row_count: int = field(init=False)

    def __post_init__(self) -> None:
        self.row_count = len(self.rows)


class TrinoClient:
    """Thin async facade: each query runs the sync client in a worker thread
    under a hard timeout, and fetches at most ``max_rows`` from the cursor
    regardless of what the SQL says (defense in depth behind the guardrails).
    """

    def __init__(self, settings: GatewaySettings) -> None:
        self._settings = settings
        # Dedicated bounded pool: a stuck query must not eat the loop's shared
        # default executor, and the bound caps how many can pile up at all.
        self._executor = ThreadPoolExecutor(max_workers=8, thread_name_prefix="trino-query")
        self._ping_cache: tuple[float, bool] | None = None

    def _connect(self) -> trino.dbapi.Connection:
        s = self._settings
        return trino.dbapi.connect(
            host=s.trino_host,
            port=s.trino_port,
            user=s.trino_user,
            catalog=s.trino_catalog,
            schema=s.trino_schema,
            http_scheme="http",
        )

    def _execute_sync(self, sql: str, max_rows: int, holder: dict[str, Any]) -> QueryResult:
        with self._connect() as conn:
            cursor = conn.cursor()
            holder["cursor"] = cursor
            try:
                cursor.execute(sql)
                rows = cursor.fetchmany(max_rows)
                columns = [d[0] for d in cursor.description or []]
                return QueryResult(columns=columns, rows=[list(r) for r in rows])
            finally:
                cursor.close()

    async def execute(self, sql: str, *, timeout: float, max_rows: int) -> QueryResult:
        # `holder` hands the cursor back across the thread boundary so the
        # timeout path can cancel the query server-side; wait_for alone only
        # abandons the await while the worker thread stays blocked in Trino.
        holder: dict[str, Any] = {}
        loop = asyncio.get_running_loop()
        future = loop.run_in_executor(self._executor, self._execute_sync, sql, max_rows, holder)
        try:
            return await asyncio.wait_for(future, timeout=timeout)
        except TimeoutError as e:
            cursor = holder.get("cursor")
            if cursor is not None:
                # best-effort server-side cancel; the timeout error wins
                with contextlib.suppress(Exception):
                    cursor.cancel()
            raise TrinoTimeoutError(f"query exceeded the {timeout:.0f}s timeout") from e
        except trino.exceptions.TrinoUserError as e:
            raise TrinoQueryError(f"{e.error_name}: {e.message}") from e
        except trino.exceptions.Error as e:
            raise TrinoQueryError(str(e)) from e

    async def ping(self, timeout: float = 5.0, cache_seconds: float = 10.0) -> bool:
        """Cached reachability check feeding ``/ready``.

        Cached like ``LLMReadiness`` so a 10s readinessProbe period does not
        open a fresh connection and log a query in Trino on every probe.
        """
        now = time.monotonic()
        if self._ping_cache is not None and now - self._ping_cache[0] < cache_seconds:
            return self._ping_cache[1]
        try:
            await self.execute("SELECT 1", timeout=timeout, max_rows=1)
            ok = True
        except Exception:
            ok = False
        self._ping_cache = (now, ok)
        return ok
