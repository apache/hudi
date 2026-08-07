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

"""A TTL cache of the lakehouse schema, auto-derived from the engine.

Feeds two model-facing surfaces (``GATEWAY_SCHEMA_HINTS``):

- the system prompt: a compact snapshot of tables and columns, so small
  models write SQL against real names instead of guessed ones;
- error hints: when a query fails on an unknown column/table, the tool's
  error payload carries the actual names ("did you mean `beds`?").

Strictly fail-open: a refresh failure leaves the previous snapshot in place
(or an empty one) and never breaks the query path -- schema hints degrade,
queries keep working. Failures are cached for the same TTL so an unreachable
engine is not hammered on every request.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from collections.abc import Awaitable, Callable

from hudi_agent_gateway.log import log_event

logger = logging.getLogger("hudi_agent_gateway.tools.schema")

#: table name -> ordered [(column name, data type), ...]
Schema = dict[str, list[tuple[str, str]]]

SchemaFetcher = Callable[[], Awaitable[Schema]]

# Hudi meta columns are dropped from hints: they are never what a user
# question is about, and steering the model toward them wastes attention.
_META_COLUMN_PREFIX = "_hoodie_"


class SchemaCache:
    """Single-flight, TTL-cached schema snapshot over a fetch callable."""

    def __init__(
        self,
        fetch: SchemaFetcher,
        *,
        ttl_seconds: float,
        max_tables: int,
        max_columns: int,
    ) -> None:
        self._fetch = fetch
        self._ttl = ttl_seconds
        self._max_tables = max_tables
        self._max_columns = max_columns
        self._data: Schema = {}
        self._fetched_at: float | None = None
        self._lock = asyncio.Lock()

    def _is_stale(self) -> bool:
        return self._fetched_at is None or time.monotonic() - self._fetched_at > self._ttl

    async def get(self) -> Schema:
        """The current schema, refreshing first if the snapshot is stale."""
        if self._is_stale():
            async with self._lock:
                if self._is_stale():  # single-flight: recheck under the lock
                    try:
                        data = await self._fetch()
                        self._data = {
                            table: [
                                (col, dtype)
                                for col, dtype in columns
                                if not col.startswith(_META_COLUMN_PREFIX)
                            ]
                            for table, columns in list(data.items())[: self._max_tables]
                        }
                        log_event(logger, "schema_cache_refreshed", tables=len(self._data))
                    except Exception as e:
                        # Fail-open: keep the previous snapshot; note the
                        # failure so the TTL below also debounces retries.
                        log_event(logger, "schema_cache_refresh_failed", error=str(e)[:200])
                    self._fetched_at = time.monotonic()
        return self._data

    def peek(self) -> Schema:
        """The current snapshot without waiting; kicks a background refresh
        when stale. Used from sync contexts (the per-turn prompt builder)."""
        if self._is_stale():
            with contextlib.suppress(RuntimeError):  # no running loop -> skip
                task = asyncio.get_running_loop().create_task(self.get())
                # retrieve the (fail-open) result so the loop never warns
                task.add_done_callback(lambda t: t.cancelled() or t.exception())
        return self._data

    def prompt_block(self) -> str:
        """A compact schema snapshot for the system prompt; "" when unknown."""
        schema = self.peek()
        if not schema:
            return ""
        lines = []
        for table, columns in list(schema.items())[: self._max_tables]:
            shown = columns[: self._max_columns]
            cols = ", ".join(f"{name} {dtype}" for name, dtype in shown)
            more = f", ... +{len(columns) - len(shown)} more" if len(columns) > len(shown) else ""
            lines.append(f"- {table}({cols}{more})")
        if len(schema) > self._max_tables:
            lines.append(f"- ... +{len(schema) - self._max_tables} more tables (list_tables)")
        return (
            "Current schema (auto-refreshed snapshot; use these exact table and "
            "column names -- call describe_table only for tables not shown):\n"
            + "\n".join(lines)
        )
