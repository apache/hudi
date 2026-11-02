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

"""Bounded async facade over the official PySpark Connect client.

Each call owns a remote session. Timeout/cancellation requests interruptAll on
that session off the event loop, but cannot guarantee when Spark stops. Capacity
is retained until the worker finishes, including session cleanup.
"""

from __future__ import annotations

import asyncio
import contextlib
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import unquote, urlparse

from pyspark.errors import PySparkException
from pyspark.sql.connect.session import SparkSession

from hudi_agent_gateway.config import GatewaySettings
from hudi_agent_gateway.tools.lakehouse_client import QueryError, QueryResult, QueryTimeoutError


class _RequestSparkSession(SparkSession):
    def __del__(self) -> None:
        # PySpark 4.1 closes even an already-stopped session again during GC.
        # close() shuts down a process-wide release executor. GC can run while
        # another thread starts a worker under that executor's lock, deadlocking
        # thread startup and every subsequent executor submission (including the
        # web event loop). Our worker already calls stop() in its finally block.
        client = getattr(self, "_client", None)
        if client is not None and client.is_closed:
            return
        super().__del__()


@dataclass
class _Operation:
    cancelled: threading.Event = field(default_factory=threading.Event)
    lock: Any = field(default_factory=threading.Lock)
    session: SparkSession | None = None

    def cancel(self) -> None:
        # Serialize cancellation with session removal/close, never with execution.
        with self.lock:
            if self.session is not None:
                with contextlib.suppress(Exception):
                    self.session.interruptAll()

    def check_cancelled(self) -> None:
        if self.cancelled.is_set():
            raise QueryError("query cancelled before execution")


class SparkConnectClient:
    def __init__(self, settings: GatewaySettings) -> None:
        self._settings = settings
        self._executor = ThreadPoolExecutor(max_workers=8, thread_name_prefix="spark-query")
        self._ping_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="spark-ping")
        self._cancel_executor = ThreadPoolExecutor(max_workers=8, thread_name_prefix="spark-cancel")
        # ThreadPoolExecutor's queue is unbounded. Hold a slot until the worker
        # finishes, even after its async caller timed out.
        self._slots = threading.BoundedSemaphore(8)
        self._ping_slot = threading.BoundedSemaphore(1)
        self._ping_cache: tuple[float, bool] | None = None
        self._ping_task: asyncio.Task[bool] | None = None
        self._closed = False
        self._operations_lock = threading.Lock()
        self._operations: dict[int, _Operation] = {}

    def close(self) -> None:
        with self._operations_lock:
            self._closed = True
            operations = list(self._operations.values())
        for op in operations:
            op.cancelled.set()
            with contextlib.suppress(RuntimeError):
                self._cancel_executor.submit(op.cancel)
        self._executor.shutdown(wait=False, cancel_futures=True)
        self._ping_executor.shutdown(wait=False, cancel_futures=True)
        self._cancel_executor.shutdown(wait=False)

    def _execute_sync(
        self, sql: str, max_rows: int, op: _Operation, describe: bool = False
    ) -> QueryResult:
        op.check_cancelled()
        # Construct directly: getOrCreate() can reuse another request's session.
        spark = _RequestSparkSession(connection=self._settings.spark_connect_uri)
        with op.lock:
            op.session = spark
        try:
            op.check_cancelled()
            spark.catalog.setCurrentCatalog(self._settings.spark_connect_catalog)
            op.check_cancelled()
            spark.catalog.setCurrentDatabase(self._settings.spark_connect_schema)
            op.check_cancelled()
            df = spark.sql(sql)
            op.check_cancelled()
            if describe:
                return QueryResult(
                    columns=["column_name", "data_type", "is_nullable"],
                    rows=[
                        [f.name, f.dataType.simpleString(), "YES" if f.nullable else "NO"]
                        for f in df.schema.fields[:max_rows]
                    ],
                )
            # Limit before collecting, including metadata queries outside guardrails.
            df = df.limit(max_rows)
            columns = df.columns
            op.check_cancelled()
            return QueryResult(columns=columns, rows=[list(row) for row in df.collect()])
        finally:
            with op.lock:
                op.session = None
            spark.stop()

    async def execute(self, sql: str, *, timeout: float, max_rows: int) -> QueryResult:
        return await self._run(sql, timeout=timeout, max_rows=max_rows)

    async def describe(self, sql: str, *, timeout: float, max_rows: int) -> QueryResult:
        return await self._run(sql, timeout=timeout, max_rows=max_rows, describe=True)

    async def _run(
        self, sql: str, *, timeout: float, max_rows: int, describe: bool = False,
        probe: bool = False,
    ) -> QueryResult:
        slots = self._ping_slot if probe else self._slots
        executor = self._ping_executor if probe else self._executor
        op = _Operation()
        with self._operations_lock:
            if self._closed:
                raise QueryError("Spark Connect client is closed")
            if not slots.acquire(blocking=False):
                raise QueryError(
                    "Spark Connect query capacity exhausted; retry after active queries finish"
                )
            try:
                work = executor.submit(self._execute_sync, sql, max_rows, op, describe)
            except Exception:
                slots.release()
                raise
            self._operations[id(op)] = op

        def release_operation(_: Any) -> None:
            with self._operations_lock:
                self._operations.pop(id(op), None)
            slots.release()

        work.add_done_callback(release_operation)
        future = asyncio.wrap_future(work)
        try:
            return await asyncio.wait_for(future, timeout)
        except (TimeoutError, asyncio.CancelledError) as e:
            op.cancelled.set()
            # No blocking Connect RPC (including interrupt/stop) runs on the event loop.
            with contextlib.suppress(RuntimeError):
                self._cancel_executor.submit(op.cancel)
            if isinstance(e, asyncio.CancelledError):
                raise
            raise QueryTimeoutError(
                f"query exceeded the {timeout:g}s timeout; Spark may still be running it"
            ) from e
        except QueryError:
            raise
        except PySparkException as e:
            message = str(e).replace(self._settings.spark_connect_uri, "[Spark Connect endpoint]")
            uri = urlparse("http" + self._settings.spark_connect_uri[2:])
            # URI parameters can contain tokens or custom authentication headers.
            for parameter in uri.params.split(";"):
                _, _, value = parameter.partition("=")
                if value:
                    message = message.replace(value, "[redacted]").replace(
                        unquote(value), "[redacted]"
                    )
            raise QueryError(message[:2000]) from e
        except Exception as e:
            raise QueryError(f"Spark Connect query failed ({type(e).__name__})") from e

    async def ping(self, timeout: float = 5.0, cache_seconds: float = 10.0) -> bool:
        if self._ping_cache is not None and time.monotonic() - self._ping_cache[0] < cache_seconds:
            return self._ping_cache[1]

        if self._ping_task is None or self._ping_task.done():
            self._ping_task = asyncio.create_task(self._probe(timeout))
        # Cancelling one readiness request must not cancel a shared probe.
        return await asyncio.shield(self._ping_task)

    async def _probe(self, timeout: float) -> bool:
        try:
            await self._run("SELECT 1", timeout=timeout, max_rows=1, probe=True)
            ok = True
        except Exception:
            ok = False
        self._ping_cache = (time.monotonic(), ok)
        return ok
