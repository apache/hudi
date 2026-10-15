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

"""Async wrapper over the Spark Thrift Server (HiveServer2 protocol) via PyHive.

The Spark Thrift Server speaks the HiveServer2 Thrift protocol on port 10000,
so any Spark cluster that can read Hudi tables (including MOR snapshots and
the 1.x unstructured types) can serve the gateway without Trino. PyHive is an
optional dependency: install the gateway with the ``spark`` extra
(``pip install 'hudi-agent-gateway[spark]'``).

Unlike Trino's stateless HTTP protocol, each HiveServer2 connection is a
server-side session that is expensive to open (TCP + SASL + OpenSession).
Connections are therefore pooled and reused across queries:

- bounded LIFO pool (``GATEWAY_SPARK_MAX_CONNECTIONS``, shared with the query
  worker pool) -- LIFO keeps hot connections hot so idle ones age out;
- connections are recycled when they exceed the idle or lifetime limits, and
  discarded (never reused) after a query timeout, since a cancelled session's
  state is unknown;
- a query that fails on a *reused* connection with a connection-level error
  (server restarted, session expired, socket died) is retried exactly once on
  a fresh connection -- SELECT-only traffic makes this safe;
- ``close()`` drains the pool on gateway shutdown.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import queue
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any

from hudi_agent_gateway.config import GatewaySettings
from hudi_agent_gateway.log import log_event
from hudi_agent_gateway.tools.common import (
    LakehouseQueryError,
    LakehouseTimeoutError,
    QueryResult,
)

logger = logging.getLogger("hudi_agent_gateway.tools.spark")

class _PyHiveUnavailable(Exception):
    """Never raised; stands in for pyhive.exc.Error when PyHive is absent so
    the ``except _PyHiveError`` clause matches nothing instead of everything."""


try:  # optional dependency; failure is reported through /ready and tool errors
    from pyhive import hive as _pyhive
    from pyhive.exc import Error as _PyHiveError

    _IMPORT_ERROR: Exception | None = None
except Exception as _e:  # pragma: no cover - depends on the environment
    _pyhive = None
    _PyHiveError = _PyHiveUnavailable
    _IMPORT_ERROR = _e

_MISSING_PYHIVE = (
    "PyHive is not installed; the Spark Thrift backend requires it. "
    "Install the gateway with the spark extra: pip install 'hudi-agent-gateway[spark]'"
)

# PyHive auth values for GATEWAY_SPARK_AUTH. "none" is HiveServer2's default
# (SASL PLAIN with an arbitrary username), matching an out-of-the-box Spark
# Thrift Server; "nosasl" matches hive.server2.authentication=NOSASL; "ldap"
# sends username/password.
_AUTH_MAP = {"none": "NONE", "nosasl": "NOSASL", "ldap": "LDAP"}

# `tbl.col` where the leading segment is a plain identifier -- HiveServer2's
# qualified result-column form. Only this exact shape gets its prefix removed.
_QUALIFIED_COLUMN_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*\.")

# Message fragments that identify a dead/expired connection (as opposed to a
# bad query): thrift transport failures and HiveServer2 session expiry after
# a server restart.
_CONNECTION_ERROR_MARKERS = (
    "tsocket read 0 bytes",
    "ttransportexception",
    "invalid sessionhandle",
    "broken pipe",
    "connection reset",
    "connection refused",
    "socket is closed",
    "eof",
)


class SparkQueryError(LakehouseQueryError):
    """A query failed on the Spark side; message is safe to surface to the model."""


class SparkTimeoutError(SparkQueryError, LakehouseTimeoutError):
    pass


def _is_connection_error(e: Exception) -> bool:
    if isinstance(e, (OSError, EOFError, ConnectionError)):
        return True
    message = str(e).lower()
    return any(marker in message for marker in _CONNECTION_ERROR_MARKERS)


@dataclass
class _PoolEntry:
    conn: Any
    created_at: float = field(default_factory=time.monotonic)
    last_used_at: float = field(default_factory=time.monotonic)


class SparkThriftClient:
    """Thin async facade over PyHive with a bounded connection pool: each query
    runs the sync client in a worker thread under a hard timeout, fetches at
    most ``max_rows`` from the cursor regardless of what the SQL says (defense
    in depth behind the guardrails), and reuses HiveServer2 sessions across
    queries. Mirrors :class:`~hudi_agent_gateway.tools.trino_client.TrinoClient`
    at the surface.
    """

    def __init__(self, settings: GatewaySettings) -> None:
        self._settings = settings
        max_connections = settings.spark_max_connections
        # Worker pool and connection pool share one bound: a query holds a
        # worker for its whole runtime and a connection only while executing,
        # so equal sizes mean the semaphore below is a leak guard, not a queue
        # (queries queue in the executor, each already under its own timeout).
        self._executor = ThreadPoolExecutor(
            max_workers=max_connections, thread_name_prefix="spark-query"
        )
        self._pool: queue.LifoQueue[_PoolEntry] = queue.LifoQueue()
        self._slots = threading.BoundedSemaphore(max_connections)
        self._closed = False
        self._ping_cache: tuple[float, bool] | None = None
        #: last connection/query failure, surfaced in /ready details
        self.last_error: str = _MISSING_PYHIVE if _pyhive is None else ""

    # ------------------------------------------------------------- pooling

    def _new_connection(self) -> Any:
        if _pyhive is None:
            raise SparkQueryError(f"{_MISSING_PYHIVE} (import error: {_IMPORT_ERROR})")
        s = self._settings
        kwargs: dict[str, Any] = {
            "host": s.spark_host,
            "port": s.spark_port,
            "username": s.spark_user,
            "database": s.spark_database,
            "auth": _AUTH_MAP[s.spark_auth],
        }
        if s.spark_auth == "ldap":
            kwargs["password"] = s.spark_password
        conn = _pyhive.connect(**kwargs)
        log_event(logger, "spark_connection_opened", host=s.spark_host, port=s.spark_port)
        return conn

    def _close_entry(self, entry: _PoolEntry, reason: str) -> None:
        with contextlib.suppress(Exception):
            entry.conn.close()
        log_event(logger, "spark_connection_closed", reason=reason)

    def _acquire(self) -> tuple[_PoolEntry, bool]:
        """Take a pooled connection (recycling stale ones) or open a new one.

        Returns ``(entry, reused)``. The slot semaphore is a hard upper bound
        on live connections; it is released by ``_release``/``_discard``.
        """
        if self._closed:
            raise SparkQueryError("client is closed")
        if not self._slots.acquire(timeout=self._settings.sql_timeout_seconds):
            raise SparkQueryError(
                "connection pool exhausted "
                f"({self._settings.spark_max_connections} connections busy)"
            )
        try:
            now = time.monotonic()
            while True:
                try:
                    entry = self._pool.get_nowait()
                except queue.Empty:
                    break
                if now - entry.created_at > self._settings.spark_pool_max_lifetime_seconds:
                    self._close_entry(entry, "max_lifetime")
                    continue
                if now - entry.last_used_at > self._settings.spark_pool_max_idle_seconds:
                    self._close_entry(entry, "max_idle")
                    continue
                return entry, True
            return _PoolEntry(conn=self._new_connection()), False
        except BaseException:
            self._slots.release()
            raise

    def _release(self, entry: _PoolEntry) -> None:
        entry.last_used_at = time.monotonic()
        if self._closed:
            self._close_entry(entry, "shutdown")
        else:
            self._pool.put(entry)
        self._slots.release()

    def _discard(self, entry: _PoolEntry, reason: str) -> None:
        self._close_entry(entry, reason)
        self._slots.release()

    def close(self) -> None:
        """Drain the pool and stop the worker pool; called on gateway shutdown."""
        self._closed = True
        while True:
            try:
                entry = self._pool.get_nowait()
            except queue.Empty:
                break
            self._close_entry(entry, "shutdown")
        self._executor.shutdown(wait=False, cancel_futures=True)

    # ------------------------------------------------------------- queries

    def _run_query(
        self, conn: Any, sql: str, max_rows: int, holder: dict[str, Any]
    ) -> QueryResult:
        cursor = conn.cursor()
        holder["cursor"] = cursor
        try:
            cursor.execute(sql)
            rows = cursor.fetchmany(max_rows)
            columns = [d[0] for d in cursor.description or []]
            # HiveServer2 column names come back qualified (`tbl.col`); strip
            # the prefix so results look like every other backend. Guarded by
            # a strict identifier pattern so an alias that merely contains a
            # dot is left untouched.
            columns = [
                _QUALIFIED_COLUMN_RE.sub("", c) for c in columns
            ]
            return QueryResult(columns=columns, rows=[list(r) for r in rows])
        finally:
            with contextlib.suppress(Exception):
                cursor.close()

    def _execute_sync(self, sql: str, max_rows: int, holder: dict[str, Any]) -> QueryResult:
        retried = False
        while True:
            entry, reused = self._acquire()
            try:
                result = self._run_query(entry.conn, sql, max_rows, holder)
            except Exception as e:
                # A cancelled/timed-out or errored session is never reused.
                self._discard(entry, "error")
                # Only retry a pooled session that died underneath us (server
                # restart, idle kill, socket drop); SELECT-only traffic makes
                # one retry on a fresh connection safe. Never retry once the
                # caller has abandoned (timed out): the outer wait_for is gone,
                # so a retry here would run unbounded, pinning a worker+slot.
                if (
                    reused
                    and not retried
                    and not holder.get("abandoned")
                    and _is_connection_error(e)
                ):
                    retried = True
                    log_event(logger, "spark_stale_connection_retry", error=str(e)[:200])
                    continue
                raise
            if holder.get("abandoned"):
                # The caller already timed out and cancelled; session state is
                # unknown, so drop the connection instead of pooling it.
                self._discard(entry, "timeout")
            else:
                self._release(entry)
            return result

    async def execute(self, sql: str, *, timeout: float, max_rows: int) -> QueryResult:
        if self._closed:
            raise SparkQueryError("client is closed")
        # `holder` hands the cursor back across the thread boundary so the
        # timeout path can cancel the query server-side; wait_for alone only
        # abandons the await while the worker thread stays blocked in Thrift.
        holder: dict[str, Any] = {}
        loop = asyncio.get_running_loop()
        future = loop.run_in_executor(self._executor, self._execute_sync, sql, max_rows, holder)
        try:
            result = await asyncio.wait_for(future, timeout=timeout)
            self.last_error = ""
            return result
        except TimeoutError as e:
            holder["abandoned"] = True
            cursor = holder.get("cursor")
            if cursor is not None:
                # best-effort server-side cancel; the timeout error wins
                with contextlib.suppress(Exception):
                    cursor.cancel()
            self.last_error = f"query exceeded the {timeout:.0f}s timeout"
            raise SparkTimeoutError(self.last_error) from e
        except SparkQueryError as e:
            self.last_error = str(e)
            raise
        except _PyHiveError as e:
            self.last_error = _format_pyhive_error(e)
            raise SparkQueryError(self.last_error) from e
        except Exception as e:  # thrift transport errors do not subclass pyhive.exc.Error
            self.last_error = str(e) or type(e).__name__
            raise SparkQueryError(self.last_error) from e

    async def ping(self, timeout: float = 5.0, cache_seconds: float = 10.0) -> bool:
        """Cached reachability check feeding ``/ready``.

        Cached so a 10s readinessProbe period does not run a query on the
        Spark Thrift Server for every probe.
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


def _format_pyhive_error(e: Exception) -> str:
    """Extract the first meaningful line from a HiveServer2 error.

    Spark Thrift errors arrive as a full ``org.apache.hive.service.cli.
    HiveSQLException`` rendering with a Java stack trace; only the message
    line helps the model self-correct.
    """
    message = str(e)
    for marker in ("Error running query: ", "AnalysisException: "):
        if marker in message:
            message = message.split(marker, 1)[1]
            break
    return message.splitlines()[0][:500] if message else type(e).__name__
