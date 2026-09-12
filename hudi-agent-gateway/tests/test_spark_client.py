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

"""SparkThriftClient behavior that does not need a live server (or PyHive)."""

from __future__ import annotations

import asyncio
import time

import pytest

from hudi_agent_gateway.config import GatewaySettings
from hudi_agent_gateway.tools.common import LakehouseQueryError, LakehouseTimeoutError
from hudi_agent_gateway.tools.spark_client import (
    SparkQueryError,
    SparkThriftClient,
    SparkTimeoutError,
    _format_pyhive_error,
)
from hudi_agent_gateway.tools.trino_client import QueryResult, TrinoQueryError, TrinoTimeoutError


@pytest.fixture()
def spark_client(spark_settings: GatewaySettings) -> SparkThriftClient:
    return SparkThriftClient(spark_settings)


def test_error_hierarchy() -> None:
    """Both engines' errors share the lakehouse base classes, so engine-generic
    code can catch one exception family."""
    assert issubclass(SparkQueryError, LakehouseQueryError)
    assert issubclass(SparkTimeoutError, LakehouseTimeoutError)
    assert issubclass(TrinoQueryError, LakehouseQueryError)
    assert issubclass(TrinoTimeoutError, LakehouseTimeoutError)


def test_format_pyhive_error_strips_java_stack() -> None:
    raw = (
        "Error running query: org.apache.spark.sql.AnalysisException: "
        "[TABLE_OR_VIEW_NOT_FOUND] The table or view `ghost` cannot be found.\n"
        "\tat org.apache.spark.sql.catalyst.analysis.package$AnalysisErrorAt..."
    )
    msg = _format_pyhive_error(Exception(raw))
    assert "cannot be found" in msg
    assert "\tat org" not in msg
    assert "\n" not in msg


def test_format_pyhive_error_bounded() -> None:
    msg = _format_pyhive_error(Exception("x" * 5000))
    assert len(msg) <= 500


async def test_execute_timeout_maps_to_spark_timeout(
    spark_client: SparkThriftClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    def hang(sql: str, max_rows: int, holder: dict) -> QueryResult:
        time.sleep(2.0)
        return QueryResult(columns=[], rows=[])

    monkeypatch.setattr(spark_client, "_execute_sync", hang)
    with pytest.raises(SparkTimeoutError):
        await spark_client.execute("SELECT 1", timeout=0.1, max_rows=1)
    assert "timeout" in spark_client.last_error


async def test_execute_wraps_unexpected_errors(
    spark_client: SparkThriftClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    def boom(sql: str, max_rows: int, holder: dict) -> QueryResult:
        raise ConnectionRefusedError("Could not connect to fake-spark:10000")

    monkeypatch.setattr(spark_client, "_execute_sync", boom)
    with pytest.raises(SparkQueryError, match="Could not connect"):
        await spark_client.execute("SELECT 1", timeout=5, max_rows=1)
    assert "Could not connect" in spark_client.last_error


async def test_ping_false_when_unreachable_and_cached(
    spark_client: SparkThriftClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = {"n": 0}

    def boom(sql: str, max_rows: int, holder: dict) -> QueryResult:
        calls["n"] += 1
        raise ConnectionRefusedError("refused")

    monkeypatch.setattr(spark_client, "_execute_sync", boom)
    assert await spark_client.ping(timeout=1) is False
    assert await spark_client.ping(timeout=1) is False  # served from cache
    assert calls["n"] == 1


async def test_execute_success_clears_last_error(
    spark_client: SparkThriftClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    spark_client.last_error = "previous failure"

    def ok(sql: str, max_rows: int, holder: dict) -> QueryResult:
        return QueryResult(columns=["one"], rows=[[1]])

    monkeypatch.setattr(spark_client, "_execute_sync", ok)
    result = await spark_client.execute("SELECT 1", timeout=5, max_rows=1)
    assert result.rows == [[1]]
    assert spark_client.last_error == ""


async def test_concurrent_executes_do_not_interfere(
    spark_client: SparkThriftClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    def slow_ok(sql: str, max_rows: int, holder: dict) -> QueryResult:
        time.sleep(0.05)
        return QueryResult(columns=["sql"], rows=[[sql]])

    monkeypatch.setattr(spark_client, "_execute_sync", slow_ok)
    results = await asyncio.gather(
        *(spark_client.execute(f"SELECT {i}", timeout=5, max_rows=1) for i in range(5))
    )
    assert sorted(r.rows[0][0] for r in results) == [f"SELECT {i}" for i in range(5)]


# ---------------------------------------------------------------- pooling


class FakeCursor:
    def __init__(self, conn: FakeConn) -> None:
        self._conn = conn
        self.description = [("one", "int")]
        self.cancelled = False

    def execute(self, sql: str) -> None:
        self._conn.executed.append(sql)
        if self._conn.fail_next is not None:
            err, self._conn.fail_next = self._conn.fail_next, None
            raise err

    def fetchmany(self, n: int):
        return [[1]]

    def cancel(self) -> None:
        self.cancelled = True

    def close(self) -> None:
        pass


class FakeConn:
    def __init__(self) -> None:
        self.executed: list[str] = []
        self.closed = False
        self.fail_next: Exception | None = None

    def cursor(self) -> FakeCursor:
        return FakeCursor(self)

    def close(self) -> None:
        self.closed = True


@pytest.fixture()
def pooled_client(spark_settings, monkeypatch: pytest.MonkeyPatch) -> SparkThriftClient:
    client = SparkThriftClient(spark_settings)
    client.created: list[FakeConn] = []  # type: ignore[attr-defined]

    def new_conn() -> FakeConn:
        conn = FakeConn()
        client.created.append(conn)  # type: ignore[attr-defined]
        return conn

    monkeypatch.setattr(client, "_new_connection", new_conn)
    return client


async def test_pool_reuses_connection_across_queries(pooled_client: SparkThriftClient) -> None:
    await pooled_client.execute("SELECT 1", timeout=5, max_rows=1)
    await pooled_client.execute("SELECT 2", timeout=5, max_rows=1)
    assert len(pooled_client.created) == 1  # second query reused the pooled session
    assert pooled_client.created[0].executed == ["SELECT 1", "SELECT 2"]


async def test_stale_pooled_connection_retries_once_on_fresh(
    pooled_client: SparkThriftClient,
) -> None:
    await pooled_client.execute("SELECT 1", timeout=5, max_rows=1)  # pool a session
    pooled_client.created[0].fail_next = OSError("TSocket read 0 bytes")
    result = await pooled_client.execute("SELECT 2", timeout=5, max_rows=1)
    assert result.rows == [[1]]  # retry on a fresh connection succeeded
    assert len(pooled_client.created) == 2
    assert pooled_client.created[0].closed is True  # dead session discarded
    assert pooled_client.created[1].executed == ["SELECT 2"]


async def test_expired_session_handle_triggers_retry(pooled_client: SparkThriftClient) -> None:
    await pooled_client.execute("SELECT 1", timeout=5, max_rows=1)
    pooled_client.created[0].fail_next = Exception("Invalid SessionHandle: SessionHandle [x]")
    result = await pooled_client.execute("SELECT 2", timeout=5, max_rows=1)
    assert result.rows == [[1]]
    assert len(pooled_client.created) == 2


async def test_fresh_connection_failure_does_not_retry(pooled_client: SparkThriftClient) -> None:
    """A brand-new connection failing means the server is down; no retry loop."""
    failing = FakeConn()
    failing.fail_next = OSError("Connection refused")

    def new_conn() -> FakeConn:
        pooled_client.created.append(failing)
        return failing

    pooled_client._new_connection = new_conn  # type: ignore[method-assign]
    with pytest.raises(SparkQueryError, match="Connection refused"):
        await pooled_client.execute("SELECT 1", timeout=5, max_rows=1)
    assert len(pooled_client.created) == 1


async def test_query_error_discards_but_does_not_retry(pooled_client: SparkThriftClient) -> None:
    """A SQL-level error on a reused session is not a connection error: no retry."""
    await pooled_client.execute("SELECT 1", timeout=5, max_rows=1)
    pooled_client.created[0].fail_next = Exception("AnalysisException: cannot resolve 'nope'")
    with pytest.raises(SparkQueryError, match="cannot resolve"):
        await pooled_client.execute("SELECT nope", timeout=5, max_rows=1)
    assert len(pooled_client.created) == 1  # no second connection was opened


async def test_timeout_discards_connection(
    pooled_client: SparkThriftClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    def hang(sql: str, max_rows: int, holder: dict) -> QueryResult:
        entry, _ = pooled_client._acquire()
        holder["cursor"] = entry.conn.cursor()
        time.sleep(1.0)
        if holder.get("abandoned"):
            pooled_client._discard(entry, "timeout")
        else:
            pooled_client._release(entry)
        return QueryResult(columns=[], rows=[])

    monkeypatch.setattr(pooled_client, "_execute_sync", hang)
    with pytest.raises(SparkTimeoutError):
        await pooled_client.execute("SELECT slow", timeout=0.05, max_rows=1)
    await asyncio.sleep(1.2)  # let the worker finish its discard path
    assert pooled_client.created[0].closed is True
    assert pooled_client._pool.qsize() == 0  # nothing was returned to the pool


async def test_lifetime_recycling(pooled_client: SparkThriftClient) -> None:
    await pooled_client.execute("SELECT 1", timeout=5, max_rows=1)
    # age the pooled entry past the lifetime limit
    entry = pooled_client._pool.queue[0]
    entry.created_at -= pooled_client._settings.spark_pool_max_lifetime_seconds + 1
    await pooled_client.execute("SELECT 2", timeout=5, max_rows=1)
    assert len(pooled_client.created) == 2  # old session recycled, new one opened
    assert pooled_client.created[0].closed is True


async def test_idle_recycling(pooled_client: SparkThriftClient) -> None:
    await pooled_client.execute("SELECT 1", timeout=5, max_rows=1)
    entry = pooled_client._pool.queue[0]
    entry.last_used_at -= pooled_client._settings.spark_pool_max_idle_seconds + 1
    await pooled_client.execute("SELECT 2", timeout=5, max_rows=1)
    assert len(pooled_client.created) == 2
    assert pooled_client.created[0].closed is True


async def test_pool_bound_respected_under_concurrency(
    spark_settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = spark_settings.model_copy(update={"spark_max_connections": 2})
    client = SparkThriftClient(settings)
    created: list[FakeConn] = []

    def new_conn() -> FakeConn:
        conn = FakeConn()
        created.append(conn)
        return conn

    monkeypatch.setattr(client, "_new_connection", new_conn)

    real_run = client._run_query

    def slow_run(conn, sql, max_rows, holder):
        time.sleep(0.05)
        return real_run(conn, sql, max_rows, holder)

    monkeypatch.setattr(client, "_run_query", slow_run)
    await asyncio.gather(
        *(client.execute(f"SELECT {i}", timeout=5, max_rows=1) for i in range(6))
    )
    assert len(created) <= 2  # never more live connections than the bound


async def test_close_drains_pool_and_rejects_new_queries(
    pooled_client: SparkThriftClient,
) -> None:
    await pooled_client.execute("SELECT 1", timeout=5, max_rows=1)
    pooled_client.close()
    assert pooled_client.created[0].closed is True
    with pytest.raises(SparkQueryError, match="closed"):
        await pooled_client.execute("SELECT 2", timeout=5, max_rows=1)


# ------------------------------------------------------- connection kwargs


def test_new_connection_kwargs_none_auth(
    spark_settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    """auth=none maps to PyHive NONE and sends no password."""
    from hudi_agent_gateway.tools import spark_client as mod

    captured: dict = {}

    class StubPyHive:
        @staticmethod
        def connect(**kwargs):
            captured.update(kwargs)
            return FakeConn()

    monkeypatch.setattr(mod, "_pyhive", StubPyHive)
    client = SparkThriftClient(spark_settings)
    client._new_connection()
    assert captured == {
        "host": "fake-spark",
        "port": 10000,
        "username": "hudi-agent-gateway",
        "database": "default",
        "auth": "NONE",
    }


def test_new_connection_kwargs_ldap_sends_password(monkeypatch: pytest.MonkeyPatch) -> None:
    from hudi_agent_gateway.tools import spark_client as mod

    captured: dict = {}

    class StubPyHive:
        @staticmethod
        def connect(**kwargs):
            captured.update(kwargs)
            return FakeConn()

    monkeypatch.setattr(mod, "_pyhive", StubPyHive)
    settings = GatewaySettings(
        engine="spark", spark_host="fake-spark", spark_auth="ldap", spark_password="s3cret"
    )
    SparkThriftClient(settings)._new_connection()
    assert captured["auth"] == "LDAP"
    assert captured["password"] == "s3cret"


def test_missing_pyhive_raises_actionable_error(
    spark_settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    from hudi_agent_gateway.tools import spark_client as mod

    monkeypatch.setattr(mod, "_pyhive", None)
    client = SparkThriftClient(spark_settings)
    with pytest.raises(SparkQueryError, match=r"hudi-agent-gateway\[spark\]"):
        client._new_connection()


def test_qualified_column_names_stripped_but_aliases_kept(
    pooled_client: SparkThriftClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`tbl.col` prefixes are stripped; an alias that merely contains a dot is not."""
    conn = FakeConn()

    class WideCursor(FakeCursor):
        def __init__(self, c):
            super().__init__(c)
            self.description = [
                ("trips.city", "string"),
                ("plain", "int"),
                ("a b.c", "int"),  # not identifier-dot-shaped: left alone
            ]

    monkeypatch.setattr(conn, "cursor", lambda: WideCursor(conn))
    result = pooled_client._run_query(conn, "SELECT ...", 10, {})
    assert result.columns == ["city", "plain", "a b.c"]


async def test_no_retry_after_caller_abandoned(pooled_client: SparkThriftClient) -> None:
    """A query the caller already abandoned (timed out) must NOT trigger a
    stale-connection retry -- the outer wait_for is gone, so the retry would
    run unbounded. The dead session is discarded and the error propagates."""
    await pooled_client.execute("SELECT 1", timeout=5, max_rows=1)  # warm the pool
    assert len(pooled_client.created) == 1
    pooled_client.created[0].fail_next = OSError("TSocket read 0 bytes")  # reused conn dies

    holder = {"abandoned": True}
    with pytest.raises(OSError, match="TSocket"):
        pooled_client._execute_sync("SELECT 2", 1, holder)

    # the guard held: no fresh connection was opened for a retry
    assert len(pooled_client.created) == 1
    assert pooled_client.created[0].closed is True  # dead session discarded
