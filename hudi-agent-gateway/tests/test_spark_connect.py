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

"""PySpark Connect lifecycle, bounded results, guardrails, and shared surfaces."""

import asyncio
import gc
import json
import threading
import weakref
from contextlib import contextmanager
from unittest.mock import Mock, patch

import httpx
import pytest
from fastmcp import Client
from pydantic import ValidationError
from pyspark.errors import PySparkException
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from hudi_agent_gateway.agent import build_system_prompt
from hudi_agent_gateway.app import create_app
from hudi_agent_gateway.config import GatewaySettings
from hudi_agent_gateway.mcp_server import build_mcp
from hudi_agent_gateway.tools import build_registry
from hudi_agent_gateway.tools.guardrails import enforce_guardrails
from hudi_agent_gateway.tools.lakehouse_client import QueryError, QueryResult, QueryTimeoutError
from hudi_agent_gateway.tools.registry import ToolInputError
from hudi_agent_gateway.tools.spark_connect_client import SparkConnectClient, _RequestSparkSession


@pytest.mark.parametrize("closed", [True, False])
def test_session_gc_does_not_close_an_already_stopped_client(closed):
    session = _RequestSparkSession.__new__(_RequestSparkSession)
    client = Mock(is_closed=closed)
    session._client = client
    # Spark's cached Catalog retains its session, so finalization happens during
    # cyclic GC, potentially in an unrelated thread while locks are held.
    session.catalog = Mock(_sparkSession=session)
    ref = weakref.ref(session)
    del session
    gc.collect()
    assert ref() is None
    assert client.close.call_count == (0 if closed else 1)


class DataFrame:
    def __init__(self, session):
        self.session = session
        self.columns = ["id"]
        self.schema = StructType(
            [StructField("id", IntegerType(), False), StructField("region", StringType())]
        )
        self.limits = []
        self.collected = False

    def limit(self, rows):
        self.limits.append(rows)
        return self

    def collect(self):
        self.collected = True
        self.session.started.set()
        if self.session.release is not None:
            self.session.release.wait(3)
        if self.session.error:
            raise self.session.error
        return [(i,) for i in range(self.limits[-1])] if self.columns else []


class Session:
    def __init__(self):
        self.catalog = self
        self.catalog_name = None
        self.schema_name = None
        self.catalog_error = None
        self.executed = []
        self.closed = threading.Event()
        self.cancelled = threading.Event()
        self.started = threading.Event()
        self.release = None
        self.error = None
        self.cancel_error = None
        self.df = DataFrame(self)

    def setCurrentCatalog(self, name):
        self.catalog_name = name
        if self.catalog_error:
            raise self.catalog_error

    def setCurrentDatabase(self, name):
        self.schema_name = name

    def sql(self, sql):
        self.executed.append(sql)
        return self.df

    def interruptAll(self):
        self.cancelled.set()
        if self.cancel_error:
            raise self.cancel_error
        return ["operation"]

    def stop(self):
        self.closed.set()


@contextmanager
def pyspark_client(settings, *sessions):
    client = SparkConnectClient(settings)
    with patch(
        "hudi_agent_gateway.tools.spark_connect_client._RequestSparkSession", side_effect=sessions
    ) as connect:
        try:
            yield client, connect
        finally:
            for session in sessions:
                if session.release is not None:
                    session.release.set()
            client.close()


def spark_settings(settings):
    return GatewaySettings(
        **{
            **settings.model_dump(),
            "sql_backend": "spark-connect",
            "spark_connect_uri": "sc://localhost:15002",
        }
    )


async def test_pyspark_collect_is_bounded_and_session_closes(settings):
    s = spark_settings(settings)
    session = Session()
    with pyspark_client(s, session) as (client, connect):
        out = await client.execute("SELECT id FROM t LIMIT 3", timeout=1, max_rows=3)
        assert out.rows == [[0], [1], [2]]
        assert session.df.limits == [3]
        assert session.closed.is_set()
        assert connect.call_args.kwargs == {"connection": s.spark_connect_uri}
        assert session.catalog_name == "spark_catalog" and session.schema_name == "default"


@pytest.mark.parametrize("failure", ["query", "catalog"])
async def test_pyspark_failures_close_session(settings, failure):
    session = Session()
    if failure == "query":
        session.error = RuntimeError("gRPC call failed")
    else:
        session.catalog_error = PySparkException("Catalog not found")
    with pyspark_client(spark_settings(settings), session) as (client, _):
        with pytest.raises(QueryError):
            await client.execute("SELECT 1", timeout=1, max_rows=1)
        assert session.closed.is_set()


async def test_pyspark_query_error_is_actionable_and_redacted(settings):
    s = spark_settings(settings).model_copy(
        update={"spark_connect_uri": "sc://localhost:15002/;token=secret%20token"}
    )
    session = Session()
    session.error = PySparkException(
        f"TABLE_NOT_FOUND at {s.spark_connect_uri}: secret token"
    )
    with pyspark_client(s, session) as (client, _):
        with pytest.raises(QueryError, match="TABLE_NOT_FOUND") as caught:
            await client.execute("SELECT * FROM absent", timeout=1, max_rows=1)
        assert "secret" not in str(caught.value)


async def test_pyspark_describe_uses_schema_without_collecting(settings):
    session = Session()
    with pyspark_client(spark_settings(settings), session) as (client, _):
        out = await client.describe("SELECT * FROM t LIMIT 0", timeout=1, max_rows=2)
        assert out.columns == ["column_name", "data_type", "is_nullable"]
        assert out.rows == [["id", "int", "NO"], ["region", "string", "YES"]]
        assert not session.df.collected
        assert session.closed.is_set()


@pytest.mark.parametrize("cancel_task", [False, True])
async def test_timeout_and_cancellation_cleanup_without_blocking_loop(settings, cancel_task):
    session = Session()
    session.release = threading.Event()
    with pyspark_client(spark_settings(settings), session) as (client, _):
        task = asyncio.create_task(client.execute("SELECT 1", timeout=0.05, max_rows=1))
        assert await asyncio.to_thread(session.started.wait, 1)
        if cancel_task:
            task.cancel()
        with pytest.raises(asyncio.CancelledError if cancel_task else QueryTimeoutError):
            await task
        assert await asyncio.to_thread(session.cancelled.wait, 1)
        assert not session.closed.is_set()  # Worker still owns the session.
        session.release.set()
        assert await asyncio.to_thread(session.closed.wait, 1)


async def test_cancellation_is_isolated_and_failure_does_not_prevent_cleanup(settings):
    slow, fast = Session(), Session()
    slow.release = threading.Event()
    slow.cancel_error = RuntimeError("interrupt RPC failed")
    with pyspark_client(spark_settings(settings), slow, fast) as (client, connect):
        task = asyncio.create_task(client.execute("SELECT 1", timeout=0.1, max_rows=1))
        assert await asyncio.to_thread(slow.started.wait, 1)
        assert (await client.execute("SELECT 2", timeout=1, max_rows=1)).rows == [[0]]
        with pytest.raises(QueryTimeoutError):
            await task
        assert await asyncio.to_thread(slow.cancelled.wait, 1)
        assert not fast.cancelled.is_set() and fast.closed.is_set()
        assert connect.call_count == 2
        slow.release.set()
        assert await asyncio.to_thread(slow.closed.wait, 1)


async def test_timed_out_workers_keep_capacity_slots(settings):
    sessions = [Session() for _ in range(8)]
    for session in sessions:
        session.release = threading.Event()
    with pyspark_client(spark_settings(settings), *sessions) as (client, _):
        jobs = [client.execute("SELECT 1", timeout=0.1, max_rows=1) for _ in range(8)]
        assert all(
            isinstance(e, QueryTimeoutError)
            for e in await asyncio.gather(*jobs, return_exceptions=True)
        )
        with pytest.raises(QueryError, match="capacity exhausted"):
            await client.execute("SELECT 1", timeout=1, max_rows=1)
        for session in sessions:
            session.release.set()
            assert await asyncio.to_thread(session.closed.wait, 1)


async def test_ping_cached_and_closed_client_rejected(settings):
    session = Session()
    with pyspark_client(spark_settings(settings), session) as (client, _):
        assert await client.ping()
        assert await client.ping()
        assert session.executed == ["SELECT 1"]
        client.close()
        with pytest.raises(QueryError, match="closed"):
            await client.execute("SELECT 1", timeout=1, max_rows=1)


@pytest.mark.parametrize(
    "sql",
    [
        "INSERT INTO t VALUES (1)",
        "UPDATE t SET a=1",
        "DELETE FROM t",
        "DROP TABLE t",
        "WITH x AS (INSERT INTO t VALUES (1)) SELECT * FROM x",
        "SELECT 1; DELETE FROM t",
        "INSERT OVERWRITE DIRECTORY '/tmp/x' SELECT 1",
        "SELECT * INTO t FROM u",
        "CACHE TABLE t",
        "SET spark.sql.shuffle.partitions=1",
    ],
)
def test_spark_writes_rejected(sql):
    with pytest.raises(ToolInputError):
        enforce_guardrails(sql, 100, dialect="spark")


@pytest.mark.parametrize("function", ["java_method", "ReFlEcT", "TRY_REFLECT"])
@pytest.mark.parametrize(
    "query",
    [
        "SELECT {call}",
        "WITH x AS (SELECT {call} AS value) SELECT value FROM x",
        "SELECT value FROM (SELECT {call} AS value) x",
        "SELECT builtin.{call}",
        "SELECT 1 UNION ALL SELECT {call}",
    ],
)
async def test_spark_reflection_rejected_before_client(settings, fake_trino, function, query):
    call = f"{function}('java.lang.System', 'setProperty', 'k', 'v')"
    sql = query.format(call=call)
    with pytest.raises(ToolInputError, match="read-only: Spark function"):
        enforce_guardrails(sql, 100, dialect="spark")
    registry = build_registry(spark_settings(settings), fake_trino)
    result = json.loads(await registry.get("query_lakehouse").handler(sql=sql))
    assert "read-only: Spark function" in result["error"]
    async with Client(build_mcp(registry)) as mcp:
        result = await mcp.call_tool("query_lakehouse", {"sql": sql})
        assert "read-only: Spark function" in json.loads(result.content[0].text)["error"]
    assert not fake_trino.executed


def test_spark_sql_quoting_and_functions():
    out = enforce_guardrails(
        "SELECT `weird col`, date_trunc('DAY', ts) FROM `t`", 100, dialect="spark"
    )
    assert "`weird col`" in out and "LIMIT 100" in out


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT 'java_method', 'reflect', 'try_reflect' AS function_names",
        "SELECT `java_method`, `reflect`, `try_reflect` FROM t",
        "SELECT 1 /* java_method('java.lang.System', 'setProperty', 'k', 'v') */",
    ],
)
def test_spark_reflection_names_without_calls_are_allowed(sql):
    assert "LIMIT 100" in enforce_guardrails(sql, 100, dialect="spark")


async def test_spark_tools_metadata_mcp_and_readonly(settings, fake_trino):
    s = spark_settings(settings)
    registry = build_registry(s, fake_trino)
    fake_trino.result = QueryResult(
        ["namespace", "tableName", "isTemporary"], [["default", "orders", False]]
    )
    out = json.loads(await registry.get("list_tables").handler())
    assert out["columns"] == ["table_schema", "table_name", "table_type"]
    assert out["rows"] == [["default", "orders", "TABLE"]]
    assert fake_trino.executed[-1] == "SHOW TABLES IN `spark_catalog`.`default`"
    fake_trino.result = QueryResult(
        ["column_name", "data_type", "is_nullable"], [["id", "int32", "YES"]]
    )
    await registry.get("describe_table").handler(table="orders")
    assert fake_trino.executed[-1] == "SELECT * FROM `spark_catalog`.`default`.`orders` LIMIT 0"
    async with Client(build_mcp(registry)) as mcp:
        assert {t.name for t in await mcp.list_tools()} == {
            "list_tables",
            "describe_table",
            "query_lakehouse",
        }
        before = len(fake_trino.executed)
        result = await mcp.call_tool("query_lakehouse", {"sql": "DELETE FROM orders"})
        assert "read-only" in json.loads(result.content[0].text)["error"]
        assert len(fake_trino.executed) == before
        await mcp.call_tool("query_lakehouse", {"sql": "SELECT `id` FROM orders"})
        assert "LIMIT 100" in fake_trino.executed[-1]
    assert "Spark Connect" in build_system_prompt(s)
    assert "spark_catalog" in build_system_prompt(s)


@pytest.mark.parametrize("port, expected_port", [("", 15002), (":1", 1), (":65535", 65535)])
async def test_spark_ready_info_and_shutdown(
    settings, fake_trino, monkeypatch, port, expected_port
):
    s = GatewaySettings(
        **{
            **spark_settings(settings).model_dump(),
            "spark_connect_uri": f"sc://localhost{port}",
        }
    )
    monkeypatch.setattr("hudi_agent_gateway.app.build_client", lambda _: fake_trino)
    app = create_app(s.model_copy(update={"mcp_enabled": False}))
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as http,
    ):
        info = (await http.get("/v1/info")).json()
        assert info["sql_backend"] == "spark-connect"
        assert info["catalog"] == "spark_catalog" and not info["trino_url"]
        assert info["lakehouse_url"] == f"sc://localhost:{expected_port}"
        fake_trino.fail_with("unreachable")
        ready = await http.get("/ready")
        assert ready.status_code == 503
        assert "spark-connect" in ready.json()["checks"]
        assert "trino" not in ready.json()["checks"]


@pytest.mark.parametrize(
    "uri, endpoint",
    [
        ("sc://localhost", "sc://localhost:15002"),
        ("sc://localhost/;use_ssl=true;token=secret%20token", "sc://localhost:15002"),
        ("sc://[::1]:15002/;token=secret", "sc://[::1]:15002"),
    ],
)
async def test_connect_uri_parameters_are_accepted_and_info_is_redacted(
    settings, fake_trino, monkeypatch, uri, endpoint
):
    from pyspark.sql.connect.client import DefaultChannelBuilder

    s = GatewaySettings(**{**spark_settings(settings).model_dump(), "spark_connect_uri": uri})
    assert DefaultChannelBuilder(uri).endpoint == endpoint.removeprefix("sc://")
    monkeypatch.setattr("hudi_agent_gateway.app.build_client", lambda _: fake_trino)
    app = create_app(s.model_copy(update={"mcp_enabled": False}))
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as http,
    ):
        response = await http.get("/v1/info")
        assert response.json()["lakehouse_url"] == endpoint
        assert "secret" not in response.text


@pytest.mark.parametrize(
    "uri",
    [
        "spark://localhost:15002?api=connect",
        "http://localhost:15002",
        "sc://localhost?api=connect",
        "sc://localhost:not-a-port",
        "sc://localhost:65536",
        "sc://localhost:-1",
        "sc://localhost:0",
        "sc://[::1]:not-a-port",
        "sc://[::1]:65536",
        "sc:///",
        "sc://user:secret@localhost:15002",
        "sc://localhost/database",
        "sc://localhost/;token",
        "sc://localhost/;token=a;",
        "sc://localhost#fragment",
    ],
)
def test_bad_connect_uri_fails_fast(uri):
    with pytest.raises(ValidationError, match="GATEWAY_SPARK_CONNECT_URI"):
        GatewaySettings(sql_backend="spark-connect", spark_connect_uri=uri)
