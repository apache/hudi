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

"""Schema hints: the TTL cache, the did-you-mean error hints, and the
schema-in-prompt rendering (GATEWAY_SCHEMA_HINTS)."""

from __future__ import annotations

import asyncio
import json

import pytest

from hudi_agent_gateway.agent import build_system_prompt, make_prompt
from hudi_agent_gateway.config import GatewaySettings
from hudi_agent_gateway.tools import build_registry, build_schema_cache
from hudi_agent_gateway.tools.common import schema_error_hint
from hudi_agent_gateway.tools.schema_cache import SchemaCache

LISTINGS_SCHEMA = {
    "listings": [
        ("listing_id", "string"),
        ("address", "string"),
        ("neighborhood", "string"),
        ("price", "double"),
        ("beds", "int"),
        ("baths", "int"),
        ("sqft", "int"),
        ("status", "string"),
        ("updated_at", "timestamp"),
    ],
    "trips": [("trip_id", "string"), ("city", "string"), ("fare", "double")],
}


def _cache(schema=LISTINGS_SCHEMA, *, ttl=300.0, max_tables=20, max_columns=40, fail=False):
    calls = {"n": 0}

    async def fetch():
        calls["n"] += 1
        if fail:
            raise ConnectionRefusedError("engine down")
        return dict(schema)

    cache = SchemaCache(
        fetch=fetch, ttl_seconds=ttl, max_tables=max_tables, max_columns=max_columns
    )
    cache.calls = calls  # type: ignore[attr-defined]
    return cache


# ------------------------------------------------------------ SchemaCache


async def test_cache_fetches_once_within_ttl() -> None:
    cache = _cache()
    assert await cache.get() == LISTINGS_SCHEMA
    assert await cache.get() == LISTINGS_SCHEMA
    assert cache.calls["n"] == 1


async def test_cache_refetches_after_ttl() -> None:
    cache = _cache(ttl=0.0)
    await cache.get()
    await asyncio.sleep(0.01)
    await cache.get()
    assert cache.calls["n"] == 2


async def test_cache_fail_open_and_debounced() -> None:
    cache = _cache(fail=True)
    assert await cache.get() == {}  # never raises
    assert await cache.get() == {}
    assert cache.calls["n"] == 1  # failure is cached for the TTL too


async def test_peek_kicks_background_refresh() -> None:
    cache = _cache()
    assert cache.peek() == {}  # nothing yet, refresh scheduled
    await asyncio.sleep(0.01)
    assert cache.peek() == LISTINGS_SCHEMA


async def test_prompt_block_renders_and_caps() -> None:
    cache = _cache(max_columns=3)
    await cache.get()
    block = cache.prompt_block()
    assert "listings(listing_id string, address string, neighborhood string, ... +6 more)" in block
    assert "trips(trip_id string, city string, fare double)" in block
    assert "exact table and column names" in block


async def test_prompt_block_empty_when_unknown() -> None:
    cache = _cache(fail=True)
    await cache.get()
    assert cache.prompt_block() == ""


# ------------------------------------------------------ schema_error_hint


def test_hint_spark_unresolved_column() -> None:
    msg = "[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column with name `bedrooms` cannot be resolved."
    hint = schema_error_hint(msg, LISTINGS_SCHEMA)
    assert "Did you mean `beds`?" in hint
    assert "Columns in `listings`" in hint and "sqft" in hint


def test_hint_trino_column_not_found() -> None:
    msg = "COLUMN_NOT_FOUND: line 1:8: Column 'bedrooms' cannot be resolved"
    hint = schema_error_hint(msg, LISTINGS_SCHEMA)
    assert "Did you mean `beds`?" in hint


def test_hint_spark_table_not_found() -> None:
    msg = "[TABLE_OR_VIEW_NOT_FOUND] The table or view `properties` cannot be found."
    hint = schema_error_hint(msg, LISTINGS_SCHEMA)
    assert "Available tables: listings, trips" in hint


def test_hint_trino_table_does_not_exist() -> None:
    msg = "TABLE_NOT_FOUND: line 1:15: Table 'hudi.default.listing' does not exist"
    hint = schema_error_hint(msg, LISTINGS_SCHEMA)
    assert "Did you mean `listings`?" in hint


def test_hint_qualified_identifier_uses_leaf() -> None:
    msg = "Column 't.bedroms' cannot be resolved"
    assert "Did you mean `beds`?" in schema_error_hint(msg, LISTINGS_SCHEMA)


def test_hint_no_close_match_lists_tables() -> None:
    msg = "[UNRESOLVED_COLUMN] A column with name `zzzzz` cannot be resolved."
    hint = schema_error_hint(msg, LISTINGS_SCHEMA)
    assert "Available tables: listings, trips" in hint
    assert "describe_table" in hint


def test_hint_ignores_unrelated_errors() -> None:
    assert schema_error_hint("DIVISION_BY_ZERO: division by zero", LISTINGS_SCHEMA) == ""


def test_hint_empty_schema_is_silent() -> None:
    assert schema_error_hint("Column 'x' cannot be resolved", {}) == ""


def test_hint_is_bounded() -> None:
    huge = {f"table_{i}": [(f"col_{j}", "string") for j in range(50)] for i in range(100)}
    msg = "[TABLE_OR_VIEW_NOT_FOUND] The table or view `nope` cannot be found."
    assert len(schema_error_hint(msg, huge)) <= 700


# ------------------------------------- end-to-end through the tool handlers


async def test_spark_query_error_carries_did_you_mean(spark_settings, fake_spark) -> None:
    cache = _cache()
    registry = build_registry(spark_settings, client=fake_spark, schema_cache=cache)
    fake_spark.fail_with(
        "[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column with name `bedrooms` cannot be resolved."
    )
    out = json.loads(
        await registry.get("query_lakehouse").handler(sql="SELECT bedrooms FROM listings")
    )
    assert "Did you mean `beds`?" in out["hint"]
    assert "listing_id" in out["hint"]  # the real column list is right there


async def test_trino_query_error_carries_did_you_mean(settings, fake_trino) -> None:
    cache = _cache()
    registry = build_registry(settings, client=fake_trino, schema_cache=cache)
    fake_trino.fail_with("COLUMN_NOT_FOUND: line 1:8: Column 'bedrooms' cannot be resolved")
    out = json.loads(
        await registry.get("query_lakehouse").handler(sql="SELECT bedrooms FROM listings")
    )
    assert "Did you mean `beds`?" in out["hint"]


async def test_hints_disabled_in_off_and_prompt_modes(spark_settings, fake_spark) -> None:
    for mode in ("off", "prompt"):
        cache = _cache()
        s = spark_settings.model_copy(update={"schema_hints": mode})
        registry = build_registry(s, client=fake_spark, schema_cache=cache)
        fake_spark.fail_with("Column `bedrooms` cannot be resolved")
        out = json.loads(
            await registry.get("query_lakehouse").handler(sql="SELECT bedrooms FROM listings")
        )
        assert out["hint"] == "Fix the SQL and try again."
        fake_spark.error = None


# ----------------------------------------------------- schema in the prompt


def test_prompt_includes_schema_block(settings) -> None:
    prompt = build_system_prompt(settings, "SCHEMA-SNAPSHOT-HERE")
    assert "SCHEMA-SNAPSHOT-HERE" in prompt
    assert "Tool strategy:" in prompt  # rest of the prompt intact


async def test_make_prompt_renders_current_schema(spark_settings) -> None:
    cache = _cache()
    await cache.get()
    prompt_fn = make_prompt(spark_settings, cache)
    assert callable(prompt_fn)
    from langchain_core.messages import HumanMessage

    msgs = prompt_fn({"messages": [HumanMessage(content="hi")]})
    assert "listings(listing_id string" in msgs[0].content
    assert msgs[1].content == "hi"


def test_make_prompt_static_when_off(spark_settings) -> None:
    s = spark_settings.model_copy(update={"schema_hints": "errors"})
    assert isinstance(make_prompt(s, _cache()), str)
    assert isinstance(make_prompt(spark_settings, None), str)


async def test_make_prompt_prefers_trimmed_llm_input(spark_settings) -> None:
    cache = _cache()
    await cache.get()
    prompt_fn = make_prompt(spark_settings, cache)
    from langchain_core.messages import HumanMessage

    trimmed = [HumanMessage(content="only-this")]
    msgs = prompt_fn({"messages": [HumanMessage(content="a"), HumanMessage(content="b")],
                      "llm_input_messages": trimmed})
    assert [m.content for m in msgs[1:]] == ["only-this"]


# ------------------------------------------------------------ build wiring


def test_build_schema_cache_none_when_off(settings, fake_trino) -> None:
    s = settings.model_copy(update={"schema_hints": "off"})
    assert build_schema_cache(s, fake_trino) is None


def test_config_defaults_and_validation() -> None:
    from pydantic import ValidationError

    s = GatewaySettings()
    assert s.schema_hints == "both"
    assert s.schema_cache_ttl_seconds == 300.0
    with pytest.raises(ValidationError):
        GatewaySettings(schema_hints="everything")  # type: ignore[arg-type]


# ------------------------------------------------------ connector fetchers


async def test_trino_fetch_schema_groups_by_table(settings, fake_trino) -> None:
    from hudi_agent_gateway.tools import get_connector
    from hudi_agent_gateway.tools.trino_client import QueryResult

    fake_trino.result = QueryResult(
        columns=["table_name", "column_name", "data_type"],
        rows=[
            ["listings", "listing_id", "varchar"],
            ["listings", "beds", "integer"],
            ["trips", "city", "varchar"],
        ],
    )
    schema = await get_connector("trino").fetch_schema(fake_trino, settings)
    assert schema == {
        "listings": [("listing_id", "varchar"), ("beds", "integer")],
        "trips": [("city", "varchar")],
    }
    assert "information_schema.columns" in fake_trino.executed[0]


async def test_spark_fetch_schema_show_then_describe(spark_settings) -> None:
    from hudi_agent_gateway.tools import get_connector
    from hudi_agent_gateway.tools.trino_client import QueryResult

    class ScriptedSpark:
        def __init__(self) -> None:
            self.executed: list[str] = []
            self.responses = {
                "SHOW TABLES IN `default`": QueryResult(
                    columns=["namespace", "tableName", "isTemporary"],
                    rows=[["default", "listings", False]],
                ),
                "DESCRIBE TABLE `default`.`listings`": QueryResult(
                    columns=["col_name", "data_type", "comment"],
                    rows=[
                        ["listing_id", "string", None],
                        ["beds", "int", None],
                        ["", "", ""],  # separator before partition section
                        ["# Partition Information", "", ""],
                        ["neighborhood", "string", None],  # partition col repeat
                    ],
                ),
            }

        async def execute(self, sql, *, timeout, max_rows):
            self.executed.append(sql)
            return self.responses[sql]

    client = ScriptedSpark()
    schema = await get_connector("spark").fetch_schema(client, spark_settings)
    # stops at the separator: partition-section repeats are not duplicated
    assert schema == {"listings": [("listing_id", "string"), ("beds", "int")]}


async def test_hoodie_meta_columns_filtered() -> None:
    schema = {
        "listings": [
            ("_hoodie_commit_time", "string"),
            ("_hoodie_record_key", "string"),
            ("listing_id", "string"),
            ("beds", "int"),
        ]
    }
    cache = _cache(schema)
    assert await cache.get() == {"listings": [("listing_id", "string"), ("beds", "int")]}
