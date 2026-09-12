<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->
# hudi-agent-gateway

**One deployable service that serves your Hudi lakehouse to AI.** A single
process hosts three surfaces over the same set of guarded lakehouse tools:

| Surface | Where | What |
|---|---|---|
| Agent chat API | `POST /v1/chat` | prompt in → LangGraph agent loop (model ↔ tools) → grounded answer out; multi-turn sessions; optional SSE streaming |
| MCP server | `/mcp` (streamable HTTP) | external agents (Claude, anything MCP) call the lakehouse tools directly |
| Chat UI | `/ui/` | first-party ChatGPT-style web UI (zero third-party code) |

The tools query the lakehouse through a pluggable SQL engine -- Trino (the
default) or a Spark Thrift Server (`GATEWAY_ENGINE=spark`): `query_lakehouse`
(guarded, read-only SQL), `list_tables`, `describe_table`. Every model-written query
passes AST-level guardrails (single statement, SELECT-only, row cap injected
as a real `LIMIT`) and every invocation is logged as structured JSON — the
seed of the gateway's trace collection.

## Quickstart (local process)

```bash
cd hudi-agent-gateway
python3.12 -m venv .venv && .venv/bin/pip install -e ".[dev]"

# point at a Trino with the Hudi connector (e.g. the local-dev stack, port-forwarded):
GATEWAY_TRINO_HOST=localhost GATEWAY_TRINO_PORT=18080 \
GATEWAY_LLM_PROVIDER=anthropic GATEWAY_LLM_MODEL=claude-haiku-4-5-20251001 \
  .venv/bin/hudi-agent-gateway serve

curl -X POST localhost:8000/v1/chat -H 'Content-Type: application/json' \
  -d '{"message": "How many trips per city?", "session_id": "s1"}'
open http://localhost:8000/ui/
```

`GET /v1/models` lists the models the configured provider offers (live:
Anthropic and OpenAI model APIs, Ollama's local tags, vLLM's served models),
and `POST /v1/chat` accepts an optional `"model"` to pick one per request —
the chat UI exposes this as a model picker. Sessions survive model switches.

For a fully local model, install [Ollama](https://ollama.com), pull a
tool-capable model, and use the default provider:

```bash
ollama pull qwen3:8b
GATEWAY_LLM_PROVIDER=ollama GATEWAY_LLM_MODEL=qwen3:8b hudi-agent-gateway serve
```

Connect an MCP client:

```bash
claude mcp add --transport http hudi-lakehouse http://localhost:8000/mcp/
```

## Using Spark instead of Trino (via Apache Kyuubi)

Set `GATEWAY_ENGINE=spark` to serve the same tools through a Spark SQL engine
over the HiveServer2 Thrift protocol. Spark reads everything Spark can read:
MOR snapshot queries, and Hudi 1.x tables carrying the unstructured types
(`BLOB`, `VECTOR`, `VARIANT`) -- e.g. `size(embedding)` over a vector column
works. Queries are validated and rendered in the Spark SQL dialect;
`list_tables`/`describe_table` use `SHOW TABLES` / `DESCRIBE TABLE`.

The recommended endpoint is **[Apache Kyuubi](https://kyuubi.apache.org/)**,
which exposes the same HiveServer2 protocol but adds what a bare Spark Thrift
Server lacks -- isolated on-demand engines, HA + service discovery, and real
authn/authz. Because the wire protocol is identical, the gateway connects to
Kyuubi with **no code change** -- only the host/port differ (Kyuubi defaults to
10009; a raw Spark Thrift Server uses 10000).

```bash
# the spark extra brings PyHive (pure-sasl, no C toolchain needed):
.venv/bin/pip install -e ".[spark,dev]"

# Point Kyuubi's engine at a Spark build with the Hudi bundle on its classpath
# (SPARK_HOME/conf/spark-defaults.conf), then start Kyuubi:
#   export SPARK_HOME=/path/to/spark-with-hudi
#   $KYUUBI_HOME/bin/kyuubi start        # HiveServer2 Thrift on :10009

GATEWAY_ENGINE=spark GATEWAY_SPARK_HOST=localhost GATEWAY_SPARK_PORT=10009 \
GATEWAY_LLM_PROVIDER=ollama GATEWAY_LLM_MODEL=qwen3:8b \
  .venv/bin/hudi-agent-gateway serve

curl localhost:8000/ready   # {"checks": {"spark": {"ok": true, ...}, ...}}
```

Kyuubi operational notes (learned the hard way -- see the PR for detail):

- **Engine share level.** For a single-service gateway, run Kyuubi with
  `kyuubi.engine.share.level=SERVER` so every connection reuses one engine and
  one catalog. With the default `USER` level each distinct connecting user
  gets its *own* engine (extra cold-start) and, with an embedded derby
  metastore, its *own* tables -- so `GATEWAY_SPARK_USER` must match whoever
  registered them, or back Kyuubi with a shared Hive Metastore.
- **First-query cold start.** The engine spins up on the first query and can
  take minutes with a large Hudi classpath; keep
  `kyuubi.session.engine.initialize.timeout` generous and give the driver
  enough memory, or the engine is killed mid-init. The gateway's readiness
  ping may report `spark` down until the engine is warm.

Any HiveServer2-compatible endpoint works, including a raw
`start-thriftserver.sh` for a quick local check (lighter, but single shared
context, no HA) -- set `GATEWAY_SPARK_PORT=10000` for that.

The chat API, UI, MCP server, guardrails, and agent behavior are identical
across engines -- only the SQL dialect and the endpoint in `/v1/info` change.
Two engine-specific notes:

- **Connections are pooled.** HiveServer2 sessions are expensive to open, so
  the client keeps up to `GATEWAY_SPARK_MAX_CONNECTIONS` live sessions,
  recycles them past idle/lifetime limits, discards any session whose query
  timed out, and transparently retries once on a fresh connection when a
  pooled session died underneath a query (server restart, idle kill).
- **Tool argument shapes follow the engine's namespace model**: `list_tables`
  takes `database` (not `catalog`/`schema_name`) and `describe_table` accepts
  `table` or `database.table` -- MCP clients that hardcode tool arguments
  should read `GET /v1/tools` rather than assume the Trino shapes. The thrift
  transport is unencrypted; `ldap` sends the password in the SASL exchange,
  so use it only on trusted networks (TLS support is a tracked follow-up).

## Deploying on Kubernetes

See `hudi-lakehouse/charts/hudi-agent-gateway` — the product Helm chart —
and `hudi-lakehouse/local-dev/` for a complete laptop environment
(MinIO + Hive Metastore + Trino + this gateway) where the gateway is
installed alongside Trino by default.

## Configuration

Environment variables (prefix `GATEWAY_` except the standard key names):

| Variable | Default | Purpose |
|---|---|---|
| `GATEWAY_LLM_PROVIDER` | `ollama` | `anthropic` \| `openai` \| `ollama` \| `openai-compatible` |
| `GATEWAY_LLM_MODEL` | `qwen3:8b` | model name for the provider |
| `ANTHROPIC_API_KEY` / `OPENAI_API_KEY` | — | required by the matching provider |
| `GATEWAY_OLLAMA_BASE_URL` | `http://localhost:11434` | Ollama endpoint |
| `GATEWAY_OPENAI_BASE_URL` | — | endpoint for `openai-compatible` (vLLM, Together, …) |
| `GATEWAY_LLM_TIMEOUT_SECONDS` | `120` | per-model-call timeout |
| `GATEWAY_ENGINE` | `trino` | lakehouse SQL engine: `trino` \| `spark` |
| `GATEWAY_TRINO_HOST` / `_PORT` | `hudi-trino.hudi-lakehouse.svc` / `8080` | Trino coordinator (engine=trino) |
| `GATEWAY_TRINO_CATALOG` / `_SCHEMA` / `_USER` | `hudi` / `default` / `hudi-agent-gateway` | query defaults |
| `GATEWAY_SPARK_HOST` / `_PORT` | `localhost` / `10000` | Spark Thrift Server (engine=spark) |
| `GATEWAY_SPARK_DATABASE` / `_USER` | `default` / `hudi-agent-gateway` | query defaults (engine=spark) |
| `GATEWAY_SPARK_AUTH` / `_PASSWORD` | `none` / — | HiveServer2 auth: `none` \| `nosasl` \| `ldap` (password required for ldap) |
| `GATEWAY_SPARK_MAX_CONNECTIONS` | `8` | pooled HiveServer2 sessions; also the query concurrency bound |
| `GATEWAY_SPARK_POOL_MAX_IDLE_SECONDS` / `_MAX_LIFETIME_SECONDS` | `300` / `1800` | pooled sessions are recycled past these limits |
| `GATEWAY_SQL_ROW_CAP` | `200` | LIMIT enforced on every query |
| `GATEWAY_SQL_TIMEOUT_SECONDS` | `120` | per-query timeout |
| `GATEWAY_TOOL_RESULT_MAX_BYTES` | `50000` | tool results truncated beyond this (with notice) |
| `GATEWAY_AGENT_MAX_ITERATIONS` | `25` | agent loop recursion limit |
| `GATEWAY_SESSION_TTL_SECONDS` / `GATEWAY_MAX_SESSIONS` | `3600` / `1000` | session store bounds |
| `GATEWAY_MAX_MESSAGES_PER_SESSION` | `40` | context window per session (trimmed pre-model) |
| `GATEWAY_SYSTEM_PROMPT_EXTRA` | — | appended to the built-in system prompt |
| `GATEWAY_MCP_ENABLED` | `true` | serve /mcp |
| `GATEWAY_HOST` / `GATEWAY_PORT` / `GATEWAY_LOG_LEVEL` | `0.0.0.0` / `8000` / `INFO` | server basics |

Startup never depends on the LLM or Trino being reachable: `/health` is
liveness, `/ready` reports per-dependency status (and gates the Kubernetes
readiness probe).

## Development

```bash
.venv/bin/pytest              # offline suite (fake Trino + scripted model)
.venv/bin/ruff check src tests
.venv/bin/mypy src

# live integration (against a port-forwarded local-dev stack):
GATEWAY_IT_TRINO_HOST=localhost GATEWAY_IT_TRINO_PORT=18080 .venv/bin/pytest tests/integration

# live integration against a Spark Thrift Server (needs the spark extra):
GATEWAY_IT_SPARK_HOST=localhost GATEWAY_IT_SPARK_PORT=10000 .venv/bin/pytest tests/integration/test_live_spark.py
```

Adding a tool: write a module under `src/hudi_agent_gateway/tools/` with a
`register(registry, ...)` function and call it from
`tools/__init__.py:build_registry`. One registration exposes it to the agent
loop, the MCP server, `GET /v1/tools`, and the invocation log.

## Design notes & limits (v1)

- **No authentication in v1**: `/v1/chat`, `/mcp` and the other endpoints are
  open to anyone who can reach the pod (and `GATEWAY_HOST` defaults to
  `0.0.0.0`). Deploy behind an authenticating proxy or on a trusted network.
- **Single replica**: sessions live in an in-memory LangGraph checkpointer.
  The seam for horizontal scale is swapping in
  `langgraph-checkpoint-postgres` inside `sessions.py`.
- **Read-only by construction**: non-SELECT statements are rejected at the
  AST level (sqlglot, fail-closed); `EXPLAIN` is also blocked in v1.
- The chat UI is deliberately first-party and dependency-free (no CDN, no
  npm) so the whole service works air-gapped and stays license-clean. The
  one remote reference is the Hudi logo image, hotlinked from
  hudi.apache.org; offline it degrades to alt text.
