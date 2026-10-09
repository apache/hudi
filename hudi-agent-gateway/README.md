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

The tools query the lakehouse through Trino (default) or the official PySpark
Connect client: `query_lakehouse` (guarded,
read-only SQL), `list_tables`, `describe_table`. Every model-written query
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
ollama pull qwen3:4b-instruct
GATEWAY_LLM_PROVIDER=ollama GATEWAY_LLM_MODEL=qwen3:4b-instruct hudi-agent-gateway serve
```

The chat UI shows progress and elapsed time while a response is running. **Stop**
closes the response stream and cancels the agent task. If the gateway does not
accept the connection within 15 seconds, the UI reports a connection timeout.
The server's chat timeout returns HTTP 504
for JSON requests or a `chat_timeout` SSE error. Provider/SQL cancellation is
best-effort; remote work may take time to stop. After Stop or an error the UI
starts a new conversation on the next message to avoid incomplete tool-call history.

Connect an MCP client:

```bash
claude mcp add --transport http hudi-lakehouse http://localhost:8000/mcp/
```

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
| `GATEWAY_LLM_MODEL` | `qwen3:4b-instruct` | model name for the provider |
| `ANTHROPIC_API_KEY` / `OPENAI_API_KEY` | — | required by the matching provider |
| `GATEWAY_OLLAMA_BASE_URL` | `http://localhost:11434` | Ollama endpoint |
| `GATEWAY_OPENAI_BASE_URL` | — | endpoint for `openai-compatible` (vLLM, Together, …) |
| `GATEWAY_LLM_TIMEOUT_SECONDS` | `120` | HTTP operation/read timeout; streaming data can keep a call alive |
| `GATEWAY_CHAT_TIMEOUT_SECONDS` | `300` | total wall-clock limit for one chat turn, including all model/tool calls |
| `GATEWAY_SQL_BACKEND` | `trino` | `trino` or `spark-connect` |
| `GATEWAY_SPARK_CONNECT_URI` | `sc://hudi-spark-connect.hudi-lakehouse.svc:15002` | PySpark Connect URI; optional `/;key=value` parameters |
| `GATEWAY_SPARK_CONNECT_CATALOG` / `_SCHEMA` | `spark_catalog` / `default` | Spark query and metadata defaults |
| `GATEWAY_TRINO_HOST` / `_PORT` | `hudi-trino.hudi-lakehouse.svc` / `8080` | Trino coordinator |
| `GATEWAY_TRINO_CATALOG` / `_SCHEMA` / `_USER` | `hudi` / `default` / `hudi-agent-gateway` | query defaults |
| `GATEWAY_SQL_ROW_CAP` | `200` | LIMIT enforced on every query |
| `GATEWAY_SQL_TIMEOUT_SECONDS` | `120` | per-query timeout |
| `GATEWAY_TOOL_RESULT_MAX_BYTES` | `50000` | tool results truncated beyond this (with notice) |
| `GATEWAY_AGENT_MAX_ITERATIONS` | `25` | agent loop recursion limit |
| `GATEWAY_SESSION_TTL_SECONDS` / `GATEWAY_MAX_SESSIONS` | `3600` / `1000` | session store bounds |
| `GATEWAY_MAX_MESSAGES_PER_SESSION` | `40` | context window per session (trimmed pre-model) |
| `GATEWAY_SYSTEM_PROMPT_EXTRA` | — | appended to the built-in system prompt |
| `GATEWAY_MCP_ENABLED` | `true` | serve /mcp |
| `GATEWAY_HOST` / `GATEWAY_PORT` / `GATEWAY_LOG_LEVEL` | `0.0.0.0` / `8000` / `INFO` | server basics |

Startup never depends on the LLM or the selected SQL backend being reachable: `/health` is
liveness, `/ready` reports per-dependency status (and gates the Kubernetes
readiness probe).

## Spark Connect backend

Install the gateway in a Python 3.11/3.12 environment. Its dependencies include
`pyspark-client==4.1.3`, matching the chart's Spark version, and PyArrow 18.1.0.
The official Python Connect client sends RPCs to the remote Spark server; the
gateway image requires no Java runtime, Spark JARs, or native ADBC driver.
PySpark also brings pandas, gRPC, and protobuf dependencies; use a dedicated
environment and upgrade the client alongside the server.

After deploying the [Spark Connect chart](../hudi-lakehouse/charts/hudi-spark-connect/README.md)
and forwarding port 15002:

```bash
GATEWAY_SQL_BACKEND=spark-connect \
GATEWAY_SPARK_CONNECT_URI='sc://localhost:15002' \
  hudi-agent-gateway serve
```

Use Helm `sqlBackend: spark-connect` and `sparkConnect.uri/catalog/schema`,
or apply the chart's `values-spark-connect.yaml` overlay. An optional
`sparkConnect.existingSecret` supplies the URI via `sparkConnect.uriSecretKey`.
`/ready` reports a `spark-connect` check (cached SELECT 1) and the LLM check;
`/v1/info`, Agent prompts, MCP tool descriptions, and the UI reflect the selected
backend. Connection credentials are omitted from `/v1/info`.

`query_lakehouse` accepts Spark SQL with backtick identifiers and enforces the
same single-SELECT/AST/LIMIT policy. `list_tables` uses SHOW TABLES and reports
`TABLE` for persistent objects (Spark's SHOW TABLES does not distinguish views
from tables), `TEMPORARY` for temporary objects. `describe_table` uses a zero-row
SELECT and PySpark's schema, preserving column/type/nullability result columns.
Types use Spark SQL names such as `int`, `bigint`, and `array<string>`. Each call
creates an independent remote session, sets its catalog/schema, and stops that
session in a finally block. Sessions are not shared across tool calls.

**Bounds:** SQL LIMIT and DataFrame.limit before collect cap query result rows;
the existing JSON size cap truncates tool responses. Wide rows can still consume
substantial client memory, and SHOW TABLES can enumerate the whole namespace
before returning limited rows. The JSON cap is a response limit, not a
server/memory budget. Queries use at most eight workers with no accumulating
queue. A timed-out call keeps its capacity slot until execution and cleanup
actually finish.

**Timeout/cancellation:** gateway timeouts return promptly and request
`SparkSession.interruptAll()` on a separate thread; task cancellation follows
the same path. Each request owns its session, so this interrupts only that
request's operations. Spark provides a server interruption RPC, but network
failures and races with operation startup mean cancellation remains best-effort.
A timeout does not prove Spark stopped. Hung RPCs can delay session cleanup;
worker pools shut down without waiting, and Python cannot forcibly terminate
worker threads. Restart the process if all eight slots remain stuck. Connect
URIs can use `/;use_ssl=true;token=...` for an appropriately configured endpoint;
the chart itself has no authentication or TLS. Gateway tools expose no DML;
direct Superset/Zeppelin writes have their own
[tutorials and limitations](../hudi-lakehouse/charts/hudi-spark-connect/README.md#spark-connect-client-tutorials).

Opt-in live tests use an existing table and do not write data:

```bash
GATEWAY_IT_SPARK_CONNECT_URI='sc://localhost:15002' \
GATEWAY_IT_SPARK_TABLE=bi_adbc_demo.orders \
  .venv/bin/python -m pytest tests/integration/test_spark_connect_live.py -q
```

Add `GATEWAY_IT_LLM=1` to exercise the real Agent loop with the configured LLM.

## Development

```bash
.venv/bin/pytest              # offline suite (fake Trino + scripted model)
.venv/bin/ruff check src tests
.venv/bin/mypy src

# live integration (against a port-forwarded local-dev stack):
GATEWAY_IT_TRINO_HOST=localhost GATEWAY_IT_TRINO_PORT=18080 .venv/bin/pytest tests/integration
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
