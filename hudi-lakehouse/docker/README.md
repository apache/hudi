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
# Docker installation

Run a local Hudi lakehouse and query it through either:

- **[MCP clients](#query-with-mcp):** use Claude Code or Codex CLI with the client's own model.
- **[Chat UI](#query-with-chat-ui):** use the built-in browser interface with a Gateway-configured LLM.

## Start the lakehouse

Start a local Hudi lakehouse with Docker Engine / Docker Desktop and Compose
2.27.2 or newer (required for `docker compose config --environment`). The installer
checks this version before contacting the daemon or starting services.
Allocate at least 4 CPUs and 8 GB RAM to Docker. The first image
build needs internet access and several GB of disk space. The host needs no Java,
Maven, Python, Kubernetes, Helm or kubectl.

Running `lakehouse.sh` without arguments (or with `--help`) displays help without
contacting Docker. Specify `up` to start the installation.

From a repository checkout:

```bash
./hudi-lakehouse/docker/lakehouse.sh up --build
```

This builds the existing Spark Connect and Gateway Dockerfiles, starts MinIO,
Derby-backed Hive Metastore, Spark Connect and Gateway, loads sample Hudi tables,
and verifies the real MCP query path. No Gateway LLM or API key is required for
MCP: your external MCP client supplies its own model. The built-in Chat UI and
HTTP Chat API use the Gateway's configured LLM. If it is unavailable or its model
is not configured, the installer reports that chat is unavailable and still exits
successfully after the lakehouse and MCP checks pass.

For subsequent starts, reuse the existing images without rebuilding:

```bash
./hudi-lakehouse/docker/lakehouse.sh up
```

Use `--build` again after changing image contents or removing the local images.

## Query with MCP

The MCP endpoint is `http://localhost:8000/mcp/` (Streamable HTTP).
Keep the lakehouse running and register it with your client. No Gateway LLM
configuration is needed for this path.

**Claude Code:**

```bash
claude mcp add --transport http hudi http://localhost:8000/mcp/
```

**Codex CLI:**

```bash
codex mcp add hudi --url http://localhost:8000/mcp/
```

Start a new client session and ask it to query the lakehouse. The sample tables are
`default.trips` (9 rows, partitioned by `trip_date`) and `default.drivers` (3 rows).
The MCP tools `list_tables`, `describe_table` and `query_lakehouse` are read-only.

Try: “List the tables”, “Describe trips”, or “For completed trips, sum fare_amount
plus tip_amount by city”. The last question should return:

| city | total |
|---|---:|
| chennai | 33.00 |
| san_francisco | 55.00 |
| sao_paulo | 77.00 |

The endpoint is local to your machine; remote hosted MCP clients cannot reach it.

## Query with Chat UI

Open **http://localhost:8000/ui/** and try the same sample questions above.
Chat UI and the HTTP Chat API use the Gateway's configured LLM. Choose one of
the following options; the commands reuse the images built in the startup step.

### Host Ollama (default)

With Ollama running on your host, download the default model and start the lakehouse:

```bash
ollama pull qwen3:8b
./hudi-lakehouse/docker/lakehouse.sh up
```

The default endpoint is `http://host.docker.internal:11434`; the Compose host-gateway
mapping also supports Linux. On Linux, Ollama must listen on an interface reachable
from containers. An inaccessible service or missing model produces a chat readiness
warning; lakehouse startup still succeeds if MCP works. The installer does not start
host Ollama or download host models automatically.

### Containerized Ollama

No host Ollama installation is required:

```bash
./hudi-lakehouse/docker/lakehouse.sh up --local-llm
```

This enables the `local-llm` profile, downloads `qwen3:8b`, and keeps models in a
named volume. Allow additional RAM/disk and time for the model download. This
profile uses CPU inference by default; it may be slow, especially on Docker Desktop.
Failure to start this explicitly requested service or download its model fails the
command. A subsequent chat readiness failure is reported without failing startup.
Use the same flag on subsequent `up` calls to keep the Gateway pointed at this Ollama.
The installer checks for the configured model locally and only pulls it when missing,
so a cached installation can restart without contacting the model registry. Model
commands run inside the existing Ollama container. To refresh an installed model
explicitly while Ollama is running, use your configured model name in this command:

```bash
docker compose -f hudi-lakehouse/docker/compose.yaml --profile local-llm exec ollama ollama pull qwen3:8b
```

### API provider

Export your key and model, then start the stack:

```bash
export OPENAI_API_KEY=...  # or ANTHROPIC_API_KEY
export GATEWAY_LLM_MODEL=...  # a model available to your provider/account
./hudi-lakehouse/docker/lakehouse.sh up
```

Without a `.env` or explicit provider, the script selects Anthropic when
`ANTHROPIC_API_KEY` is set, then OpenAI when `OPENAI_API_KEY` is set, otherwise
host Ollama. Keys are passed to the container and are never written by the installer.

### Save chat configuration

For persistent configuration, copy `.env.example` to `.env` in this directory and
set `GATEWAY_LLM_PROVIDER`, `GATEWAY_LLM_MODEL` and the corresponding key there.
`.env` is gitignored; shell variables override it. An OpenAI-compatible service uses
`GATEWAY_LLM_PROVIDER=openai-compatible`, `GATEWAY_OPENAI_BASE_URL`,
`GATEWAY_LLM_MODEL` and, if required, `OPENAI_API_KEY`.
For chat installations, non-Ollama providers need an explicit model. The
installer reads the values resolved by Compose, including `.env` interpolation
and shell overrides. A missing model produces a chat configuration warning but
does not prevent the lakehouse or MCP from starting. Only this directory's
`.env` is loaded; a `.env` in the caller's working directory is ignored.
An explicitly selected API provider still needs its required Gateway configuration
(for example, an OpenAI API key or an OpenAI-compatible base URL); invalid Gateway
configuration can prevent the service itself from starting.

## Manage the lakehouse

- **Start:** `./hudi-lakehouse/docker/lakehouse.sh up` starts services and verifies
  sample data in the seed job before starting the Gateway, then checks MCP and
  reports chat readiness separately.
- **Inspect status:** `./hudi-lakehouse/docker/lakehouse.sh status` lists the
  installation's containers and their state.
- **View logs:** `./hudi-lakehouse/docker/lakehouse.sh logs` shows recent service logs.
- **Verify data and queries:** `./hudi-lakehouse/docker/lakehouse.sh test` checks
  stored data and queries the running Gateway over MCP without reseeding or
  requiring an LLM.
- **Stop:** `./hudi-lakehouse/docker/lakehouse.sh down` removes containers and the
  network, preserving MinIO, HMS and Ollama volumes. Starting again with the same
  Compose project reuses them.

To intentionally **delete this installation's data**, run:

```bash
docker compose -f hudi-lakehouse/docker/compose.yaml --profile local-llm down --volumes
```

Use `COMPOSE_PROJECT_NAME` and the three port settings in `.env.example` for separate
installations. Preserve the same project name across `up`, `test` and `down` calls.
Do not run `docker compose config` in shared logs when API keys are configured:
Compose renders container environment values.

## Reference

### Services, storage and sample data

| Interface | Default address |
|---|---|
| Chat UI | http://localhost:8000/ui/ |
| MCP (Streamable HTTP) | http://localhost:8000/mcp/ |
| Spark Connect | sc://localhost:15002 |
| MinIO console | http://localhost:9001 (admin / password) |

All published ports bind to `127.0.0.1`. This is a local demo with fixed storage
credentials and unauthenticated services. HMS and the MinIO S3 API are internal
to the Compose network.

The existing catalog path is preserved:

```text
Chat / MCP -> Gateway tools -> Spark Connect -> spark_catalog (HoodieCatalog)
                                                   | metadata: Hive Metastore
                                                   | Hudi data: MinIO
```

`conf/hive-site.xml` is shared by schema initialization and the HMS server, including
the persistent Derby database path and S3A settings for HMS access to MinIO.
`conf/spark-defaults.conf` configures Spark Connect, its HMS client and Hudi storage access.
HMS is single-process Derby, matching the existing local-dev deployment; its image
runs under amd64 emulation on ARM hosts. Spark runs one shared `local[2]` driver.
If HMS initialization completed before its marker was saved, the next start validates
the existing Derby schema and restores the marker. A partial or invalid database is
preserved and startup stops with repair/restore guidance; it is never reinitialized
in place.

The seed container uses direct Spark Connect SQL to create the sample tables.
Dates are yesterday at the first seed. Stable record keys and inserts filtered against
existing keys make retries safe without overwriting existing demo rows. The seed
verifies counts, partitions and totals before the Gateway starts. Reserve these demo
table names: existing incompatible schemas or modified sample data fail validation
rather than being dropped or reset. No semantic-layer service or catalog abstraction
is added.

### Chat readiness checks

- **Gateway liveness — `/health`:** checks that the Gateway process is serving requests.
- **Dependency readiness — `/ready`:** checks SQL and the configured LLM. The
  installer verifies MCP independently before checking chat readiness when the
  chat model is configured.
- **Ollama model availability — `/api/tags`:** checks that the configured model
  exists on the Ollama server.
- **OpenAI-compatible model availability — `/models`:** checks the provider's
  model list, using authentication when configured. These model checks use the
  provider's own API, not the Gateway UI's fallback model list.
- **Hosted API providers:** readiness confirms that a key is configured; it does
  not validate the key or make a paid inference call.
- **Chat unavailable:** missing models and discovery failures produce warnings
  without failing installation. Chat routes remain exposed regardless of readiness.
- **Installation failures:** service startup, sample-data validation and MCP
  failures still return a nonzero exit status.

### Release images

The base `compose.yaml` consumes images only; source builds are isolated in
`compose.build.yaml` and enabled by `--build`. Once images are available in a
release registry, set `SPARK_CONNECT_IMAGE` and `GATEWAY_IMAGE` to their published
references in `.env`, then run `lakehouse.sh up` without `--build`. Compose pulls
missing images. The seed reuses the exact Connect image and its pinned Python
client instead of maintaining another Spark runtime.

Current default image names match the Helm charts and local build scripts. They
are **local build tags, not a claim that ASF publishes those images**. The RFC's
public registry namespace, versioned release downloads and `hudi.apache.org/lakehouse.sh`
bootstrap require release infrastructure decisions. This checkout provides the
Compose installation and contributor build path; it does not publish those endpoints.
Optional BI/Trino profiles, semantic models and minikube changes are separate work.

### Developer validation

Offline installer and Compose contract checks (Python standard library + Compose CLI):

```bash
python3 -m unittest discover -s hudi-lakehouse/tests -p 'test_docker_*.py' -v
```

To verify persistence without a seed masking data loss:

```bash
docker compose -f hudi-lakehouse/docker/compose.yaml up -d --force-recreate --wait spark-connect
./hudi-lakehouse/docker/lakehouse.sh test
```

The opt-in `Hudi Lakehouse Docker live acceptance` GitHub Actions workflow builds
the images, starts a clean installation, checks MCP, recreates containers without
reseeding to check persistence, and repeats installation to check seed idempotency.
It requires no LLM credentials. A real model chat can be tested separately through
the UI using the sample question above.
