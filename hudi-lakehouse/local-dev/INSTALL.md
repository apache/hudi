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
# Local minikube installation

This checkout-based installer runs MinIO, a persistent Hive Metastore, Spark
Connect and the Agent Gateway (Chat UI and MCP). It reuses the local-dev
manifests, existing Helm charts and Docker installation's sample/probe scripts.
No host JVM or Maven is required. It does not publish images, add a semantic
layer, or configure a production deployment.

## Install

Requirements: Python 3.8+, Docker, minikube, kubectl and Helm 3. Allocate at
least 4 CPUs and 8 GiB to the cluster and sufficient Docker memory for any
other running workloads. First builds and cluster creation require network
access. The stock HMS image is amd64-only; ARM hosts require working emulation.

From the repository root:

```bash
# Build application images from this checkout, load them into minikube, install,
# initialize missing samples, verify SQL + MCP, and attempt a real chat tool call.
./hudi-lakehouse/local-dev/lakehouse.sh up --build

# Subsequent installations reuse Docker images (including Compose-built images)
# or images already loaded into this profile. Nothing is rebuilt automatically.
./hudi-lakehouse/local-dev/lakehouse.sh up
```

By default, all `lakehouse.sh` commands use the **hudi-lakehouse** profile and
namespace. You can omit `--profile`. To use a different profile for the shell
session, run `export HUDI_MINIKUBE_PROFILE=my-lakehouse`. An explicit
`--profile` overrides that setting for a single command.

The installer starts minikube with a temporary kubeconfig. On first install it
adds the profile to your kubeconfig and preserves your current context.
Before accessing workloads, it checks that the host context named for the
profile points to the profile's API server. If that name belongs to another
cluster, choose a different profile. The namespace is fixed because the existing
manifests, service DNS names and charts use it. Use separate profiles for
isolated installations. An existing Trino Gateway in the selected profile
causes `up` to refuse to repoint it.

`up` can be repeated after failure; there is no automatic rollback or data
cleanup. It waits for storage, HMS, Connect and Gateway before querying data.
A failed seed, SQL probe or MCP check exits nonzero. An unreachable/missing LLM
or failed chat tool call is reported separately and leaves MCP usable. An
invalid provider configuration that prevents the Gateway process from starting
is a deployment failure, not a successful MCP installation.

No arguments or `--help` prints help without contacting Docker or Kubernetes.
Use `--driver`, `--cpus`, and `--memory` to configure a new minikube profile.
When restarting a stopped profile created with a non-Docker driver, pass the
same `--driver` to `up`. If the initial install was interrupted before its
context was registered, run `lakehouse.sh stop` for that profile and repeat
your original `up` command.

## Images and Helm overrides

The default application tags are **local build tags, not published ASF images**:

| Component | Default image |
|---|---|
| Spark Connect | `hudi-lakehouse-spark-connect:4.1.3-hudi1.2.0` |
| Agent Gateway | `hudi-lakehouse-agent-gateway:0.1.0` |
| MinIO | `cgr.dev/chainguard/minio:latest` |
| Bucket initializer | `cgr.dev/chainguard/minio-client:latest-dev` |
| HMS | `apache/hive:3.1.3` |

The normal path compares image IDs and skips unchanged images already in the
profile; changed or missing images are loaded with `minikube image load`. Application
pods use `Never` pull policy, so missing unpublished images fail with actionable
instructions. Infrastructure images are reused when locally available and may
otherwise be pulled. Application images already in the cluster can also be
reused. Host Docker must still be available for local-image inspection.

For images you have already built under different tags:

```bash
./hudi-lakehouse/local-dev/lakehouse.sh up \
  --connect-image my-connect:dev --gateway-image my-gateway:dev
```

Registry images accept an explicit tag or a digest (`repository@sha256:<64 hex digits>`):

```bash
./hudi-lakehouse/local-dev/lakehouse.sh up --pull-images \
  --connect-image registry.example.com/team/connect:dev \
  --gateway-image registry.example.com/team/gateway:dev
```

`--pull-images` uses `IfNotPresent`, performs no builds or host image loads,
and relies on cluster registry access. Use a new tag or digest for each registry
update. In local-image mode, a plain `up` compares the image IDs and updates the
affected Deployments even when you rebuild under the same tag. Unchanged images
keep their running pods. `--build` also accepts custom application tags.
`SPARK_CONNECT_IMAGE`, `GATEWAY_IMAGE`, `MINIO_IMAGE`, `MINIO_CLIENT_IMAGE` and
`HMS_IMAGE` provide equivalent image defaults; infrastructure CLI overrides are
`--minio-image`, `--mc-image`, `--hms-image`.

`--connect-values path.yaml` and `--gateway-values path.yaml` merge additional
Helm settings after local defaults. Image selection and the local Gateway
readiness path are installer-owned. Keep the local service names, Spark backend,
S3/HMS locations and namespace consistent. Save your chosen command/values and
reuse them on subsequent `up` calls; overrides are not inferred from old Helm
releases. Do not run two installers against the same profile concurrently.

## Query through MCP or Spark Connect

Keep this command running in a terminal:

```bash
./hudi-lakehouse/local-dev/lakehouse.sh forward
# --port 18000 chooses a different localhost Gateway port.
```

Chat UI: <http://127.0.0.1:8000/ui/>. MCP: <http://127.0.0.1:8000/mcp/>.
The tunnel binds only to `127.0.0.1`; Ctrl-C closes it. No detached background
process or PID file is created. For an external MCP client:

```bash
claude mcp add --transport http hudi http://127.0.0.1:8000/mcp/
codex mcp add hudi --url http://127.0.0.1:8000/mcp/
```

External MCP clients supply their own model. They do not require the Gateway's
LLM provider. Discover `list_tables`, inspect `describe_table`, then use
`query_lakehouse` for read-only SQL. Fresh samples contain 9 trips and 3 drivers:

```sql
SELECT city, SUM(fare_amount + tip_amount) AS gross_bookings
FROM trips WHERE trip_status = 'completed' GROUP BY city ORDER BY city;
-- chennai 33.00; san_francisco 55.00; sao_paulo 77.00

SELECT d.home_city, COUNT(*) AS trips FROM trips t
JOIN drivers d ON t.driver_id = d.driver_id
GROUP BY d.home_city ORDER BY d.home_city;
-- 3 trips in each city
```

Direct Spark Connect and MinIO console access:

```bash
kubectl --context hudi-lakehouse -n hudi-lakehouse port-forward --address 127.0.0.1 svc/hudi-spark-connect 15002:15002
kubectl --context hudi-lakehouse -n hudi-lakehouse port-forward --address 127.0.0.1 svc/minio 9001:9001
```

Use `sc://127.0.0.1:15002` with a compatible Spark Connect client. MinIO uses
`admin` / `password`. These services are unauthenticated local development
services with fixed storage credentials; keep tunnels on localhost.

## Built-in chat

By default the Gateway uses host Ollama at
`http://host.minikube.internal:11434` with `qwen3:4b` (the existing local-dev
chart default). Make the provider reachable **from the minikube container** and
pull a tool-capable model. A host-only listener may be unreachable from pods;
use a provider address that your driver can route to, and restrict network
access if changing the host listener. No model is downloaded by this installer.

Example Gateway override file:

```yaml
gateway:
  provider: ollama
  model: qwen3:4b
  ollama:
    baseUrl: http://host.minikube.internal:11434
```

Apply it with `up --gateway-values /path/to/gateway.yaml`. For hosted providers,
set `gateway.provider` and `gateway.model` explicitly and use
`gateway.existingSecret` containing `OPENAI_API_KEY` or `ANTHROPIC_API_KEY`.
For an OpenAI-compatible endpoint also set `gateway.openaiCompatible.baseUrl`.
The installer does not source `.env` files or infer a provider from ambient API
keys. See the Gateway chart values for supported settings. Do not commit keys
in values files. Hosted calls can incur provider charges.

`up` first verifies MCP, then checks provider/model readiness and attempts a real
`/v1/chat` request. The chat check requires a successful `query_lakehouse` tool
result for `SELECT 1 AS installation_probe`; model-list availability or a
plausible answer alone is not considered success. It does not require the
sample data to remain unchanged. Use `lakehouse.sh chat` for a strict rerun
(nonzero if unavailable or tool verification fails). For a fresh sample dataset,
ask the UI: “Show completed-trip gross bookings by city” or “Join trips and
drivers and count trips by home city.”

The local install sets the Gateway readiness probe to `/health` so Kubernetes
keeps routing MCP even when the LLM is absent. `/ready` still reports both SQL
and LLM status. The chart defaults to `readinessProbePath: /ready`. Small local
models can answer the probe without calling the SQL tool. If MCP passed and chat
reports a missing tool call, retry `lakehouse.sh chat` or select a model with
more reliable tool calling.

## Status, diagnostics and lifecycle

| Command | Behavior |
|---|---|
| `status` | Profile status, deployments, pods, services, jobs, PVCs, access commands |
| `doctor` | Status plus events, bounded service logs and separate LLM readiness output |
| `test` | Read-only strict sample verification: counts, city totals, partition and MCP join checks; never reseeds |
| `chat` | SQL/MCP probes, provider readiness and actual built-in chat SQL tool call |
| `down` | Checks that the named context points to the running minikube profile, then stops workloads and preserves the namespace, Helm releases and PVCs |
| `stop` | Stops the minikube profile, preserving its local disks |
| `up` | Starts/resumes and creates only missing sample tables |

For a stopped profile, `status` and `doctor` print the minikube status, skip
workload queries and return minikube's exit code.
`doctor` logs may contain application/query information; review before sharing.
Image errors: inspect pod events and use `up --build`, load local images, or
supply registry tags with `--pull-images`. Pending PVCs: check the minikube
storage provisioner/default storage class. HMS startup failure: inspect Derby
validation logs and back up the volume before repair. Connect failure: inspect
HMS/S3 connectivity and Spark logs. MCP failure fails installation even if the
LLM is healthy. Chat failure with passing MCP points to the provider, model,
network, credentials or tool-call support. No remediation deletes data.

For a cached Docker-runtime minikube image on Kubernetes 1.36+, an exec error
containing `//[::]:.../cri/exec` and `HTTP response to HTTPS client` can come from
[the cri-dockerd streaming bug fixed in 0.4.8](https://github.com/Mirantis/cri-dockerd/releases/tag/v0.4.8).
Upgrade the node runtime/base image, or use the client-side workaround
`KUBECTL_REMOTE_COMMAND_WEBSOCKETS=false` when invoking the installer and manual
`kubectl exec` checks. For port-forward transport problems on such cached
runtimes, try `KUBECTL_PORT_FORWARD_WEBSOCKETS=false`; local acceptance used
this setting for the host-facing MCP tunnel. With a host proxy, include
localhost, `127.0.0.1`, `::1`
and the profile IP in both `NO_PROXY` and `no_proxy`; preserve any existing
exclusions. The installer does not rewrite cluster runtime settings or proxy
configuration. A Ready pod alone does not prove the exec/data checks passed.

### Data protection and existing developer installations

`minio-data` (10 GiB) holds Hudi objects and `hive-metastore-data` (1 GiB) holds
the Derby catalog. **Keep both together.** Both deployments use `Recreate` to
avoid concurrent writers to a local volume. HMS reuses its existing database;
an incomplete database fails validation rather than being initialized over.
The bootstrap and S3 configuration are shared with the Docker installation.

On the first upgrade from the previous local-dev manifest, HMS finds a Derby
database without a `.schema-initialized` marker. It runs `schematool -validate`
before writing the marker. In a minikube upgrade test, both PVCs retained their
UIDs, HMS logged `Done with metastore validation: [SUCCESS]`, and Spark Connect
read back a Hudi row written before the upgrade.

Sample initialization uses the existing Docker seed: it creates only missing
`default.trips` and `default.drivers`, validates newly created samples, and skips
all existing tables, including empty or modified tables. It does not repair,
drop, truncate or overwrite them. An interrupted creation may leave a table
that needs manual inspection; reruns preserve it. `up` uses small readability
probes for existing data. `test` deliberately fails if data differs from the
original sample, without changing it. The original Spark writer example uses a
different trips schema; use an isolated profile for this demo.

**Behavior change:** `local-dev/scripts/down.sh` previously deleted the entire
namespace and thus its PVCs. It now stops known workloads and leaves data and
Helm releases in place. It still removes disposable SparkApplication objects
while their operator is alive, before stopping the operator. The original
`up.sh` (Trino/operator) and `quickstart.sh` flows remain available;
`up.sh --spark-connect` alone retains its earlier Connect-only behavior.
The new installer opts in with `--with-agent-gateway`. No Trino or Spark 3.5
writer image build is required by this installation.

To permanently erase local tables and the catalog, back up both volumes first.
Set `profile` to the installed profile, then stop workloads and delete the namespace:

```bash
profile=${HUDI_MINIKUBE_PROFILE:-hudi-lakehouse}
./hudi-lakehouse/local-dev/lakehouse.sh down --profile "$profile" &&
  kubectl --context "$profile" delete namespace hudi-lakehouse
```

## Tests

```bash
uv run --with pyyaml python -m unittest discover -s hudi-lakehouse/tests -p 'test_minikube_installation.py'
uv run --with pyyaml python -m unittest discover -s hudi-lakehouse/tests -p 'test_*chart.py'
python3 -m unittest discover -s hudi-lakehouse/tests -p 'test_docker_*.py'
```

For live acceptance use a fresh `--profile`, run `up`, `test`, repeat `up`,
record PVC UIDs, run `down`/`up` and confirm UIDs and samples are unchanged.
In a disposable profile, modify a sample and empty one table through Connect,
then repeat `down`/`up`: edits/emptiness must remain, MCP probes must pass, and
`test` must reject the modified samples. Run `chat` with a reachable tool-capable
LLM and repeat with an unreachable provider to verify independent MCP access.

The shared destructive acceptance helper is only for that **disposable** profile:

```bash
kubectl --context disposable-test -n hudi-lakehouse exec -i deploy/hudi-spark-connect -- \
  python3 - modify --connect-uri sc://127.0.0.1:15002 < hudi-lakehouse/tests/docker_data_lifecycle.py
./hudi-lakehouse/local-dev/lakehouse.sh down --profile disposable-test
./hudi-lakehouse/local-dev/lakehouse.sh up --profile disposable-test
kubectl --context disposable-test -n hudi-lakehouse exec -i deploy/hudi-spark-connect -- \
  python3 - verify --connect-uri sc://127.0.0.1:15002 < hudi-lakehouse/tests/docker_data_lifecycle.py
# Expected nonzero: samples were intentionally modified, and are not repaired.
./hudi-lakehouse/local-dev/lakehouse.sh test --profile disposable-test
```
