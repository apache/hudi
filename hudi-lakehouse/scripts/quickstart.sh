#!/usr/bin/env bash
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

# One-command, idempotent bring-up of the local-dev Hudi lakehouse.
#
#   ./hudi-lakehouse/scripts/quickstart.sh [--run-example]
#
# Safe to re-run at any time: every step checks whether its work is already
# done (jars built, minikube running, images present, releases installed)
# and skips or converges accordingly.
#
# Steps: preflight -> minikube -> maven artifacts (spark bundle + trino
# plugin; a JDK 23 is auto-provisioned into a gitignored .tools/ dir if none
# is installed) -> container images (into minikube's docker daemon) ->
# MinIO + Hive Metastore + Spark Operator + hudi-trino chart.
#
# Flags:
#   --run-example       run the example Spark job at the end, so a Hudi table
#                       is registered and ready to query from Trino
#   --rebuild-jars      force the maven builds even if artifacts exist
#   --rebuild-images    force the docker builds even if images exist

set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO_ROOT="$(cd "$HERE/.." && pwd)"
RUN_EXAMPLE=0 REBUILD_JARS=0 REBUILD_IMAGES=0
# One Spark line for the quickstart; the example manifest pins the same tag.
SPARK_VERSION="3.5" SCALA_VERSION="2.12"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --run-example)    RUN_EXAMPLE=1; shift ;;
    --rebuild-jars)   REBUILD_JARS=1; shift ;;
    --rebuild-images) REBUILD_IMAGES=1; shift ;;
    *) echo "unknown arg: $1" >&2; exit 1 ;;
  esac
done

step() { printf '\n\033[1m==> %s\033[0m\n' "$*"; }

# ---------------------------------------------------------------- preflight
step "Preflight"
MISSING=()
for t in docker minikube helm kubectl mvn java curl; do
  command -v "$t" >/dev/null || MISSING+=("$t")
done
if [[ ${#MISSING[@]} -gt 0 ]]; then
  echo "ERROR: missing required tools: ${MISSING[*]}" >&2
  echo "       install them and re-run (macOS: brew install docker minikube helm kubectl maven temurin)" >&2
  exit 1
fi
echo "all required tools present"

# ----------------------------------------------------------------- minikube
step "Minikube"
if minikube status >/dev/null 2>&1; then
  echo "minikube already running"
else
  minikube start --cpus 4 --memory 10g
fi

# ---------------------------------------------------- locate / provision JDKs
# The trino plugin needs JDK 23+; the rest of the repo builds on the default
# JDK. Resolution order for 23: $JDK23_HOME -> /usr/libexec/java_home (macOS)
# -> auto-download Temurin into the gitignored .tools/ directory.
find_jdk23() {
  if [[ -n "${JDK23_HOME:-}" && -x "$JDK23_HOME/bin/java" ]]; then
    echo "$JDK23_HOME"; return
  fi
  if [[ "$(uname)" == "Darwin" ]]; then
    local jh
    jh=$(/usr/libexec/java_home -v 23+ 2>/dev/null || true)
    [[ -n "$jh" ]] && { echo "$jh"; return; }
  fi
  local cached
  cached=$(ls -d "$HERE"/.tools/jdk23/*/Contents/Home "$HERE"/.tools/jdk23/*/ 2>/dev/null | head -1 || true)
  if [[ -n "$cached" && -x "$cached/bin/java" ]]; then
    echo "$cached"; return
  fi
  echo "downloading Temurin JDK 23 into $HERE/.tools/jdk23 (one-time)..." >&2
  local os arch
  case "$(uname)" in Darwin) os=mac ;; *) os=linux ;; esac
  case "$(uname -m)" in arm64|aarch64) arch=aarch64 ;; *) arch=x64 ;; esac
  mkdir -p "$HERE/.tools/jdk23"
  curl -sL "https://api.adoptium.net/v3/binary/latest/23/ga/${os}/${arch}/jdk/hotspot/normal/eclipse" \
    | tar xz -C "$HERE/.tools/jdk23"
  find_jdk23
}

# ------------------------------------------------------------ maven artifacts
step "Maven artifacts"
BUNDLE_JAR=$(ls "$REPO_ROOT"/packaging/hudi-spark-bundle/target/hudi-spark${SPARK_VERSION}-bundle_${SCALA_VERSION}-*.jar 2>/dev/null | grep -v sources | grep -v original | head -1 || true)
if [[ -z "$BUNDLE_JAR" || "$REBUILD_JARS" == 1 ]]; then
  "$HERE/scripts/build-jars.sh" --skip-plugin
else
  echo "spark bundle present: $(basename "$BUNDLE_JAR")"
fi

PLUGIN_DIR=$(ls -d "$REPO_ROOT"/hudi-trino-plugin/target/trino-hudi-*/ 2>/dev/null | head -1 || true)
if [[ -z "$PLUGIN_DIR" || "$REBUILD_JARS" == 1 ]]; then
  JDK23="$(find_jdk23)"
  echo "building trino plugin with JDK at $JDK23"
  JAVA_HOME="$JDK23" "$HERE/scripts/build-jars.sh" --skip-bundle
else
  echo "trino plugin present: $(basename "${PLUGIN_DIR%/}")"
fi

# ------------------------------------------------------------------- images
step "Container images (minikube docker daemon)"
eval "$(minikube docker-env)"
NEED_IMAGES=0
docker image inspect hudi-lakehouse-trino:472 >/dev/null 2>&1 || NEED_IMAGES=1
docker image inspect "hudi-lakehouse-spark:${SPARK_VERSION}" >/dev/null 2>&1 || NEED_IMAGES=1
docker image inspect hudi-lakehouse-agent-gateway:0.1.0 >/dev/null 2>&1 || NEED_IMAGES=1
if [[ "$NEED_IMAGES" == 1 || "$REBUILD_IMAGES" == 1 ]]; then
  "$HERE/scripts/build-images.sh"
else
  echo "images present: hudi-lakehouse-trino:472, hudi-lakehouse-spark:${SPARK_VERSION}, hudi-lakehouse-agent-gateway:0.1.0"
fi

# ------------------------------------------------------------------ bring-up
step "Lakehouse bring-up (MinIO, Hive Metastore, Spark Operator, hudi-trino)"
"$HERE/local-dev/scripts/up.sh"

# ------------------------------------------------------------------- example
if [[ "$RUN_EXAMPLE" == 1 ]]; then
  step "Example job (Spark write + hive-sync)"
  "$HERE/local-dev/scripts/run-example.sh"
  step "Querying from Trino"
  kubectl -n hudi-lakehouse exec deploy/hudi-trino -- trino --output-format ALIGNED \
    --execute "SELECT city, count(*) AS trips FROM hudi.default.trips GROUP BY city ORDER BY city"
fi

step "Done"
cat <<'EOF'
The local Hudi lakehouse is up. Useful commands:

  # write a Hudi table and register it in the metastore (idempotent)
  ./hudi-lakehouse/local-dev/scripts/run-example.sh

  # query it from Trino
  kubectl -n hudi-lakehouse exec deploy/hudi-trino -- \
    trino --execute "SELECT city, count(*) FROM hudi.default.trips GROUP BY city"

  # consoles
  kubectl -n hudi-lakehouse port-forward svc/hudi-trino 8080:8080   # Trino UI
  kubectl -n hudi-lakehouse port-forward svc/minio 9001:9001        # MinIO (admin/password)

  # tear everything down
  ./hudi-lakehouse/local-dev/scripts/down.sh
EOF
