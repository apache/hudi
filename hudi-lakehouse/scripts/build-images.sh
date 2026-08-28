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

# Builds the hudi-lakehouse container images from locally-built Maven
# artifacts. Jars are staged into gitignored images/*/target/ directories so
# no binaries ever enter version control.
#
# For minikube, run `eval "$(minikube docker-env)"` first so images land in
# the cluster's Docker daemon.
#
# Usage: build-images.sh [--registry <prefix>] [--push]

set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO_ROOT="$(cd "$HERE/.." && pwd)"
REGISTRY=""
PUSH=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --registry)      REGISTRY="${2%/}/"; shift 2 ;;
    --push)          PUSH=1; shift ;;
    *) echo "unknown arg: $1" >&2; exit 1 ;;
  esac
done

# One Spark line for the lakehouse images; local-dev/example/spark-app.yaml
# pins the matching image tag and spark/scala versions.
SPARK_VERSION="3.5" SCALA_VERSION="2.12"
SPARK_IMAGE_TAG="3.5.7-scala2.12-java17-python3-ubuntu"
HADOOP_AWS_VERSION="3.3.4" AWS_SDK_VERSION="1.12.772"

BUNDLE_JAR=$(ls "$REPO_ROOT"/packaging/hudi-spark-bundle/target/hudi-spark${SPARK_VERSION}-bundle_${SCALA_VERSION}-*.jar 2>/dev/null | grep -v sources | grep -v original | head -1 || true)
PLUGIN_DIR=$(ls -d "$REPO_ROOT"/hudi-trino-plugin/target/trino-hudi-*/ 2>/dev/null | head -1 || true)

[[ -n "$BUNDLE_JAR" ]] || { echo "ERROR: spark bundle not found; run scripts/build-jars.sh first" >&2; exit 1; }
[[ -n "$PLUGIN_DIR" ]] || { echo "ERROR: trino plugin not found; run scripts/build-jars.sh first" >&2; exit 1; }

TRINO_IMAGE="${REGISTRY}hudi-lakehouse-trino:472"
SPARK_IMAGE="${REGISTRY}hudi-lakehouse-spark:${SPARK_VERSION}"
GATEWAY_IMAGE="${REGISTRY}hudi-lakehouse-agent-gateway:0.1.0"

# The gateway image needs no maven artifacts -- its build context is the
# python module itself.
echo ">>> Building $GATEWAY_IMAGE"
docker build -t "$GATEWAY_IMAGE" "$REPO_ROOT/hudi-agent-gateway"

echo ">>> Staging trino plugin: $(basename "${PLUGIN_DIR%/}")"
rm -rf "$HERE/images/trino/target" && mkdir -p "$HERE/images/trino/target"
cp -R "${PLUGIN_DIR%/}" "$HERE/images/trino/target/"
echo ">>> Building $TRINO_IMAGE"
docker build -t "$TRINO_IMAGE" "$HERE/images/trino"

echo ">>> Staging spark bundle: $(basename "$BUNDLE_JAR")"
rm -rf "$HERE/images/spark/target" && mkdir -p "$HERE/images/spark/target"
cp "$BUNDLE_JAR" "$HERE/images/spark/target/"
echo ">>> Building $SPARK_IMAGE"
docker build -t "$SPARK_IMAGE" \
  --build-arg SPARK_IMAGE_TAG="$SPARK_IMAGE_TAG" \
  --build-arg HADOOP_AWS_VERSION="$HADOOP_AWS_VERSION" \
  --build-arg AWS_SDK_VERSION="$AWS_SDK_VERSION" \
  --build-arg HUDI_SPARK_BUNDLE_JAR="$(basename "$BUNDLE_JAR")" \
  "$HERE/images/spark"

if [[ "$PUSH" == 1 ]]; then
  [[ -n "$REGISTRY" ]] || { echo "ERROR: --push requires --registry" >&2; exit 1; }
  docker push "$TRINO_IMAGE"
  docker push "$SPARK_IMAGE"
  docker push "$GATEWAY_IMAGE"
fi

echo ">>> Images ready: $TRINO_IMAGE, $SPARK_IMAGE, $GATEWAY_IMAGE"
