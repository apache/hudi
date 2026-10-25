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

# Brings up the full local-dev lakehouse on the current kube context:
#   MinIO + Hive Metastore (manifests) + Apache Spark Kubernetes Operator
#   (upstream helm chart) + the hudi-trino product chart with local values.
#
# Prereqs: images built into the cluster daemon (scripts/build-images.sh
# after `eval "$(minikube docker-env)"`), kubectl context pointing at the
# target cluster.

set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
NS=hudi-lakehouse
SPARK_OPERATOR_VERSION="${SPARK_OPERATOR_VERSION:-1.7.0}"

echo ">>> Applying local-dev manifests (MinIO, Hive Metastore)"
# A previous down.sh may have left the namespace terminating; wait it out.
while [ "$(kubectl get namespace "$NS" -o jsonpath='{.status.phase}' 2>/dev/null)" = "Terminating" ]; do
  echo "waiting for namespace $NS to finish terminating..."; sleep 5
done
kubectl apply -f "$HERE/manifests/namespace.yaml"
kubectl apply -f "$HERE/manifests/minio.yaml"
kubectl apply -f "$HERE/manifests/hive-metastore.yaml"
# Jobs are immutable: delete any previous run so re-applying converges
# (the job itself is a no-op when the bucket already exists).
kubectl -n "$NS" delete job minio-make-bucket --ignore-not-found
kubectl apply -f "$HERE/manifests/minio-bucket-job.yaml"

echo ">>> Installing Apache Spark Kubernetes Operator (chart ${SPARK_OPERATOR_VERSION})"
helm repo add spark-kubernetes-operator https://apache.github.io/spark-kubernetes-operator 2>/dev/null || true
helm repo update spark-kubernetes-operator >/dev/null
helm upgrade --install spark-kubernetes-operator \
  spark-kubernetes-operator/spark-kubernetes-operator \
  --version "$SPARK_OPERATOR_VERSION" \
  --namespace "$NS"

echo ">>> Installing hudi-trino chart with local-dev values"
helm upgrade --install hudi-trino "$ROOT/charts/hudi-trino" \
  --namespace "$NS" \
  -f "$ROOT/charts/hudi-trino/values-local-dev.yaml" \
  --wait --timeout 10m

if [[ "${INSTALL_VLLM:-0}" == 1 ]]; then
  # Optional: serve an open-weight model in-cluster with vLLM (GPU required)
  # and point the gateway at it instead of the default Ollama-on-host.
  VLLM_MODEL="${VLLM_MODEL:-Qwen/Qwen3-8B}"
  echo ">>> Installing vllm chart (model: $VLLM_MODEL; requires a GPU node)"
  helm upgrade --install vllm "$ROOT/charts/vllm" \
    --namespace "$NS" \
    --set fullnameOverride=vllm \
    --set model="$VLLM_MODEL"
  GATEWAY_EXTRA_ARGS=(
    --set gateway.provider=openai-compatible
    --set gateway.openaiCompatible.baseUrl="http://vllm.$NS.svc.cluster.local:8000/v1"
    --set gateway.model="$VLLM_MODEL"
  )
else
  GATEWAY_EXTRA_ARGS=()
fi

if [[ "${INSTALL_AGENT_GATEWAY:-1}" == 1 ]]; then
  echo ">>> Installing hudi-agent-gateway chart with local-dev values"
  # No --wait: readiness deliberately reflects LLM reachability, and the
  # default local-dev provider is Ollama on the host, which may not be
  # running yet. The pod goes Ready once `ollama serve` is reachable.
  helm upgrade --install hudi-agent-gateway "$ROOT/charts/hudi-agent-gateway" \
    --namespace "$NS" \
    -f "$ROOT/charts/hudi-agent-gateway/values-local-dev.yaml" \
    "${GATEWAY_EXTRA_ARGS[@]}"
fi

echo ">>> Waiting for MinIO + Hive Metastore readiness"
kubectl -n "$NS" rollout status deploy/minio --timeout=300s
kubectl -n "$NS" rollout status deploy/hive-metastore --timeout=600s
kubectl -n "$NS" wait --for=condition=complete job/minio-make-bucket --timeout=300s

echo ">>> local-dev lakehouse is up:"
kubectl -n "$NS" get pods
echo
echo "Next: ./local-dev/scripts/run-example.sh  (writes a Hudi table, syncs to HMS)"
