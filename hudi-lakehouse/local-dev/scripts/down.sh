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

# Stop local-dev workloads without deleting PVCs or Helm configuration.
set -euo pipefail
NS=hudi-lakehouse
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
if [[ $# -ne 0 ]]; then
  echo 'Usage: down.sh (stops workloads and preserves data; no deletion flags)' >&2
  exit 2
fi
# SparkApplication finalizers need the running operator. Preserve the original
# developer cleanup ordering; application objects are disposable, PVCs are not.
spark_resources="$(kubectl api-resources --api-group=spark.apache.org -o name)"
if grep -qx 'sparkapplications.spark.apache.org' <<< "$spark_resources"; then
  kubectl -n "$NS" delete sparkapplications.spark.apache.org --all --timeout=120s
fi
for deployment in hudi-agent-gateway hudi-spark-connect hudi-trino vllm minio hive-metastore spark-kubernetes-operator; do
  existing="$(kubectl -n "$NS" get deployment "$deployment" --ignore-not-found -o name)"
  if [[ -n "$existing" ]]; then
    kubectl -n "$NS" scale deployment "$deployment" --replicas=0
    kubectl -n "$NS" rollout status deployment/"$deployment" --timeout=180s
  fi
done
# Replica counts can reach zero while a terminating pod still holds its volume.
kubectl -n "$NS" wait --for=delete pod -l 'app.kubernetes.io/name in (minio,hive-metastore)' --timeout=180s
kubectl -n "$NS" delete job minio-make-bucket --ignore-not-found --wait=true
echo '>>> Workloads stopped. Namespace, Helm releases, MinIO and HMS PVCs are preserved.'
echo 'Run the same up command to resume. Namespace/profile deletion would destroy local data.'
