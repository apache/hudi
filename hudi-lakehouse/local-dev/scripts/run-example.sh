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

# Runs the example Hudi write job:
#   1. uploads local-dev/example/hudi_table_writer.py to s3a://warehouse/jobs/
#      (ConfigMap -> one-shot mc Job)
#   2. applies the SparkApplication (Apache Spark Kubernetes Operator)
#   3. waits for completion and prints the driver log tail

set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NS=hudi-lakehouse
APP=hudi-table-writer

echo ">>> Uploading example job script to MinIO"
kubectl -n "$NS" create configmap example-job-script \
  --from-file="$HERE/example/hudi_table_writer.py" \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl -n "$NS" delete job upload-example-script --ignore-not-found
kubectl -n "$NS" apply -f - <<'EOF'
apiVersion: batch/v1
kind: Job
metadata:
  name: upload-example-script
  namespace: hudi-lakehouse
spec:
  backoffLimit: 4
  template:
    spec:
      restartPolicy: OnFailure
      containers:
        - name: mc
          image: quay.io/minio/mc:RELEASE.2024-11-21T17-21-54Z
          command:
            - /bin/sh
            - -c
            - |
              set -e
              mc alias set local http://minio:9000 admin password
              mc cp /scripts/hudi_table_writer.py local/warehouse/jobs/hudi_table_writer.py
          volumeMounts:
            - name: scripts
              mountPath: /scripts
      volumes:
        - name: scripts
          configMap:
            name: example-job-script
EOF
kubectl -n "$NS" wait --for=condition=complete job/upload-example-script --timeout=120s

echo ">>> Submitting SparkApplication"
kubectl -n "$NS" delete sparkapplication "$APP" --ignore-not-found
kubectl apply -f "$HERE/example/spark-app.yaml"

echo ">>> Waiting for the application to complete"
for i in $(seq 1 60); do
  STATE=$(kubectl -n "$NS" get sparkapplication "$APP" \
            -o jsonpath='{.status.currentState.currentStateSummary}' 2>/dev/null || echo "")
  case "$STATE" in
    Succeeded|ResourceReleased|TerminatedWithoutReleaseResources)
      echo ">>> Application state: $STATE"; break ;;
    Failed|SchedulingFailure|*TimedOut|DriverEvicted)
      echo "ERROR: application state: $STATE" >&2
      kubectl -n "$NS" logs "${APP}-0-driver" --tail=50 || true
      exit 1 ;;
    *) sleep 10 ;;
  esac
  if [[ "$i" == 60 ]]; then echo "ERROR: timed out waiting for $APP (last state: $STATE)" >&2; exit 1; fi
done

echo ">>> Driver log tail:"
kubectl -n "$NS" logs "${APP}-0-driver" --tail=20 | grep -v "^$" || true
kubectl -n "$NS" logs "${APP}-0-driver" | grep "DEMO_OK" \
  || { echo "ERROR: DEMO_OK marker not found in driver log" >&2; exit 1; }
echo ">>> Example job succeeded"
