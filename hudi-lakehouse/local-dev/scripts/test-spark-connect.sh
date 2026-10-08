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

# Opt-in acceptance test for an ALREADY deployed endpoint. Restarts only the
# named deployment; do not run against a service used by other clients.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON="${PYTHON:-python3}"
NAMESPACE="${NAMESPACE:-hudi-lakehouse}"
DEPLOYMENT="${DEPLOYMENT:-hudi-spark-connect}"
SERVICE="${SERVICE:-hudi-spark-connect}"
PORT="${PORT:-15002}"
SERVICE_PORT="${SERVICE_PORT:-15002}"
DRIVER="${ADBC_DRIVER:-spark}"
DATABASE="connect_smoke_$(date +%s)_${RANDOM}"
SCRIPT="$HERE/example/connect_smoke.py"
LOG_DIR="$(mktemp -d)"
PF_PID=""
CREATED_MARKER="$LOG_DIR/database-created"

if [[ "${1:-}" != "--restart-endpoint" || $# != 1 ]]; then
  echo "Usage: $0 --restart-endpoint" >&2
  echo "Runs CRUD and restarts $NAMESPACE/$DEPLOYMENT; active sessions will be lost." >&2
  exit 1
fi
[[ "$PORT" =~ ^[0-9]+$ && "$SERVICE_PORT" =~ ^[0-9]+$ ]] || { echo "ports must be integers" >&2; exit 1; }

stop_forward() {
  if [[ -n "$PF_PID" ]]; then
    kill "$PF_PID" 2>/dev/null || true
    wait "$PF_PID" 2>/dev/null || true
    PF_PID=""
  fi
}
start_forward() {
  kubectl -n "$NAMESPACE" port-forward --address 127.0.0.1 "svc/$SERVICE" "$PORT:$SERVICE_PORT" >"$LOG_DIR/port-forward.log" 2>&1 &
  PF_PID=$!
  for ((i=0; i<60; i++)); do
    if ! kill -0 "$PF_PID" 2>/dev/null; then
      cat "$LOG_DIR/port-forward.log" >&2
      return 1
    fi
    if grep -q "Forwarding from 127.0.0.1:$PORT" "$LOG_DIR/port-forward.log"; then return 0; fi
    sleep 1
  done
  echo "Timed out waiting for port-forward; see $LOG_DIR" >&2
  return 1
}
smoke() {
  "$PYTHON" "$SCRIPT" "$@" --database "$DATABASE" --driver "$DRIVER" \
    --uri "spark://127.0.0.1:$PORT?api=connect&auth_type=none&tls=false"
}
finish() {
  status=$?
  trap - EXIT
  if [[ -f "$CREATED_MARKER" && -n "$PF_PID" ]] && kill -0 "$PF_PID" 2>/dev/null; then
    smoke cleanup || echo "Cleanup failed; retry connect_smoke.py cleanup --database $DATABASE after reconnecting." >&2
  elif [[ -f "$CREATED_MARKER" ]]; then
    echo "Reconnect and run connect_smoke.py cleanup --database $DATABASE" >&2
  fi
  stop_forward
  echo "Port-forward log: $LOG_DIR/port-forward.log"
  exit "$status"
}
trap finish EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

kubectl -n "$NAMESPACE" rollout status "deployment/$DEPLOYMENT" --timeout=300s
start_forward
echo "Test database: $DATABASE (use connect_smoke.py cleanup if prepare is interrupted)"
smoke prepare --created-marker "$CREATED_MARKER"
stop_forward
kubectl -n "$NAMESPACE" rollout restart "deployment/$DEPLOYMENT"
kubectl -n "$NAMESPACE" rollout status "deployment/$DEPLOYMENT" --timeout=300s
start_forward
smoke verify
smoke cleanup
rm "$CREATED_MARKER"
echo "PASS: ADBC CRUD and persistence across endpoint restart"
