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

# End-to-end smoke test on the current kube context (expects images already
# built into the cluster daemon; see scripts/build-images.sh):
#   up.sh -> run-example.sh -> assert the table is queryable from Trino.

set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NS=hudi-lakehouse
EXPECTED_ROWS=100

"$HERE/up.sh"
"$HERE/run-example.sh"

echo ">>> Querying the table from Trino"
TRINO_DEPLOY=deploy/hudi-trino
ROWS=$(kubectl -n "$NS" exec "$TRINO_DEPLOY" -- \
  trino --output-format TSV --execute "SELECT count(*) FROM hudi.default.trips" | tail -1)

if [[ "$ROWS" != "$EXPECTED_ROWS" ]]; then
  echo "FAIL: expected $EXPECTED_ROWS rows from Trino, got '$ROWS'" >&2
  exit 1
fi

echo ">>> Partition group-by:"
kubectl -n "$NS" exec "$TRINO_DEPLOY" -- \
  trino --output-format ALIGNED --execute \
  "SELECT city, count(*) AS trips, round(avg(fare), 2) AS avg_fare
   FROM hudi.default.trips GROUP BY city ORDER BY city"

echo "SMOKE TEST PASSED (${ROWS} rows served by Trino)"
