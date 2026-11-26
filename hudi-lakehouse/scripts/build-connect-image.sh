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

# Uses pinned release artifacts, independently of the Spark 3.5 writer build.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REGISTRY=""
PUSH=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    --registry)
      [[ $# -ge 2 && -n "$2" && "$2" != --* ]] || { echo "--registry requires a prefix" >&2; exit 1; }
      REGISTRY="${2%/}/"; shift 2 ;;
    --push) PUSH=1; shift ;;
    *) echo "unknown arg: $1" >&2; exit 1 ;;
  esac
done
if [[ "$PUSH" == 1 && -z "$REGISTRY" ]]; then
  echo "--push requires --registry" >&2; exit 1
fi
IMAGE="${REGISTRY}hudi-lakehouse-spark-connect:4.1.3-hudi1.2.0"
docker build -t "$IMAGE" "$HERE/images/spark-connect"
if [[ "$PUSH" == 1 ]]; then
  docker push "$IMAGE"
fi
echo ">>> Image ready: $IMAGE"
