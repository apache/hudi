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

# Tears down everything up.sh created (the whole hudi-lakehouse namespace).
set -euo pipefail
NS=hudi-lakehouse

# Delete SparkApplications FIRST, while the operator is still alive to
# process their finalizers -- otherwise the namespace hangs in Terminating.
kubectl -n "$NS" delete sparkapplications --all --timeout=120s 2>/dev/null || true

helm uninstall hudi-trino -n "$NS" 2>/dev/null || true
helm uninstall spark-kubernetes-operator -n "$NS" 2>/dev/null || true
kubectl delete namespace "$NS" --ignore-not-found --timeout=300s
echo ">>> local-dev lakehouse removed"
