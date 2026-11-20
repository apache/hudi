#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Measure a perf change on CLOUD storage (S3), where the cost structure is latency/throughput-bound
# rather than CPU-bound, so the wall time reflects real ranged GETs, listing, and throttling that a
# local SSD run hides. There are two ways to do this; pick per what you are measuring.
#
# --------------------------------------------------------------------------------------------------
# OPTION A (recommended): run on a cloud VM in the SAME region as the bucket, reading via s3a://.
#   A laptop reading S3 over the WAN inflates latency and is not representative. On an in-region VM:
#     1. Ensure credentials: an instance role, or exported AWS_ACCESS_KEY_ID/SECRET (and SESSION).
#     2. Ensure the test classpath has hadoop-aws + aws-java-sdk-bundle matching the Hadoop version
#        (the default build pulls hadoop 2.10.x; add hadoop-aws:2.10.x if the module lacks it as a test dep).
#     3. Point the benchmark's data property at an s3a:// URI so it streams from S3:
#          run_perf_s3.sh hudi-hadoop-common TestMyRealFileGroup \
#            -Dmy.real.fg.dir=s3a://bucket/table/.hoodie/metadata/record_index/<file-group-dir>
#   Report S3 wall time next to the local (CPU-bound) number; the gap is the I/O component.
#
# OPTION B (most faithful): run the change inside a real Spark job on the cluster over S3 data and
#   capture an async-profiler flamegraph (CPU with -e itimer AND wall with -e wall) on an executor,
#   then diff baseline vs change. The wall profile exposes S3 parking (getAllLogFiles listing, block
#   GETs) that neither a local run nor a single-JVM s3a run shows. Analyze with
#   parse_flamegraph.py (in this directory). Capture pattern (on the executor pod/host):
#     asprof -d 240 -e itimer -o flamegraph -f /tmp/cpu.html  <pid>
#     asprof -d 240 -e wall   -o flamegraph -f /tmp/wall.html <pid>
#     asprof -d 240 -e alloc  -o flamegraph -f /tmp/alloc.html <pid>
#
# This wrapper implements Option A: it checks for credentials and delegates to run_perf_test.sh with
# your s3a data property. s3a's default credential providers pick up exported AWS_* variables and an
# instance role. It does NOT install hadoop-aws for you.
# --------------------------------------------------------------------------------------------------
set -euo pipefail

if [ -z "${AWS_ACCESS_KEY_ID:-}" ] && [ ! -e "$HOME/.aws/credentials" ] && [ -z "${AWS_WEB_IDENTITY_TOKEN_FILE:-}" ]; then
  echo "WARN: no obvious AWS credentials found (env, ~/.aws, or web-identity). An instance role may still apply." >&2
fi

echo "Running perf test against S3 (Option A: single-JVM s3a stream). For the fullest cloud signal"
echo "use Option B (real Spark job + async-profiler), documented in this script's header."
exec "$(dirname "$0")/run_perf_test.sh" "$@"
