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
# Run a manual JVM benchmark or a gated real-data analysis in a Hudi Maven repo against LOCAL data.
# Encodes the non-obvious invocation details learned the hard way:
#   * manual benchmarks are named *Benchmark (not Test*/ *Test) so CI never runs them; you must ask
#     for them by name with -Dsurefire.failIfNoSpecifiedTests=false;
#   * Maven's incremental compiler SKIPS a new/edited bench file ("target directory newer than
#     sources") unless you disable it, so the stale class runs and your edits appear to do nothing;
#   * the surefire fork runs with the root pom's argLine heap (-Xmx4g); -DargLine on the command
#     line never reaches it, so a bigger heap means raising that property locally.
#
# Usage:
#   run_perf_test.sh <module> <TestClass[#method]> [extra -Dkey=val ...]
#
# Examples:
#   run_perf_test.sh hudi-hadoop-common MyCompactionBenchmark
#   run_perf_test.sh hudi-hadoop-common TestMyRealFileGroup \
#       -Dmy.real.fg.dir=/abs/path/to/file-group
#
# Env overrides:
#   JAVA_HOME_11  JDK 11 home, used on both master and branch-0.x (default:
#                 `/usr/libexec/java_home -v 11`; the script fails if no JDK 11 is found).
#   MVN_PROFILES  Maven profiles. Default: "-Dspark3.5 -Dscala-2.12", leaving Flink on the branch's
#                 default profile.
#   MVN_THREADS   -T value (default: 0.5C on a laptop).
set -euo pipefail

MODULE="${1:?module, e.g. hudi-hadoop-common or hudi-client/hudi-java-client}"; shift
TEST="${1:?TestClass or TestClass#method}"; shift

# Resolve JDK 11 at runtime: master compiles at source 11, and branch-0.x is built locally on 11 too.
JAVA_HOME_11="${JAVA_HOME_11:-$(/usr/libexec/java_home -v 11 2>/dev/null || true)}"
if [ -n "$JAVA_HOME_11" ] && [ -x "$JAVA_HOME_11/bin/java" ]; then
  JAVA11_VERSION=$("$JAVA_HOME_11/bin/java" -version 2>&1)
else
  JAVA11_VERSION=""
fi
case "$JAVA11_VERSION" in
  *'"11.'*) ;;
  *)
    echo "ERROR: no JDK 11 found (tried JAVA_HOME_11, then '/usr/libexec/java_home -v 11')." >&2
    echo "       Install a Temurin/Corretto 11, or set JAVA_HOME_11 to its home." >&2
    exit 1 ;;
esac

MVN_PROFILES="${MVN_PROFILES:--Dspark3.5 -Dscala-2.12}"
MVN_THREADS="${MVN_THREADS:-0.5C}"

# Touch the test source (if found) so incremental compilation cannot skip it.
SRC=$(find . -path "*/test/*/${TEST%%#*}.java" 2>/dev/null | head -1 || true)
[ -n "$SRC" ] && touch "$SRC" && echo "touched $SRC"

set -x
JAVA_HOME="$JAVA_HOME_11" mvn test -pl "$MODULE" -am -T "$MVN_THREADS" \
  $MVN_PROFILES \
  -Dtest="$TEST" \
  -Dsurefire.failIfNoSpecifiedTests=false \
  -Dmaven.compiler.useIncrementalCompilation=false \
  "$@"
