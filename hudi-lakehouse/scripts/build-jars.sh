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

# Builds the two Maven artifacts the hudi-lakehouse images need:
#   1. the Hudi Spark bundle   (packaging/hudi-spark-bundle)   -- JDK 11/17
#   2. the Hudi Trino plugin   (hudi-trino-plugin)             -- JDK 23+
#
# Usage: build-jars.sh [--skip-bundle] [--skip-plugin]

set -euo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# The lakehouse images pin one Spark line; everything runs dockerised anyway.
SPARK_VERSION="3.5"
BUILD_BUNDLE=1
BUILD_PLUGIN=1

while [[ $# -gt 0 ]]; do
  case "$1" in
    --skip-bundle)   BUILD_BUNDLE=0; shift ;;
    --skip-plugin)   BUILD_PLUGIN=0; shift ;;
    *) echo "unknown arg: $1" >&2; exit 1 ;;
  esac
done

MVN_ARGS=(-DskipTests -Dcheckstyle.skip=true -Dscalastyle.skip=true -Drat.skip=true -Dmaven.javadoc.skip=true -Dgpg.skip=true)

if [[ "$BUILD_BUNDLE" == 1 ]]; then
  # Profiles must be activated explicitly: activating any -D profile property
  # disables Maven's activeByDefault profiles, so relying on defaults is fragile.
  echo ">>> Building Hudi Spark bundle (spark${SPARK_VERSION})"
  (cd "$REPO_ROOT" && mvn clean package -pl packaging/hudi-spark-bundle -am \
      "-Dspark${SPARK_VERSION}" -Dflink2.1 "${MVN_ARGS[@]}")
fi

if [[ "$BUILD_PLUGIN" == 1 ]]; then
  # Probe the JDK Maven will actually use: Maven honours JAVA_HOME, PATH may differ.
  JAVA_MAJOR=$("${JAVA_HOME:+$JAVA_HOME/bin/}java" -version 2>&1 | sed -n 's/.*version "\([0-9]*\).*/\1/p' | head -1)
  if [[ "${JAVA_MAJOR:-0}" -lt 23 ]]; then
    echo "ERROR: hudi-trino-plugin requires JDK 23+ (found ${JAVA_MAJOR:-unknown})." >&2
    echo "       Set JAVA_HOME to a JDK 23 installation and re-run with --skip-bundle." >&2
    exit 1
  fi
  echo ">>> Building Hudi Trino plugin (JDK ${JAVA_MAJOR})"
  (cd "$REPO_ROOT/hudi-trino-plugin" && mvn clean package -DskipTests)
fi

echo ">>> Done. Artifacts:"
ls -d "$REPO_ROOT"/packaging/hudi-spark-bundle/target/hudi-spark*-bundle_*.jar 2>/dev/null | grep -v sources | grep -v original || true
ls -d "$REPO_ROOT"/hudi-trino-plugin/target/trino-hudi-*/ 2>/dev/null || true
