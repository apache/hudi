#!/bin/bash

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

##
## Builds the trinodb/trino modules that hudi-trino compiles and tests against
## and installs them into the local Maven repository.
##
## On master, hudi-trino tracks trinodb/trino master at the commit pinned by the
## <trino.sha> property in the root pom (whose project version is <trino.version>,
## e.g. 484-SNAPSHOT). Trino publishes no SNAPSHOT artifacts, so every io.trino
## dependency must be installed from source by this script before hudi-trino can
## build. On release branches <trino.version> is a released number and only the
## test-jars (never published by Trino) need this script.
##
## Usage: bootstrap_trino.sh <path-to-trino-checkout> [--ref <sha>] [--skip-checkout] [--keep-m2]
##
##   <path-to-trino-checkout>  a trinodb/trino git checkout (clean worktree)
##   --ref <sha>               build at this commit instead of the pinned <trino.sha>
##                             (used by the nightly pin-advance workflow at trino HEAD;
##                             relaxes the version cross-check to a warning)
##   --skip-checkout           assume the worktree is already at the right commit
##                             (CI checks out the SHA itself via actions/checkout)
##   --keep-m2                 do not purge ~/.m2/repository/io/trino first
##                             (the purge guards against stale artifacts under the
##                             same SNAPSHOT coordinates from an older pin)
##

set -euo pipefail

HUDI_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

TRINO_REPO=""
REF=""
SKIP_CHECKOUT="false"
KEEP_M2="false"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --ref)
      REF="$2"
      shift 2
      ;;
    --skip-checkout)
      SKIP_CHECKOUT="true"
      shift
      ;;
    --keep-m2)
      KEEP_M2="true"
      shift
      ;;
    -h|--help)
      grep '^##' "$0" | sed 's/^## \{0,1\}//'
      exit 0
      ;;
    *)
      if [[ -z "$TRINO_REPO" ]]; then
        TRINO_REPO="$1"
        shift
      else
        echo "ERROR: unexpected argument: $1" >&2
        exit 1
      fi
      ;;
  esac
done

if [[ -z "$TRINO_REPO" || ! -d "$TRINO_REPO/.git" && ! -f "$TRINO_REPO/.git" ]]; then
  echo "ERROR: first argument must be a trinodb/trino git checkout" >&2
  echo "Usage: $0 <path-to-trino-checkout> [--ref <sha>] [--skip-checkout] [--keep-m2]" >&2
  exit 1
fi

# The pin lives in the root pom; keep each property on one line (this sed and the
# CI workflows depend on it).
PINNED_SHA=$(sed -n 's|.*<trino.sha>\(.*\)</trino.sha>.*|\1|p' "$HUDI_ROOT/pom.xml")
PINNED_VERSION=$(sed -n 's|.*<trino.version>\(.*\)</trino.version>.*|\1|p' "$HUDI_ROOT/pom.xml")

if [[ -z "$PINNED_VERSION" ]]; then
  echo "ERROR: could not read <trino.version> from $HUDI_ROOT/pom.xml" >&2
  exit 1
fi

RELAXED_VERSION_CHECK="false"
if [[ -n "$REF" ]]; then
  RELAXED_VERSION_CHECK="true"
else
  REF="$PINNED_SHA"
  if [[ -z "$REF" ]]; then
    echo "ERROR: could not read <trino.sha> from $HUDI_ROOT/pom.xml and no --ref given" >&2
    exit 1
  fi
fi

# JDK gate: trino at head enforces JDK 25.
JAVA_MAJOR=$(java -version 2>&1 | awk -F[\".] '/version/ {print $2}')
if [[ "$JAVA_MAJOR" != "25" ]]; then
  echo "ERROR: JDK 25 required to build trino (found major version: ${JAVA_MAJOR:-unknown})." >&2
  echo "Hint: export JAVA_HOME=\$(/usr/libexec/java_home -v 25)" >&2
  exit 1
fi

if ! git -C "$TRINO_REPO" rev-parse --verify --quiet "${REF}^{commit}" > /dev/null; then
  echo "ERROR: commit $REF not present in $TRINO_REPO" >&2
  echo "Hint: git -C $TRINO_REPO fetch https://github.com/trinodb/trino.git master --tags" >&2
  exit 1
fi

if [[ "$SKIP_CHECKOUT" == "false" ]]; then
  # Refuse tracked modifications; untracked files are harmless for a checkout.
  if [[ -n "$(git -C "$TRINO_REPO" status --porcelain -uno)" ]]; then
    echo "ERROR: $TRINO_REPO has uncommitted tracked changes; commit or stash them first" >&2
    exit 1
  fi
  git -C "$TRINO_REPO" -c advice.detachedHead=false checkout --detach "$REF"
fi

ACTUAL_VERSION=$("$TRINO_REPO/mvnw" -q -N -f "$TRINO_REPO/pom.xml" help:evaluate -Dexpression=project.version -DforceStdout)
if [[ "$ACTUAL_VERSION" != "$PINNED_VERSION" ]]; then
  if [[ "$RELAXED_VERSION_CHECK" == "true" ]]; then
    echo "WARNING: trino at $REF has version $ACTUAL_VERSION but the pom pins <trino.version>$PINNED_VERSION</trino.version> (version rollover?)"
  else
    echo "ERROR: trino at pinned sha $REF has version $ACTUAL_VERSION, but the root pom says <trino.version>$PINNED_VERSION</trino.version>." >&2
    echo "The pin properties must advance together; fix the pom or your checkout." >&2
    exit 1
  fi
fi

if [[ "$KEEP_M2" == "false" ]]; then
  # SNAPSHOT coordinates do not change when the pin does, so artifacts from an
  # older pin are indistinguishable from current ones. Purge to be safe.
  echo "Purging ~/.m2/repository/io/trino (use --keep-m2 to skip)"
  rm -rf "$HOME/.m2/repository/io/trino"
fi

export MAVEN_OPTS="${MAVEN_OPTS:--Xmx4g}"

# Module list rationale:
# - trino-hive, trino-filesystem-manager, trino-parquet, trino-plugin-toolkit and
#   their -am closure cover every compile-scope io.trino dependency of hudi-trino
#   (trino-spi, trino-cache, trino-filesystem, trino-hive-formats,
#   trino-memory-context, trino-metastore come in transitively).
# - The remaining modules are hudi-trino's test-scope dependencies.
# - trino-spi, trino-filesystem, trino-hive, trino-main are the four test-jar
#   producers needed by the hudi-trino-tests profile; -DskipTests (NOT
#   -Dmaven.test.skip) is load-bearing: test-jars need compiled test classes.
# - The -am closure also installs the trino-root pom, which the hudi-trino BOM
#   import and the docker/trino/shim parent resolve.
# - The blob-cache plugins back the cache managers the hudi-trino tests load
#   (memory unconditionally via HudiQueryRunner, alluxio in the caching tests).
echo "Building trino modules at $REF (version $ACTUAL_VERSION); this takes roughly 10-30 minutes"
(cd "$TRINO_REPO" && ./mvnw install -am -DskipTests -Dair.check.skip-all=true -T1C \
  -pl :trino-hive,:trino-filesystem-manager,:trino-parquet,:trino-plugin-toolkit,:trino-main,:trino-testing,:trino-testing-containers,:trino-testing-services,:trino-client,:trino-parser,:trino-hdfs,:trino-tpch,:trino-spi,:trino-filesystem,:trino-blob-cache-memory,:trino-blob-cache-alluxio)

echo "Done. io.trino artifacts for $ACTUAL_VERSION are installed in ~/.m2."
