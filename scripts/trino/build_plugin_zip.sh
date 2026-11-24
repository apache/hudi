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
## Builds the Hudi Trino plugin zip: hudi-trino, the in-repo shim (docker/trino/shim:
## HudiPlugin and its service descriptor) and every runtime dependency, all compiled
## against one Trino version. Unpacked into a Trino server's plugin/hudi directory
## (keeping that directory's hdfs/ subdirectory), it replaces Trino's bundled Hudi
## connector with hudi-trino.
##
## Usage: build_plugin_zip.sh [--trino-version <version>] [--output-dir <dir>]
##
##   --trino-version <version>  Trino version to build against. Default: the root pom's
##                              trino.e2e.version, the latest Trino release, which resolves
##                              from Maven Central. The root pom's trino.version (a -SNAPSHOT
##                              of trinodb/trino master) also works once
##                              scripts/trino/bootstrap_trino.sh has installed it.
##   --output-dir <dir>         Where to write the zip (default: <hudi>/target/hudi-trino-plugin).
##
## Output, in the output directory:
##   hudi-trino-plugin-<hudi version>-trino-<trino version>.zip
##   hudi-trino-plugin-<hudi version>-trino-<trino version>/   the same jars, unpacked
##
## Requires JDK 25, zip and unzip. The Hudi modules hudi-trino depends on must resolve at
## this checkout's version: from Maven Central for a Hudi release, or, for a -SNAPSHOT,
## installed into the local Maven repository first (JDK 17):
##   mvn install -pl :hudi-common,:hudi-hive-sync,:hudi-io,:hudi-sync-common,:hudi-client-common,:hudi-java-client \
##     -am -Dmaven.test.skip=true
##
## Side effect: installs hudi-trino, built against <trino version>, into the local Maven
## repository, because the shim resolves it from there. Extra Maven arguments can be
## passed through the MVN_ARGS environment variable.
##

set -euo pipefail

HUDI_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
SHIM_DIR="$HUDI_ROOT/docker/trino/shim"

TRINO_VERSION=""
OUTPUT_DIR="$HUDI_ROOT/target/hudi-trino-plugin"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --trino-version)
      TRINO_VERSION="$2"
      shift 2
      ;;
    --output-dir)
      OUTPUT_DIR="$2"
      shift 2
      ;;
    -h|--help)
      grep '^##' "$0" | sed 's/^## \{0,1\}//'
      exit 0
      ;;
    *)
      echo "ERROR: unexpected argument: $1" >&2
      echo "Usage: $0 [--trino-version <version>] [--output-dir <dir>]" >&2
      exit 1
      ;;
  esac
done

# Keep each property on one line in the root pom; this sed depends on it.
if [[ -z "$TRINO_VERSION" ]]; then
  TRINO_VERSION=$(sed -n 's|.*<trino.e2e.version>\(.*\)</trino.e2e.version>.*|\1|p' "$HUDI_ROOT/pom.xml")
  if [[ -z "$TRINO_VERSION" ]]; then
    echo "ERROR: could not read <trino.e2e.version> from $HUDI_ROOT/pom.xml and no --trino-version given" >&2
    exit 1
  fi
fi

# JDK gate: hudi-trino's enforce-jdk25 rule fails the build on anything older.
JAVA_MAJOR=$(java -version 2>&1 | awk -F[\".] '/version/ {print $2}')
if [[ "$JAVA_MAJOR" != "25" ]]; then
  echo "ERROR: JDK 25 required (found major version: ${JAVA_MAJOR:-unknown})." >&2
  echo "Hint: export JAVA_HOME=\$(/usr/libexec/java_home -v 25)" >&2
  exit 1
fi

for tool in zip unzip; do
  if ! command -v "$tool" > /dev/null 2>&1; then
    echo "ERROR: $tool is required on PATH" >&2
    exit 1
  fi
done

# Trino publishes no -SNAPSHOT artifacts; those come only from bootstrap_trino.sh.
if [[ "$TRINO_VERSION" == *-SNAPSHOT ]] && [[ ! -d "$HOME/.m2/repository/io/trino/trino-root/$TRINO_VERSION" ]]; then
  echo "ERROR: Trino $TRINO_VERSION is not in the local Maven repository." >&2
  echo "Hint: scripts/trino/bootstrap_trino.sh <path-to-trino-checkout>" >&2
  exit 1
fi

# Without MVN_ARGS: flags such as -V print to stdout and would end up in the version.
HUDI_VERSION=$(mvn -q -ntp -N -f "$HUDI_ROOT/pom.xml" help:evaluate -Dexpression=project.version -DforceStdout)
# MVN_ARGS is a whitespace-separated list of flags, so it is expanded unquoted below.
MVN_ARGS="${MVN_ARGS:-}"
NAME="hudi-trino-plugin-${HUDI_VERSION}-trino-${TRINO_VERSION}"
echo "Building $NAME"

# 1. The connector, compiled against the requested Trino version.
# shellcheck disable=SC2086
mvn $MVN_ARGS -f "$HUDI_ROOT/pom.xml" -Phudi-trino -pl hudi-trino install \
  -Dmaven.test.skip=true -Dtrino.version="$TRINO_VERSION"

WORK_DIR=$(mktemp -d)
trap 'rm -rf "$WORK_DIR"' EXIT

# The installed hudi-trino pom imports trino-root:${trino.version}. A consumer such as the shim
# reads that property from the root pom's default (the pinned -SNAPSHOT), not from
# -Dtrino.version: Maven does not apply command-line properties to a dependency's pom. Reinstall
# the jar with a pom that sets trino.version to the version it was built against, or a release
# build fails on a -SNAPSHOT trino-root it does not have.
awk -v v="$TRINO_VERSION" '!done && /<properties>/ { print; print "        <trino.version>" v "</trino.version>"; done = 1; next } { print }' \
  "$HUDI_ROOT/hudi-trino/pom.xml" > "$WORK_DIR/hudi-trino.pom"
# shellcheck disable=SC2086
mvn $MVN_ARGS -q -N -f "$HUDI_ROOT/pom.xml" install:install-file \
  -Dfile="$HUDI_ROOT/hudi-trino/target/hudi-trino-${HUDI_VERSION}.jar" -DpomFile="$WORK_DIR/hudi-trino.pom"

# 2. The shim, on a copy whose parent is trino-root at the requested version: Maven does not
#    interpolate a parent version, so the copy carries it literally. The copy sits outside
#    git, so Trino's git-commit-id plugin is skipped.
mkdir -p "$WORK_DIR/shim"
cp -R "$SHIM_DIR/src" "$WORK_DIR/shim/src"
sed "/<parent>/,/<\/parent>/ s|<version>.*</version>|<version>${TRINO_VERSION}</version>|" \
  "$SHIM_DIR/pom.xml" > "$WORK_DIR/shim/pom.xml"
# shellcheck disable=SC2086
mvn $MVN_ARGS -f "$WORK_DIR/shim/pom.xml" package -DskipTests \
  -Ddep.hudi.version="$HUDI_VERSION" -Dmaven.gitcommitid.skip=true

# 3. Repackage under a name that says which Hudi and Trino versions it holds.
SHIM_ZIP="$WORK_DIR/shim/target/trino-hudi-${TRINO_VERSION}.zip"
if [[ ! -f "$SHIM_ZIP" ]]; then
  echo "ERROR: the shim build did not produce $SHIM_ZIP" >&2
  exit 1
fi
unzip -q "$SHIM_ZIP" -d "$WORK_DIR/unpacked"
mkdir -p "$OUTPUT_DIR"
rm -rf "${OUTPUT_DIR:?}/$NAME" "$OUTPUT_DIR/$NAME.zip"
mv "$WORK_DIR/unpacked/trino-hudi-${TRINO_VERSION}" "$OUTPUT_DIR/$NAME"
(cd "$OUTPUT_DIR" && zip -q -r "$NAME.zip" "$NAME")

# A plugin directory Trino can load holds the service descriptor naming HudiPlugin and the connector.
for jar in "trino-hudi-${TRINO_VERSION}-services.jar" "org.apache.hudi_hudi-trino-${HUDI_VERSION}.jar"; do
  if [[ ! -f "$OUTPUT_DIR/$NAME/$jar" ]]; then
    echo "ERROR: $NAME is missing $jar" >&2
    exit 1
  fi
done

echo "Done: $OUTPUT_DIR/$NAME.zip ($(find "$OUTPUT_DIR/$NAME" -name '*.jar' | wc -l | tr -d ' ') jars)"
