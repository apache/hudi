#!/bin/bash
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.

##
## Builds a local Trino server image from the trinodb/trino commit pinned by
## <trino.sha> in the root pom, so the server and the hudi-trino connector baked
## on top of it (docker/trino/build_image.sh --base-image) come from the same
## commit. The image is local-only and never published.
##
## Usage: build_trino_server_image.sh <path-to-trino-checkout> [--image <name:tag>] [--arch <amd64|arm64>]
##
##   <path-to-trino-checkout>  a trinodb/trino git checkout at exactly <trino.sha>
##   --image <name:tag>        tag for the resulting image
##                             (default: hudi-trino-server:<trino.sha>)
##   --arch <amd64|arm64>      image architecture (default: the Docker daemon's arch)
##
## Requires JDK 25, jq and docker on PATH.
##

set -euo pipefail

HUDI_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

TRINO_REPO=""
IMAGE=""
ARCH=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --image)
      IMAGE="$2"
      shift 2
      ;;
    --arch)
      ARCH="$2"
      shift 2
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
  echo "Usage: $0 <path-to-trino-checkout> [--image <name:tag>] [--arch <amd64|arm64>]" >&2
  exit 1
fi
TRINO_REPO="$(cd "$TRINO_REPO" && pwd)"

# Keep each property on one line in the root pom; this sed depends on it.
TRINO_SHA=$(sed -n 's|.*<trino.sha>\(.*\)</trino.sha>.*|\1|p' "$HUDI_ROOT/pom.xml")
TRINO_VERSION=$(sed -n 's|.*<trino.version>\(.*\)</trino.version>.*|\1|p' "$HUDI_ROOT/pom.xml")
if [[ -z "$TRINO_SHA" || -z "$TRINO_VERSION" ]]; then
  echo "ERROR: could not read <trino.sha>/<trino.version> from $HUDI_ROOT/pom.xml" >&2
  exit 1
fi

# JDK gate: trino at head enforces JDK 25.
JAVA_MAJOR=$(java -version 2>&1 | awk -F[\".] '/version/ {print $2}')
if [[ "$JAVA_MAJOR" != "25" ]]; then
  echo "ERROR: JDK 25 required to build trino (found major version: ${JAVA_MAJOR:-unknown})." >&2
  echo "Hint: export JAVA_HOME=\$(/usr/libexec/java_home -v 25)" >&2
  exit 1
fi

for tool in jq docker; do
  if ! command -v "$tool" > /dev/null 2>&1; then
    echo "ERROR: $tool is required on PATH (trino's core/docker/build.sh needs jq and docker)" >&2
    exit 1
  fi
done

ACTUAL_SHA=$(git -C "$TRINO_REPO" rev-parse HEAD)
if [[ "$ACTUAL_SHA" != "$TRINO_SHA" ]]; then
  echo "ERROR: $TRINO_REPO is at $ACTUAL_SHA, but the root pom pins <trino.sha>$TRINO_SHA</trino.sha>." >&2
  echo "Hint: git -C $TRINO_REPO -c advice.detachedHead=false checkout --detach $TRINO_SHA" >&2
  exit 1
fi

ACTUAL_VERSION=$("$TRINO_REPO/mvnw" -q -N -f "$TRINO_REPO/pom.xml" help:evaluate -Dexpression=project.version -DforceStdout)
if [[ "$ACTUAL_VERSION" != "$TRINO_VERSION" ]]; then
  echo "ERROR: trino at pinned sha $TRINO_SHA has version $ACTUAL_VERSION, but the root pom says <trino.version>$TRINO_VERSION</trino.version>." >&2
  echo "The pin properties must advance together; fix the pom or your checkout." >&2
  exit 1
fi

if [[ -z "$ARCH" ]]; then
  ARCH=$(docker version --format '{{.Server.Arch}}')
fi
if [[ "$ARCH" != "amd64" && "$ARCH" != "arm64" ]]; then
  echo "ERROR: --arch must be amd64 or arm64 (got: ${ARCH:-empty})" >&2
  exit 1
fi

if [[ -z "$IMAGE" ]]; then
  IMAGE="hudi-trino-server:${TRINO_SHA}"
fi

export MAVEN_OPTS="${MAVEN_OPTS:--Xmx4g}"

# Why the whole repo (minus docs) instead of -pl :trino-server -am: the server's provisio
# descriptors pull in plugin zips that are not Maven dependencies, so -am would not build
# them. Why the full trino-server tarball instead of trino-server-core: only the full server
# packages plugin/hudi, whose hdfs/ jar set docker/trino/Dockerfile carries over.
# package, NOT install: ~/.m2/repository/io/trino belongs to scripts/trino/bootstrap_trino.sh.
echo "Building the trino server at $TRINO_SHA (version $TRINO_VERSION)"
(cd "$TRINO_REPO" && ./mvnw package -B -DskipTests -Dair.check.skip-all=true -Dmaven.source.skip=true -T1C \
  -pl '!:trino-docs')

# Without -r, trino's build.sh builds from the locally built server tarball and CLI jar and
# tags <prefix>:<project.version>-<arch>; -x skips its image tests.
BUILD_PREFIX="hudi-trino-server-build"
echo "Building the trino server image for $ARCH"
(cd "$TRINO_REPO/core/docker" && ./build.sh -a "$ARCH" -t "$BUILD_PREFIX" -x)

docker tag "${BUILD_PREFIX}:${TRINO_VERSION}-${ARCH}" "$IMAGE"
docker rmi "${BUILD_PREFIX}:${TRINO_VERSION}-${ARCH}" > /dev/null

echo "Done: $IMAGE"
