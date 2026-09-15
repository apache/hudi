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

# Builds the apachehudi/hudi-trino-e2e image with a locally-built trino-hudi
# plugin baked in. The plugin dir (typically the in-repo shim's
# docker/trino/shim/target/trino-hudi-<v>, see docker/trino/shim/pom.xml) is
# staged into the build context at docker/trino/plugin/ (gitignored), then
# baked into the image.
# Usage: ./build_image.sh --plugin-dir <path> [--base-image <image>] [--trino-version <v>] [--image-tag <t>]
# Typical: ./build_image.sh --plugin-dir "$(dirname "$0")/shim/target/trino-hudi-<trino.version>" \
#            --base-image hudi-trino-server:<trino.sha>
# Note: --base-image (e.g. the output of build_trino_server_image.sh, built from the pinned
# trino.sha) takes precedence over --trino-version. --trino-version is the released-image
# fallback, trinodb/trino:<v> (default: the root pom's trino.e2e.version), not the version
# the plugin was built at; it only boots when the pin's SPI matches that release.

set -e

# Directory of this script, so the build context and pom lookups are stable regardless of cwd
SCRIPT_DIR=$(cd $(dirname "$0") && pwd)

# Default values. The server version defaults to the root pom's trino.e2e.version (the
# nightly pin-advance job keeps that current; a literal default here would rot).
PLUGIN_DIR=""
BASE_IMAGE=""
TRINO_VERSION=$(sed -n 's|.*<trino.e2e.version>\(.*\)</trino.e2e.version>.*|\1|p' "$SCRIPT_DIR/../../pom.xml")
IMAGE_TAG="latest"

# Parse command-line arguments
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --plugin-dir) PLUGIN_DIR="$2"; shift ;;
        --base-image) BASE_IMAGE="$2"; shift ;;
        --trino-version) TRINO_VERSION="$2"; shift ;;
        --image-tag) IMAGE_TAG="$2"; shift ;;
        *) echo "Unknown parameter passed: $1"; exit 1 ;;
    esac
    shift
done

if [ -z "$TRINO_VERSION" ]; then
  echo "Error: could not read trino.e2e.version from the root pom and no --trino-version given." >&2
  exit 1
fi

# Validate --plugin-dir: required, must exist and be non-empty
if [ -z "$PLUGIN_DIR" ]; then
  echo "Error: --plugin-dir <path> is required (the locally-built trino-hudi plugin directory)." >&2
  exit 1
fi
if [ ! -d "$PLUGIN_DIR" ]; then
  echo "Error: plugin dir '$PLUGIN_DIR' does not exist." >&2
  exit 1
fi
if [ -z "$(ls -A "$PLUGIN_DIR" 2>/dev/null)" ]; then
  echo "Error: plugin dir '$PLUGIN_DIR' is empty." >&2
  exit 1
fi

# Stage the plugin into the build context (plugin/ must be IN the context to be COPY-able)
STAGE_DIR="$SCRIPT_DIR/plugin"
echo "Staging plugin from '$PLUGIN_DIR' into '$STAGE_DIR'"
rm -rf "$STAGE_DIR"
cp -r "$PLUGIN_DIR" "$STAGE_DIR"

IMAGE="apachehudi/hudi-trino-e2e:${IMAGE_TAG}"
BUILD_ARGS=(--build-arg TRINO_VERSION="${TRINO_VERSION}")
if [ -n "$BASE_IMAGE" ]; then
  echo "Building $IMAGE on base image ${BASE_IMAGE}"
  BUILD_ARGS+=(--build-arg TRINO_BASE_IMAGE="${BASE_IMAGE}")
else
  echo "Building $IMAGE on released base image trinodb/trino:${TRINO_VERSION}"
fi
docker build "${BUILD_ARGS[@]}" -t "$IMAGE" "$SCRIPT_DIR"

# Clean up the staged plugin dir
echo "Cleaning up staged plugin dir '$STAGE_DIR'"
rm -rf "$STAGE_DIR"

echo "Done: $IMAGE"
