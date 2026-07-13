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

# Builds the apachehudi/hudi-trino_<version> image with a locally-built
# trino-hudi plugin baked in. The plugin dir (typically the in-repo shim's
# docker/trino/shim/target/trino-hudi-<v>, see docker/trino/shim/pom.xml) is
# staged into the build context at docker/trino/plugin/ (gitignored), then
# baked into the image.
# Usage: ./build_image.sh --plugin-dir <path> [--trino-version <v>] [--image-tag <t>]
# Typical: ./build_image.sh --plugin-dir "$(dirname "$0")/shim/target/trino-hudi-481"
# Note: --trino-version must match the shim pom's parent version and the root
# pom's trino.version property.

set -e

# Default values
PLUGIN_DIR=""
TRINO_VERSION="481"
IMAGE_TAG="latest"

# Parse command-line arguments
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --plugin-dir) PLUGIN_DIR="$2"; shift ;;
        --trino-version) TRINO_VERSION="$2"; shift ;;
        --image-tag) IMAGE_TAG="$2"; shift ;;
        *) echo "Unknown parameter passed: $1"; exit 1 ;;
    esac
    shift
done

# Directory of this script, so the build context path is stable regardless of cwd
SCRIPT_DIR=$(cd $(dirname "$0") && pwd)

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

IMAGE="apachehudi/hudi-trino_${TRINO_VERSION}:${IMAGE_TAG}"
echo "Building $IMAGE (TRINO_VERSION=${TRINO_VERSION})"
docker build --build-arg TRINO_VERSION="${TRINO_VERSION}" -t "$IMAGE" "$SCRIPT_DIR"

# Clean up the staged plugin dir
echo "Cleaning up staged plugin dir '$STAGE_DIR'"
rm -rf "$STAGE_DIR"

echo "Done: $IMAGE"
