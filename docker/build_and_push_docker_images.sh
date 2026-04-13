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

set -e

# -----------------------------------------------------------------------------
# build_and_push_docker_images.sh
#
# Usage:
#   ./build_and_push_docker_images.sh [push_flag]
#
# Example:
#   ./build_and_push_docker_images.sh        # build only
#   ./build_and_push_docker_images.sh true   # build and push
# -----------------------------------------------------------------------------

validate_docker_install() {
  # Check Docker availability
  if ! command -v docker &> /dev/null; then
    echo "ERROR: Docker command not found. Please ensure Docker is installed and available in PATH."
    exit 1
  fi
}

validate_docker_install

# ========================
# CONFIGURATION VARIABLES
# ========================

export PUSH_IMAGES_FLAG=${1:-false}

ARCHITECTURE=$(uname -m)
case "$ARCHITECTURE" in
  x86_64|amd64)
    DOCKER_PLATFORM='linux/amd64'
    ;;
  aarch64|arm64)
    DOCKER_PLATFORM='linux/arm64'
    ;;
  *)
    echo "Unsupported architecture: $ARCHITECTURE"
    exit 1
    ;;
esac
export DOCKER_DEFAULT_PLATFORM="$DOCKER_PLATFORM"

# Get script directory
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)

# Versions
HADOOP_VERSION="3.3.4"
SPARK_VERSION="3.5.3"
HIVE_VERSION="3.1.3"
VERSION_TAG="4e6ebba26cb097cb26cddcbb3958d99dda476320"
LATEST_TAG="latest"

DOCKER_CONTEXT_DIR="hoodie/hadoop"
HUDI_HADOOP_IMAGE_CONTEXT="apachehudi/hudi-hadoop_${HADOOP_VERSION}"

# ========================
# IMAGE DEFINITIONS
# ========================

# Each entry: <subdir>|<image_base_name>
DOCKER_IMAGES=(
  "base|${HUDI_HADOOP_IMAGE_CONTEXT}-base"
  "datanode|${HUDI_HADOOP_IMAGE_CONTEXT}-datanode"
  "historyserver|${HUDI_HADOOP_IMAGE_CONTEXT}-history"
  "hive_base|${HUDI_HADOOP_IMAGE_CONTEXT}-hive_${HIVE_VERSION}"
  "namenode|${HUDI_HADOOP_IMAGE_CONTEXT}-namenode"
  "spark_base|${HUDI_HADOOP_IMAGE_CONTEXT}-hive_${HIVE_VERSION}-sparkbase_${SPARK_VERSION}"
  "sparkadhoc|${HUDI_HADOOP_IMAGE_CONTEXT}-hive_${HIVE_VERSION}-sparkadhoc_${SPARK_VERSION}"
  "sparkmaster|${HUDI_HADOOP_IMAGE_CONTEXT}-hive_${HIVE_VERSION}-sparkmaster_${SPARK_VERSION}"
  "sparkworker|${HUDI_HADOOP_IMAGE_CONTEXT}-hive_${HIVE_VERSION}-sparkworker_${SPARK_VERSION}"
)

TAGS=("$LATEST_TAG" "$VERSION_TAG")

build_images() {
  echo "=============================="
  echo "Starting Docker Image Build..."
  echo "=============================="

  for IMAGE_CONFIG in "${DOCKER_IMAGES[@]}"; do
    IFS='|' read -r SUBDIR IMAGE_BASE <<< "$IMAGE_CONFIG"
    IMAGE_CONTEXT="$DOCKER_CONTEXT_DIR/$SUBDIR"
    TAG_LATEST="$IMAGE_BASE:$LATEST_TAG"
    TAG_VERSIONED="$IMAGE_BASE:$VERSION_TAG"

    echo "Building $IMAGE_CONTEXT as:"
    echo "  - $TAG_LATEST"
    echo "  - $TAG_VERSIONED"

    if ! docker build "$IMAGE_CONTEXT" -t "$TAG_LATEST" -t "$TAG_VERSIONED"; then
      echo "Error: Failed to build docker image for $IMAGE_CONTEXT"
      exit 1
    fi
    echo "Successfully built $IMAGE_CONTEXT"
    echo "----------------------------------"
  done

  echo "All Docker images built successfully."
}

push_images() {
  echo
  echo "=============================="
  echo "Starting Docker Image Push..."
  echo "=============================="

  SUCCESS_COUNT=0
  FAILURE_COUNT=0

  echo "Preparing image list for push..."
  IMAGE_NAMES=()
  for IMAGE_CONFIG in "${DOCKER_IMAGES[@]}"; do
    IFS='|' read -r _ IMAGE_BASE <<< "$IMAGE_CONFIG"
    IMAGE_NAMES+=("$IMAGE_BASE")
  done

  echo "Pushing ${#IMAGE_NAMES[@]} images with ${#TAGS[@]} tags each"
  echo "----------------------------------"

  for IMAGE in "${IMAGE_NAMES[@]}"; do
    for TAG in "${TAGS[@]}"; do
      FULL_IMAGE="${IMAGE}:${TAG}"
      echo "Attempting to push: ${FULL_IMAGE}"

      if ! docker image inspect "${FULL_IMAGE}" &> /dev/null; then
        echo "[SKIPPED] Image ${FULL_IMAGE} not found locally. Skipping."
        FAILURE_COUNT=$((FAILURE_COUNT + 1))
        continue
      fi

      if docker push "${FULL_IMAGE}"; then
        echo "[SUCCESS] Pushed ${FULL_IMAGE}"
        SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
      else
        echo "[FAILED] Push failed for ${FULL_IMAGE}. Check Docker login and network."
        FAILURE_COUNT=$((FAILURE_COUNT + 1))
      fi

      echo "----------------------------------"
    done
  done

  echo
  echo "=============================="
  echo "Docker Push Summary"
  echo "=============================="
  echo "Total Attempts: $(( ${#IMAGE_NAMES[@]} * ${#TAGS[@]} ))"
  echo "Successful: ${SUCCESS_COUNT}"
  echo "Failed: ${FAILURE_COUNT}"

  if [ "${FAILURE_COUNT}" -eq 0 ]; then
    echo "All images pushed successfully!"
  else
    echo "Some pushes failed. Review logs above."
  fi
}

# ========================
# MAIN EXECUTION
# ========================
build_images

if [ "$PUSH_IMAGES_FLAG" == "true" ]; then
  push_images
fi
