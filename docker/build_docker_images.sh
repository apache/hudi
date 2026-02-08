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

# This script builds all required Hudi Docker images for Hadoop, Spark, and Hive components.
# It detects the system architecture, sets up Docker build platform, and loops through image configs.
# Usage: ./build_docker_images.sh [--hadoop-version <version>] [--spark-version <version>] [--hive-version <version>] [--version-tag <tag>]

# Default versions for Hadoop, Spark, and Hive
HADOOP_VERSION="2.8.4"
SPARK_VERSION="3.5.3"
HIVE_VERSION="2.3.10"
VERSION_TAG_ARG="" # Initialize to empty, will be set by command-line arg if provided

# Function to get Maven project version
get_hudi_project_version() {
  local pom_path="$1"
  if [ -f "$pom_path" ]; then
    mvn -f "$pom_path" help:evaluate -Dexpression=project.version -q -DforceStdout 2>/dev/null
  else
    echo "Error: pom.xml not found at $pom_path" >&2
    exit 1
  fi
}

# Parse command-line arguments
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --hadoop-version) HADOOP_VERSION="$2"; shift ;;
        --spark-version) SPARK_VERSION="$2"; shift ;;
        --hive-version) HIVE_VERSION="$2"; shift ;;
        --version-tag) VERSION_TAG_ARG="$2"; shift ;;
        *) echo "Unknown parameter passed: $1"; exit 1 ;;
    esac
    shift
done

# Detect system architecture and set Docker platform accordingly
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
export BUILDX_EXPERIMENTAL=1
# Get the directory of this script for relative paths
SCRIPT_DIR=$(cd $(dirname "$0") && pwd)

# Determine VERSION_TAG (command line arg or Maven project version)
if [ -n "$VERSION_TAG_ARG" ]; then
  VERSION_TAG="$VERSION_TAG_ARG"
else
  # Path to the root pom.xml relative to SCRIPT_DIR
  ROOT_POM_PATH="$SCRIPT_DIR/../pom.xml"
  VERSION_TAG=$(get_hudi_project_version "$ROOT_POM_PATH")
  if [ -z "$VERSION_TAG" ]; then
    echo "Error: Could not determine Maven project version. Please ensure Maven is installed and a valid pom.xml exists, or specify --version-tag manually." >&2
    exit 1
  fi
fi

# Docker image tags
LATEST_TAG="latest"
DOCKER_CONTEXT_DIR="hoodie/hadoop"
# List of images to build: "subdir|image_base_name"
# Each entry: <subdir>|<image_base_name>
DOCKER_IMAGES=(
  "base_java11|apachehudi/hudi-hadoop_${HADOOP_VERSION}-base"
  "datanode|apachehudi/hudi-hadoop_${HADOOP_VERSION}-datanode"
  "historyserver|apachehudi/hudi-hadoop_${HADOOP_VERSION}-history"
  "hive_base|apachehudi/hudi-hadoop_${HADOOP_VERSION}-hive_${HIVE_VERSION}"
  "namenode|apachehudi/hudi-hadoop_${HADOOP_VERSION}-namenode"
  "spark_base|apachehudi/hudi-hadoop_${HADOOP_VERSION}-hive_${HIVE_VERSION}-sparkbase_${SPARK_VERSION}"
  "sparkadhoc|apachehudi/hudi-hadoop_${HADOOP_VERSION}-hive_${HIVE_VERSION}-sparkadhoc_${SPARK_VERSION}"
  "sparkmaster|apachehudi/hudi-hadoop_${HADOOP_VERSION}-hive_${HIVE_VERSION}-sparkmaster_${SPARK_VERSION}"
  "sparkworker|apachehudi/hudi-hadoop_${HADOOP_VERSION}-hive_${HIVE_VERSION}-sparkworker_${SPARK_VERSION}"
)
# Build each Docker image in the list
for IMAGE_CONFIG in "${DOCKER_IMAGES[@]}"; do
  # Split config into subdir and image base name
  IFS='|' read -r SUBDIR IMAGE_BASE <<< "$IMAGE_CONFIG"
  IMAGE_CONTEXT="$DOCKER_CONTEXT_DIR/$SUBDIR"
  TAG_LATEST="$IMAGE_BASE:$LATEST_TAG"
  TAG_VERSIONED="$IMAGE_BASE:$VERSION_TAG"
  echo "Building $IMAGE_CONTEXT as $TAG_LATEST and $TAG_VERSIONED"
  # Build the Docker image with both latest and versioned tags
  if ! docker build \
    --build-arg HADOOP_VERSION=${HADOOP_VERSION} \
    --build-arg SPARK_VERSION=${SPARK_VERSION} \
    --build-arg HIVE_VERSION=${HIVE_VERSION} \
    "$IMAGE_CONTEXT" -t "$TAG_LATEST" -t "$TAG_VERSIONED"; then
    echo "Error: Failed to build docker image for $IMAGE_CONTEXT"
    exit 1
  fi
done

