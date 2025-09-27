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
# Usage: ./build_docker_images.sh
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
# Versions for Hadoop, Spark, and Hive
HADOOP_VERSION="3.3.4"
SPARK_VERSION="3.5.3"
HIVE_VERSION="3.1.3"
# Docker image tags
VERSION_TAG="1.1.0"
LATEST_TAG="latest"
DOCKER_CONTEXT_DIR="hoodie/hadoop"
# List of images to build: "subdir|image_base_name"
# Each entry: <subdir>|<image_base_name>
DOCKER_IMAGES=(
  "base|apachehudi/hudi-hadoop_${HADOOP_VERSION}-base"
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
  if ! docker build "$IMAGE_CONTEXT" -t "$TAG_LATEST" -t "$TAG_VERSIONED"; then
    echo "Error: Failed to build docker image for $IMAGE_CONTEXT"
    exit 1
  fi
done

