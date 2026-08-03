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
# build_and_publish_docker_images.sh
#
# Builds the Hudi demo images for Hadoop, Spark and Hive, and optionally publishes them.
#
# Building is safe for anyone: the images only go into the local Docker daemon. Publishing pushes
# them to the apachehudi Docker Hub organisation, which is a release action for Hudi maintainers
# with write access there, so it is off unless you ask for it.
#
# Usage:
#   ./build_and_publish_docker_images.sh [--hadoop-version <version>] [--spark-version <version>]
#                                        [--hive-version <version>] [--version-tag <tag>]
#                                        [--multi-arch <true|false>] [--publish <true|false>]
#
# Example:
#   ./build_and_publish_docker_images.sh                       # build for this machine, publish nothing
#   ./build_and_publish_docker_images.sh --spark-version 4.0.1 # build against Spark 4.0.1 (Java 17 base)
#   ./build_and_publish_docker_images.sh --multi-arch true     # build amd64 + arm64
#   ./build_and_publish_docker_images.sh --publish true        # build and publish (maintainers)
#
# All versions default to the set docker-compose_hadoop334_hive313_spark353_{amd64,arm64}.yml uses,
# which is what setup_demo.sh runs. Keeping multi-arch images locally needs the containerd image
# store, so --multi-arch true without --publish true requires it.
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

usage() {
  sed -n '21,42p' "$0" | sed -e 's/^# \{0,1\}//' -e '/^-----/d'
}

# Named rather than positional, so that what the run is about to do is readable in the command
# itself. Publishing defaults to false and has to be asked for; multi-arch defaults to true so the
# images built here match the amd64 plus arm64 set that is published.
# Default versions, matching docker-compose_hadoop334_hive313_spark353_{amd64,arm64}.yml, which is
# the set setup_demo.sh runs. Override any of them on the command line.
HADOOP_VERSION="3.3.4"
SPARK_VERSION="3.5.3"
HIVE_VERSION="3.1.3"
VERSION_TAG_ARG=""

PUBLISH=false
MULTI_ARCH=false
while [[ "$#" -gt 0 ]]; do
  case $1 in
    --publish)
      PUBLISH="$2"
      if [ "$PUBLISH" != "true" ] && [ "$PUBLISH" != "false" ]; then
        echo "Error: --publish expects true or false, got '${2:-<missing>}'" >&2
        exit 1
      fi
      shift ;;
    --hadoop-version) HADOOP_VERSION="$2"; shift ;;
    --spark-version) SPARK_VERSION="$2"; shift ;;
    --hive-version) HIVE_VERSION="$2"; shift ;;
    --version-tag) VERSION_TAG_ARG="$2"; shift ;;
    --multi-arch)
      MULTI_ARCH="$2"
      if [ "$MULTI_ARCH" != "true" ] && [ "$MULTI_ARCH" != "false" ]; then
        echo "Error: --multi-arch expects true or false, got '${2:-<missing>}'" >&2
        exit 1
      fi
      shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown parameter passed: $1" >&2; usage >&2; exit 1 ;;
  esac
  shift
done

# Detect system architecture and set Docker platform accordingly
if [ "$MULTI_ARCH" = true ]; then
  DOCKER_PLATFORM='linux/amd64,linux/arm64'
  echo "Building multi-arch images (amd64 + arm64)"
else
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
fi
export BUILDX_EXPERIMENTAL=1

# A multi-arch build has to say where its result goes, buildx cannot leave it nowhere: --push when
# publishing, --load to keep it in the local image store otherwise. Keeping it locally also needs
# the builder backed by the current docker context, because a docker-container builder resolves
# FROM against registries only and every image here starts FROM the previous one.
BUILDX_OUTPUT_ARG=""
BUILDX_BUILDER_ARG=""
if [ "$MULTI_ARCH" = true ]; then
  if [ "$PUBLISH" = true ]; then
    BUILDX_OUTPUT_ARG="--push"
  else
    BUILDX_OUTPUT_ARG="--load"
    BUILDX_BUILDER_ARG="--builder $(docker context show 2>/dev/null)"
    # Only the containerd image store can hold a multi-platform image locally.
    if ! docker info --format '{{.DriverStatus}}' 2>/dev/null | grep -q "io.containerd.snapshotter"; then
      echo "Error: building multi-arch without publishing needs the containerd image store, which" >&2
      echo "       is not enabled here. Enable it in Docker Desktop, or pass --publish true, or" >&2
      echo "       pass --multi-arch false to build for the current architecture only." >&2
      exit 1
    fi
  fi
fi

# Get script directory
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)

# Spark 4.0+ requires Java 17, which lives in its own base module. Select the base from the Spark
# version so that the whole image set stays on one JDK. The legacy Java 8 /hoodie/hadoop/base
# module is kept for historical reference only and is never selected here.
SPARK_MAJOR=$(echo "$SPARK_VERSION" | cut -d. -f1)
if ! [[ "$SPARK_MAJOR" =~ ^[0-9]+$ ]]; then
  echo "Error: invalid SPARK_VERSION='$SPARK_VERSION'" >&2
  exit 1
fi
if [ "$SPARK_MAJOR" -ge 4 ]; then
  BASE_IMAGE_DIR="base_java17"
  BASE_IMAGE_TAG="java17"
  echo "Using Java 17 base image for Spark ${SPARK_VERSION}"
else
  BASE_IMAGE_DIR="base_java11"
  BASE_IMAGE_TAG="java11"
  echo "Using Java 11 base image for Spark ${SPARK_VERSION}"
fi

# Select hadoop-aws/aws-sdk versions from the Hadoop line each Spark distribution bundles:
# the jars land on Spark's classpath next to its own hadoop-client, not the cluster Hadoop.
# hadoop-aws 3.4+ is built against AWS SDK v2 (software.amazon.awssdk:bundle); 3.3.x uses
# SDK v1 (com.amazonaws:aws-java-sdk-bundle). spark_base picks the artifact from the SDK major.
# hadoop-aws 3.4.2+ also compiles against analyticsaccelerator-s3 for its analytics input
# stream: opt-in via fs.s3a.input.stream.type in 3.4.2, the default from 3.5.0 on, so S3A
# init fails there without the jar. Each arm pins the version its hadoop-project pom declares.
SPARK_MAJOR_MINOR=$(echo "$SPARK_VERSION" | cut -d. -f1,2)
case "$SPARK_MAJOR_MINOR" in
  4.0)
    # Spark 4.0.x bundles Hadoop 3.4.1
    HADOOP_AWS_VERSION="3.4.1"
    AWS_SDK_VERSION="2.24.6"
    ANALYTICS_ACCELERATOR_VERSION="" # no analytics stream type before hadoop-aws 3.4.2
    ;;
  4.1)
    # Spark 4.1.x bundles Hadoop 3.4.2
    HADOOP_AWS_VERSION="3.4.2"
    AWS_SDK_VERSION="2.29.52"
    ANALYTICS_ACCELERATOR_VERSION="1.2.1"
    ;;
  4.2)
    # Spark 4.2.x bundles Hadoop 3.5.0
    HADOOP_AWS_VERSION="3.5.0"
    AWS_SDK_VERSION="2.35.4"
    ANALYTICS_ACCELERATOR_VERSION="1.3.1"
    ;;
  *)
    if [ "$SPARK_MAJOR" -ge 4 ]; then
      # Unmapped 4+ line (4.3, 5.x, ...): fall back to the newest mapped pairing and say so,
      # rather than silently shipping hadoop-aws from an older Hadoop line than Spark bundles.
      echo "Warning: no hadoop-aws mapping for Spark ${SPARK_VERSION}; using the Spark 4.2 pairing" >&2
      HADOOP_AWS_VERSION="3.5.0"
      AWS_SDK_VERSION="2.35.4"
      ANALYTICS_ACCELERATOR_VERSION="1.3.1"
    else
      # Spark 3.x bundles Hadoop 3.3.x
      HADOOP_AWS_VERSION="3.3.4"
      AWS_SDK_VERSION="1.12.734"
      ANALYTICS_ACCELERATOR_VERSION=""
    fi
    ;;
esac

LATEST_TAG="latest"

# Second tag applied to every image, alongside :latest. Taken from the root pom.xml so the images
# carry the Hudi version they were built from, rather than a hardcoded value that goes stale.
# Override by exporting VERSION_TAG before running, e.g. VERSION_TAG=my-test ./build_and_publish_docker_images.sh
get_hudi_project_version() {
  local pom_path="$1"
  if [ ! -f "$pom_path" ]; then
    echo "Error: pom.xml not found at $pom_path" >&2
    return 1
  fi
  if ! command -v mvn &> /dev/null; then
    return 1
  fi
  mvn -f "$pom_path" help:evaluate -Dexpression=project.version -q -DforceStdout 2>/dev/null || true
}

if [ -n "$VERSION_TAG_ARG" ]; then
  VERSION_TAG="$VERSION_TAG_ARG"
elif [ -z "${VERSION_TAG:-}" ]; then
  VERSION_TAG=$(get_hudi_project_version "$SCRIPT_DIR/../pom.xml" || true)
fi
if [ -z "$VERSION_TAG" ]; then
  echo "Error: could not determine the version tag from the root pom.xml." >&2
  echo "       Ensure maven is on PATH, or set it yourself:" >&2
  echo "         ./build_and_publish_docker_images.sh --version-tag <tag>" >&2
  exit 1
fi
echo "Using version tag: $VERSION_TAG"

DOCKER_CONTEXT_DIR="hoodie/hadoop"
HUDI_HADOOP_IMAGE_CONTEXT="apachehudi/hudi-hadoop_${HADOOP_VERSION}"

# ========================
# IMAGE DEFINITIONS
# ========================

# Each entry: <subdir>|<image_base_name>
# The base image is the Java 11 one. The legacy /hoodie/hadoop/base module is Java 8 and is kept
# for historical reference only, and the downstream Dockerfiles resolve their parent by the
# -${BASE_IMAGE_TAG} suffix, so the built name has to carry it.
DOCKER_IMAGES=(
  "${BASE_IMAGE_DIR}|${HUDI_HADOOP_IMAGE_CONTEXT}-base-${BASE_IMAGE_TAG}"
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

    # The Dockerfiles declare their own ARG defaults, which are not all the same. Passing the
    # versions explicitly is what keeps every image in the set on one Hadoop/Hive/Spark combination.
    if [ "$MULTI_ARCH" = true ]; then
      if ! docker buildx build ${BUILDX_BUILDER_ARG} --platform "$DOCKER_PLATFORM" ${BUILDX_OUTPUT_ARG} \
        --build-arg HADOOP_VERSION="${HADOOP_VERSION}" \
        --build-arg SPARK_VERSION="${SPARK_VERSION}" \
        --build-arg HIVE_VERSION="${HIVE_VERSION}" \
        --build-arg BASE_IMAGE_TAG="${BASE_IMAGE_TAG}" \
        --build-arg HADOOP_AWS_VERSION="${HADOOP_AWS_VERSION}" \
        --build-arg AWS_SDK_VERSION="${AWS_SDK_VERSION}" \
        --build-arg ANALYTICS_ACCELERATOR_VERSION="${ANALYTICS_ACCELERATOR_VERSION}" \
        "$IMAGE_CONTEXT" -t "$TAG_LATEST" -t "$TAG_VERSIONED"; then
        echo "Error: Failed to build docker image for $IMAGE_CONTEXT"
        exit 1
      fi
    else
      if ! docker build \
        --build-arg HADOOP_VERSION="${HADOOP_VERSION}" \
        --build-arg SPARK_VERSION="${SPARK_VERSION}" \
        --build-arg HIVE_VERSION="${HIVE_VERSION}" \
        --build-arg BASE_IMAGE_TAG="${BASE_IMAGE_TAG}" \
        --build-arg HADOOP_AWS_VERSION="${HADOOP_AWS_VERSION}" \
        --build-arg AWS_SDK_VERSION="${AWS_SDK_VERSION}" \
        --build-arg ANALYTICS_ACCELERATOR_VERSION="${ANALYTICS_ACCELERATOR_VERSION}" \
        "$IMAGE_CONTEXT" -t "$TAG_LATEST" -t "$TAG_VERSIONED"; then
        echo "Error: Failed to build docker image for $IMAGE_CONTEXT"
        exit 1
      fi
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

if [ "$PUBLISH" = true ]; then
  if [ "$MULTI_ARCH" = true ]; then
    echo "Multi-arch images were pushed by buildx during the build."
  else
    push_images
  fi
fi
