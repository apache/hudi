#!/bin/bash

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

set -euo pipefail

export HUDI_VERSION=${HUDI_VERSION:-1.0.2}
export HUDI_VERSION_TAG=${HUDI_VERSION}
export SPARK_VERSION=${SPARK_VERSION:-3.4.4}
export HIVE_VERSION=${HIVE_VERSION:-3.1.3}
export HIVE_VERSION_TAG=${HIVE_VERSION}
export TRINO_VERSION=${TRINO_VERSION:-477}
export TRINO_VERSION_TAG=${TRINO_VERSION}
export PRESTO_VERSION=${PRESTO_VERSION:-0.296}
export PRESTO_VERSION_TAG=${PRESTO_VERSION}
export JAVA_VERSION=${JAVA_VERSION:-11}
export SCALA_VERSION=${SCALA_VERSION:-2.12}
export HADOOP_VERSION=${HADOOP_VERSION:-3.3.4}
export AWS_SDK_VERSION=${AWS_SDK_VERSION:-1.12.772}

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

echo "Building Spark Hudi Docker image using Spark version: $SPARK_VERSION and Hudi version: $HUDI_VERSION"

docker build \
    --build-arg HUDI_VERSION="$HUDI_VERSION" \
    --build-arg SPARK_VERSION="$SPARK_VERSION" \
    --build-arg JAVA_VERSION="$JAVA_VERSION" \
    --build-arg SCALA_VERSION="$SCALA_VERSION" \
    --build-arg HADOOP_VERSION="$HADOOP_VERSION" \
    --build-arg AWS_SDK_VERSION="$AWS_SDK_VERSION" \
    -t apachehudi/spark-hudi:latest \
    -t apachehudi/spark-hudi:"$HUDI_VERSION_TAG" \
    -f "$SCRIPT_DIR"/Dockerfile.spark .

echo "Building Hive Docker image using Hive version: $HIVE_VERSION"

export TARGET_PLATFORM=linux/amd64
docker build \
    --platform $TARGET_PLATFORM \
    --build-arg HIVE_VERSION="$HIVE_VERSION" \
    -t apachehudi/hive:latest \
    -t apachehudi/hive:"$HIVE_VERSION_TAG" \
    -f "$SCRIPT_DIR"/Dockerfile.hive .

echo "Building Trino Docker image using Trino version: $TRINO_VERSION"

docker build \
    --build-arg TRINO_VERSION="$TRINO_VERSION" \
    -t apachehudi/trino:latest \
    -t apachehudi/trino:"$TRINO_VERSION_TAG" \
    -f "$SCRIPT_DIR"/Dockerfile.trino .

echo "Building Presto Docker image using Presto version: $PRESTO_VERSION"

docker build \
    --build-arg PRESTO_VERSION="$PRESTO_VERSION" \
    -t apachehudi/presto:latest \
    -t apachehudi/presto:"$PRESTO_VERSION_TAG" \
    -f "$SCRIPT_DIR"/Dockerfile.presto .