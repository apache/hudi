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

set -eux

export HUDI_VERSION=${HUDI_VERSION:-1.0.2}
export HUDI_VERSION_TAG=${HUDI_VERSION}
export SPARK_VERSION=${SPARK_VERSION:-3.5.7}
export HIVE_VERSION=${HIVE_VERSION:-3.1.3}
export HIVE_VERSION_TAG=${HIVE_VERSION_TAG:-3.1.3}

SCRIPT_DIR=$(cd $(dirname $0); pwd)

echo "Building Spark Hudi Docker image using Spark version: $SPARK_VERSION and Hudi version: $HUDI_VERSION"

docker build \
    --build-arg HUDI_VERSION="$HUDI_VERSION" \
    --build-arg SPARK_VERSION="$SPARK_VERSION" \
    -t apachehudi/spark-hudi:latest \
    -t apachehudi/spark-hudi:"$HUDI_VERSION_TAG" \
    -f "$SCRIPT_DIR"/Dockerfile.spark .

echo "Building Hive Docker image using Hive version: $HIVE_VERSION"

docker build \
    --build-arg HIVE_VERSION="$HIVE_VERSION" \
    -t apachehudi/hive:latest \
    -t apachehudi/hive:"$HIVE_VERSION_TAG" \
    -f "$SCRIPT_DIR"/Dockerfile.hive .