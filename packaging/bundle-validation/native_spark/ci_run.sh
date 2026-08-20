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
# Builds a glibc Java 17 image and validates the native spark bundle inside it.
#
# env vars:
#   SPARK_RUNTIME:        spark version to validate against, e.g. 3.5.5
#   SPARK_HADOOP_VERSION: hadoop version suffix of the spark distribution, defaults to 3
#   GITHUB_WORKSPACE:     repository root
##
set -o errexit
set -o nounset

SPARK_HADOOP_VERSION=${SPARK_HADOOP_VERSION:-3}
WORKDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
CONTAINER_NAME=hudi-native-bundle-validation

NATIVE_BUNDLE_JAR=$(ls ${GITHUB_WORKSPACE}/packaging/hudi-native-spark-bundle/target/hudi-native-spark*-bundle_*.jar \
  | grep -vE -- '-(sources|javadoc|tests)\.jar$' | head -1)
if [ -z "$NATIVE_BUNDLE_JAR" ]; then
    echo "::error::no native spark bundle jar found, was it built for this Spark profile?"
    exit 1
fi
echo "validating $NATIVE_BUNDLE_JAR against Spark $SPARK_RUNTIME"

TMP_JARS_DIR=/tmp/native-jars/$(date +%s)
mkdir -p $TMP_JARS_DIR
cp "$NATIVE_BUNDLE_JAR" $TMP_JARS_DIR/native-spark.jar

docker build \
  --build-arg SPARK_VERSION=$SPARK_RUNTIME \
  --build-arg SPARK_HADOOP_VERSION=$SPARK_HADOOP_VERSION \
  -t $CONTAINER_NAME:$SPARK_RUNTIME \
  "$WORKDIR"

docker run --rm \
  -v $TMP_JARS_DIR:/opt/native-jars \
  -v "$WORKDIR":/opt/native-spark \
  -e NATIVE_BUNDLE_JAR=/opt/native-jars/native-spark.jar \
  $CONTAINER_NAME:$SPARK_RUNTIME bash /opt/native-spark/validate.sh
