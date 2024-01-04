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

VERSION=0.14.1

JARS=(
"$HOME/.m2/repository/org/apache/hudi/hudi-utilities-bundle_2.12/$VERSION/hudi-utilities-bundle_2.12-$VERSION.jar"
"$HOME/.m2/repository/org/apache/hudi/hudi-spark3.2-bundle_2.12/$VERSION/hudi-spark3.2-bundle_2.12-$VERSION.jar"
"$HOME/.m2/repository/org/apache/hudi/hudi-flink1.17-bundle/$VERSION/hudi-flink1.17-bundle-$VERSION.jar"
"$HOME/.m2/repository/org/apache/hudi/hudi-kafka-connect-bundle/$VERSION/hudi-kafka-connect-bundle-$VERSION.jar"
"$HOME/.m2/repository/org/apache/hudi/hudi-datahub-sync-bundle/$VERSION/hudi-datahub-sync-bundle-$VERSION.jar"
"$HOME/.m2/repository/org/apache/hudi/hudi-gcp-bundle/$VERSION/hudi-gcp-bundle-$VERSION.jar"
"$HOME/.m2/repository/org/apache/hudi/hudi-aws-bundle/$VERSION/hudi-aws-bundle-$VERSION.jar"
"$HOME/.m2/repository/org/apache/hudi/hudi-hadoop-mr-bundle/$VERSION/hudi-hadoop-mr-bundle-$VERSION.jar"
"$HOME/.m2/repository/org/apache/hudi/hudi-hive-sync-bundle/$VERSION/hudi-hive-sync-bundle-$VERSION.jar"
"$HOME/.m2/repository/org/apache/hudi/hudi-metaserver-server-bundle/$VERSION/hudi-metaserver-server-bundle-$VERSION.jar"
)

printf -v CLASSPATH ':%s' "${JARS[@]}"
echo "CLASSPATH=$CLASSPATH"

java -cp target/hudi-utils-0.14.1-jar-with-dependencies.jar$CLASSPATH \
org.apache.hudi.utils.HoodieConfigDocGenerator

cp /tmp/configurations.md ../website/docs/configurations.md
cp /tmp/basic_configurations.md ../website/docs/basic_configurations.md
