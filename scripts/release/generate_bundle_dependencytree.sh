#!/bin/bash

#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

declare -a ARTIFACT_IDS=(
"hudi-aws-bundle"
"hudi-datahub-sync-bundle"
"hudi-flink1.13-bundle"
"hudi-flink1.14-bundle"
"hudi-flink1.15-bundle"
"hudi-gcp-bundle"
"hudi-hadoop-mr-bundle"
"hudi-hive-sync-bundle"
"hudi-integ-test-bundle"
"hudi-kafka-connect-bundle"
"hudi-presto-bundle"
"hudi-spark-bundle_2.11"
"hudi-spark2.4-bundle_2.11"
"hudi-spark3-bundle_2.12"
"hudi-spark3.1-bundle_2.12"
"hudi-spark3.2-bundle_2.12"
"hudi-spark3.3-bundle_2.12"
"hudi-timeline-server-bundle"
"hudi-trino-bundle"
"hudi-utilities-bundle_2.11"
"hudi-utilities-bundle_2.12"
"hudi-utilities-slim-bundle_2.11"
"hudi-utilities-slim-bundle_2.12"
)

VERSION=0.12.2
SCALA_OPT=""
OUTPUT_DIRECTORY="packaging/dep-tree/$VERSION"

mkdir -p $OUTPUT_DIRECTORY

for aid in "${ARTIFACT_IDS[@]}"
do
  if [[ $aid == *_2.11 ]]; then
    SCALA_OPT="-Pscala-2.11";
  elif [[ $aid == *_2.12 ]]; then
    SCALA_OPT="-Pscala-2.12";
  fi

  cmd="mvn \
  com.github.ferstl:depgraph-maven-plugin:4.0.2:for-artifact \
  -DgraphFormat=text -DshowGroupIds=true -DshowVersions=true -DrepeatTransitiveDependenciesInTextGraph \
  -DoutputDirectory=$OUTPUT_DIRECTORY -DoutputFileName=$aid.deptree.$VERSION.txt \
  -DgroupId=org.apache.hudi -DartifactId=$aid -Dversion=$VERSION $SCALA_OPT"
  $cmd
done