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

# fail immediately
set -o errexit
set -o nounset

REPO=$1
VERSION=$2

STAGING_REPO="https://repository.apache.org/content/repositories/${REPO}/org/apache/hudi"

declare -a BUNDLE_URLS=(
"${STAGING_REPO}/hudi-datahub-sync-bundle/${VERSION}/hudi-datahub-sync-bundle-${VERSION}.jar"
"${STAGING_REPO}/hudi-flink1.13-bundle_2.11/${VERSION}/hudi-flink1.13-bundle_2.11-${VERSION}.jar"
"${STAGING_REPO}/hudi-flink1.13-bundle_2.12/${VERSION}/hudi-flink1.13-bundle_2.12-${VERSION}.jar"
"${STAGING_REPO}/hudi-flink1.14-bundle_2.11/${VERSION}/hudi-flink1.14-bundle_2.11-${VERSION}.jar"
"${STAGING_REPO}/hudi-flink1.14-bundle_2.12/${VERSION}/hudi-flink1.14-bundle_2.12-${VERSION}.jar"
"${STAGING_REPO}/hudi-gcp-bundle/${VERSION}/hudi-gcp-bundle-${VERSION}.jar"
"${STAGING_REPO}/hudi-hadoop-mr-bundle/${VERSION}/hudi-hadoop-mr-bundle-${VERSION}.jar"
"${STAGING_REPO}/hudi-hive-sync-bundle/${VERSION}/hudi-hive-sync-bundle-${VERSION}.jar"
"${STAGING_REPO}/hudi-integ-test-bundle/${VERSION}/hudi-integ-test-bundle-${VERSION}.jar"
"${STAGING_REPO}/hudi-kafka-connect-bundle/${VERSION}/hudi-kafka-connect-bundle-${VERSION}.jar"
"${STAGING_REPO}/hudi-presto-bundle/${VERSION}/hudi-presto-bundle-${VERSION}.jar"
"${STAGING_REPO}/hudi-spark-bundle_2.11/${VERSION}/hudi-spark-bundle_2.11-${VERSION}.jar"
"${STAGING_REPO}/hudi-spark-bundle_2.12/${VERSION}/hudi-spark-bundle_2.12-${VERSION}.jar"
"${STAGING_REPO}/hudi-spark2.4-bundle_2.11/${VERSION}/hudi-spark2.4-bundle_2.11-${VERSION}.jar"
"${STAGING_REPO}/hudi-spark2.4-bundle_2.12/${VERSION}/hudi-spark2.4-bundle_2.12-${VERSION}.jar"
"${STAGING_REPO}/hudi-spark3-bundle_2.12/${VERSION}/hudi-spark3-bundle_2.12-${VERSION}.jar"
"${STAGING_REPO}/hudi-spark3.1-bundle_2.12/${VERSION}/hudi-spark3.1-bundle_2.12-${VERSION}.jar"
"${STAGING_REPO}/hudi-spark3.2-bundle_2.12/${VERSION}/hudi-spark3.2-bundle_2.12-${VERSION}.jar"
"${STAGING_REPO}/hudi-timeline-server-bundle/${VERSION}/hudi-timeline-server-bundle-${VERSION}.jar"
"${STAGING_REPO}/hudi-trino-bundle/${VERSION}/hudi-trino-bundle-${VERSION}.jar"
"${STAGING_REPO}/hudi-utilities-bundle_2.11/${VERSION}/hudi-utilities-bundle_2.11-${VERSION}.jar"
"${STAGING_REPO}/hudi-utilities-bundle_2.12/${VERSION}/hudi-utilities-bundle_2.12-${VERSION}.jar"
"${STAGING_REPO}/hudi-utilities-slim-bundle_2.11/${VERSION}/hudi-utilities-slim-bundle_2.11-${VERSION}.jar"
"${STAGING_REPO}/hudi-utilities-slim-bundle_2.12/${VERSION}/hudi-utilities-slim-bundle_2.12-${VERSION}.jar"
)

NOW=$(date +%s)
TMP_DIR_FOR_BUNDLES=/tmp/${NOW}
mkdir "$TMP_DIR_FOR_BUNDLES"
for url in "${BUNDLE_URLS[@]}"; do
   echo "downloading $url"
   wget "$url" -P "$TMP_DIR_FOR_BUNDLES"
done

ls -l "$TMP_DIR_FOR_BUNDLES"
