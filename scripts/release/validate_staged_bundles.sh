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

declare -a extensions=("-javadoc.jar" "-javadoc.jar.asc" "-javadoc.jar.md5" "-javadoc.jar.sha1" "-sources.jar"
"-sources.jar.asc" "-sources.jar.md5" "-sources.jar.sha1" ".jar" ".jar.asc" ".jar.md5" ".jar.sha1" ".pom" ".pom.asc"
".pom.md5" ".pom.sha1")

declare -a bundles=("hudi-aws-bundle" "hudi-cli-bundle_2.11" "hudi-cli-bundle_2.12" "hudi-datahub-sync-bundle" "hudi-flink1.13-bundle" "hudi-flink1.14-bundle"
"hudi-flink1.15-bundle" "hudi-flink1.16-bundle" "hudi-flink1.17-bundle" "hudi-flink1.18-bundle" "hudi-gcp-bundle" "hudi-hadoop-mr-bundle" "hudi-hive-sync-bundle" "hudi-integ-test-bundle"
"hudi-kafka-connect-bundle" "hudi-metaserver-server-bundle" "hudi-presto-bundle" "hudi-spark-bundle_2.11" "hudi-spark-bundle_2.12"
"hudi-spark2.4-bundle_2.11" "hudi-spark2.4-bundle_2.12" "hudi-spark3-bundle_2.12" "hudi-spark3.0-bundle_2.12" "hudi-spark3.1-bundle_2.12"
"hudi-spark3.2-bundle_2.12" "hudi-spark3.3-bundle_2.12" "hudi-spark3.4-bundle_2.12" "hudi-spark3.5-bundle_2.12" "hudi-timeline-server-bundle"
"hudi-trino-bundle" "hudi-utilities-bundle_2.11" "hudi-utilities-bundle_2.12" "hudi-utilities-slim-bundle_2.11"
"hudi-utilities-slim-bundle_2.12")

NOW=$(date +%s)
TMP_DIR_FOR_BUNDLES=/tmp/${NOW}
mkdir "$TMP_DIR_FOR_BUNDLES"

for bundle in "${bundles[@]}"
do
   for extension in "${extensions[@]}"
   do
       url=${STAGING_REPO}/$bundle/${VERSION}/$bundle-${VERSION}$extension
       if curl --output "$TMP_DIR_FOR_BUNDLES/$bundle-${VERSION}$extension" --head --fail "$url"; then
         echo "Artifact exists: $url"
       else
         echo "Artifact missing: $url"
         exit 1
       fi
   done
done

echo "All artifacts exist. Validation succeeds."
