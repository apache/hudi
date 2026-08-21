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
## Variables with defaults (if not overwritten by environment)
##
MVN=${MVN:-mvn}
# fail immediately
set -o errexit
set -o nounset

CURR_DIR=$(pwd)
if [ ! -d "$CURR_DIR/packaging" ] ; then
  echo "You have to call the script from the repository root dir that contains 'packaging/'"
  exit 1
fi

# Validate Java version (hudi-trino requires Java 25)
EXPECTED_JAVA_VERSION=25
JAVA_VERSION_OUTPUT=$("${JAVA_HOME:+$JAVA_HOME/bin/}java" -version 2>&1)
JAVA_MAJOR_VERSION=$(echo "$JAVA_VERSION_OUTPUT" | awk -F[\".] '/version/ {print ($2 == "1" ? $3 : $2); exit}')
if [ "$JAVA_MAJOR_VERSION" != "$EXPECTED_JAVA_VERSION" ]; then
  echo "Error: Java $EXPECTED_JAVA_VERSION is required for this script, but found:"
  echo "$JAVA_VERSION_OUTPUT"
  echo "Set JAVA_HOME to a Java $EXPECTED_JAVA_VERSION installation and retry."
  exit 1
fi

if [ "$#" -gt "1" ]; then
  echo "Only accept 0 or 1 argument. Use -h to see examples."
  exit 1
fi

declare -a ALL_VERSION_OPTS=(
# org.apache.hudi:hudi-trino (RFC-105), a plain library jar compiled with JDK 25.
# No -am: Lombok cannot run on JDK 25, so upstream Hudi modules (hudi-common,
# hudi-hive-sync, hudi-io:shaded, hudi-sync-common) must already be in the local m2
# from a prior deploy_staging_jars.sh (JDK 11) run.
"-Phudi-trino -pl hudi-trino"
)
printf -v joined "'%s'\n" "${ALL_VERSION_OPTS[@]}"

if [ "${1:-}" == "-h" ]; then
  echo "
Usage: $(basename "$0") [OPTIONS]

Options:
<version option>  One of the version options below
${joined}
-h, --help
"
  exit 0
fi

VERSION_OPT=${1:-}
valid_version_opt=false
for v in "${ALL_VERSION_OPTS[@]}"; do
    [[ $VERSION_OPT == "$v" ]] && valid_version_opt=true
done

if [ "$valid_version_opt" = true ]; then
  # run deploy for only specified version option
  ALL_VERSION_OPTS=("$VERSION_OPT")
elif [ "$#" == "1" ]; then
  echo "Version option $VERSION_OPT is invalid. Use -h to see examples."
  exit 1
fi

# -Dmaven.test.skip=true, not -DskipTests: test-compile needs hudi-trino-tests profile deps that are absent here.
COMMON_OPTIONS="-DdeployArtifacts=true -Dmaven.test.skip=true -DretryFailedDeploymentCount=10"
for v in "${ALL_VERSION_OPTS[@]}"
do
  echo "Deploying to repository.apache.org with options ${v}"
  # Single pass: unlike deploy_staging_jars.sh there is no -am here, so a separate
  # install pass would rebuild exactly what deploy builds.
  $MVN clean deploy $COMMON_OPTIONS ${v}
done
