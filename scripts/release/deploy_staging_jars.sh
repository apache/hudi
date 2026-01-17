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

if [ "$#" -gt "1" ]; then
  echo "Only accept 0 or 1 argument. Use -h to see examples."
  exit 1
fi

declare -a ALL_VERSION_OPTS=(
# For Spark 3.5, Scala 2.13:
# hudi-spark-common_2.13
# hudi-spark_2.13
# hudi-spark3.5.x_2.13
# hudi-utilities_2.13
# hudi-spark3.5-bundle_2.13
# hudi-utilities-bundle_2.13
# hudi-utilities-slim-bundle_2.13
# hudi-cli-bundle_2.13
"-T 1C -Dscala-2.13 -Dspark3.5 -pl hudi-spark-datasource/hudi-spark-common,hudi-spark-datasource/hudi-spark3.5.x,hudi-spark-datasource/hudi-spark,hudi-utilities,packaging/hudi-spark-bundle,packaging/hudi-utilities-bundle,packaging/hudi-utilities-slim-bundle,packaging/hudi-cli-bundle -am"
# For Spark 3.3, Scala 2.12:
# hudi-spark3.3.x_2.12
# hudi-spark3.3-bundle_2.12
"-T 1C -Dscala-2.12 -Dspark3.3 -pl hudi-spark-datasource/hudi-spark3.3.x,packaging/hudi-spark-bundle -am"
# For Spark 3.4, Scala 2.12:
# hudi-spark3.4.x_2.12
# hudi-spark3.4-bundle_2.12
"-T 1C -Dscala-2.12 -Dspark3.4 -pl hudi-spark-datasource/hudi-spark3.4.x,packaging/hudi-spark-bundle -am"
# For all modules spark3.5
"-Dscala-2.12 -Dspark3.5"

# Upload legacy Spark bundles (not overwriting previous uploads as these jar names are unique)
"-T 1C -Dscala-2.12 -Dspark3 -pl packaging/hudi-spark-bundle -am" # for legacy bundle name hudi-spark3-bundle_2.12

# Upload Flink bundles (overwriting previous uploads)
"-T 1C -Dscala-2.12 -Dflink1.17 -Davro.version=1.11.4 -Dparquet.version=1.12.3 -pl packaging/hudi-flink-bundle -am"
"-T 1C -Dscala-2.12 -Dflink1.18 -Davro.version=1.11.4 -Dparquet.version=1.13.1 -pl packaging/hudi-flink-bundle -am"
"-T 1C -Dscala-2.12 -Dflink1.19 -Davro.version=1.11.4 -Dparquet.version=1.13.1 -pl packaging/hudi-flink-bundle -am"
"-T 1C -Dscala-2.12 -Dflink1.20 -Davro.version=1.11.4 -Dparquet.version=1.13.1 -pl packaging/hudi-flink-bundle -am"
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

COMMON_OPTIONS="-DdeployArtifacts=true -DskipTests -DretryFailedDeploymentCount=10"
for v in "${ALL_VERSION_OPTS[@]}"
do
  # TODO: consider cleaning all modules by listing directories instead of specifying profile
  echo "Cleaning everything before any deployment"
  $MVN clean $COMMON_OPTIONS ${v}
  echo "Building with options ${v}"
  $MVN install $COMMON_OPTIONS ${v}

  echo "Deploying to repository.apache.org with version options ${v%-am}"
  # remove `-am` option to only deploy intended modules
  $MVN deploy $COMMON_OPTIONS ${v%-am}
done
