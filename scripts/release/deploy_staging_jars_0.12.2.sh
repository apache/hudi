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

BUNDLE_MODULES=$(find -s packaging -name 'hudi-*-bundle' -type d)
BUNDLE_MODULES_EXCLUDED="-${BUNDLE_MODULES//$'\n'/,-}"
BUNDLE_MODULES_EXCLUDED="-packaging/hudi-aws-bundle,-packaging/hudi-datahub-sync-bundle,-packaging/hudi-flink-bundle,-packaging/hudi-gcp-bundle,-packaging/hudi-integ-test-bundle,-packaging/hudi-kafka-connect-bundle"

declare -a ALL_VERSION_OPTS=(
# upload all module jars and bundle jars
#"-Dscala-2.11 -Dspark2.4 -pl $BUNDLE_MODULES_EXCLUDED"
#"-Dscala-2.12 -Dspark2.4 -pl $BUNDLE_MODULES_EXCLUDED"
#"-Dscala-2.12 -Dspark3.3 -pl $BUNDLE_MODULES_EXCLUDED"
#"-Dscala-2.12 -Dspark3.2 -pl $BUNDLE_MODULES_EXCLUDED"
#"-Dscala-2.12 -Dspark3.1"  # this profile goes last in this section to ensure bundles use avro 1.8

# spark bundles
"-Dscala-2.11 -Dspark2.4 -pl packaging/hudi-spark-bundle -am -DartifactId=hudi-spark2.4-bundle_2.11"
"-Dscala-2.12 -Dspark2.4 -pl packaging/hudi-spark-bundle -am -DartifactId=hudi-spark2.4-bundle_2.12"
"-Dscala-2.12 -Dspark3.3 -pl packaging/hudi-spark-bundle -am -DartifactId=hudi-spark3.3-bundle_2.12"
"-Dscala-2.12 -Dspark3.2 -pl packaging/hudi-spark-bundle -am -DartifactId=hudi-spark3.2-bundle_2.12"
"-Dscala-2.12 -Dspark3.1 -pl packaging/hudi-spark-bundle -am -DartifactId=hudi-spark3.1-bundle_2.12"

# spark bundles (legacy) (not overwriting previous uploads as these jar names are unique)
"-Dscala-2.11 -Dspark2 -pl packaging/hudi-spark-bundle -am -DartifactId=hudi-spark-bundle_2.11" # for legacy bundle name hudi-spark-bundle_2.11
"-Dscala-2.12 -Dspark2 -pl packaging/hudi-spark-bundle -am -DartifactId=hudi-spark-bundle_2.12" # for legacy bundle name hudi-spark-bundle_2.12
"-Dscala-2.12 -Dspark3 -pl packaging/hudi-spark-bundle -am -DartifactId=hudi-spark3-bundle_2.12" # for legacy bundle name hudi-spark3-bundle_2.12

# utilities slim bundles
"-Dscala-2.11 -Dspark2.4 -pl packaging/hudi-utilities-slim-bundle -am -DartifactId=hudi-utilities-slim-bundle_2.11" # hudi-utilities-slim-bundle_2.11
"-Dscala-2.12 -Dspark3.1 -pl packaging/hudi-utilities-slim-bundle -am -DartifactId=hudi-utilities-slim-bundle_2.12" # hudi-utilities-slim-bundle_2.12

# utilities bundles (legacy) (overwriting previous uploads)
"-Dscala-2.11 -Dspark2.4 -pl packaging/hudi-utilities-bundle -am -DartifactId=hudi-utilities-bundle_2.11" # hudi-utilities-bundle_2.11 is for spark 2.4 only
"-Dscala-2.12 -Dspark3.1 -pl packaging/hudi-utilities-bundle -am -DartifactId=hudi-utilities-bundle_2.12" # hudi-utilities-bundle_2.12 is for spark 3.1 only

# flink bundles (overwriting previous uploads)
"-Dscala-2.12 -Dflink1.13 -Davro.version=1.10.0 -pl packaging/hudi-flink-bundle -am -DartifactId=hudi-flink1.13-bundle"
"-Dscala-2.12 -Dflink1.14 -Davro.version=1.10.0 -pl packaging/hudi-flink-bundle -am -DartifactId=hudi-flink1.14-bundle"
"-Dscala-2.12 -Dflink1.15 -Davro.version=1.10.0 -pl packaging/hudi-flink-bundle -am -DartifactId=hudi-flink1.15-bundle"
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

for v in "${ALL_VERSION_OPTS[@]}"
do
  # clean everything before any round of depoyment
  #$MVN clean
  #echo "Building with options ${v}"
  #echo "install Command: $MVN install "$COMMON_OPTIONS" "${v}""
  #$MVN install "${v}" "$COMMON_OPTIONS"
  #echo "Deploying to repository.apache.org with version options $COMMON_OPTIONS ${v%-am}"
  #echo "Command execute: $MVN clean install deploy "${v%-am}" "$COMMON_OPTIONS""
  # remove `-am` option to only deploy intended modules
  #$MVN clean deploy "$COMMON_OPTIONS" "${v%-am}"
  COMMON_OPTIONS="${v} -DdeployArtifacts=true -DskipTests -DretryFailedDeploymentCount=10"
  VERSION=0.12.2
  OUTPUT_DIRECTORY="packaging/dep-tree/$VERSION"
  aid=${v##* -DartifactId=}
  echo $aid
  cmd="mvn com.github.ferstl:depgraph-maven-plugin:4.0.2:for-artifact \
    -DgraphFormat=text -DshowGroupIds=true -DshowVersions=true -DrepeatTransitiveDependenciesInTextGraph \
    -DoutputDirectory=$OUTPUT_DIRECTORY \
    -DgroupId=org.apache.hudi -Dversion=$VERSION $v -DoutputFileName=$aid.deptree.txt $COMMON_OPTIONS"
  echo $cmd
  $cmd
done
