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

declare -a ALL_VERSION_OPTS=(
# upload all module jars and bundle jars
#"-Dscala-2.11 -Dspark2.4 -pl $BUNDLE_MODULES_EXCLUDED"
#"-Dscala-2.12 -Dspark2.4 -pl $BUNDLE_MODULES_EXCLUDED"
#"-Dscala-2.12 -Dspark3.3 -pl $BUNDLE_MODULES_EXCLUDED"
#"-Dscala-2.12 -Dspark3.2 -pl $BUNDLE_MODULES_EXCLUDED"
#"-Dscala-2.12 -Dspark3.1"  # this profile goes last in this section to ensure bundles use avro 1.8

# spark bundles
"-Dscala-2.11 -Dspark2.4 -pl packaging/hudi-spark-bundle,packaging/hudi-cli-bundle -am -DartifactId=hudi-spark2.4-bundle_2.11"
"-Dscala-2.12 -Dspark2.4 -pl packaging/hudi-spark-bundle -am -DartifactId=hudi-spark2.4-bundle_2.12"
"-Dscala-2.12 -Dspark3.3 -pl packaging/hudi-spark-bundle,packaging/hudi-cli-bundle -am -DartifactId=hudi-spark3.3-bundle_2.12"
"-Dscala-2.12 -Dspark3.2 -pl packaging/hudi-spark-bundle -am -DartifactId=hudi-spark3.2-bundle_2.12"
"-Dscala-2.12 -Dspark3.1 -pl packaging/hudi-spark-bundle -am -DartifactId=hudi-spark3.1-bundle_2.12"

# spark bundles (legacy) (not overwriting previous uploads as these jar names are unique)
"-Dscala-2.11 -Dspark2 -pl packaging/hudi-spark-bundle -am -DartifactId=hudi-spark-bundle_2.11" # for legacy bundle name hudi-spark-bundle_2.11
"-Dscala-2.12 -Dspark2 -pl packaging/hudi-spark-bundle -am -DartifactId=hudi-spark-bundle_2.12" # for legacy bundle name hudi-spark-bundle_2.12
"-Dscala-2.12 -Dspark3 -pl packaging/hudi-spark-bundle -am -DartifactId=hudi-spark3-bundle_2.12" # for legacy bundle name hudi-spark3-bundle_2.12

# utilities bundles (legacy) (overwriting previous uploads)
"-Dscala-2.11 -Dspark2.4 -pl packaging/hudi-utilities-bundle -am -DartifactId=hudi-utilities-bundle_2.11" # hudi-utilities-bundle_2.11 is for spark 2.4 only
"-Dscala-2.12 -Dspark3.1 -pl packaging/hudi-utilities-bundle -am -DartifactId=hudi-utilities-bundle_2.12" # hudi-utilities-bundle_2.12 is for spark 3.1 only

# utilities slim bundles
"-Dscala-2.11 -Dspark2.4 -pl packaging/hudi-utilities-slim-bundle -am -DartifactId=hudi-utilities-slim-bundle_2.11" # hudi-utilities-slim-bundle_2.11
"-Dscala-2.12 -Dspark3.1 -pl packaging/hudi-utilities-slim-bundle -am -DartifactId=hudi-utilities-slim-bundle_2.12" # hudi-utilities-slim-bundle_2.12

# flink bundles (overwriting previous uploads)
"-Dscala-2.12 -Dflink1.13 -Davro.version=1.10.0 -pl packaging/hudi-flink-bundle -am -DartifactId=hudi-flink1.13-bundle"
"-Dscala-2.12 -Dflink1.14 -Davro.version=1.10.0 -pl packaging/hudi-flink-bundle -am -DartifactId=hudi-flink1.14-bundle"
"-Dscala-2.12 -Dflink1.15 -Davro.version=1.10.0 -pl packaging/hudi-flink-bundle -am -DartifactId=hudi-flink1.15-bundle"
"-Dscala-2.12 -Dflink1.16 -Davro.version=1.10.0 -pl packaging/hudi-flink-bundle -am -DartifactId=hudi-flink1.16-bundle"
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
#COMMON_OPTIONS="-DskipTests -DretryFailedDeploymentCount=10"
for v in "${ALL_VERSION_OPTS[@]}"
do
  # TODO: consider cleaning all modules by listing directories instead of specifying profile
  if [[ "$v" == *"$BUNDLE_MODULES_EXCLUDED" ]]; then
    # When deploying jars with bundle exclusions, we still need to build the bundles,
    # by removing "-pl -packaging/hudi-aws-bundle...", otherwise the build fails.
    v1=${v%${BUNDLE_MODULES_EXCLUDED}}
    echo "Cleaning everything before any deployment"
    $MVN clean $COMMON_OPTIONS ${v1%-pl }
    echo "Building with options ${v1%-pl }"
    $MVN install $COMMON_OPTIONS ${v1%-pl }
  else
    echo "Cleaning everything before any deployment"
    $MVN clean $COMMON_OPTIONS ${v}
    echo "Building with options ${v}"
    #$MVN install $COMMON_OPTIONS ${v}
  fi
  echo "Deploying to repository.apache.org with version options ${v%-am}"
  # remove `-am` option to only deploy intended modules
  #$MVN deploy $COMMON_OPTIONS ${v%-am}
  VERSION=0.14.0-SNAPSHOT
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

