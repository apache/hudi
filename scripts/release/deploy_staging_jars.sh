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
# print command before executing
set -o xtrace

CURR_DIR=$(pwd)
if [ ! -d "$CURR_DIR/packaging" ] ; then
  echo "You have to call the script from the repository root dir that contains 'packaging/'"
  exit 1
fi

declare -a SUPPORTED_VERSION_OPTS=(
"-Dscala-2.11 -Dspark2.4 -Dflink1.13"
"-Dscala-2.12 -Dspark2.4 -Dflink1.13"
"-Dscala-2.12 -Dspark3.1 -Dflink1.14"
"-Dscala-2.12 -Dspark3.2 -Dflink1.14"
)

for v in "${SUPPORTED_VERSION_OPTS[@]}"
do
  echo "Deploying to repository.apache.org with version option ${v}"
  COMMON_OPTIONS="${v} -Prelease -DskipTests -DretryFailedDeploymentCount=10 -DdeployArtifacts=true"
  $MVN clean deploy $COMMON_OPTIONS
done
