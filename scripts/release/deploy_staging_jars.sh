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

CURR_DIR=`pwd`
if [[ `basename $CURR_DIR` != "scripts" ]] ; then
  echo "You have to call the script from the scripts/ dir"
  exit 1
fi

if [[ $# -lt 1 ]]; then
    echo "This script will deploy artifacts to staging repositories"
    echo "There is one param required:"
    echo "--scala_version=\${SCALA_VERSION}"
    exit
else
    for param in "$@"
    do
	if [[ $param =~ --scala_version\=(2\.1[1-2]) ]]; then
		SCALA_VERSION=${BASH_REMATCH[1]}
	fi
    done
fi

###########################

cd ..

echo "Deploying to repository.apache.org with scala version ${SCALA_VERSION}"

COMMON_OPTIONS="-Dscala-${SCALA_VERSION} -Prelease -DskipTests -DretryFailedDeploymentCount=10 -DdeployArtifacts=true"
$MVN clean deploy $COMMON_OPTIONS
