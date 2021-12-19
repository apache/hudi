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

RELEASE_VERSION=`grep -A 5 "<artifactId>hudi</artifactId>" ../pom.xml  | grep '<version>' | sed -e 's/<version>//' -e 's/<\/version>//' -e 's/ //g'`

if [ -z "${RELEASE_VERSION}" ]; then
    echo "RELEASE_VERSION was not set."
    exit 1
fi

echo "RELEASE_VERSION=${RELEASE_VERSION}"

if [ "$(uname)" == "Darwin" ]; then
    SHASUM="shasum -a 512"
else
    SHASUM="sha512sum"
fi

###########################

cd ..

HUDI_DIR=`pwd`
RELEASE_DIR=${HUDI_DIR}/src_release
CLONE_DIR=${RELEASE_DIR}/hudi-tmp-clone

echo "Creating source package"

rm -rf ${RELEASE_DIR}
mkdir -p ${RELEASE_DIR}

# create a temporary git clone to ensure that we have a pristine source release
git clone ${HUDI_DIR} ${CLONE_DIR}
cd ${CLONE_DIR}

rsync -a \
  --exclude ".git" --exclude ".gitignore" --exclude ".gitattributes" --exclude ".travis.yml" \
  --exclude ".github" --exclude "target" \
  --exclude ".idea" --exclude "*.iml" --exclude ".DS_Store" --exclude "build-target" \
  --exclude "docs/content" --exclude ".rubydeps" \
  --exclude "rfc" \
  . hudi-$RELEASE_VERSION

tar czf ${RELEASE_DIR}/hudi-${RELEASE_VERSION}.src.tgz hudi-$RELEASE_VERSION
gpg --armor --detach-sig ${RELEASE_DIR}/hudi-${RELEASE_VERSION}.src.tgz
cd ${RELEASE_DIR}
$SHASUM hudi-${RELEASE_VERSION}.src.tgz > hudi-${RELEASE_VERSION}.src.tgz.sha512

cd ${CURR_DIR}
rm -rf ${CLONE_DIR}
