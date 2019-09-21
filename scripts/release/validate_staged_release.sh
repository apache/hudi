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

# fail immediately
set -o errexit
set -o nounset
# print command before executing
set -o xtrace

CURR_DIR=`pwd`
if [[ `basename $CURR_DIR` != "release" ]] ; then
  echo "You have to call the script from the release/ dir"
  exit 1
fi

if [ "$(uname)" == "Darwin" ]; then
    SHASUM="shasum -a 512"
else
    SHASUM="sha512sum"
fi


# Get to a scratch dir
RELEASE_TOOL_DIR=`pwd`
WORK_DIR=${RELEASE_TOOL_DIR}/validation_scratch_dir
rm -rf $WORK_DIR
mkdir $WORK_DIR
pushd $WORK_DIR

# Checkout dist incubator repo
LOCAL_SVN_DIR=local_svn_dir
ROOT_SVN_URL=https://dist.apache.org/repos/dist/
DEV_REPO=dev/incubator
#RELEASE_REPO=release/incubator
HUDI_REPO=hudi

rm -rf $LOCAL_SVN_DIR
mkdir $LOCAL_SVN_DIR
cd $LOCAL_SVN_DIR
svn co ${ROOT_SVN_URL}/${DEV_REPO}/${HUDI_REPO}

cd ${HUDI_REPO}/hudi-${RELEASE_VERSION}-incubating-rc${RC_NUM}
$SHASUM hudi-${RELEASE_VERSION}-incubating-rc${RC_NUM}.src.tgz > got.sha512

echo "Checking Checksum of Source Release"
diff -u hudi-${RELEASE_VERSION}-incubating-rc${RC_NUM}.src.tgz.sha512 got.sha512
echo "Checksum Check of Source Release - [OK]"

# GPG Check
echo "Checking Signature"
gpg --import ../KEYS
gpg --verify hudi-${RELEASE_VERSION}-incubating-rc${RC_NUM}.src.tgz.asc hudi-${RELEASE_VERSION}-incubating-rc${RC_NUM}.src.tgz 
echo "Signature Check - [OK]"

# Untar 
tar -zxf hudi-${RELEASE_VERSION}-incubating-rc${RC_NUM}.src.tgz 
pushd hudi-${RELEASE_VERSION}-incubating-rc${RC_NUM}

### BEGIN: Binary Files Check
echo "Checking for binary files in source release"
numBinaryFiles=`find . -iname '*' | grep -v '.git' | xargs -I {} file -0 -I {} | grep -va directory | grep -va 'text/' | wc -l`

if [ "$numBinaryFiles" > "0" ]; then
  echo "There were non-text files in source release. Please check below\n"
  find . -iname '*' | grep -v '.git' | xargs -I {} file -0 -I {} | grep -va directory | grep -va 'text/'
  exit -1
fi

echo "Binary Files in Source Release Check - [OK]"
### END: Binary Files Check

### Checking for DISCLAIMER
disclaimerFile="./DISCLAIMER"
if [ ! -f "$disclaimerFile" ]; then
  echo "DISCLAIMER file missing"
  exit -1
fi
echo "DISCLAIMER file exists ? [OK]"

### Checking for LICENSE and NOTICE
licenseFile="./LICENSE"
noticeFile="./NOTICE"
if [ ! -f "$licenseFile" ]; then
  echo "License file missing"
  exit -1
fi
echo "License file exists ? [OK]"

if [ ! -f "$noticeFile" ]; then
  echo "Notice file missing"
  exit -1
fi
echo "Notice file exists ? [OK]"

### Checking for RAT
echo "Running RAT Check Check"
mvn rat:check
echo "RAT Check Passed [OK]"

### Licensing Check
echo "Performing custom Licensing Check "
numfilesWithNoLicense=`find . -iname '*' | grep -v "\.git" | grep -v NOTICE | grep -v LICENSE | xargs grep -L "Licensed to the Apache Software Foundation (ASF)" | wc -l`
if [ "$numfilesWithNoLicense" > "0" ]; then
  echo "There were some source files that did not have Apache License"
  find . -iname '*' | grep -v "\.git" | grep -v NOTICE | grep -v LICENSE | xargs grep -L "Licensed to the Apache Software Foundation (ASF)"
  exit -1
fi
echo "Licensing Check Passed [OK]"
popd
