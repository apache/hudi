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
#set -o xtrace

CURR_DIR=`pwd`
if [[ `basename $CURR_DIR` != "scripts" ]] ; then
  echo "You have to call the script from the scripts/ dir"
  exit 1
fi

REDIRECT=' > /dev/null 2>&1'
if [[ $# -lt 1 ]]; then
    echo "This script will validate source release candidate published in dist for apache hudi"
    echo "You can set 3 args to this script: release version is mandatory, while rest two are optional. Default release_type is \"dev\". Or you can set to \"release\""
    echo "--release=\${CURRENT_RELEASE_VERSION}"
    echo "--rc_num=\${RC_NUM}"
    echo "--release_type=release"
    exit
else
    for param in "$@"
    do
	if [[ $param =~ --release\=([0-9]\.[0-9]*\.[0-9]) ]]; then
		RELEASE_VERSION=${BASH_REMATCH[1]}
	fi
	if [[ $param =~ --rc_num\=([0-9]*) ]]; then
                RC_NUM=${BASH_REMATCH[1]}
        fi	
        if [[ $param =~ --release_type\="release" ]]; then
                RELEASE_TYPE="release"
        fi
	if [[ $param =~ --verbose ]]; then
               REDIRECT=""
        fi
    done
fi

if [ "$(uname)" == "Darwin" ]; then
    SHASUM="shasum -a 512"
else
    SHASUM="sha512sum"
fi

if [ -z ${RC_NUM+x} ]; then
   RC_NUM=-1
fi

if [ -z ${RELEASE_TYPE+x} ]; then
   RELEASE_TYPE=dev
fi

# Get to a scratch dir
RELEASE_TOOL_DIR=`pwd`
WORK_DIR=/tmp/validation_scratch_dir_001
rm -rf $WORK_DIR
mkdir $WORK_DIR
pushd $WORK_DIR

# Checkout dist repo
LOCAL_SVN_DIR=local_svn_dir
ROOT_SVN_URL=https://dist.apache.org/repos/dist
REPO_TYPE=${RELEASE_TYPE}
HUDI_REPO=hudi

if [ $RC_NUM == -1 ]; then
    ARTIFACT_SUFFIX=${RELEASE_VERSION}
else
    ARTIFACT_SUFFIX=${RELEASE_VERSION}-rc${RC_NUM}
fi


rm -rf $LOCAL_SVN_DIR
mkdir $LOCAL_SVN_DIR
cd $LOCAL_SVN_DIR

echo "Downloading from svn co ${ROOT_SVN_URL}/${REPO_TYPE}/${HUDI_REPO}"

(bash -c "svn co ${ROOT_SVN_URL}/${REPO_TYPE}/${HUDI_REPO} $REDIRECT") || (echo -e "\t\t Unable to checkout  ${ROOT_SVN_URL}/${REPO_TYPE}/${HUDI_REPO} to $REDIRECT. Please run with --verbose to get details\n" && exit -1)

echo "Validating hudi-${ARTIFACT_SUFFIX} with release type \"${REPO_TYPE}\""
if [ $RELEASE_TYPE == "release" ]; then
  ARTIFACT_PREFIX=
elif [ $RELEASE_TYPE == "dev" ]; then
  ARTIFACT_PREFIX='hudi-'
else
  echo "Unexpected RELEASE_TYPE: $RELEASE_TYPE"
  exit 1;
fi
cd ${HUDI_REPO}/${ARTIFACT_PREFIX}${ARTIFACT_SUFFIX}
$SHASUM hudi-${ARTIFACT_SUFFIX}.src.tgz > got.sha512

echo "Checking Checksum of Source Release"
diff -u hudi-${ARTIFACT_SUFFIX}.src.tgz.sha512 got.sha512
echo -e "\t\tChecksum Check of Source Release - [OK]\n"

# Download KEYS file
curl https://dist.apache.org/repos/dist/release/hudi/KEYS > ../KEYS

# GPG Check
echo "Checking Signature"
(bash -c "gpg --import ../KEYS $REDIRECT" && bash -c "gpg --verify hudi-${ARTIFACT_SUFFIX}.src.tgz.asc hudi-${ARTIFACT_SUFFIX}.src.tgz $REDIRECT" && echo -e "\t\tSignature Check - [OK]\n") || (echo -e "\t\tSignature Check - [ERROR]\n\t\t Run with --verbose to get details\n" && exit 1)

# Untar 
(bash -c "tar -zxf hudi-${ARTIFACT_SUFFIX}.src.tgz $REDIRECT") || (echo -e "\t\t Unable to untar hudi-${ARTIFACT_SUFFIX}.src.tgz - [ERROR]\n\t\t Please run with --verbose to get details\n" && exit 1)
cd hudi-${ARTIFACT_SUFFIX}

### BEGIN: Binary Files Check
echo "Checking for binary files in source release"
numBinaryFiles=`find . -iname '*' | xargs -I {} file -I {} | grep -va directory | grep -v "/src/test/" | grep -va 'application/json' | grep -va 'text/' | grep -va 'application/xml' | grep -va 'application/json' | wc -l | sed -e s'/ //g'`

if [ "$numBinaryFiles" -gt "0" ]; then
  echo -e "There were non-text files in source release. [ERROR]\n Please check below\n"
  find . -iname '*' | xargs -I {} file -I {} | grep -va directory | grep -v "/src/test/" | grep -va 'application/json' | grep -va 'text/' |  grep -va 'application/xml'
  exit 1
fi
echo -e "\t\tNo Binary Files in Source Release? - [OK]\n"
### END: Binary Files Check

### Checking for DISCLAIMER
echo "Checking for DISCLAIMER"
disclaimerFile="./DISCLAIMER"
if [ -f "$disclaimerFile" ]; then
  echo "DISCLAIMER file should not be present [ERROR]"
  exit 1
fi
echo -e "\t\tDISCLAIMER file exists ? [OK]\n"

### Checking for LICENSE and NOTICE
echo "Checking for LICENSE and NOTICE"
licenseFile="./LICENSE"
noticeFile="./NOTICE"
if [ ! -f "$licenseFile" ]; then
  echo "License file missing [ERROR]"
  exit 1
fi
echo -e "\t\tLicense file exists ? [OK]"

if [ ! -f "$noticeFile" ]; then
  echo "Notice file missing [ERROR]"
  exit 1
fi
echo -e "\t\tNotice file exists ? [OK]\n"

### Licensing Check
echo "Performing custom Licensing Check "
numfilesWithNoLicense=`find . -iname '*' -type f | grep -v NOTICE | grep -v LICENSE | grep -v '.json' | grep -v '.hfile' | grep -v '.data' | grep -v '.commit' | grep -v DISCLAIMER | grep -v KEYS | grep -v '.mailmap' | grep -v '.sqltemplate' | grep -v 'banner.txt' | grep -v "fixtures" | xargs grep -L "Licensed to the Apache Software Foundation (ASF)" | wc -l`
if [ "$numfilesWithNoLicense" -gt  "0" ]; then
  echo "There were some source files that did not have Apache License [ERROR]"
  find . -iname '*' -type f | grep -v NOTICE | grep -v LICENSE | grep -v '.json' | grep -v '.hfile' | grep -v '.data' | grep -v '.commit' | grep -v DISCLAIMER | grep -v '.sqltemplate' | grep -v KEYS | grep -v '.mailmap' | grep -v 'banner.txt' | grep -v "fixtures" | xargs grep -L "Licensed to the Apache Software Foundation (ASF)"
  exit 1
fi
echo -e "\t\tLicensing Check Passed [OK]\n"

### Checking for RAT
echo "Running RAT Check"
(bash -c "mvn apache-rat:check $REDIRECT") || (echo -e "\t\t Rat Check Failed. [ERROR]\n\t\t Please run with --verbose to get details\n" && exit 1)
echo -e "\t\tRAT Check Passed [OK]\n"

popd
