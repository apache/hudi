#!/bin/bash
#
#    Licensed to the Apache Software Foundation (ASF) under one or more
#    contributor license agreements.  See the NOTICE file distributed with
#    this work for additional information regarding copyright ownership.
#    The ASF licenses this file to You under the Apache License, Version 2.0
#    (the "License"); you may not use this file except in compliance with
#    the License.  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#

# This script will update apache hudi master branch with next release version
# and cut release branch for current development version.

# Parse parameters passing into the script

set -e

function clean_up(){
  echo "Do you want to clean local clone repo? [y|N]"
  read confirmation
  if [[ $confirmation = "y" ]]; then
    cd ~
    rm -rf ${LOCAL_CLONE_DIR}
    echo "Clean up local repo."
  fi
}

if [[ $# -eq 1 && $1 = "-h" ]]; then
	echo "This script will update apache hudi master branch with next release version and cut release branch for current development version."
	echo "There are 3 params required:"
	echo "--release=\${CURRENT_RELEASE_VERSION}"
	echo "--next_release=\${NEXT_RELEASE_VERSION}"
	echo "--rc_num=\${RC_NUM}"
	exit
else
	for param in "$@"
	do
		if [[ $param =~ --release\=([0-9]\.[0-9]*\.[0-9]) ]]; then
			RELEASE=${BASH_REMATCH[1]}
		fi
		if [[ $param =~ --next_release\=([0-9]\.[0-9]*\.[0-9]) ]]; then
			NEXT_VERSION_IN_BASE_BRANCH=${BASH_REMATCH[1]}
		fi
		if [[ $param =~ --rc_num\=([0-9]*) ]]; then
                        RC_NUM=${BASH_REMATCH[1]}
		fi
	done
fi

if [[ -z "$RELEASE" || -z "$NEXT_VERSION_IN_BASE_BRANCH" || -z "$RC_NUM" ]]; then
	echo "This script needs to be ran with params, please run with -h to get more instructions."
	exit
fi


MASTER_BRANCH=master
NEXT_VERSION_BRANCH=MINOR-move-to-${NEXT_VERSION_IN_BASE_BRANCH}
RELEASE_BRANCH=release-${RELEASE}
GITHUB_REPO_URL=git@github.com:apache/hudi.git
HUDI_ROOT_DIR=hudi
LOCAL_CLONE_DIR=hudi_release_${RELEASE}

echo "=====================Environment Variables====================="
echo "version: ${RELEASE}"
echo "next_release: ${NEXT_VERSION_IN_BASE_BRANCH}"
echo "working master branch: ${MASTER_BRANCH}"
echo "working next-version branch: ${NEXT_VERSION_BRANCH}"
echo "working release branch: ${RELEASE_BRANCH}"
echo "local repo dir: ~/${LOCAL_CLONE_DIR}/${HUDI_ROOT_DIR}"
echo "RC_NUM: $RC_NUM"
echo "==============================================================="

cd ~
if [[ -d ${LOCAL_CLONE_DIR} ]]; then
  rm -rf ${LOCAL_CLONE_DIR}
fi

mkdir ${LOCAL_CLONE_DIR}
cd ${LOCAL_CLONE_DIR}
git clone ${GITHUB_REPO_URL}
cd ${HUDI_ROOT_DIR}

# Now, create local release branch
git branch ${RELEASE_BRANCH}

git checkout ${MASTER_BRANCH}
git checkout -b ${NEXT_VERSION_BRANCH}

echo "====================Current working branch====================="
echo ${NEXT_VERSION_BRANCH}
echo "==============================================================="

# Update master branch
mvn versions:set -DnewVersion=${NEXT_VERSION_IN_BASE_BRANCH}-SNAPSHOT

echo "===========Update next-version branch as following============="
git diff
echo "==============================================================="

echo "Please make sure all changes above are expected. Do you confirm to commit?: [y|N]"
read confirmation
if [[ $confirmation != "y" ]]; then
  echo "Exit without committing any changes on master branch."
  clean_up
  exit
fi

git commit -am "[MINOR] Moving to ${NEXT_VERSION_IN_BASE_BRANCH}-SNAPSHOT on master branch."

echo "==============================================================="
echo "!!Please open a PR based on ${NEXT_VERSION_BRANCH} branch for approval!! [Press ENTER to continue]"
read confirmation

# Checkout and update release branch
git checkout ${RELEASE_BRANCH}
mvn versions:set -DnewVersion=${RELEASE}-rc${RC_NUM}

echo "==================Current working branch======================="
echo ${RELEASE_BRANCH}
echo "==============================================================="

echo "===============Update release branch as following=============="
git diff
echo "==============================================================="

echo "Please make sure all changes above are expected. Do you confirm to commit?: [y|N]"
read confirmation
if [[ $confirmation != "y" ]]; then
  echo "Exit without committing any changes on release branch."
  clean_up
  exit
fi

git commit -am "Create release branch for version ${RELEASE}."
git push --set-upstream origin ${RELEASE_BRANCH}

clean_up
