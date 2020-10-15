#!/bin/bash

#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.

#
# Usage: ./scripts/pr_push_command.sh
#
# When run from a PR branch checked out by checkout_pr.sh, provides a command
# that can push local PR branch to its corresponding remote.
# NOTE: Always double check correctness of command, before pushing.
#

set -eou pipefail

CURR_BRANCH=$(git status | grep "On branch" | awk '{print $NF}')
REMOTE="apache"

# Get PR number from branch
if [[ ${CURR_BRANCH} = pull/* ]]
then
	PR_NUM=$(echo "${CURR_BRANCH}" | awk -F'/' '{print $NF}')
else
	echo "Not on a PR branch?"
	exit 1
fi

# Parse the pr's remote, branch & add a remote if needed.
PR_RESP=$(curl https://api.github.com/repos/${REMOTE}/hudi/pulls/${PR_NUM})
if ! echo ${PR_RESP} | jq '.url' | grep "hudi/pulls/${PR_NUM}" ; then
  echo "Unable to find PR number ${PR_NUM} in remote ${REMOTE}"
  exit 1
fi

PR_SSH_URL=$(echo ${PR_RESP} |  jq -r '.head.repo.ssh_url')
PR_REMOTE_BRANCH=$(echo ${PR_RESP} | jq -r '.head.ref')
PR_REMOTE=$(echo ${PR_RESP} | jq -r '.head.repo.owner.login')

# Add a new remote, if needed.
if ! git remote -v | grep ${PR_REMOTE} ; then
    echo "Adding new remote ${PR_REMOTE} with ssh url ${PR_SSH_URL}"
    git remote add ${PR_REMOTE} ${PR_SSH_URL}
fi

# Push local branch to PR remote/branch
echo "If you are sure, execute the following command to push"
echo "  git push ${PR_REMOTE} ${CURR_BRANCH}:${PR_REMOTE_BRANCH}"
