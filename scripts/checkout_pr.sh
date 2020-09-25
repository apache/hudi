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
# Usage: ./scripts/checkout_pr.sh
#
# Checkout a PR given the PR number into a local branch. PR branches are named
# using the convention "pull/<PR_NUMBER>", to enable pr_push_command.sh to work
# in tandem.
#
set -eou pipefail

function printUsage {
  echo "Usage: $(basename "${0}") [-r REMOTE] [-f] <PR_NUMBER>" 2>&1
  echo '   -r  REMOTE remote to grab PR from (default: apache)'
  echo '   -f         force overwrite of local branch (default: fail if exists)'
  exit 1
}

if [[ ${#} -eq 0 ]]; then
  printUsage
fi

REMOTE="origin"
FORCE=""
while getopts ":r:f" arg; do
  case "${arg}" in
    r)
      REMOTE="${OPTARG}"
      ;;
    f)
      FORCE="--force"
      ;;
    ?)
      printUsage
      ;;
  esac
done
shift "$(($OPTIND -1))"

# Debug output
PR_NUM=$1

# Checkout the PR into a local branch.
git fetch ${REMOTE} pull/${PR_NUM}/head:pull/${PR_NUM} ${FORCE}
git checkout pull/${PR_NUM}
