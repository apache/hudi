#!/usr/bin/env bash
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

set -eou pipefail
set -x

export LC_ALL=C

PWD=$(cd "$(dirname "$0")"/.. || exit; pwd)

function printUsage() {
  echo "Usage: $(basename "${0}") [-p <artifactId>] -r " 2>&1
  echo '   -r   [OPTIONAL] to replace the old dependencyList file with new dependencies'
  echo '   -p   [MUST] to generate new dependencyList file for the specified module'
}

function build_classpath() {
  mvn dependency:build-classpath -pl :${PL} -Dmdep.localRepoProperty=EMPTY_REPO |\
    grep -E -v "INFO|WARNING" | \
    tr ":" "\n" | \
    awk -F '/' '{
      artifact_id=$(NF-2);
      version=$(NF-1);
      jar_name=$NF;
      group_start_index=length("EMPTY_REPO/") + 1;
      group_end_index=length($0) - (length(jar_name) + length(version) + length(artifact_id) + 3);
      group=substr($0, group_start_index, group_end_index - group_start_index + 1);
      gsub(/\//, ".", group);
      classifier_start_index=length(artifact_id"-"version"-") + 1;
      classifier_end_index=index(jar_name, ".jar") - 1;
      classifier=substr(jar_name, classifier_start_index, classifier_end_index - classifier_start_index + 1);
      print artifact_id"/"group"/"version"/"classifier"/"jar_name
    }' | grep -v "hudi" | sort >> "${DEP_PR}"
}

function check_diff() {
    set +e
    the_diff=$(diff ${DEP} ${DEP_PR})
    set -e
    rm -rf "${DEP_PR}"
    if [[ -n $the_diff ]]; then
        echo "Dependency List Changed Detected: "
        echo ${the_diff}
        echo "To update the dependency file, refer to the usage:"
        printUsage
        exit 1
    fi
}

if [[ ${#} -eq 0 ]]; then
  printUsage
fi

PL=''
REPLACE='false'

while getopts "rp:" arg; do
  case "${arg}" in
    r)
      REPLACE="true"
      ;;
    p)
      PL=$OPTARG
      ;;
    ?)
      printUsage
      ;;
  esac
done

shift "$(( OPTIND - 1 ))"

# check must option
if [ -z "$PL" ]; then
  echo 'Missing -p argument' >&2
  exit 1
fi

DEP_PR="${PWD}"/dev/dependencyList"${PL}".txt.tmp
DEP="${PWD}"/dev/dependencyList_"${PL}".txt

rm -rf "${DEP_PR}"

cat >"${DEP_PR}"<<EOF
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

EOF

build_classpath

if [ $REPLACE == "true" ]; then
  rm -rf "${DEP}"
  mv "${DEP_PR}" "${DEP}"
  exit 0
fi

check_diff
