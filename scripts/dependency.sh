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
#set -x
export LC_ALL=C

PWD=$(cd "$(dirname "$0")"/.. || exit; pwd)

function generate_dependencies() {
  mvn --also-make dependency:tree -P $PROFILE | \
  grep maven-dependency-plugin | \
  grep bundle | \
  awk '{
    print $(NF-1);
  }' | \
  while read line; do
    FILE_NAME="${PWD}"/dependencies/"$line".txt
    build_classpath "$line" "-P "$PROFILE $FILE_NAME
  done
}

function build_classpath() {
  cat >"$3"<<EOF
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

  mvn dependency:build-classpath -pl :$1 $2 -Dmdep.localRepoProperty=EMPTY_REPO |\
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
  }' | grep -v "hudi" | sort >> "$3"
}

function check_diff() {
  mvn --also-make dependency:tree -P $PROFILE | \
  grep maven-dependency-plugin | \
  grep bundle | \
  awk '{
    print $(NF-1);
  }' | \
  while read line; do
    FILE_NAME="${PWD}"/dependencies/"$line".txt
    BACKUP_FILE_NAME=$FILE_NAME".bkp"
    mv $FILE_NAME $BACKUP_FILE_NAME
    build_classpath "$line" "-P "$PROFILE $FILE_NAME
    set +e
    the_diff=$(diff $FILE_NAME $BACKUP_FILE_NAME)
    set -e
    rm -rf "$BACKUP_FILE_NAME"
    if [[ -n $the_diff ]]; then
      echo "Dependency List Changed Detected [$line]: "
      echo ${the_diff}
      echo "To update the dependency file, refer to the usage:"
      printUsage
      exit 1
    fi
  done
}

function printUsage() {
  echo "Usage: $(basename "${0}") [-p <profile>] -c " 2>&1
  echo '   -c   [OPTIONAL] to check the dependencies diff'
  echo '   -p   [MUST] to generate new dependencyList file for all bundle module with given profile list'
}

if [[ ${#} -eq 0 ]]; then
  printUsage
fi

PROFILE=''
CHECK_DIFF='false'

while getopts "cp:" arg; do
  case "${arg}" in
    c)
      CHECK_DIFF="true"
      ;;
    p)
      PROFILE=$OPTARG
      ;;
    ?)
      printUsage
      ;;
  esac
done

shift "$(( OPTIND - 1 ))"

# check must option
if [ -z "$PROFILE" ]; then
  echo 'Missing -p argument' >&2
  exit 1
fi

if [ $CHECK_DIFF == "true" ]; then
  check_diff
else
  generate_dependencies
fi