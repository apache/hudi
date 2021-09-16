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

set -o pipefail
set -e
set -x

export LC_ALL=C

PWD=$(cd "$(dirname "$0")"/.. || exit; pwd)

DEP_PR="${PWD}"/dev/dependencyList.tmp
DEP="${PWD}"/dev/dependencyList

function build_classpath() {
  mvn dependency:build-classpath -pl :hudi-spark-bundle_2.11 |\
    grep -E -v "INFO|WARNING" | \
    tr ":" "\n" | \
    awk -F '/' '{
      artifact_id=$(NF-2);
      version=$(NF-1);
      jar_name=$NF;
      classifier_start_index=length(artifact_id"-"version"-") + 1;
      classifier_end_index=index(jar_name, ".jar") - 1;
      classifier=substr(jar_name, classifier_start_index, classifier_end_index - classifier_start_index + 1);
      print artifact_id"/"version"/"classifier"/"jar_name
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
        echo "To update the dependency file, run './build/dependency.sh --replace'."
        exit 1
    fi
}

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

if [[ "$1" == "--replace" ]]; then
    rm -rf "${DEP}"
    mv "${DEP_PR}" "${DEP}"
    exit 0
fi

check_diff
