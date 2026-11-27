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

# Runs a scale-dependent selection of existing tests against a release-candidate source tree that has
# already been installed with -DskipTests (see validate_rc.sh). Uses the JDK the source pom declares.
#
# Usage: run_tests.sh --source_dir=<dir> --scale=decent|large --log_dir=<dir> [--maven_args="..."]

set -o errexit
set -o nounset

source "$(dirname "$0")/common.sh"

MAVEN_ARGS="-Dspark3.5 -Dscala-2.12"
for param in "$@"; do
  case $param in
    --source_dir=*) SOURCE_DIR=${param#*=} ;;
    --scale=*) SCALE=${param#*=} ;;
    --log_dir=*) LOG_DIR=${param#*=} ;;
    --maven_args=*) MAVEN_ARGS=${param#*=} ;;
    *) die "Unknown argument: $param" ;;
  esac
done
[ -n "${SOURCE_DIR:-}" ] && [ -n "${SCALE:-}" ] && [ -n "${LOG_DIR:-}" ] \
  || die "--source_dir, --scale and --log_dir are required"

# The embedded Hive server used by the HoodieStreamer test harness only works on the JDK the release
# builds with, and classes compiled on a newer JDK fail on an older one, so always match the pom.
JAVA_HOME=$(resolve_java_home "$(pom_java_version "$SOURCE_DIR")")
export JAVA_HOME
SKIP_CHECKS="-Drat.skip=true -Dscalastyle.skip=true -Dspotless.check.skip=true -Djacoco.skip=true -Dcheckstyle.skip=true"
mkdir -p "$LOG_DIR"
FAILED=0

# A green build is not proof that tests ran: require a non-zero surefire/scalatest total with no failures.
check_surefire() {
  local name=$1 log_file=$2
  local summary
  summary=$(grep -E '^\[(INFO|WARNING|ERROR)\] Tests run: [0-9]+, Failures: [0-9]+, Errors: [0-9]+, Skipped: [0-9]+$' "$log_file" | tail -1)
  if [[ $summary =~ Tests\ run:\ ([0-9]+),\ Failures:\ ([0-9]+),\ Errors:\ ([0-9]+) ]] \
     && [ "${BASH_REMATCH[1]}" -gt 0 ] && [ "${BASH_REMATCH[2]}" -eq 0 ] && [ "${BASH_REMATCH[3]}" -eq 0 ]; then
    echo "TESTS PASS $name (${summary#*] })"
  else
    echo "TESTS FAIL $name (${summary:-no surefire summary}, see $log_file)"
    FAILED=1
  fi
}

check_scalatest() {
  local name=$1 log_file=$2
  local summary
  summary=$(grep -E 'Tests: succeeded [0-9]+, failed [0-9]+' "$log_file" | tail -1)
  if [[ $summary =~ succeeded\ ([0-9]+),\ failed\ ([0-9]+) ]] && [ "${BASH_REMATCH[1]}" -gt 0 ] \
     && [ "${BASH_REMATCH[2]}" -eq 0 ] && grep -q 'aborted 0' "$log_file"; then
    echo "TESTS PASS $name (${summary})"
  else
    echo "TESTS FAIL $name (${summary:-no scalatest summary}, see $log_file)"
    FAILED=1
  fi
}

cd "$SOURCE_DIR"

STREAMER_TESTS='TestHoodieDeltaStreamer#testUpsertsCOWContinuousMode+testUpsertsMORContinuousMode+testParquetDFSSourceWithSourceSchemaFileAndNoTransformer+testParquetDFSSourceWithoutSchemaProviderAndNoTransformer'
log "Running HoodieStreamer tests on JDK $(pom_java_version "$SOURCE_DIR") (~10 min)"
# shellcheck disable=SC2086
mvn $MAVEN_ARGS -Pspark3,unit-tests -pl hudi-utilities test -Dtest="$STREAMER_TESTS" -DfailIfNoTests=false \
  $SKIP_CHECKS > "$LOG_DIR/tests-streamer.log" 2>&1 || true
check_surefire "HoodieStreamer" "$LOG_DIR/tests-streamer.log"

if [ "$SCALE" == "large" ]; then
  # One invocation runs both the JUnit datasource classes (surefire, -Dtest) and the Spark SQL suites
  # (scalatest, -Dsuites); both share the module's target directory so they must not run concurrently.
  log "Running Spark datasource and Spark SQL tests (~100 min)"
  # shellcheck disable=SC2086
  mvn $MAVEN_ARGS -Pspark3,unit-tests -pl hudi-spark-datasource/hudi-spark test \
    -Dtest='TestCOWDataSource,TestMORDataSource' -DfailIfNoTests=false \
    -Dsuites='org.apache.spark.sql.hudi.dml.insert.TestInsertTable,org.apache.spark.sql.hudi.dml.others.TestMergeIntoTable' \
    $SKIP_CHECKS > "$LOG_DIR/tests-spark.log" 2>&1 || true
  check_surefire "Spark datasource (TestCOWDataSource, TestMORDataSource)" "$LOG_DIR/tests-spark.log"
  check_scalatest "Spark SQL (TestInsertTable, TestMergeIntoTable)" "$LOG_DIR/tests-spark.log"
fi

exit $FAILED
