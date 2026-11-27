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

# End-to-end validation of a Hudi release candidate before voting, at a chosen scale:
#
#   minimal (~5-10 min)  source release checks, quickstart on the staged bundles, short HoodieStreamer
#                        continuous run on MOR
#   decent  (~15-30 min) minimal + build from the source release + HoodieStreamer tests + a larger
#                        HoodieStreamer run
#   large   (~2-4 h)     decent + Spark datasource and Spark SQL test suites + high-volume
#                        HoodieStreamer runs on both MOR and COW
#
# Steps that run against binaries use the bundles staged in the Maven staging repository, i.e. the
# artifacts being voted on. Each step's log is kept under --work_dir; a summary is printed at the end.

set -o nounset

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/rc_validation/common.sh"

usage() {
  cat <<EOF
Usage: $0 --release=<X.Y.Z> --rc_num=<N> --staging_repo=<url> [options]

  --release=X.Y.Z          release version (required)
  --rc_num=N               release candidate number (required)
  --staging_repo=URL       Maven staging repository from the vote email (required for binary steps),
                           e.g. https://repository.apache.org/content/repositories/orgapachehudi-1198/
  --scale=minimal|decent|large   default: minimal
  --spark_home=DIR         Spark distribution matching --spark_version (default: \$SPARK_HOME)
  --spark_version=3.5      Spark version of the bundles to validate (default: 3.5)
  --scala_version=2.12     Scala version of the bundles to validate (default: 2.12)
  --bundle_version=V       version of the staged bundles (default: X.Y.Z-rcN)
  --signing_key=ID         expected signing key id or fingerprint suffix from the vote email
  --source_dir=DIR         build and test this checkout instead of the source release tarball
  --maven_args="..."       profile flags for build/tests (default: -Dspark<spark_version> -Dscala-<scala_version>)
  --work_dir=DIR           default: /tmp/hudi-rc-validation-X.Y.Z-rcN
  --steps=a,b,...          override the steps implied by --scale: release,quickstart,e2e,build,tests
EOF
  exit 1
}

SCALE=minimal
SPARK_VERSION=3.5
SCALA_VERSION=2.12
for param in "$@"; do
  case $param in
    --release=*) RELEASE=${param#*=} ;;
    --rc_num=*) RC_NUM=${param#*=} ;;
    --staging_repo=*) STAGING_REPO=${param#*=} ;;
    --scale=*) SCALE=${param#*=} ;;
    --spark_home=*) SPARK_HOME=${param#*=} ;;
    --spark_version=*) SPARK_VERSION=${param#*=} ;;
    --scala_version=*) SCALA_VERSION=${param#*=} ;;
    --bundle_version=*) BUNDLE_VERSION=${param#*=} ;;
    --signing_key=*) SIGNING_KEY=${param#*=} ;;
    --source_dir=*) SOURCE_DIR=${param#*=} ;;
    --maven_args=*) MAVEN_ARGS=${param#*=} ;;
    --work_dir=*) WORK_DIR=${param#*=} ;;
    --steps=*) STEPS=${param#*=} ;;
    *) usage ;;
  esac
done
[ -n "${RELEASE:-}" ] && [ -n "${RC_NUM:-}" ] || usage
export SPARK_HOME=${SPARK_HOME:-}
BUNDLE_VERSION=${BUNDLE_VERSION:-$RELEASE-rc$RC_NUM}
MAVEN_ARGS=${MAVEN_ARGS:-"-Dspark$SPARK_VERSION -Dscala-$SCALA_VERSION"}
WORK_DIR=${WORK_DIR:-/tmp/hudi-rc-validation-$RELEASE-rc$RC_NUM}
case $SCALE in
  minimal) STEPS=${STEPS:-release,quickstart,e2e} ;;
  decent|large) STEPS=${STEPS:-release,quickstart,e2e,build,tests} ;;
  *) usage ;;
esac
has_step() { [[ ",$STEPS," == *",$1,"* ]]; }

mkdir -p "$WORK_DIR"
SUMMARY="$WORK_DIR/summary.txt"
: > "$SUMMARY"
record() {
  printf '%-12s %-5s %6ss  %s\n' "$1" "$2" "$3" "$4" | tee -a "$SUMMARY"
}
run_step() {
  local name=$1 log_file=$2
  shift 2
  log "Step $name started (log: $log_file)"
  local start=$SECONDS
  # Subshell, so a step that dies fails only that step and the remaining steps still run.
  if ("$@") > "$log_file" 2>&1; then
    record "$name" PASS $((SECONDS - start)) "$log_file"
  else
    record "$name" FAIL $((SECONDS - start)) "$log_file"
  fi
  grep -E '^(QS|E2E|TESTS) (PASS|FAIL)|\[OK\]|\[ERROR\]|Good signature|BAD signature|Signing key' "$log_file" | sed 's/^/    /' || true
}

log "Validating hudi $RELEASE-rc$RC_NUM at scale '$SCALE' (steps: $STEPS) in $WORK_DIR"

if has_step quickstart || has_step e2e; then
  [ -n "${STAGING_REPO:-}" ] || die "--staging_repo is required for the quickstart and e2e steps"
  check_spark_home "$SPARK_VERSION"
  log "Downloading staged bundles $BUNDLE_VERSION"
  SPARK_BUNDLE=$(fetch_bundle "$STAGING_REPO" "hudi-spark${SPARK_VERSION}-bundle_${SCALA_VERSION}" "$BUNDLE_VERSION" "$WORK_DIR/bundles")
  UTILITIES_BUNDLE=$(fetch_bundle "$STAGING_REPO" "hudi-utilities-slim-bundle_${SCALA_VERSION}" "$BUNDLE_VERSION" "$WORK_DIR/bundles")
fi

step_release() {
  (cd "$SCRIPT_DIR/.." && bash release/validate_staged_release.sh --release="$RELEASE" --rc_num="$RC_NUM" --verbose) || return 1
  local svn_dir=/tmp/validation_scratch_dir_001/local_svn_dir/hudi-$RELEASE-rc$RC_NUM
  if [ -n "${SIGNING_KEY:-}" ]; then
    local fingerprint
    fingerprint=$(gpg --verify --status-fd 1 "$svn_dir/hudi-$RELEASE-rc$RC_NUM.src.tgz.asc" "$svn_dir/hudi-$RELEASE-rc$RC_NUM.src.tgz" 2>/dev/null \
      | awk '/VALIDSIG/ {print $3}')
    echo "Signing key fingerprint: $fingerprint"
    [[ "$fingerprint" == *"$(echo "$SIGNING_KEY" | tr -d ' ' | tr '[:lower:]' '[:upper:]')" ]] \
      || { echo "[ERROR] Signing key does not match $SIGNING_KEY"; return 1; }
    echo "Signing key matches $SIGNING_KEY [OK]"
  fi
  # Keep the voted source release for the build step; validate_staged_release.sh wipes its own dir on rerun.
  rm -rf "$WORK_DIR/source" && mkdir -p "$WORK_DIR/source"
  tar -xzf "$svn_dir/hudi-$RELEASE-rc$RC_NUM.src.tgz" -C "$WORK_DIR/source"
}

step_quickstart() {
  rm -rf "$WORK_DIR/quickstart" && mkdir -p "$WORK_DIR/quickstart"
  run_spark_script "$SCRIPT_DIR/rc_validation/quickstart.scala" "$WORK_DIR/quickstart/spark-shell.log" \
    --master 'local[4]' --driver-memory 3g --jars "$SPARK_BUNDLE" \
    --conf spark.hudi.rc.quickstart.dir="$WORK_DIR/quickstart" --conf spark.sql.warehouse.dir="$WORK_DIR/quickstart/warehouse"
  local rc=$?
  grep -oE 'QS (PASS|FAIL|SUMMARY).*' "$WORK_DIR/quickstart/spark-shell.log"
  return $rc
}

step_e2e() {
  local table_types="MERGE_ON_READ" failed=0
  [ "$SCALE" == "large" ] && table_types="MERGE_ON_READ COPY_ON_WRITE"
  for table_type in $table_types; do
    bash "$SCRIPT_DIR/rc_validation/run_streamer_e2e.sh" --spark_bundle="$SPARK_BUNDLE" \
      --utilities_bundle="$UTILITIES_BUNDLE" --work_dir="$WORK_DIR" --table_type="$table_type" --scale="$SCALE" || failed=1
  done
  return $failed
}

resolve_source_dir() {
  if [ -z "${SOURCE_DIR:-}" ]; then
    SOURCE_DIR=$(ls -d "$WORK_DIR"/source/hudi-* 2>/dev/null | head -1)
    [ -n "$SOURCE_DIR" ] || die "No source release extracted under $WORK_DIR/source; run the release step or pass --source_dir"
  fi
}

step_build() {
  resolve_source_dir
  local java_version
  java_version=$(pom_java_version "$SOURCE_DIR")
  JAVA_HOME=$(resolve_java_home "$java_version") || return 1
  export JAVA_HOME
  echo "Building $SOURCE_DIR with JDK $java_version ($JAVA_HOME), maven args: $MAVEN_ARGS"
  # shellcheck disable=SC2086
  (cd "$SOURCE_DIR" && mvn clean install -DskipTests $MAVEN_ARGS \
    -Drat.skip=true -Dcheckstyle.skip=true -Dscalastyle.skip=true -Dspotless.check.skip=true -Djacoco.skip=true \
    -pl hudi-utilities,hudi-spark-datasource/hudi-spark,packaging/hudi-spark-bundle,packaging/hudi-utilities-slim-bundle -am)
}

step_tests() {
  resolve_source_dir
  bash "$SCRIPT_DIR/rc_validation/run_tests.sh" --source_dir="$SOURCE_DIR" --scale="$SCALE" \
    --log_dir="$WORK_DIR/tests" --maven_args="$MAVEN_ARGS"
}

for step in release quickstart e2e build tests; do
  has_step $step && run_step $step "$WORK_DIR/$step.log" "step_$step"
done

echo
echo "==== hudi $RELEASE-rc$RC_NUM validation summary (scale: $SCALE) ===="
cat "$SUMMARY"
! grep -q ' FAIL ' "$SUMMARY"
