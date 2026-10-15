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

# Helpers shared by the release-candidate validation scripts. Source, do not execute.

RC_VALIDATION_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

log() {
  echo "[$(date '+%H:%M:%S')] $*"
}

die() {
  echo "[$(date '+%H:%M:%S')] ERROR: $*" >&2
  exit 1
}

# Prints a JAVA_HOME for the given major version (8, 11, 17, ...).
# Honors JAVA<N>_HOME first, then falls back to macOS java_home.
resolve_java_home() {
  local major=$1
  local var="JAVA${major}_HOME"
  if [ -n "${!var:-}" ]; then
    echo "${!var}"
    return
  fi
  if [ -x /usr/libexec/java_home ]; then
    local spec=$major
    [ "$major" == "8" ] && spec=1.8
    /usr/libexec/java_home -v "$spec" 2>/dev/null && return
  fi
  die "No JDK $major found. Set ${var} to a JDK $major installation."
}

# Prints the <java.version> declared by the root pom of a source tree.
pom_java_version() {
  grep -m1 -oE '<java.version>[0-9.]+</java.version>' "$1/pom.xml" | sed -E 's#<[^>]+>##g; s#^1\.##'
}

# Downloads a bundle jar from a Maven repository and verifies its sha1.
# Usage: fetch_bundle <repo-url> <artifactId> <version> <dest-dir>; prints the jar path.
fetch_bundle() {
  local repo=$1 artifact=$2 version=$3 dest=$4
  local url="${repo%/}/org/apache/hudi/${artifact}/${version}/${artifact}-${version}.jar"
  local jar="${dest}/${artifact}-${version}.jar"
  mkdir -p "$dest"
  if [ ! -s "$jar" ]; then
    curl -fsSL "$url" -o "$jar" || die "Unable to download $url"
  fi
  local expected actual
  expected=$(curl -fsSL "${url}.sha1" | awk '{print $1}') || die "Unable to download ${url}.sha1"
  actual=$(shasum -a 1 "$jar" | awk '{print $1}')
  [ "$expected" == "$actual" ] || die "sha1 mismatch for $jar (expected $expected, got $actual)"
  echo "$jar"
}

# Fails unless SPARK_HOME points at a Spark distribution whose major.minor matches $1 (e.g. 3.5).
check_spark_home() {
  local want=$1
  [ -n "${SPARK_HOME:-}" ] && [ -x "$SPARK_HOME/bin/spark-submit" ] \
    || die "SPARK_HOME is not set to a Spark distribution (pass --spark_home)."
  local jar
  jar=$(ls "$SPARK_HOME"/jars/spark-core_*.jar 2>/dev/null | head -1)
  [ -n "$jar" ] || die "Cannot find spark-core jar under $SPARK_HOME/jars"
  local have
  have=$(basename "$jar" | sed -E 's/spark-core_[0-9.]+-([0-9]+\.[0-9]+)\..*/\1/')
  [ "$have" == "$want" ] || die "SPARK_HOME is Spark $have but the bundle targets Spark $want"
}

# Common spark-shell/spark-submit configs for Hudi.
hudi_spark_confs() {
  echo "--conf spark.serializer=org.apache.spark.serializer.KryoSerializer" \
       "--conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension" \
       "--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog" \
       "--conf spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar" \
       "--conf spark.ui.enabled=false"
}

# Runs a scala script through spark-shell. stdin is /dev/null so that a script which throws
# before reaching System.exit terminates instead of dropping into an interactive REPL. A script
# that fails to compile or throws still makes spark-shell exit 0 at EOF, so treat any REPL
# "error:" or reaching the prompt as a failure; the scripts themselves always System.exit.
run_spark_script() {
  local script=$1 log_file=$2
  shift 2
  # shellcheck disable=SC2046
  "$SPARK_HOME/bin/spark-shell" $(hudi_spark_confs) "$@" -i "$script" < /dev/null > "$log_file" 2>&1 || return 1
  ! grep -qE '^scala> |\.scala:[0-9]+: error:' "$log_file"
}
