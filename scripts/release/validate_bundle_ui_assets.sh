#!/bin/bash

#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

#
# Guards the "public/**" shade exclusions added for the Hudi Timeline UI (RFC-94).
# The Timeline UI assets (vis-timeline, Bootstrap, renderjson) must ship ONLY in
# hudi-timeline-server-bundle. The five engine bundles exclude "public/**" so that
# the bundled-library LICENSE obligations stay scoped to the server bundle. RFC-94
# calls this negative assertion load-bearing: without it the exclusion can regress
# silently and an engine bundle would ship UI assets whose licenses are not carried
# there.
#
# Usage: validate_bundle_ui_assets.sh [dir]
#   dir  Optional directory to search for built bundle jars.
#        Defaults to the repo "packaging" directory (scans packaging/*/target/).
#
# CI: invoked by the validate-bundles* jobs in .github/workflows/bot.yml right after the bundle build.
#

set -u

# Resolve the search root.
scriptDir="$(cd "$(dirname "$0")" && pwd)"
repoRoot="$(cd "$scriptDir/../.." && pwd)"
searchDir="${1:-$repoRoot/packaging}"

if [ ! -d "$searchDir" ]; then
  echo "Search directory does not exist: $searchDir [ERROR]"
  exit 1
fi

# List the entries inside a jar. Prefer "jar", fall back to "unzip -l".
listJar() {
  local jar="$1"
  if command -v jar >/dev/null 2>&1; then
    jar tf "$jar"
  elif command -v unzip >/dev/null 2>&1; then
    unzip -l "$jar" | awk 'NR>3 {print $4}'
  else
    echo "__NO_TOOL__"
  fi
}

# Find bundle jars matching a name pattern, skipping the unshaded original-*.jar
# and the sources/javadoc/tests attachments.
findBundleJars() {
  local pattern="$1"
  find "$searchDir" -type f -name "$pattern" \
    ! -name 'original-*.jar' \
    ! -name '*-sources.jar' \
    ! -name '*-javadoc.jar' \
    ! -name '*-tests.jar' 2>/dev/null
}

violations=0

# --- Positive assertion: the server bundle MUST carry the UI assets ---
serverPattern='hudi-timeline-server-bundle-*.jar'
serverJars=$(findBundleJars "$serverPattern")
if [ -z "$serverJars" ]; then
  echo "WARNING: no $serverPattern found under $searchDir; the positive assertion did not run"
else
  while IFS= read -r jar; do
    [ -z "$jar" ] && continue
    entries=$(listJar "$jar")
    if [ "$entries" = "__NO_TOOL__" ]; then
      echo "Neither 'jar' nor 'unzip' is available to inspect jars [ERROR]"
      exit 1
    fi
    hasIndex=1
    hasLib=1
    echo "$entries" | grep -q '^public/index.html$' || hasIndex=0
    echo "$entries" | grep -q '^public/lib/' || hasLib=0
    if [ "$hasIndex" -eq 0 ]; then
      echo "MISSING public/index.html in $jar [ERROR]"
      violations=$((violations + 1))
    fi
    if [ "$hasLib" -eq 0 ]; then
      echo "MISSING public/lib/ entries in $jar [ERROR]"
      violations=$((violations + 1))
    fi
    if [ "$hasIndex" -eq 1 ] && [ "$hasLib" -eq 1 ]; then
      echo "OK: $jar carries the Timeline UI assets"
    fi
  done <<EOF
$serverJars
EOF
fi

# --- Negative assertion: engine bundles must NOT carry any UI assets ---
# One pattern per bundle so it stays obvious which bundle maps to which expectation.
# Spark and utilities jar names carry a scala suffix (hudi-spark3.5-bundle_2.12-*.jar,
# hudi-utilities-bundle_2.12-*.jar), so their patterns must not assume "-bundle-".
enginePatterns=(
  'hudi-spark*-bundle*.jar'
  'hudi-flink*-bundle-*.jar'
  'hudi-utilities-bundle_*.jar'
  'hudi-kafka-connect-bundle-*.jar'
  'hudi-integ-test-bundle-*.jar'
)

for pattern in "${enginePatterns[@]}"; do
  jars=$(findBundleJars "$pattern")
  if [ -z "$jars" ]; then
    echo "SKIP: no $pattern found under $searchDir (not built)"
    continue
  fi
  while IFS= read -r jar; do
    [ -z "$jar" ] && continue
    entries=$(listJar "$jar")
    if [ "$entries" = "__NO_TOOL__" ]; then
      echo "Neither 'jar' nor 'unzip' is available to inspect jars [ERROR]"
      exit 1
    fi
    offending=$(echo "$entries" | grep '^public/')
    if [ -n "$offending" ]; then
      echo "FORBIDDEN public/ entries found in $jar [ERROR]:"
      echo "$offending" | sed 's/^/    /'
      violations=$((violations + 1))
    else
      echo "OK: $jar carries no public/ assets"
    fi
  done <<EOF
$jars
EOF
done

if [ "$violations" -ne 0 ]; then
  echo ""
  echo "Bundle UI asset validation FAILED with $violations violation(s) [ERROR]"
  exit 1
fi

echo ""
echo "Bundle UI asset validation passed [OK]"
exit 0
