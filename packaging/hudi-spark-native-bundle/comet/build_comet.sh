#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Builds Apache DataFusion Comet from the commit pinned in COMET_PIN, with the
# Hudi overlay applied, and installs the artifact into the local Maven
# repository as COMET_VERSION. The overlay holds full copies of every changed
# or added Comet file; OVERLAY_MANIFEST records the upstream blob SHA of each
# changed file so a pin bump that touches an overlaid file fails loudly here
# instead of silently discarding the upstream change.
#
# Requirements: git, a Rust toolchain (rustup honors Comet's
# rust-toolchain.toml), and JAVA_HOME pointing at JDK 17.
#
#   COMET_BUILD_DIR   checkout/build location
#                     (default: <this dir>/target/comet-build; cached in CI)
#   COMET_SPARK_PROFILE  Comet Maven profile (default: spark-3.5)
#   COMET_MAKE_TARGET    Comet make target (default: release, which compiles
#                     for the build machine's CPU; use Comet's per-platform
#                     release targets for portable artifacts)

set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source <(grep -E '^(COMET_REPO|COMET_COMMIT|COMET_VERSION)=' "$DIR/COMET_PIN")

BUILD_DIR="${COMET_BUILD_DIR:-$DIR/target/comet-build}"
SPARK_PROFILE="${COMET_SPARK_PROFILE:-spark-3.5}"
MAKE_TARGET="${COMET_MAKE_TARGET:-release}"

echo "Building Comet $COMET_COMMIT as $COMET_VERSION in $BUILD_DIR"

if [ ! -d "$BUILD_DIR/.git" ]; then
  mkdir -p "$(dirname "$BUILD_DIR")"
  git clone --filter=blob:none "$COMET_REPO" "$BUILD_DIR"
fi
git -C "$BUILD_DIR" fetch --quiet origin "$COMET_COMMIT" 2>/dev/null || git -C "$BUILD_DIR" fetch --quiet origin
git -C "$BUILD_DIR" checkout --quiet --force "$COMET_COMMIT"
git -C "$BUILD_DIR" clean -qfd

failed=0
while read -r sha path; do
  case "$sha" in ''|'#'*) continue ;; esac
  if [ "$sha" = "new" ]; then
    if git -C "$BUILD_DIR" rev-parse -q --verify "$COMET_COMMIT:$path" > /dev/null 2>&1; then
      echo "ERROR: $path is marked new in OVERLAY_MANIFEST but exists upstream at $COMET_COMMIT" >&2
      failed=1
    fi
  else
    actual="$(git -C "$BUILD_DIR" rev-parse -q --verify "$COMET_COMMIT:$path" 2>/dev/null || true)"
    if [ "$actual" != "$sha" ]; then
      echo "ERROR: upstream $path changed (expected blob $sha, found ${actual:-missing})." >&2
      echo "       Re-merge the overlay copy onto the new upstream content and update OVERLAY_MANIFEST." >&2
      failed=1
    fi
  fi
done < "$DIR/OVERLAY_MANIFEST"
if [ "$failed" -ne 0 ]; then
  exit 1
fi

(cd "$DIR/overlay" && find . -type f | while read -r f; do
  f="${f#./}"
  mkdir -p "$BUILD_DIR/$(dirname "$f")"
  cp "$f" "$BUILD_DIR/$f"
done)

cd "$BUILD_DIR"
./mvnw -q -B versions:set -DnewVersion="$COMET_VERSION" -DgenerateBackupPoms=false -P"$SPARK_PROFILE"
make "$MAKE_TARGET" PROFILES="-P$SPARK_PROFILE"

echo "Installed Comet $COMET_VERSION for $SPARK_PROFILE"
