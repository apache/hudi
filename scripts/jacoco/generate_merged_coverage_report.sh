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
# Generates a merged JaCoCo coverage report from all exec files in the workspace.
# Produces jacoco-report.xml suitable for upload to Codecov.
#
# Usage: ./scripts/jacoco/generate_merged_coverage_report.sh [workspace_dir]
#   workspace_dir: root of the source tree (defaults to current directory)

set -euo pipefail

WORKSPACE_DIR="${1:-.}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Skip if no exec files exist (e.g., tests were skipped for this matrix entry)
if ! find "$WORKSPACE_DIR" -path '*/jacoco-agent/*.exec' -type f | grep -q .; then
  echo "No JaCoCo exec files found — skipping coverage report"
  exit 0
fi

"$SCRIPT_DIR/download_jacoco.sh"
"$SCRIPT_DIR/merge_jacoco_exec_files.sh" jacoco-lib/lib/jacococli.jar "$WORKSPACE_DIR"
mv merged-jacoco.exec jacoco.exec

mkdir -p aggregate-classes aggregate-sources
find "$WORKSPACE_DIR" -path '*/target/classes' -type d | while read d; do cp -r "$d"/. aggregate-classes/ 2>/dev/null; done
find "$WORKSPACE_DIR" -path '*/src/main/java' -type d | while read d; do cp -r "$d"/. aggregate-sources/ 2>/dev/null; done
find "$WORKSPACE_DIR" -path '*/src/main/scala' -type d | while read d; do cp -r "$d"/. aggregate-sources/ 2>/dev/null; done

"$SCRIPT_DIR/generate_jacoco_coverage_report.sh" jacoco-lib/lib/jacococli.jar "$WORKSPACE_DIR"
