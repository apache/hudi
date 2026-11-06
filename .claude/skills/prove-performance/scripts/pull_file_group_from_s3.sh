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
# Download a Hudi file group (base file + its delta log files) from S3 to a local directory, for
# real-data performance analysis. All files of one file group in a partition directory share the
# same file id (e.g. record-index-0000-0); delta log files are hidden (leading dot). This matches
# both by file id so you get the base HFile/parquet AND the .log.* files.
#
# Usage:
#   pull_file_group_from_s3.sh <s3-partition-uri> <file-id> [dest-dir]
#
# Example (metadata table record_index partition):
#   pull_file_group_from_s3.sh \
#     s3://my-bucket/my-table/.hoodie/metadata/record_index/ \
#     record-index-0000-0  ./rli-fg
#
# Then analyze locally, e.g. with a gated real-data test of your own:
#   run_perf_test.sh hudi-hadoop-common TestMyRealFileGroup \
#     -Dmy.real.fg.dir="$PWD/rli-fg/<file-id-dir>"
#
# Requires: awscli configured with read access to the bucket.
set -euo pipefail

S3_PARTITION="${1:?s3 partition uri, e.g. s3://bucket/table/.hoodie/metadata/record_index/}"
FILE_ID="${2:?file id, e.g. record-index-0000-0}"
DEST="${3:-./$FILE_ID}"

mkdir -p "$DEST"
echo "Pulling file group '$FILE_ID' from ${S3_PARTITION%/}/ -> $DEST"
# --recursive + include-by-file-id pulls the base and every delta log for this file group.
aws s3 cp "${S3_PARTITION%/}/" "$DEST/" --recursive \
  --exclude "*" --include "*${FILE_ID}*"

echo
echo "Downloaded:"
ls -la "$DEST"
echo
echo "Total size:"
du -sh "$DEST"
