#!/usr/bin/env bash
#
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
#

# Run every demo combination sequentially.
# Activate the venv first, then: ./run_demos.sh
#
# Set SKIP_LANCE=1 to skip lance combos (e.g. if LANCE_BUNDLE_JAR isn't set).

set -euo pipefail
cd "$(dirname "$0")"

# Wipe leftover state before each run.
# `DROP TABLE IF EXISTS` only removes the catalog entry — the data dir and
# `.hoodie/` timeline at LOCATION persist, so old commits get queried alongside
# new ones on re-run. The blob container file at /tmp/pets_blob_container.bin
# is a single shared path that different blob_modes overwrite; stale tables
# then end up with offsets past EOF, causing file boundary errors during read.
clean() {
  rm -rf /tmp/hudi_*_pets \
         /tmp/pets_blob_container.bin \
         /tmp/staging_pets_*.parquet \
         spark-warehouse
}

run() {
  local label="$1"; shift
  clean
  echo
  echo "============================================================"
  echo ">>> $label"
  echo "============================================================"
  env "$@"
}

# blob reader: format x {out_of_line, inline+content, inline+descriptor}
run "blob reader / parquet / out_of_line"          HUDI_BASE_FILE_FORMAT=parquet HUDI_BLOB_MODE=out_of_line                                python hudi_blob_reader_demo.py
run "blob reader / parquet / inline + content"     HUDI_BASE_FILE_FORMAT=parquet HUDI_BLOB_MODE=inline      HUDI_INLINE_READ_MODE=content    python hudi_blob_reader_demo.py
run "blob reader / parquet / inline + descriptor"  HUDI_BASE_FILE_FORMAT=parquet HUDI_BLOB_MODE=inline      HUDI_INLINE_READ_MODE=descriptor python hudi_blob_reader_demo.py

if [[ "${SKIP_LANCE:-0}" != "1" ]]; then
  run "blob reader / lance / out_of_line"          HUDI_BASE_FILE_FORMAT=lance   HUDI_BLOB_MODE=out_of_line                                python hudi_blob_reader_demo.py
  run "blob reader / lance / inline + content"     HUDI_BASE_FILE_FORMAT=lance   HUDI_BLOB_MODE=inline      HUDI_INLINE_READ_MODE=content    python hudi_blob_reader_demo.py
  run "blob reader / lance / inline + descriptor"  HUDI_BASE_FILE_FORMAT=lance   HUDI_BLOB_MODE=inline      HUDI_INLINE_READ_MODE=descriptor python hudi_blob_reader_demo.py
fi

# sql demo: format only
run "sql / parquet"                                HUDI_BASE_FILE_FORMAT=parquet python hudi_sql_vector_blob_demo.py
[[ "${SKIP_LANCE:-0}" != "1" ]] && \
run "sql / lance"                                  HUDI_BASE_FILE_FORMAT=lance   python hudi_sql_vector_blob_demo.py

# dataframe demo: format only
run "dataframe / parquet"                          HUDI_BASE_FILE_FORMAT=parquet python hudi_dataframe_vector_blob_demo.py
[[ "${SKIP_LANCE:-0}" != "1" ]] && \
run "dataframe / lance"                            HUDI_BASE_FILE_FORMAT=lance   python hudi_dataframe_vector_blob_demo.py

echo
echo "All combinations finished."
