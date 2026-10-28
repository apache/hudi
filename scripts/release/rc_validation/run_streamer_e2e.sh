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

# Runs HoodieStreamer in continuous mode against a ParquetDFSSource with a FilebasedSchemaProvider
# while a generator keeps landing parquet batches, then validates commits, async compaction (MOR),
# cleaning and the resulting data.
#
# Usage: run_streamer_e2e.sh --spark_bundle=<jar> --utilities_bundle=<slim jar> --work_dir=<dir>
#          [--table_type=MERGE_ON_READ|COPY_ON_WRITE] [--scale=minimal|decent|large]

set -o errexit
set -o nounset

source "$(dirname "$0")/common.sh"

TABLE_TYPE=MERGE_ON_READ
SCALE=minimal
for param in "$@"; do
  case $param in
    --spark_bundle=*) SPARK_BUNDLE=${param#*=} ;;
    --utilities_bundle=*) UTILITIES_BUNDLE=${param#*=} ;;
    --work_dir=*) WORK_DIR=${param#*=} ;;
    --table_type=*) TABLE_TYPE=${param#*=} ;;
    --scale=*) SCALE=${param#*=} ;;
    *) die "Unknown argument: $param" ;;
  esac
done
[ -n "${SPARK_BUNDLE:-}" ] && [ -n "${UTILITIES_BUNDLE:-}" ] && [ -n "${WORK_DIR:-}" ] \
  || die "--spark_bundle, --utilities_bundle and --work_dir are required"

# batches, initial keys, updates/batch, inserts/batch, seconds between batches, min commits,
# max delta commits before compaction, commits retained by the cleaner, archival min/max commits
case $SCALE in
  minimal) BATCHES=12; INITIAL=1000;    UPDATES=300;    INSERTS=200;    INTERVAL=15; MIN_COMMITS=10
           COMPACT_EVERY=3; CLEAN_RETAINED=2; ARCHIVE_MIN=4; ARCHIVE_MAX=6 ;;
  decent)  BATCHES=24; INITIAL=50000;   UPDATES=20000;  INSERTS=10000;  INTERVAL=20; MIN_COMMITS=15
           COMPACT_EVERY=4; CLEAN_RETAINED=3; ARCHIVE_MIN=5; ARCHIVE_MAX=8 ;;
  large)   BATCHES=60; INITIAL=1000000; UPDATES=300000; INSERTS=100000; INTERVAL=30; MIN_COMMITS=25
           COMPACT_EVERY=5; CLEAN_RETAINED=4; ARCHIVE_MIN=6; ARCHIVE_MAX=10 ;;
  *) die "Unknown scale: $SCALE" ;;
esac

DIR="$WORK_DIR/streamer_e2e_$(echo "$TABLE_TYPE" | tr '[:upper:]' '[:lower:]')"
rm -rf "$DIR"
mkdir -p "$DIR/source" "$DIR/staging"
cp "$RC_VALIDATION_DIR/trips.avsc" "$DIR/"

# hoodie.parquet.small.file.limit=0: on a small table every base file counts as a small file, so new
# inserts get bin-packed into existing file groups and every MOR commit rewrites base files instead of
# appending log files -- compaction would then never have anything to do.
cat > "$DIR/streamer.properties" <<EOF
hoodie.datasource.write.recordkey.field=uuid
hoodie.datasource.write.partitionpath.field=city
hoodie.datasource.write.keygenerator.class=org.apache.hudi.keygen.SimpleKeyGenerator
hoodie.streamer.source.dfs.root=file://$DIR/source
hoodie.streamer.schemaprovider.source.schema.file=file://$DIR/trips.avsc
hoodie.streamer.schemaprovider.target.schema.file=file://$DIR/trips.avsc
hoodie.parquet.small.file.limit=0
hoodie.compact.inline.max.delta.commits=$COMPACT_EVERY
hoodie.cleaner.policy=KEEP_LATEST_COMMITS
hoodie.clean.commits.retained=$CLEAN_RETAINED
hoodie.keep.min.commits=$ARCHIVE_MIN
hoodie.keep.max.commits=$ARCHIVE_MAX
hoodie.upsert.shuffle.parallelism=4
hoodie.insert.shuffle.parallelism=4
EOF

log "Streamer e2e: $TABLE_TYPE, scale=$SCALE, $BATCHES batches every ${INTERVAL}s, working dir $DIR"
# shellcheck disable=SC2046
"$SPARK_HOME/bin/spark-submit" --master 'local[4]' --driver-memory 4g \
  --class org.apache.hudi.utilities.streamer.HoodieStreamer --jars "$SPARK_BUNDLE" \
  $(hudi_spark_confs) "$UTILITIES_BUNDLE" \
  --table-type "$TABLE_TYPE" --op UPSERT \
  --source-class org.apache.hudi.utilities.sources.ParquetDFSSource \
  --schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider \
  --source-ordering-field ts --target-base-path "file://$DIR/table" --target-table rc_e2e_trips \
  --props "file://$DIR/streamer.properties" --continuous --min-sync-interval-seconds 10 \
  > "$DIR/streamer.log" 2>&1 &
STREAMER_PID=$!
trap 'kill $STREAMER_PID 2>/dev/null || true' EXIT

run_spark_script "$RC_VALIDATION_DIR/streamer_e2e_datagen.scala" "$DIR/datagen.log" --master 'local[2]' --jars "$SPARK_BUNDLE" \
  --conf spark.hudi.rc.e2e.dir="$DIR" --conf spark.hudi.rc.e2e.batches=$BATCHES \
  --conf spark.hudi.rc.e2e.initial=$INITIAL --conf spark.hudi.rc.e2e.updates=$UPDATES \
  --conf spark.hudi.rc.e2e.inserts=$INSERTS --conf spark.hudi.rc.e2e.interval.seconds=$INTERVAL \
  || die "Data generator failed, see $DIR/datagen.log"
EXPECTED_TOTAL=$(cat "$DIR/expected_total")
log "Generator done ($EXPECTED_TOTAL keys); waiting for the streamer to drain the source"

# Drain: the source is fully consumed once a sync that started after the generator finished reports no
# new data. The first such report may come from a sync already running when the last batch landed, so
# wait for two. Then leave time for an in-flight async compaction to commit. Give up after 15 minutes.
LOG_MARK=$(wc -l < "$DIR/streamer.log" | tr -d ' ')
DEADLINE=$(($(date +%s) + 900))
until [ "$(tail -n +"$LOG_MARK" "$DIR/streamer.log" | grep -c "No new data, source checkpoint has not changed")" -ge 2 ]; do
  kill -0 $STREAMER_PID 2>/dev/null || die "HoodieStreamer exited early, see $DIR/streamer.log"
  [ "$(date +%s)" -lt $DEADLINE ] || { log "Drain timeout reached"; break; }
  sleep 10
done
sleep 60
kill $STREAMER_PID 2>/dev/null || true
wait $STREAMER_PID 2>/dev/null || true
trap - EXIT

FAILED=0
report() {
  local status=$1 name=$2 detail=$3
  echo "E2E $status $name ($detail)"
  [ "$status" == "PASS" ] || FAILED=1
}

COMMITS=$(grep -oE 'totalRecords=[0-9]+, totalErrorRecords=[0-9]+' "$DIR/streamer.log" | grep -vc '^totalRecords=0,' || true)
ERROR_RECORDS=$(grep -oE 'totalErrorRecords=[0-9]+' "$DIR/streamer.log" | awk -F= '{s+=$2} END {print s+0}')
ERROR_LINES=$(grep -c ' ERROR ' "$DIR/streamer.log" || true)
[ "$COMMITS" -ge $MIN_COMMITS ] && report PASS "streamer commits" "commits=$COMMITS min=$MIN_COMMITS" \
  || report FAIL "streamer commits" "commits=$COMMITS min=$MIN_COMMITS"
[ "$ERROR_RECORDS" -eq 0 ] && report PASS "no error records" "errorRecords=0" \
  || report FAIL "no error records" "errorRecords=$ERROR_RECORDS"
[ "$ERROR_LINES" -eq 0 ] && report PASS "no ERROR log lines" "errorLines=0" \
  || report FAIL "no ERROR log lines" "errorLines=$ERROR_LINES, see $DIR/streamer.log"

run_spark_script "$RC_VALIDATION_DIR/streamer_e2e_validate.scala" "$DIR/validate.log" \
  --master 'local[4]' --driver-memory 4g --jars "$SPARK_BUNDLE" \
  --conf spark.sql.warehouse.dir="$DIR/warehouse" \
  --conf spark.hudi.rc.e2e.table.path="file://$DIR/table" --conf spark.hudi.rc.e2e.table.type="$TABLE_TYPE" \
  --conf spark.hudi.rc.e2e.expected.total="$EXPECTED_TOTAL" --conf spark.hudi.rc.e2e.batches=$BATCHES || FAILED=1
grep -oE 'E2E (PASS|FAIL) .*' "$DIR/validate.log" || { echo "E2E FAIL validation (no output, see $DIR/validate.log)"; FAILED=1; }

exit $FAILED
