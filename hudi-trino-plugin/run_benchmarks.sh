#!/usr/bin/env bash
# =============================================================================
# Hudi Column Stats Index Benchmarking Script
#
# 1. Bootstraps a synthetic Hudi table via MetadataBenchmarkingTool (Spark)
# 2. Benchmarks the Trino query path via TrinoBenchmarkToolV2
#
# Usage: ./run_benchmarks.sh
#
# Edit the CONFIG section below before running.
# =============================================================================

set -euo pipefail

# =============================================================================
# CONFIG — edit these before running
# =============================================================================

# Root of the hudi repo (auto-detected; override if needed)
HUDI_REPO="$(cd "$(dirname "$0")/.." && pwd)"

# Local path where the synthetic Hudi table will be written (bootstrap) or read from (skip bootstrap).
TABLE_BASE_PATH="/tmp/hudi_bench_table_500K"

# Total stub parquet files to create across all partitions.
NUM_FILES=500000

# Number of columns to index in the column stats metadata partition.
# 1 = tenantID only; 2 = tenantID + age. More columns → larger MDT, slower index build.
NUM_COLS_TO_INDEX=1

# Range of tenantID values distributed across files (tenantID min = 30000, max = 30000 + TENANT_ID_RANGE).
# Wider range = more files survive the tenantID filter (lower pruning efficiency).
TENANT_ID_RANGE=3000000

# Simulated processing time per file-slice split in TrinoBenchmarkToolV2 (milliseconds).
# Models the real cost of opening a Parquet footer per split. Higher = amplifies the speedup signal.
FILE_SLICE_PROCESSING_MS=10

# Trino session property: max time to wait for the column stats index lookup before falling back
# to scanning all splits. Increase if the MDT read is timing out on large tables.
COL_STATS_TIMEOUT="2s"

# ----- Query filters (used by TrinoBenchmarkToolV2) -----
# Data filter applied to a non-partition column to exercise column-stats pruning.
# Passed as col/op/val to avoid single-quote stripping by Maven exec.args parsing.
# op: EQ | GT | GTE | LT | LTE | RANGE  (RANGE: DATA_FILTER_VAL = "lo,hi")
DATA_FILTER_COL="tenantID"
DATA_FILTER_OP="EQ"
DATA_FILTER_VAL="35000"

# Partition pruning filter applied to the "dt" partition column (format: YYYY-MM-DD).
# Passed as separate start/end args — Java builds the quoted SQL predicate internally.
# Leave PARTITION_START empty to run without a partition filter.
PARTITION_START="2025-01-01"
PARTITION_END="2025-01-31"

# Extra Hoodie config passed to MetadataBenchmarkingTool via --hoodie-conf.
# Format: key=value. Repeat the variable (and the --hoodie-conf flag) for multiple configs.
HOODIE_CONF="hoodie.metadata.file.cache.max.size.mb=200"

# Set to true to skip the Spark bootstrap step and reuse the table already at TABLE_BASE_PATH.
SKIP_BOOTSTRAP=false

# ----- Bootstrap settings (MetadataBenchmarkingTool / Spark) -----

# Number of date partitions to create (one per day starting 2025-01-01).
# More partitions = finer partition pruning granularity but larger MDT FILES partition.
NUM_PARTITIONS=365

# Number of file groups for the column_stats metadata partition.
# More file groups = more parallelism when reading MDT, but higher metadata overhead.
COL_STATS_FG_COUNT=1

# Path to a local Spark installation used to run MetadataBenchmarkingTool.
SPARK_HOME="$HOME/Applications/spark-3.5.3-bin-hadoop3/"
SPARK_MASTER="local[*]"

# ----- Trino benchmark settings (TrinoBenchmarkToolV2) -----

# Warm-up query runs before measurement (results discarded). Lets the JIT and caches stabilise.
WARMUP_RUNS=1

# Number of timed query executions per scenario (col-stats ON and col-stats OFF).
MEASUREMENT_RUNS=5

# =============================================================================
# Helpers
# =============================================================================

log() { echo "[$(date '+%H:%M:%S')] $*"; }
die() { echo "ERROR: $*" >&2; exit 1; }

# =============================================================================
# Prerequisite checks
# =============================================================================

# Java 23
JAVA_HOME_23="$(/usr/libexec/java_home -v 23 2>/dev/null || true)"
[[ -n "$JAVA_HOME_23" ]] || die "Java 23 not found. Install it or set JAVA_HOME_23 manually."
log "Java 23 found at: $JAVA_HOME_23"

# Spark
[[ -n "$SPARK_HOME" ]] || die "SPARK_HOME is not set. Please set it in the CONFIG section."
SPARK_SUBMIT="$SPARK_HOME/bin/spark-submit"
[[ -x "$SPARK_SUBMIT" ]] || die "spark-submit not found at $SPARK_SUBMIT"
log "Spark found at: $SPARK_HOME"

# =============================================================================
# Resolve artifact paths dynamically
# =============================================================================

TRINO_PLUGIN_DIR="$HUDI_REPO/hudi-trino-plugin"

HUDI_VERSION=$(grep -m1 "^  <version>" "$HUDI_REPO/pom.xml" \
    | sed 's/.*<version>\(.*\)<\/version>/\1/' | tr -d ' ')
[[ -n "$HUDI_VERSION" ]] || die "Could not determine Hudi version from $HUDI_REPO/pom.xml"
log "Hudi version: $HUDI_VERSION"

SCALA_VERSION=$(grep -m1 "scala.version\|scala-2\." "$HUDI_REPO/pom.xml" \
    | grep -oE '2\.[0-9]+' | head -1 || true)
SCALA_VERSION="${SCALA_VERSION:-2.12}"
log "Scala version: $SCALA_VERSION"

BUNDLE_JAR="$HUDI_REPO/packaging/hudi-utilities-bundle/target/hudi-utilities-bundle_${SCALA_VERSION}-${HUDI_VERSION}.jar"

# =============================================================================
# Step 1 — Build hudi if jars are missing
# =============================================================================

if [[ ! -f "$BUNDLE_JAR" ]]; then
    log "hudi-utilities-bundle jar not found at $BUNDLE_JAR — building (this may take a few minutes)..."
    (cd "$HUDI_REPO" && mvn clean install -DskipTests -Dspark3.5 -Dscala-2.12 \
        -pl packaging/hudi-utilities-bundle -am -q)
    [[ -f "$BUNDLE_JAR" ]] || die "Build succeeded but jar still not found at $BUNDLE_JAR"
    log "Build complete."
else
    log "hudi-utilities-bundle jar found — skipping build."
fi

# =============================================================================
# Step 2 — Bootstrap table via MetadataBenchmarkingTool
# =============================================================================

if [[ "$SKIP_BOOTSTRAP" == "true" ]]; then
    log "SKIP_BOOTSTRAP=true — using existing table at $TABLE_BASE_PATH"
else
    log "==================================================================="
    log " BOOTSTRAP — creating table at $TABLE_BASE_PATH"
    log "   files=$NUM_FILES  partitions=$NUM_PARTITIONS  cols=$NUM_COLS_TO_INDEX  fg=$COL_STATS_FG_COUNT"
    log "==================================================================="

    # Reference:
    # spark-submit --class org.apache.hudi.utilities.benchmarking.MetadataBenchmarkingTool <bundle.jar> \
    #   --mode BOOTSTRAP --table-base-path <path> --num-files-to-bootstrap <n> \
    #   --num-partitions <n> --num-cols-to-index <n> --col-stats-file-group-count <n>
    "$SPARK_SUBMIT" \
        --class org.apache.hudi.utilities.benchmarking.MetadataBenchmarkingTool \
        --master "$SPARK_MASTER" \
        "$BUNDLE_JAR" \
        --mode BOOTSTRAP \
        --table-base-path "$TABLE_BASE_PATH" \
        --num-files-to-bootstrap "$NUM_FILES" \
        --num-partitions "$NUM_PARTITIONS" \
        --num-cols-to-index "$NUM_COLS_TO_INDEX" \
        --col-stats-file-group-count "$COL_STATS_FG_COUNT" \
        --tenant-id-range "$TENANT_ID_RANGE" \
        --hoodie-conf "$HOODIE_CONF"

    log "Bootstrap complete."
fi

# =============================================================================
# Step 3 — Benchmark Trino query path via TrinoBenchmarkToolV2
# =============================================================================

log "==================================================================="
log " TRINO QUERY BENCHMARK — table=$TABLE_BASE_PATH"
log "   data-filter=$DATA_FILTER_COL:$DATA_FILTER_OP:$DATA_FILTER_VAL  partition=$PARTITION_START..$PARTITION_END  tenant-id-range=$TENANT_ID_RANGE"
log "   timeout=$COL_STATS_TIMEOUT  warmup=$WARMUP_RUNS  runs=$MEASUREMENT_RUNS"
log "   file-slice-processing-ms=$FILE_SLICE_PROCESSING_MS"
log "==================================================================="

V2_EXEC_ARGS="--table-base-path $TABLE_BASE_PATH"
[[ -n "$DATA_FILTER_COL" ]] && V2_EXEC_ARGS="$V2_EXEC_ARGS --data-filter-col $DATA_FILTER_COL --data-filter-op $DATA_FILTER_OP --data-filter-val $DATA_FILTER_VAL"
[[ -n "$PARTITION_START" ]] && V2_EXEC_ARGS="$V2_EXEC_ARGS --partition-start $PARTITION_START --partition-end $PARTITION_END"
V2_EXEC_ARGS="$V2_EXEC_ARGS --col-stats-timeout $COL_STATS_TIMEOUT"
V2_EXEC_ARGS="$V2_EXEC_ARGS --warmup-runs $WARMUP_RUNS"
V2_EXEC_ARGS="$V2_EXEC_ARGS --measurement-runs $MEASUREMENT_RUNS"
V2_EXEC_ARGS="$V2_EXEC_ARGS --file-slice-processing-ms $FILE_SLICE_PROCESSING_MS"

log "Compiling test classes..."
(cd "$TRINO_PLUGIN_DIR" && JAVA_HOME="$JAVA_HOME_23" ./mvnw test-compile -q -Dcheckstyle.skip=true)

(cd "$TRINO_PLUGIN_DIR" && \
    JAVA_HOME="$JAVA_HOME_23" ./mvnw exec:exec \
        -Dexec.executable="$JAVA_HOME_23/bin/java" \
        -Dexec.classpathScope="test" \
        -Dexec.args="-classpath %classpath io.trino.plugin.hudi.benchmarking.TrinoBenchmarkToolV2 $V2_EXEC_ARGS" \
        -e)

log "Done."
