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

# Root of the hudi repo
HUDI_REPO="$(cd "$(dirname "$0")/.." && pwd)"

# Where to write/read the synthetic Hudi table
TABLE_BASE_PATH="/tmp/hudi_bench_table"

NUM_FILES=10000                   # Total data files across all partitions
NUM_COLS_TO_INDEX=2               # 1 = tenantID only; 2 = tenantID + age
V2_FILE_SLICE_PROCESSING_MS=10    # simulated time (ms) spent processing each file slice
V2_COL_STATS_TIMEOUT="2s"         # column_stats_wait_timeout session property
V2_PARTITION_FILTER=true          # true = restrict query to datePartition = '2025-01-01'

SKIP_BOOTSTRAP=false              # Set to true to skip bootstrap and reuse an existing table

# --- Bootstrap (MetadataBenchmarkingTool) ---
NUM_PARTITIONS=3                  # Date partitions (2025-01-01, 2025-01-02, …)
COL_STATS_FG_COUNT=200            # File groups for the column_stats metadata partition
SPARK_HOME="$HOME/Applications/spark-3.5.3-bin-hadoop3/"  # Path to Spark installation
SPARK_MASTER="local[*]"

# --- Trino query benchmarks (TrinoBenchmarkToolV2) ---
V2_FILTERS="--filter tenantID:RANGE:40000:50000"  # col:RANGE:lo:hi | col:GT/GTE/LT/LTE/EQ:val
V2_WARMUP_RUNS=2
V2_MEASUREMENT_RUNS=5

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

    "$SPARK_SUBMIT" \
        --class org.apache.hudi.utilities.benchmarking.MetadataBenchmarkingTool \
        --master "$SPARK_MASTER" \
        "$BUNDLE_JAR" \
        --mode BOOTSTRAP \
        --table-base-path "$TABLE_BASE_PATH" \
        --num-files "$NUM_FILES" \
        --num-partitions "$NUM_PARTITIONS" \
        --num-cols-to-index "$NUM_COLS_TO_INDEX" \
        --col-stats-file-group-count "$COL_STATS_FG_COUNT"

    log "Bootstrap complete."
fi

# =============================================================================
# Step 3 — Benchmark Trino query path via TrinoBenchmarkToolV2
# =============================================================================

log "==================================================================="
log " TRINO QUERY BENCHMARK — table=$TABLE_BASE_PATH"
log "   filters=$V2_FILTERS  timeout=$V2_COL_STATS_TIMEOUT"
log "   warmup=$V2_WARMUP_RUNS  runs=$V2_MEASUREMENT_RUNS"
log "   partition-filter=$V2_PARTITION_FILTER  file-slice-processing-ms=$V2_FILE_SLICE_PROCESSING_MS"
log "==================================================================="

V2_EXEC_ARGS="--table-base-path $TABLE_BASE_PATH"
V2_EXEC_ARGS="$V2_EXEC_ARGS $V2_FILTERS"
V2_EXEC_ARGS="$V2_EXEC_ARGS --col-stats-timeout $V2_COL_STATS_TIMEOUT"
V2_EXEC_ARGS="$V2_EXEC_ARGS --warmup-runs $V2_WARMUP_RUNS"
V2_EXEC_ARGS="$V2_EXEC_ARGS --measurement-runs $V2_MEASUREMENT_RUNS"
V2_EXEC_ARGS="$V2_EXEC_ARGS --file-slice-processing-ms $V2_FILE_SLICE_PROCESSING_MS"
[[ "$V2_PARTITION_FILTER" == "true" ]] && V2_EXEC_ARGS="$V2_EXEC_ARGS --partition-filter"

log "Compiling test classes..."
(cd "$TRINO_PLUGIN_DIR" && JAVA_HOME="$JAVA_HOME_23" ./mvnw test-compile -q -Dcheckstyle.skip=true)

(cd "$TRINO_PLUGIN_DIR" && \
    JAVA_HOME="$JAVA_HOME_23" ./mvnw exec:exec \
        -Dexec.executable="$JAVA_HOME_23/bin/java" \
        -Dexec.classpathScope="test" \
        -Dexec.args="-classpath %classpath io.trino.plugin.hudi.benchmarking.TrinoBenchmarkToolV2 $V2_EXEC_ARGS" \
        -e)

log "Done."
