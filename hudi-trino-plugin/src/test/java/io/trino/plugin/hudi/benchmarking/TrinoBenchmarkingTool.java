/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.hudi.benchmarking;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.hudi.HudiConfig;
import io.trino.plugin.hudi.HudiSessionProperties;
import io.trino.plugin.hudi.query.index.HudiColumnStatsIndexSupport;
import io.trino.plugin.hudi.storage.TrinoStorageConfiguration;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.Type;
import io.trino.testing.TestingConnectorSession;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.util.Lazy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.plugin.hudi.HudiUtil.getFileSystemView;

/**
 * Benchmarks the Trino column stats index (HudiColumnStatsIndexSupport) for file slice filtering.
 *
 * <p>Data preparation: Run MetadataBenchmarkingTool with --mode BOOTSTRAP first to create the table.
 *
 * <p>Usage example:
 * <pre>
 *   java -cp ... io.trino.plugin.hudi.benchmarking.TrinoBenchmarkingTool \
 *     --table-base-path /path/to/table \
 *     --filter tenantID:RANGE:40000:50000 \
 *     --timeout-sweep-ms 100,500,1000,5000
 * </pre>
 */
public class TrinoBenchmarkingTool
{
    private static final Logger log = Logger.get(TrinoBenchmarkingTool.class);

    // Column name constants matching MetadataBenchmarkingTool
    private static final String COL_TENANT_ID = "tenantID";
    private static final String COL_AGE = "age";
    private static final String DEFAULT_TABLE_NAME = "test_mdt_stats_tbl";

    public static class Config
    {
        @Parameter(names = {"--table-base-path", "-tbp"}, description = "Base path for the existing Hudi table (bootstrapped via MetadataBenchmarkingTool)", required = true)
        public String tableBasePath;

        @Parameter(names = {"--schema-name", "-sn"}, description = "Schema name for SchemaTableName")
        public String schemaName = "default";

        @Parameter(names = {"--table-name", "-tn"}, description = "Table name for SchemaTableName")
        public String tableName = DEFAULT_TABLE_NAME;

        @Parameter(names = {"--filter", "-f"}, description = "Filter spec: col:RANGE:lo:hi or col:GT:val or col:LT:val or col:GTE:val or col:LTE:val or col:EQ:val. "
                + "Examples: 'tenantID:RANGE:40000:50000', 'age:GT:70'. Repeat for multiple filters.")
        public List<String> filters = ImmutableList.of("tenantID:RANGE:40000:50000");

        @Parameter(names = {"--col-stats-wait-timeout-ms", "-timeout"}, description = "Timeout in ms for HudiColumnStatsIndexSupport async stats loading")
        public long columnStatsWaitTimeoutMs = 1000;

        @Parameter(names = {"--timeout-sweep-ms"}, description = "Comma-separated list of timeouts to sweep (ms). Overrides --col-stats-wait-timeout-ms if set. "
                + "Demonstrates trade-off: higher timeout = more files filtered (better skipping) but higher latency. "
                + "Example: '100,500,1000,5000'")
        public String timeoutSweepMs = "";

        @Parameter(names = {"--warmup-runs", "-w"}, description = "Number of warm-up runs before measurement (discarded)")
        public int warmupRuns = 3;

        @Parameter(names = {"--measurement-runs", "-r"}, description = "Number of timed measurement runs")
        public int measurementRuns = 10;

        @Parameter(names = {"--baseline", "-b"}, description = "Also run with TupleDomain.all() (no filtering) as a baseline comparison")
        public boolean baseline = true;

        @Parameter(names = {"--help", "-h"}, help = true)
        public boolean help = false;

        public List<Long> getTimeoutsToRun()
        {
            if (timeoutSweepMs != null && !timeoutSweepMs.isEmpty()) {
                return Arrays.stream(timeoutSweepMs.split(","))
                        .map(String::trim)
                        .filter(s -> !s.isEmpty())
                        .map(Long::parseLong)
                        .collect(Collectors.toList());
            }
            return ImmutableList.of(columnStatsWaitTimeoutMs);
        }
    }

    /**
     * Holds results from a single benchmark run.
     */
    private static class BenchmarkMetrics
    {
        final long statsLoadAndFirstEvalNs;
        final long totalFileSlices;
        final long skippedFileSlices;
        final List<Long> perFileEvalTimesNs;
        final long heapUsedBeforeBytes;
        final long heapUsedAfterBytes;

        BenchmarkMetrics(long statsLoadAndFirstEvalNs, long totalFileSlices, long skippedFileSlices,
                List<Long> perFileEvalTimesNs, long heapUsedBeforeBytes, long heapUsedAfterBytes)
        {
            this.statsLoadAndFirstEvalNs = statsLoadAndFirstEvalNs;
            this.totalFileSlices = totalFileSlices;
            this.skippedFileSlices = skippedFileSlices;
            this.perFileEvalTimesNs = perFileEvalTimesNs;
            this.heapUsedBeforeBytes = heapUsedBeforeBytes;
            this.heapUsedAfterBytes = heapUsedAfterBytes;
        }

        double skippingRatio()
        {
            return totalFileSlices == 0 ? 0.0 : (double) skippedFileSlices / totalFileSlices * 100.0;
        }
    }

    private final Config cfg;

    public TrinoBenchmarkingTool(Config cfg)
    {
        this.cfg = cfg;
    }

    public static void main(String[] args) throws Exception
    {
        Config cfg = new Config();
        JCommander cmd = JCommander.newBuilder().addObject(cfg).build();
        cmd.parse(args);

        if (cfg.help || args.length == 0) {
            cmd.usage();
            System.exit(1);
        }

        new TrinoBenchmarkingTool(cfg).run();
    }

    public void run() throws Exception
    {
        log.info("=== Trino Column Stats Index Benchmarking Tool ===");
        log.info("Table: %s", cfg.tableBasePath);
        log.info("Filters: %s", cfg.filters);
        log.info("Warm-up runs: %d, Measurement runs: %d", cfg.warmupRuns, cfg.measurementRuns);

        // Load metaClient once (shared across all runs)
        TrinoStorageConfiguration storageConf = new TrinoStorageConfiguration();
        HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder()
                .setConf(storageConf)
                .setBasePath(cfg.tableBasePath)
                .build();

        // Load tableMetadata once (shared across all runs)
        HoodieEngineContext engineContext = new HoodieLocalEngineContext(metaClient.getStorage().getConf());
        HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder().enable(true).build();
        HoodieTableMetadata tableMetadata = HoodieTableMetadata.create(
                engineContext, metaClient.getStorage(), metadataConfig, cfg.tableBasePath, true);

        // Fetch all file slices once
        List<FileSlice> allSlices = fetchAllFileSlices(tableMetadata, metaClient);
        log.info("Total file slices across all partitions: %d", allSlices.size());

        if (allSlices.isEmpty()) {
            log.error("No file slices found. Ensure the table was bootstrapped via MetadataBenchmarkingTool --mode BOOTSTRAP.");
            return;
        }

        // Build TupleDomain predicate from filter config
        TupleDomain<String> predicate = buildPredicate(cfg.filters);

        SchemaTableName schemaTableName = new SchemaTableName(cfg.schemaName, cfg.tableName);

        // Capture final references for use in lambdas
        HoodieTableMetaClient finalMetaClient = metaClient;
        HoodieTableMetadata finalTableMetadata = tableMetadata;

        // Run baseline (no filtering) if requested
        if (cfg.baseline) {
            log.info("\n--- Baseline (no column stats filtering) ---");
            runBenchmark("baseline", cfg.warmupRuns, cfg.measurementRuns, TupleDomain.all(),
                    schemaTableName, allSlices,
                    () -> finalMetaClient, () -> finalTableMetadata, 0);
        }

        // Run benchmark for each timeout value
        for (long timeoutMs : cfg.getTimeoutsToRun()) {
            log.info("\n--- timeout=%dms ---", timeoutMs);
            runBenchmark("timeout=" + timeoutMs + "ms", cfg.warmupRuns, cfg.measurementRuns, predicate,
                    schemaTableName, allSlices,
                    () -> finalMetaClient, () -> finalTableMetadata, timeoutMs);
        }
    }

    private void runBenchmark(
            String label,
            int warmupRuns,
            int measurementRuns,
            TupleDomain<String> predicate,
            SchemaTableName schemaTableName,
            List<FileSlice> allSlices,
            java.util.function.Supplier<HoodieTableMetaClient> metaClientSupplier,
            java.util.function.Supplier<HoodieTableMetadata> tableMetadataSupplier,
            long timeoutMs)
    {
        ConnectorSession session = buildSession(timeoutMs);

        // Warm-up
        log.info("[%s] Running %d warm-up runs...", label, warmupRuns);
        for (int i = 0; i < warmupRuns; i++) {
            runSingle(session, schemaTableName, allSlices, predicate, metaClientSupplier, tableMetadataSupplier);
        }

        // Measurement
        log.info("[%s] Running %d measurement runs...", label, measurementRuns);
        List<BenchmarkMetrics> results = new ArrayList<>();
        for (int i = 0; i < measurementRuns; i++) {
            results.add(runSingle(session, schemaTableName, allSlices, predicate, metaClientSupplier, tableMetadataSupplier));
        }

        printSummary(label, results, allSlices.size());
    }

    private BenchmarkMetrics runSingle(
            ConnectorSession session,
            SchemaTableName schemaTableName,
            List<FileSlice> allSlices,
            TupleDomain<String> predicate,
            java.util.function.Supplier<HoodieTableMetaClient> metaClientSupplier,
            java.util.function.Supplier<HoodieTableMetadata> tableMetadataSupplier)
    {
        Runtime rt = Runtime.getRuntime();
        long heapBefore = rt.totalMemory() - rt.freeMemory();

        // Construct HudiColumnStatsIndexSupport — fires async stats load in constructor
        long t0 = System.nanoTime();
        HudiColumnStatsIndexSupport indexSupport = new HudiColumnStatsIndexSupport(
                session,
                schemaTableName,
                Lazy.lazily(metaClientSupplier::get),
                Lazy.lazily(tableMetadataSupplier::get),
                predicate);

        // First call blocks until stats are loaded (or timeout) — captures stats load + first eval
        long skipped = 0;
        boolean firstResult = indexSupport.shouldSkipFileSlice(allSlices.get(0));
        long statsLoadAndFirstEvalNs = System.nanoTime() - t0;
        if (firstResult) {
            skipped++;
        }

        long heapAfter = rt.totalMemory() - rt.freeMemory();

        // Remaining slices — pure per-file eval time
        List<Long> perFileEvalTimesNs = new ArrayList<>(allSlices.size() - 1);
        for (int i = 1; i < allSlices.size(); i++) {
            long tEval = System.nanoTime();
            boolean skip = indexSupport.shouldSkipFileSlice(allSlices.get(i));
            perFileEvalTimesNs.add(System.nanoTime() - tEval);
            if (skip) {
                skipped++;
            }
        }

        return new BenchmarkMetrics(statsLoadAndFirstEvalNs, allSlices.size(), skipped,
                perFileEvalTimesNs, heapBefore, heapAfter);
    }

    private void printSummary(String label, List<BenchmarkMetrics> results, long totalSlices)
    {
        // Stats load + first eval
        List<Long> statsLoadTimes = results.stream()
                .map(m -> m.statsLoadAndFirstEvalNs)
                .sorted()
                .collect(Collectors.toList());
        long avgStatsLoadMs = (long) statsLoadTimes.stream().mapToLong(Long::longValue).average().orElse(0) / 1_000_000;
        long p95StatsLoadMs = statsLoadTimes.get((int) (statsLoadTimes.size() * 0.95)) / 1_000_000;
        long p99StatsLoadMs = statsLoadTimes.get((int) (statsLoadTimes.size() * 0.99)) / 1_000_000;
        long minStatsLoadMs = statsLoadTimes.get(0) / 1_000_000;
        long maxStatsLoadMs = statsLoadTimes.get(statsLoadTimes.size() - 1) / 1_000_000;

        // Per-file eval (all runs combined, excluding the first-slice measurement)
        List<Long> allPerFileTimes = results.stream()
                .flatMap(m -> m.perFileEvalTimesNs.stream())
                .sorted()
                .collect(Collectors.toList());
        long avgPerFileNs = (long) allPerFileTimes.stream().mapToLong(Long::longValue).average().orElse(0);
        long p95PerFileNs = allPerFileTimes.isEmpty() ? 0 : allPerFileTimes.get((int) (allPerFileTimes.size() * 0.95));
        long p99PerFileNs = allPerFileTimes.isEmpty() ? 0 : allPerFileTimes.get((int) (allPerFileTimes.size() * 0.99));

        // Skipping ratio
        double avgSkipped = results.stream().mapToDouble(BenchmarkMetrics::skippingRatio).average().orElse(0);
        long avgSkippedCount = (long) results.stream().mapToLong(m -> m.skippedFileSlices).average().orElse(0);

        // Memory
        long avgHeapDeltaMb = (long) results.stream()
                .mapToLong(m -> m.heapUsedAfterBytes - m.heapUsedBeforeBytes)
                .average().orElse(0) / (1024 * 1024);

        log.info("[%s] Stats load (incl. first eval):  avg=%dms  min=%dms  max=%dms  p95=%dms  p99=%dms",
                label, avgStatsLoadMs, minStatsLoadMs, maxStatsLoadMs, p95StatsLoadMs, p99StatsLoadMs);
        log.info("[%s] Per-file eval (remaining files): avg=%dns  p95=%dns  p99=%dns",
                label, avgPerFileNs, p95PerFileNs, p99PerFileNs);
        log.info("[%s] Skipping ratio:                  avg=%.2f%%  (avg %d/%d file slices skipped)",
                label, avgSkipped, avgSkippedCount, totalSlices);
        log.info("[%s] Heap delta:                      avg=%dMB", label, avgHeapDeltaMb);
    }

    /**
     * Fetches all file slices from all partitions for the latest completed commit.
     */
    private List<FileSlice> fetchAllFileSlices(HoodieTableMetadata tableMetadata, HoodieTableMetaClient metaClient)
            throws Exception
    {
        String latestCommitTime = metaClient.getActiveTimeline()
                .getCommitsTimeline()
                .filterCompletedInstants()
                .lastInstant()
                .get()
                .requestedTime();

        HoodieTableFileSystemView fsView = getFileSystemView(tableMetadata, metaClient);
        fsView.loadAllPartitions();

        List<String> partitions = tableMetadata.getAllPartitionPaths();
        log.info("Found %d partitions; fetching file slices at commit %s", partitions.size(), latestCommitTime);

        List<FileSlice> allSlices = new ArrayList<>();
        for (String partition : partitions) {
            fsView.getLatestFileSlicesBeforeOrOn(partition, latestCommitTime, false)
                    .forEach(allSlices::add);
        }

        fsView.close();
        return allSlices;
    }

    /**
     * Builds a ConnectorSession configured with the given column stats wait timeout.
     */
    private ConnectorSession buildSession(long timeoutMs)
    {
        HudiConfig hudiConfig = new HudiConfig()
                .setColumnStatsWaitTimeout(new Duration(timeoutMs, TimeUnit.MILLISECONDS));
        HudiSessionProperties sessionProperties = new HudiSessionProperties(hudiConfig, new ParquetReaderConfig());
        return TestingConnectorSession.builder()
                .setPropertyMetadata(sessionProperties.getSessionProperties())
                .build();
    }

    /**
     * Builds a TupleDomain<String> predicate from a list of filter specs.
     * Format: col:RANGE:lo:hi | col:GT:val | col:GTE:val | col:LT:val | col:LTE:val | col:EQ:val
     */
    private TupleDomain<String> buildPredicate(List<String> filterSpecs)
    {
        ImmutableMap.Builder<String, Domain> domains = ImmutableMap.builder();
        for (String spec : filterSpecs) {
            String[] parts = spec.split(":");
            if (parts.length < 3) {
                throw new IllegalArgumentException("Invalid filter spec: " + spec
                        + ". Expected format: col:OP:val or col:RANGE:lo:hi");
            }
            String col = parts[0];
            String op = parts[1].toUpperCase();
            Type type = inferColumnType(col);

            Domain domain;
            switch (op) {
                case "RANGE":
                    if (parts.length < 4) {
                        throw new IllegalArgumentException("RANGE filter requires lo and hi: " + spec);
                    }
                    domain = Domain.create(
                            ValueSet.ofRanges(Range.range(type, parseValue(type, parts[2]), true,
                                    parseValue(type, parts[3]), true)), false);
                    break;
                case "GT":
                    domain = Domain.create(ValueSet.ofRanges(Range.greaterThan(type, parseValue(type, parts[2]))), false);
                    break;
                case "GTE":
                    domain = Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(type, parseValue(type, parts[2]))), false);
                    break;
                case "LT":
                    domain = Domain.create(ValueSet.ofRanges(Range.lessThan(type, parseValue(type, parts[2]))), false);
                    break;
                case "LTE":
                    domain = Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(type, parseValue(type, parts[2]))), false);
                    break;
                case "EQ":
                    domain = Domain.singleValue(type, parseValue(type, parts[2]));
                    break;
                default:
                    throw new IllegalArgumentException("Unknown filter operator: " + op + " in spec: " + spec);
            }
            domains.put(col, domain);
        }
        return TupleDomain.withColumnDomains(domains.buildOrThrow());
    }

    /**
     * Infers the Trino type for known columns matching the MetadataBenchmarkingTool schema.
     * tenantID -> BIGINT, age -> INTEGER.
     */
    private Type inferColumnType(String col)
    {
        switch (col) {
            case COL_TENANT_ID:
                return BIGINT;
            case COL_AGE:
                return INTEGER;
            default:
                // Default to BIGINT for unknown numeric columns
                log.warn("Unknown column '%s', defaulting type to BIGINT", col);
                return BIGINT;
        }
    }

    private Object parseValue(Type type, String val)
    {
        if (type.equals(BIGINT)) {
            return Long.parseLong(val);
        }
        if (type.equals(INTEGER)) {
            return (long) Integer.parseInt(val);  // Trino INTEGER uses Long internally
        }
        return Long.parseLong(val);
    }
}
