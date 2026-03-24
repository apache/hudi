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
import io.trino.Session;
import io.trino.filesystem.Location;
import io.trino.metastore.Column;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.HiveMetastoreFactory;
import io.trino.metastore.Partition;
import io.trino.metastore.PartitionStatistics;
import io.trino.metastore.PartitionWithStatistics;
import io.trino.metastore.PrincipalPrivileges;
import io.trino.metastore.StorageFormat;
import io.trino.metastore.Table;
import io.trino.plugin.hudi.HudiBenchmarkPageSourceProvider;
import io.trino.plugin.hudi.HudiConnector;
import io.trino.plugin.hudi.HudiQueryRunner;
import io.trino.plugin.hudi.SessionBuilder;
import io.trino.plugin.hudi.testing.HudiTablesInitializer;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.trino.hive.formats.HiveClassNames.HUDI_PARQUET_INPUT_FORMAT;
import static io.trino.hive.formats.HiveClassNames.MAPRED_PARQUET_OUTPUT_FORMAT_CLASS;
import static io.trino.hive.formats.HiveClassNames.PARQUET_HIVE_SERDE_CLASS;
import static io.trino.metastore.HiveType.HIVE_INT;
import static io.trino.metastore.HiveType.HIVE_LONG;
import static io.trino.metastore.HiveType.HIVE_STRING;
import static io.trino.plugin.hive.TableType.EXTERNAL_TABLE;

/**
 * Benchmarks the full Trino query path against a Hudi table bootstrapped by MetadataBenchmarkingTool.
 *
 * <p>Runs a configurable SQL query twice — once with the Hudi metadata table (column stats) enabled
 * and once with it disabled — and reports elapsed time and speedup.
 *
 * <p>Prerequisites:
 * <ol>
 *   <li>Bootstrap a table: run {@code MetadataBenchmarkingTool --mode BOOTSTRAP}</li>
 *   <li>HudiPageSourceProvider should return empty pages (real parquet reads are not needed)</li>
 * </ol>
 *
 * <p>Usage example:
 * <pre>
 *   java -cp ... io.trino.plugin.hudi.benchmarking.TrinoBenchmarkToolV2 \
 *     --table-base-path /tmp/hudi_bench_table \
 *     --filter tenantID:RANGE:40000:50000 \
 *     --measurement-runs 5
 * </pre>
 */
public class TrinoBenchmarkToolV2
{
    private static final Logger log = Logger.get(TrinoBenchmarkToolV2.class);

    public static class Config
    {
        @Parameter(names = {"--table-base-path", "-tbp"}, description = "Path to the Hudi table bootstrapped by MetadataBenchmarkingTool", required = true)
        public String tableBasePath;

        @Parameter(names = {"--table-name", "-tn"}, description = "Table name (must match the name used during bootstrap)")
        public String tableName = "test_mdt_stats_tbl";

        @Parameter(names = {"--filter", "-f"}, description = "Filter spec: col:RANGE:lo:hi | col:GT:val | col:GTE:val | col:LT:val | col:LTE:val | col:EQ:val. "
                + "Repeat for multiple columns. Example: 'tenantID:RANGE:40000:50000'")
        public List<String> filters = new ArrayList<>(List.of("tenantID:RANGE:40000:50000"));

        @Parameter(names = {"--col-stats-timeout", "-cst"}, description = "Value for the hudi.column_stats_wait_timeout session property (e.g. '10s')")
        public String colStatsTimeout = "2s";

        @Parameter(names = {"--warmup-runs", "-w"}, description = "Number of warm-up query executions before measurement (discarded)")
        public int warmupRuns = 2;

        @Parameter(names = {"--measurement-runs", "-r"}, description = "Number of timed query executions per scenario")
        public int measurementRuns = 5;

        @Parameter(names = {"--file-slice-processing-ms", "-fspm"}, description = "Simulated time spent processing each file slice in milliseconds (sets HudiBenchmarkPageSourceProvider.SLEEP_MS)")
        public long fileSliceProcessingMs = 10;

        @Parameter(names = {"--partition-filter", "-pf"}, description = "If enabled, adds a partition filter predicate to the query (default: datePartition = '2025-01-01')")
        public boolean partitionFilter;

        @Parameter(names = {"--help", "-h"}, help = true)
        public boolean help;
    }

    private final Config cfg;

    public TrinoBenchmarkToolV2(Config cfg)
    {
        this.cfg = cfg;
    }

    public static void main(String[] args)
            throws Exception
    {
        Config cfg = new Config();
        JCommander cmd = JCommander.newBuilder().addObject(cfg).build();
        cmd.parse(args);

        if (cfg.help || args.length == 0) {
            cmd.usage();
            System.exit(1);
        }

        new TrinoBenchmarkToolV2(cfg).run();
    }

    public void run()
            throws Exception
    {
        String sqlFilter = buildSqlFilter(cfg.filters);

        if (cfg.partitionFilter) {
            String partitionFilterClause = "datePartition = '2025-01-01'";
            sqlFilter = sqlFilter.isEmpty() ? partitionFilterClause : sqlFilter + " AND " + partitionFilterClause;
            log.info("Partition filter enabled: %s", partitionFilterClause);
        }

        String sql = "SELECT COUNT(*) FROM hudi.tests." + cfg.tableName
                + (sqlFilter.isEmpty() ? "" : " WHERE " + sqlFilter);

        HudiBenchmarkPageSourceProvider.SLEEP_MS.set(cfg.fileSliceProcessingMs);

        log.info("=== TrinoBenchmarkToolV2 ===");
        log.info("Table:   %s", cfg.tableBasePath);
        log.info("SQL:     %s", sql);
        log.info("Sleep:   %dms/split", cfg.fileSliceProcessingMs);
        log.info("Warm-up: %d  Measurement: %d", cfg.warmupRuns, cfg.measurementRuns);

        try (DistributedQueryRunner queryRunner = HudiQueryRunner.builder()
                .setDataLoader(new BenchmarkHudiTablesInitializer(cfg.tableBasePath, cfg.tableName))
                .build()) {
            Session withColStats = SessionBuilder.from(queryRunner.getDefaultSession())
                    .withMdtEnabled(true)
                    .withColStatsIndexEnabled(true)
                    .withPartitionStatsIndexEnabled(true)
                    .withColumnStatsTimeout(cfg.colStatsTimeout)
                    .withResolveColumnNameCasingEnabled(false)
                    .build();
            Session withoutColStats = SessionBuilder.from(queryRunner.getDefaultSession())
                    .withMdtEnabled(true)
                    .withColStatsIndexEnabled(false)
                    .withRecordLevelIndexEnabled(false)
                    .withPartitionStatsIndexEnabled(false)
                    .withResolveColumnNameCasingEnabled(false)
                    .build();

            // Warm-up
            log.info("Running %d warm-up runs...", cfg.warmupRuns);
            for (int i = 0; i < cfg.warmupRuns; i++) {
                queryRunner.execute(withColStats, sql);
            }

            // Measurement — without column stats
            log.info("Measuring with columns stats DISABLED (%d runs)...", cfg.measurementRuns);
            List<Long> disabledTimes = new ArrayList<>();
            List<Long> splitsProcessed = new ArrayList<>();
            for (int i = 0; i < cfg.measurementRuns; i++) {
                HudiBenchmarkPageSourceProvider.SPLITS_PROCESSED.set(0);
                long t0 = System.nanoTime();
                MaterializedResult result = queryRunner.execute(withoutColStats, sql);
                disabledTimes.add(System.nanoTime() - t0);
                splitsProcessed.add(HudiBenchmarkPageSourceProvider.SPLITS_PROCESSED.get());
                log.info("  [col-stats=OFF #%d] count=%-8s  splits=%d  time=%dms",
                        i + 1, result.getOnlyValue(), splitsProcessed.get(i), disabledTimes.get(i) / 1_000_000);
            }

            // Measurement — with column stats
            log.info("Measuring with metadata table ENABLED (%d runs)...", cfg.measurementRuns);
            List<Long> enabledTimes = new ArrayList<>();
            List<Long> enabledSplits = new ArrayList<>();
            for (int i = 0; i < cfg.measurementRuns; i++) {
                HudiBenchmarkPageSourceProvider.SPLITS_PROCESSED.set(0);
                long t0 = System.nanoTime();
                MaterializedResult result = queryRunner.execute(withColStats, sql);
                enabledTimes.add(System.nanoTime() - t0);
                enabledSplits.add(HudiBenchmarkPageSourceProvider.SPLITS_PROCESSED.get());
                log.info("  [col-stats=ON  #%d] count=%-8s  splits=%d  time=%dms",
                        i + 1, result.getOnlyValue(), enabledSplits.get(i), enabledTimes.get(i) / 1_000_000);
            }
            printSummary(sql, enabledTimes, enabledSplits, disabledTimes, splitsProcessed);
        }
    }

    private String buildSqlFilter(List<String> filterSpecs)
    {
        return filterSpecs.stream()
                .map(this::specToSql)
                .collect(Collectors.joining(" AND "));
    }

    private String specToSql(String spec)
    {
        String[] parts = spec.split(":");
        String col = parts[0];
        String op = parts[1].toUpperCase();
        switch (op) {
            case "RANGE":
                return col + " BETWEEN " + parts[2] + " AND " + parts[3];
            case "GT":
                return col + " > " + parts[2];
            case "GTE":
                return col + " >= " + parts[2];
            case "LT":
                return col + " < " + parts[2];
            case "LTE":
                return col + " <= " + parts[2];
            case "EQ":
                return col + " = " + parts[2];
            default:
                throw new IllegalArgumentException("Unknown filter op: " + op + " in spec: " + spec);
        }
    }

    private void printSummary(String sql, List<Long> enabledTimesNs, List<Long> enabledSplits, List<Long> disabledTimesNs, List<Long> processedSplits)
    {
        long avgEnabledMs = avg(enabledTimesNs) / 1_000_000;
        long avgDisabledMs = avg(disabledTimesNs) / 1_000_000;
        double speedup = avgDisabledMs > 0 ? (double) avgDisabledMs / avgEnabledMs : 1.0;

        String sep = "=".repeat(70);
        log.info(sep);
        log.info("  TRINO QUERY BENCHMARK SUMMARY");
        log.info("  SQL: %s", sql);
        log.info(sep);
        log.info("  With col stats ON:   avg=%dms  avg-splits=%d  [%s]",
                avgEnabledMs, avg(enabledSplits), timingStats(enabledTimesNs));
        log.info("  With col stats OFF:  avg=%dms  avg-splits=%d  [%s]",
                avgDisabledMs, avg(processedSplits), timingStats(disabledTimesNs));
        log.info("  Speedup: %.2fx  (%s)",
                speedup,
                speedup >= 1.0 ? "col stats is faster" : "col stats is slower — check MDT health");
        log.info(sep);
    }

    private static long avg(List<Long> values)
    {
        return values.stream().mapToLong(Long::longValue).sum() / values.size();
    }

    private static String timingStats(List<Long> timesNs)
    {
        List<Long> sorted = timesNs.stream().sorted().collect(Collectors.toList());
        return String.format("min=%dms max=%dms p95=%dms",
                sorted.get(0) / 1_000_000,
                sorted.get(sorted.size() - 1) / 1_000_000,
                sorted.get(Math.max(0, (int) (sorted.size() * 0.95) - 1)) / 1_000_000);
    }

    // =========================================================================
    // Table initializer — registers the bootstrapped table in the Trino metastore
    // =========================================================================

    /**
     * Registers the MetadataBenchmarkingTool-bootstrapped Hudi table in the Trino file metastore.
     *
     * <p>Creates a symlink from the Trino data directory into the real table path so that the
     * {@code local://} filesystem scheme resolves correctly. The table schema is hard-coded to
     * match what MetadataBenchmarkingTool creates. Partition paths are read dynamically from the
     * Hudi metadata table.
     */
    private static class BenchmarkHudiTablesInitializer
            implements HudiTablesInitializer
    {
        // Hard-coded schema matching MetadataBenchmarkingTool output
        private static final List<Column> ALL_DATA_COLUMNS = ImmutableList.of(
                new Column("_hoodie_commit_time", HIVE_STRING, Optional.empty(), Map.of()),
                new Column("_hoodie_commit_seqno", HIVE_STRING, Optional.empty(), Map.of()),
                new Column("_hoodie_record_key", HIVE_STRING, Optional.empty(), Map.of()),
                new Column("_hoodie_partition_path", HIVE_STRING, Optional.empty(), Map.of()),
                new Column("_hoodie_file_name", HIVE_STRING, Optional.empty(), Map.of()),
                new Column("tenantID", HIVE_LONG, Optional.empty(), Map.of()),
                new Column("age", HIVE_INT, Optional.empty(), Map.of()),
                new Column("name", HIVE_STRING, Optional.empty(), Map.of()),
                new Column("event_time", HIVE_STRING, Optional.empty(), Map.of()));

        private static final List<Column> PARTITION_COLUMNS = ImmutableList.of(
                new Column("datePartition", HIVE_STRING, Optional.empty(), Map.of()));

        private final String tableBasePath;
        private final String tableName;

        private BenchmarkHudiTablesInitializer(String tableBasePath, String tableName)
        {
            this.tableBasePath = tableBasePath;
            this.tableName = tableName;
        }

        @Override
        public void initializeTables(QueryRunner queryRunner, Location externalLocation, String schemaName)
                throws Exception
        {
            // Create a symlink so local:///{tableName} resolves to the real table path.
            // TestingHudiPlugin uses LocalFileSystemFactory with root = {baseDataDir}/hudi_data,
            // so local:///test_mdt_stats_tbl -> {baseDataDir}/hudi_data/test_mdt_stats_tbl -> real path.
            DistributedQueryRunner dqr = (DistributedQueryRunner) queryRunner;
            Path hudiDataRoot = dqr.getCoordinator().getBaseDataDir().resolve("hudi_data");
            Files.createDirectories(hudiDataRoot);
            Files.createSymbolicLink(hudiDataRoot.resolve(tableName), Paths.get(tableBasePath));
            Location tableLocation = Location.of("local:///" + tableName);

            // Delete .crc checksum files created by Hadoop's LocalFileSystem during Spark bootstrap.
            // Their names contain ".log." which makes Hudi's log scanner treat them as log files,
            // causing "Did not find the magic bytes" errors when reading the metadata table.
            try (java.util.stream.Stream<Path> stream = Files.walk(Paths.get(tableBasePath))) {
                stream.filter(p -> p.toString().endsWith(".crc"))
                        .forEach(p -> {
                            try {
                                Files.delete(p);
                            }
                            catch (java.io.IOException e) {
                                log.warn("Failed to delete .crc file %s: %s", p, e.getMessage());
                            }
                        });
            }

            // Discover partitions from the Hudi metadata table
            HadoopStorageConfiguration storageConf = new HadoopStorageConfiguration(new Configuration());
            HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder()
                    .setConf(storageConf)
                    .setBasePath(tableBasePath)
                    .build();
            HoodieTableMetadata tableMetadata = HoodieTableMetadata.create(
                    new HoodieLocalEngineContext(storageConf),
                    metaClient.getStorage(),
                    HoodieMetadataConfig.newBuilder().enable(true).build(),
                    tableBasePath,
                    true);
            List<String> partitionPaths = tableMetadata.getAllPartitionPaths();
            log.info("Registering '%s' with %d partitions at %s", tableName, partitionPaths.size(), tableLocation);

            // Register the table and partitions in the file-based Hive metastore
            HiveMetastore metastore = ((HudiConnector) dqr.getCoordinator().getConnector("hudi"))
                    .getInjector()
                    .getInstance(HiveMetastoreFactory.class)
                    .createMetastore(Optional.empty());

            StorageFormat storageFormat = StorageFormat.create(
                    PARQUET_HIVE_SERDE_CLASS, HUDI_PARQUET_INPUT_FORMAT, MAPRED_PARQUET_OUTPUT_FORMAT_CLASS);

            metastore.createTable(
                    Table.builder()
                            .setDatabaseName(schemaName)
                            .setTableName(tableName)
                            .setTableType(EXTERNAL_TABLE.name())
                            .setOwner(Optional.of("public"))
                            .setDataColumns(ALL_DATA_COLUMNS)
                            .setPartitionColumns(PARTITION_COLUMNS)
                            .setParameters(ImmutableMap.of("serialization.format", "1", "EXTERNAL", "TRUE"))
                            .withStorage(sb -> sb.setStorageFormat(storageFormat).setLocation(tableLocation.toString()))
                            .build(),
                    PrincipalPrivileges.NO_PRIVILEGES);

            List<PartitionWithStatistics> partitions = new ArrayList<>();
            for (String partitionPath : partitionPaths) {
                partitions.add(new PartitionWithStatistics(
                        Partition.builder()
                                .setDatabaseName(schemaName)
                                .setTableName(tableName)
                                .setValues(List.of(partitionPath))
                                .withStorage(sb -> sb.setStorageFormat(storageFormat)
                                        .setLocation(tableLocation.appendPath(partitionPath).toString()))
                                .setColumns(ALL_DATA_COLUMNS)
                                .build(),
                        PARTITION_COLUMNS.get(0).getName() + "=" + partitionPath,
                        PartitionStatistics.empty()));
            }
            if (!partitions.isEmpty()) {
                metastore.addPartitions(schemaName, tableName, partitions);
            }
        }
    }
}
