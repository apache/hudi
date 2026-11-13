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
package io.trino.plugin.hudi.benchmark;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.trino.Session;
import io.trino.execution.QueryStats;
import io.trino.plugin.hudi.util.FileOperationUtils;
import io.trino.server.DynamicFilterService.DynamicFilterDomainStats;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner.MaterializedResultWithPlan;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.hudi.benchmark.TpcdsMorTablesInitializer.DATE_DIM;
import static io.trino.plugin.hudi.benchmark.TpcdsMorTablesInitializer.STORE_SALES;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Comparator.comparingDouble;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

/**
 * End-to-end MOR read benchmark for the Trino Hudi connector, runnable on a laptop with no cluster and no S3.
 * It reads a generated TPC-DS-shaped MOR table and reports the numbers each read-path fix should move:
 * <ul>
 *   <li><b>Merge-path CPU</b> (record size estimation, Page/Avro conversion, key-based merge): {@code _rt} vs
 *   {@code _ro} CPU on the {@code scan} scenario.</li>
 *   <li><b>Per-split metadata reads</b> (meta client + {@code TableSchemaResolver} per split): table-properties and
 *   timeline reads per query, which should not grow with the number of splits.</li>
 *   <li><b>Dynamic partition pruning</b> missing its wait window: the {@code dpp} scenario reports the date filter's
 *   collection time against {@code hudi.dynamic-filtering.wait-timeout} (session property
 *   {@code dynamic_filtering_wait_timeout}) and the fact table's base-file input.</li>
 * </ul>
 *
 * <p>{@code input} is the query's physical input from {@link QueryStats}, which for {@code _rt} covers the
 * base files only: log files are read through the Hudi storage layer and their bytes are not counted.
 *
 * <p>Named {@code *Benchmark} so surefire never runs it in CI. Run it on demand (JDK 25, after the Trino bootstrap
 * described in the module README):
 * <pre>
 * mvn -Phudi-trino,hudi-trino-tests -pl hudi-trino test -Dtest=HudiMorReadBenchmark \
 *     -Dsurefire.failIfNoSpecifiedTests=false \
 *     -Dbench.partitions=48 -Dbench.rowsPerPartition=40000 -Dbench.rounds=3 \
 *     -Dbench.s3.getMillis=20 -Dbench.s3.headMillis=15 -Dbench.s3.listMillis=40
 * </pre>
 * {@code -Dbench.s3.latency.enabled=0} turns the injected storage latency off (CPU-bound, laptop disk only);
 * keep it on to see the per-split metadata reads and the dynamic-filter timing the way S3 shows them.
 * Results print to stdout and append to {@code target/hudi-mor-read-benchmark.jsonl}.
 */
@TestInstance(PER_CLASS)
final class HudiMorReadBenchmark
{
    private static final int ROUNDS = Integer.getInteger("bench.rounds", 3);
    private static final int WORKERS = Integer.getInteger("bench.workers", 1);
    private static final int TARGET_MONTH = 11;

    private final LatencyInjectingFileSystemFactory.Counters counters = new LatencyInjectingFileSystemFactory.Counters();
    private TpcdsMorTablesInitializer tables;
    private DistributedQueryRunner queryRunner;
    private final List<String> report = new ArrayList<>();

    @BeforeAll
    void setUp()
            throws Exception
    {
        checkArgument(ROUNDS >= 1, "bench.rounds must be at least 1: %s", ROUNDS);
        tables = TpcdsMorTablesInitializer.fromSystemProperties();
        // Partition p has d_moy = p % 12 + 1, so the dpp filter d_moy = TARGET_MONTH needs partition TARGET_MONTH - 1.
        checkArgument(tables.partitions() >= TARGET_MONTH, "bench.partitions must be at least %s for a d_moy = %s partition: %s",
                TARGET_MONTH, TARGET_MONTH, tables.partitions());
        LatencyInjectingFileSystemFactory.Latency latency = LatencyInjectingFileSystemFactory.Latency.fromSystemProperties();
        queryRunner = BenchmarkHudiQueryRunner.create(WORKERS, latency, counters, ImmutableMap.of(), tables);
        report.add(format("tables=%s latency=%s workers=%s rounds=%s", tables, latency, WORKERS, ROUNDS));
    }

    @AfterAll
    void tearDown()
    {
        report.forEach(System.out::println);
        if (queryRunner != null) {
            queryRunner.close();
        }
    }

    @Test
    void scan()
    {
        long[] expectedRt = tables.expectedRtTotals();
        long[] expectedRo = tables.expectedRoTotals();
        String sql = "SELECT count(*), sum(ss_quantity), sum(ss_net_paid) FROM %s";
        for (Variant variant : Variant.values()) {
            Measurement ro = measure("scan", "ro", variant, format(sql, STORE_SALES + "_ro"), result -> {
                assertThat(result.getMaterializedRows().getFirst().getField(0)).isEqualTo(expectedRo[0]);
                assertThat(result.getMaterializedRows().getFirst().getField(1)).isEqualTo(expectedRo[1]);
                assertThat(result.getMaterializedRows().getFirst().getField(2)).isEqualTo(BigDecimal.valueOf(expectedRo[2], 2));
            });
            Measurement rt = measure("scan", "rt", variant, format(sql, STORE_SALES + "_rt"), result -> {
                assertThat(result.getMaterializedRows().getFirst().getField(0)).isEqualTo(expectedRt[0]);
                assertThat(result.getMaterializedRows().getFirst().getField(1)).isEqualTo(expectedRt[1]);
                assertThat(result.getMaterializedRows().getFirst().getField(2)).isEqualTo(BigDecimal.valueOf(expectedRt[2], 2));
            });
            report.add(format("  scan %-18s rt/ro: wall %.1fx, cpu %.1fx, bytes %.1fx",
                    variant, rt.wallSeconds / ro.wallSeconds, rt.cpuSeconds / ro.cpuSeconds, (double) rt.physicalInputBytes / ro.physicalInputBytes));
        }
    }

    @Test
    void dynamicPartitionPruning()
    {
        Map<Long, long[]> expectedRo = tables.expectedRoByYearForMonth(TARGET_MONTH);
        Map<Long, long[]> expectedRt = tables.expectedRtByYearForMonth(TARGET_MONTH);
        String sql = "SELECT d.d_year, count(*), sum(ss.ss_quantity) FROM %s ss JOIN %s d ON ss.ss_sold_date_sk = d.d_date_sk " +
                "WHERE d.d_moy = " + TARGET_MONTH + " GROUP BY d.d_year ORDER BY 1";
        for (Variant variant : Variant.values()) {
            measure("dpp", "ro", variant, format(sql, STORE_SALES + "_ro", DATE_DIM + "_ro"), result -> assertByYear(result, expectedRo));
            measure("dpp", "rt", variant, format(sql, STORE_SALES + "_rt", DATE_DIM + "_rt"), result -> assertByYear(result, expectedRt));
        }
    }

    private static void assertByYear(MaterializedResult result, Map<Long, long[]> expected)
    {
        Map<Long, long[]> actual = new TreeMap<>();
        for (MaterializedRow row : result.getMaterializedRows()) {
            actual.put(((Number) row.getField(0)).longValue(), new long[] {(Long) row.getField(1), (Long) row.getField(2)});
        }
        assertThat(actual.keySet()).isEqualTo(expected.keySet());
        expected.forEach((year, totals) -> assertThat(actual.get(year)).containsExactly(totals));
    }

    private Measurement measure(String scenario, String table, Variant variant, String sql, Consumer<MaterializedResult> verifier)
    {
        Session session = variant.apply(queryRunner.getDefaultSession());
        // Warm up: JIT, metastore and connector caches. Not measured.
        verifier.accept(queryRunner.executeWithPlan(session, sql).result());

        List<Measurement> runs = new ArrayList<>();
        for (int round = 0; round < ROUNDS; round++) {
            counters.reset();
            long start = System.nanoTime();
            MaterializedResultWithPlan result = queryRunner.executeWithPlan(session, sql);
            double wall = (System.nanoTime() - start) / 1e9;
            verifier.accept(result.result());
            runs.add(Measurement.of(wall, queryRunner.getCoordinator().getFullQueryInfo(result.queryId()).getQueryStats(), counters, queryRunner.getSpans()));
        }
        Measurement median = Measurement.median(runs);
        if (Boolean.getBoolean("bench.s3.samples")) {
            report.add(format("    samples %s %s %s:%n%s", scenario, table, variant, counters.samples()));
        }
        String line = format(Locale.ROOT, "%-5s %-3s %-18s wall=%6.2fs cpu=%7.2fs input=%8.1fMB storage[%s] fileOps%s dateDF=%s",
                scenario, table, variant, median.wallSeconds, median.cpuSeconds, median.physicalInputBytes / 1e6,
                median.storage, median.fileOps, median.dateFilterCollection);
        report.add(line);
        appendJson(scenario, table, variant, median);
        return median;
    }

    private static void appendJson(String scenario, String table, Variant variant, Measurement m)
    {
        String json = format(Locale.ROOT,
                "{\"scenario\":\"%s\",\"table\":\"%s\",\"variant\":\"%s\",\"wall_s\":%.3f,\"cpu_s\":%.3f,\"input_bytes\":%d,\"storage\":\"%s\",\"file_ops\":\"%s\",\"date_df\":\"%s\"}%n",
                scenario, table, variant, m.wallSeconds, m.cpuSeconds, m.physicalInputBytes, m.storage, m.fileOps, m.dateFilterCollection);
        try {
            Path output = Path.of("target", "hudi-mor-read-benchmark.jsonl");
            Files.createDirectories(output.getParent());
            Files.writeString(output, json, UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Session variants; each isolates one candidate fix that is reachable through configuration today. */
    enum Variant
    {
        DEFAULT(Map.of()),
        /** Resolves the table schema once on the coordinator, which removes the per-split {@code TableSchemaResolver}. */
        SCHEMA_IN_HANDLE(Map.of("resolve_column_name_casing_enabled", "true")),
        /** Waits long enough for the dimension's dynamic filter, isolating the pruning-timing effect. */
        DF_WAIT_10S(Map.of("dynamic_filtering_wait_timeout", "10s"));

        private final Map<String, String> properties;

        Variant(Map<String, String> properties)
        {
            this.properties = properties;
        }

        Session apply(Session session)
        {
            Session.SessionBuilder builder = Session.builder(session);
            properties.forEach((name, value) -> builder.setCatalogSessionProperty(BenchmarkHudiQueryRunner.CATALOG, name, value));
            return builder.build();
        }

        @Override
        public String toString()
        {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    private record Measurement(double wallSeconds, double cpuSeconds, long physicalInputBytes, String storage, String fileOps, String dateFilterCollection)
    {
        static Measurement of(double wall, QueryStats stats, LatencyInjectingFileSystemFactory.Counters counters, List<SpanData> spans)
        {
            Map<FileOperationUtils.FileType, Integer> ops = new EnumMap<>(FileOperationUtils.FileType.class);
            for (SpanData span : spans) {
                String name = span.getName();
                if (!(name.startsWith("InputFile.newStream") || name.startsWith("Input.readTail") || name.startsWith("Input.readFully") || name.startsWith("FileSystem.listFiles"))) {
                    continue;
                }
                String location = span.getAttributes().asMap().entrySet().stream()
                        .filter(entry -> entry.getKey().getKey().endsWith("location"))
                        .map(entry -> String.valueOf(entry.getValue()))
                        .findFirst()
                        .orElse(null);
                if (location == null || location.contains("/.trino") || location.contains("/managed/")) {
                    continue;
                }
                try {
                    ops.merge(FileOperationUtils.FileType.fromFilePath(location), 1, Integer::sum);
                }
                catch (IllegalArgumentException ignored) {
                    // Directory listings and other non-file locations are counted by the storage counters instead.
                }
            }
            String dateFilter = stats.getDynamicFiltersStats().getDynamicFilterDomainStats().stream()
                    .map(DynamicFilterDomainStats::getCollectionDuration)
                    .flatMap(Optional::stream)
                    .map(Duration::toString)
                    .reduce((a, b) -> a + "," + b)
                    .orElse("-");
            return new Measurement(wall, stats.getTotalCpuTime().getValue(MILLISECONDS) / 1000.0,
                    stats.getPhysicalInputDataSize().toBytes(), counters + " | " + counters.breakdown(), ops.toString(), dateFilter);
        }

        static Measurement median(List<Measurement> runs)
        {
            List<Measurement> sorted = new ArrayList<>(runs);
            sorted.sort(comparingDouble(Measurement::wallSeconds));
            return sorted.get(sorted.size() / 2);
        }
    }
}
