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
package io.trino.plugin.hudi;

import io.trino.Session;
import io.trino.execution.QueryStats;
import io.trino.plugin.hudi.reader.HudiVectorizedMorPageSource;
import io.trino.plugin.hudi.testing.CommitTimeOrderingHudiTablesInitializer;
import io.trino.plugin.hudi.testing.CompositeHudiTablesInitializer;
import io.trino.plugin.hudi.testing.PositionalMergeHudiTablesInitializer;
import io.trino.spi.QueryId;
import io.trino.spi.metrics.Count;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.apache.hudi.common.config.RecordMergeMode;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;

import static io.trino.plugin.hudi.testing.PositionalMergeHudiTablesInitializer.COMMIT_TIME_TABLE_NAME;
import static io.trino.plugin.hudi.testing.PositionalMergeHudiTablesInitializer.EVENT_TIME_TABLE_NAME;
import static io.trino.plugin.hudi.testing.PositionalMergeHudiTablesInitializer.ROWS;
import static io.trino.plugin.hudi.testing.PositionalMergeHudiTablesInitializer.expectedValues;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The vectorized MOR read on tables whose log blocks carry record positions
 * ({@link PositionalMergeHudiTablesInitializer}), and on a table whose log blocks do not
 * ({@link CommitTimeOrderingHudiTablesInitializer}, keyed by record key). Results are checked against the snapshot the
 * fixture computes from its own commits, and the split's path against the connector metrics.
 */
public class TestHudiVectorizedMorRead
        extends AbstractTestQueryFramework
{
    private static final String COMMIT_TIME_RT = COMMIT_TIME_TABLE_NAME + "_rt";
    private static final String EVENT_TIME_RT = EVENT_TIME_TABLE_NAME + "_rt";

    enum Mode
    {
        MERGE_READ,
        VECTORIZED,
        VECTORIZED_NO_PUSHDOWN,
        VECTORIZED_NO_PREFETCH,
        VECTORIZED_NO_EARLY_BASE_OPEN,
        VECTORIZED_SYNCHRONOUS_LOAD,
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HudiQueryRunner.builder()
                .setDataLoader(new CompositeHudiTablesInitializer(
                        PositionalMergeHudiTablesInitializer.commitTimeOrdering(),
                        PositionalMergeHudiTablesInitializer.eventTimeOrdering(),
                        new CommitTimeOrderingHudiTablesInitializer()))
                .build();
    }

    @ParameterizedTest
    @EnumSource(Mode.class)
    void testSnapshotMatchesTheCommits(Mode mode)
    {
        for (RecordMergeMode mergeMode : new RecordMergeMode[] {RecordMergeMode.COMMIT_TIME_ORDERING, RecordMergeMode.EVENT_TIME_ORDERING}) {
            String table = rtTable(mergeMode);
            Map<String, Long> expected = expectedValues(mergeMode);
            assertQuery(session(mode), "SELECT key, value FROM " + table, values(expected, _ -> true));
            assertQuery(session(mode), "SELECT count(*) FROM " + table, "VALUES " + expected.size());
            // The other columns of a merged row come from its log record
            assertQuery(session(mode), "SELECT name FROM " + table + " WHERE key IN ('k0010', 'k0505')",
                    "VALUES 'name_k0010_5000', 'name_k0505_10505'");
        }
    }

    @Test
    void testSplitsTakeThePositionalMerge()
    {
        for (String table : new String[] {COMMIT_TIME_RT, EVENT_TIME_RT}) {
            assertThat(metric(session(Mode.VECTORIZED), "SELECT key, value FROM " + table, HudiVectorizedMorPageSource.POSITION_MERGE_SPLITS))
                    .as(table).isPositive();
            assertThat(metric(session(Mode.MERGE_READ), "SELECT key, value FROM " + table, HudiVectorizedMorPageSource.VECTORIZED_MOR_SPLITS))
                    .as(table).isZero();
            assertThat(metric(session(Mode.MERGE_READ), "SELECT key, value FROM " + table, HudiPageSource.TRINO_ROW_MERGE_SPLITS))
                    .as(table).isPositive();
        }
        // Log blocks without positions are matched by record key
        String keyed = CommitTimeOrderingHudiTablesInitializer.RT_TABLE_NAME;
        assertThat(metric(session(Mode.VECTORIZED), "SELECT key, value FROM " + keyed, HudiVectorizedMorPageSource.VECTORIZED_MOR_SPLITS)).isPositive();
        assertThat(metric(session(Mode.VECTORIZED), "SELECT key, value FROM " + keyed, HudiVectorizedMorPageSource.POSITION_MERGE_SPLITS)).isZero();
        assertQuery(session(Mode.VECTORIZED), "SELECT key, name, value FROM " + keyed,
                "VALUES ('k1', 'k1_updated', CAST(11 AS BIGINT)), ('k3', 'k3_base', 30)");
    }

    @Test
    void testEarlyBaseOpenIsReadOnlyWhenLogRecordsMatchByPosition()
    {
        String query = "SELECT key, value FROM ";
        // Commit time ordering with positions: the base file opened while the log records loaded is read
        assertThat(metric(session(Mode.VECTORIZED), query + COMMIT_TIME_RT, HudiVectorizedMorPageSource.EARLY_BASE_OPEN_SPLITS)).isPositive();
        assertThat(metric(session(Mode.VECTORIZED), query + COMMIT_TIME_RT, HudiVectorizedMorPageSource.EARLY_BASE_DISCARDED_SPLITS)).isZero();
        assertThat(metric(session(Mode.VECTORIZED_NO_EARLY_BASE_OPEN), query + COMMIT_TIME_RT, HudiVectorizedMorPageSource.EARLY_BASE_OPEN_SPLITS)).isZero();
        // Event time ordering reads the ordering fields as well: no early open
        assertThat(metric(session(Mode.VECTORIZED), query + EVENT_TIME_RT, HudiVectorizedMorPageSource.EARLY_BASE_OPEN_SPLITS)).isZero();
        assertThat(metric(session(Mode.VECTORIZED), query + EVENT_TIME_RT, HudiVectorizedMorPageSource.EARLY_BASE_DISCARDED_SPLITS)).isZero();
        // Log blocks without positions match by key: the early open is dropped and the base file opened with the key
        String keyed = CommitTimeOrderingHudiTablesInitializer.RT_TABLE_NAME;
        assertThat(metric(session(Mode.VECTORIZED), query + keyed, HudiVectorizedMorPageSource.EARLY_BASE_DISCARDED_SPLITS)).isPositive();
        assertThat(metric(session(Mode.VECTORIZED), query + keyed, HudiVectorizedMorPageSource.EARLY_BASE_OPEN_SPLITS)).isZero();
        assertQuery(session(Mode.VECTORIZED), "SELECT key, name, value FROM " + keyed,
                "VALUES ('k1', 'k1_updated', CAST(11 AS BIGINT)), ('k3', 'k3_base', 30)");
    }

    @ParameterizedTest
    @EnumSource(Mode.class)
    void testPredicateOnAColumnTheLogRecordsChange(Mode mode)
    {
        // k0010 moves from 10 to 5000, out of the range, and k0900 from 900 to 1, into it. With pushdown the base row
        // group holding k0900 is pruned, so its log record comes back as a leftover; the engine filters the output.
        for (RecordMergeMode mergeMode : new RecordMergeMode[] {RecordMergeMode.COMMIT_TIME_ORDERING, RecordMergeMode.EVENT_TIME_ORDERING}) {
            String table = rtTable(mergeMode);
            Map<String, Long> expected = expectedValues(mergeMode);
            assertQuery(session(mode), "SELECT key, value FROM " + table + " WHERE value < 50", values(expected, value -> value < 50));
            assertQuery(session(mode), "SELECT key, value FROM " + table + " WHERE value > 4000", values(expected, value -> value > 4000));
        }
        assertThat(values(expectedValues(RecordMergeMode.COMMIT_TIME_ORDERING), value -> value < 50)).contains("'k0900'").doesNotContain("'k0010'");
    }

    @Test
    void testPushdownPrunesOnlyCommitTimePositionalReads()
    {
        String query = "SELECT key, value FROM %s WHERE value < 50";
        // Row groups of about a hundred rows: the predicate keeps the first one
        assertThat(physicalInputPositions(session(Mode.VECTORIZED), query.formatted(COMMIT_TIME_RT))).isLessThan(ROWS / 2);
        assertThat(physicalInputPositions(session(Mode.VECTORIZED_NO_PUSHDOWN), query.formatted(COMMIT_TIME_RT))).isEqualTo(ROWS);
        // A pruned base row may still beat its log record on event time
        assertThat(physicalInputPositions(session(Mode.VECTORIZED), query.formatted(EVENT_TIME_RT))).isEqualTo(ROWS);
    }

    @ParameterizedTest
    @EnumSource(Mode.class)
    void testDynamicFilterOnAColumnTheLogRecordsChange(Mode mode)
    {
        // The build side is the base value of k0001, 1, which k0900 now holds too after its update
        @Language("SQL") String query = "SELECT r.key FROM " + COMMIT_TIME_RT + " r JOIN " + COMMIT_TIME_TABLE_NAME + " o"
                + " ON r.value = o.value WHERE o.key = 'k0001'";
        Session session = Session.builder(session(mode))
                .setSystemProperty("join_distribution_type", "BROADCAST")
                .build();
        assertQuery(session, query, "VALUES 'k0001', 'k0900'");
        if (mode == Mode.VECTORIZED) {
            // Both scans read one row group each: the probe side's dynamic filter pruned the rest
            assertThat(physicalInputPositions(session, query)).isLessThan(ROWS);
        }
    }

    private static String rtTable(RecordMergeMode mergeMode)
    {
        return mergeMode == RecordMergeMode.COMMIT_TIME_ORDERING ? COMMIT_TIME_RT : EVENT_TIME_RT;
    }

    private static String values(Map<String, Long> expected, Predicate<Long> filter)
    {
        return "VALUES " + expected.entrySet().stream()
                .filter(entry -> filter.test(entry.getValue()))
                .map(entry -> "('%s', CAST(%s AS BIGINT))".formatted(entry.getKey(), entry.getValue()))
                .collect(joining(", "));
    }

    private Session session(Mode mode)
    {
        return SessionBuilder.from(getSession())
                .withSchemaColumnTypesEnabled(true)
                .withMorVectorizedReadEnabled(mode != Mode.MERGE_READ)
                .withMorVectorizedReadPredicatePushdownEnabled(mode != Mode.VECTORIZED_NO_PUSHDOWN)
                .withMorVectorizedReadLogPrefetchEnabled(mode != Mode.VECTORIZED_NO_PREFETCH)
                .withMorVectorizedReadEarlyBaseOpenEnabled(mode != Mode.VECTORIZED_NO_EARLY_BASE_OPEN)
                .withMorVectorizedReadAsyncLoadEnabled(mode != Mode.VECTORIZED_SYNCHRONOUS_LOAD)
                .build();
    }

    private long metric(Session session, @Language("SQL") String sql, String name)
    {
        QueryId queryId = getQueryRunner().executeWithPlan(session, sql).queryId();
        return getQueryRunner().getCoordinator().getQueryManager().getFullQueryInfo(queryId).getQueryStats().getOperatorSummaries().stream()
                .map(operator -> operator.getConnectorMetrics().getMetrics().get(name))
                .filter(Objects::nonNull)
                .mapToLong(metric -> ((Count<?>) metric).getTotal())
                .sum();
    }

    private long physicalInputPositions(Session session, @Language("SQL") String sql)
    {
        QueryId queryId = getQueryRunner().executeWithPlan(session, sql).queryId();
        QueryStats stats = getQueryRunner().getCoordinator().getQueryManager().getFullQueryInfo(queryId).getQueryStats();
        return stats.getPhysicalInputPositions();
    }
}
