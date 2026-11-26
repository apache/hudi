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

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.hudi.testing.ColumnTypesHudiTablesInitializer;
import io.trino.plugin.hudi.testing.CommitTimeOrderingHudiTablesInitializer;
import io.trino.plugin.hudi.testing.CompositeHudiTablesInitializer;
import io.trino.plugin.hudi.testing.EventTimeDeletesHudiTablesInitializer;
import io.trino.plugin.hudi.testing.SchemaColumnTypesHudiTablesInitializer;
import io.trino.plugin.hudi.util.TrinoTypeMapper;
import io.trino.spi.QueryId;
import io.trino.spi.metrics.Count;
import io.trino.spi.type.TypeOperators;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.plugin.hudi.testing.SchemaColumnTypesHudiTablesInitializer.TABLE_SCHEMA;
import static java.lang.String.join;
import static java.time.format.DateTimeFormatter.ofPattern;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end MoR snapshot-read tests for the merge-mode dispatch in
 * {@code HudiTrinoReaderContext.getRecordMerger} with deletes (issue apache/hudi#18898), on tables
 * written by {@link EventTimeDeletesHudiTablesInitializer} and
 * {@link CommitTimeOrderingHudiTablesInitializer}:
 * <ul>
 *   <li>EVENT_TIME_ORDERING: updates and soft deletes apply only when their ordering value wins;
 *       obsolete (lower-ordering) updates and soft deletes must LOSE against the base row.</li>
 *   <li>Hard deletes (native delete log files, read back through the connector's own
 *       {@code getFileRecordIterator}) always win.</li>
 *   <li>COMMIT_TIME_ORDERING: the latest write wins even with a LOWER ordering value -- the exact
 *       mirror of the event-time obsolete-update case, discriminating the two merger dispatches.</li>
 * </ul>
 * The merge-semantics tests run with {@code schema_column_types_enabled} off and on: on, the merge runs on Trino rows
 * ({@code HudiTrinoRowReaderContext}). The table written by {@link SchemaColumnTypesHudiTablesInitializer} holds one
 * column per HoodieSchema type and is read with the property on, so every type goes through the Trino-row merge.
 */
public class TestHudiMorMergeModeSemantics
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HudiQueryRunner.builder()
                .setDataLoader(new CompositeHudiTablesInitializer(
                        new EventTimeDeletesHudiTablesInitializer(),
                        new CommitTimeOrderingHudiTablesInitializer(),
                        EventTimeDeletesHudiTablesInitializer.spillingEveryLogRecord(),
                        CommitTimeOrderingHudiTablesInitializer.spillingEveryLogRecord(),
                        new ColumnTypesHudiTablesInitializer(),
                        ColumnTypesHudiTablesInitializer.spillingEveryLogRecord(),
                        new SchemaColumnTypesHudiTablesInitializer()))
                .addConnectorProperties(connectorProperties())
                .build();
    }

    /**
     * Catalog properties of the query runner, on top of the defaults.
     */
    protected Map<String, String> connectorProperties()
    {
        return Map.of();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testReadOptimizedShowsAllBaseRows(boolean schemaColumnTypes)
    {
        // Deletes and updates live in log files only; the read-optimized tables reflect the base commit
        assertQuery(
                session(schemaColumnTypes),
                "SELECT key, name, value FROM " + EventTimeDeletesHudiTablesInitializer.TABLE_NAME + " ORDER BY key",
                "VALUES ('k1', 'k1_base', CAST(10 AS BIGINT)), ('k2', 'k2_base', 20), ('k3', 'k3_base', 30),"
                        + " ('k4', 'k4_base', 40), ('k5', 'k5_base', 50), ('k6', 'k6_base', 60)");
        assertQuery(
                session(schemaColumnTypes),
                "SELECT key, name, value FROM " + CommitTimeOrderingHudiTablesInitializer.TABLE_NAME + " ORDER BY key",
                "VALUES ('k1', 'k1_base', CAST(10 AS BIGINT)), ('k2', 'k2_base', 20), ('k3', 'k3_base', 30)");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testEventTimeMergeWithDeletes(boolean schemaColumnTypes)
    {
        // k1: higher-ts update wins; k2: hard-deleted; k3: soft-deleted (higher ts);
        // k4: OBSOLETE soft delete (lower ts) -> base row survives;
        // k5: untouched; k6: OBSOLETE update (lower ts) -> base row survives
        assertQuery(
                session(schemaColumnTypes),
                "SELECT key, name, value FROM " + EventTimeDeletesHudiTablesInitializer.RT_TABLE_NAME + " ORDER BY key",
                "VALUES ('k1', 'k1_updated', CAST(11 AS BIGINT)), ('k4', 'k4_base', 40),"
                        + " ('k5', 'k5_base', 50), ('k6', 'k6_base', 60)");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testHardDeleteRemovesRowOnSnapshotRead(boolean schemaColumnTypes)
    {
        // The hard delete is a native delete log file, resolved through the connector's
        // getFileRecordIterator with the synthetic delete-log schema (record key + ordering field)
        assertThat(computeScalar(session(schemaColumnTypes), "SELECT count(*) FROM " + EventTimeDeletesHudiTablesInitializer.RT_TABLE_NAME + " WHERE key = 'k2'"))
                .isEqualTo(0L);
        assertThat(computeScalar(session(schemaColumnTypes), "SELECT count(*) FROM " + EventTimeDeletesHudiTablesInitializer.TABLE_NAME + " WHERE key = 'k2'"))
                .isEqualTo(1L);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testSoftDeleteRemovesRowOnSnapshotRead(boolean schemaColumnTypes)
    {
        // _hoodie_is_deleted=true log record with a winning (higher) ordering value
        assertThat(computeScalar(session(schemaColumnTypes), "SELECT count(*) FROM " + EventTimeDeletesHudiTablesInitializer.RT_TABLE_NAME + " WHERE key = 'k3'"))
                .isEqualTo(0L);
        assertThat(computeScalar(session(schemaColumnTypes), "SELECT count(*) FROM " + EventTimeDeletesHudiTablesInitializer.TABLE_NAME + " WHERE key = 'k3'"))
                .isEqualTo(1L);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testObsoleteSoftDeleteLosesUnderEventTimeOrdering(boolean schemaColumnTypes)
    {
        // The k4 soft delete carries ts=50 < base ts=100: event-time merging must keep the base row
        assertQuery(
                session(schemaColumnTypes),
                "SELECT key, name, value FROM " + EventTimeDeletesHudiTablesInitializer.RT_TABLE_NAME + " WHERE key = 'k4'",
                "VALUES ('k4', 'k4_base', CAST(40 AS BIGINT))");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testOrderingMergesReadTrinoRowsWithSchemaColumnTypes(boolean schemaColumnTypes)
    {
        // The suites pass on either record context, so which one a merged split took is checked directly
        for (String table : List.of(EventTimeDeletesHudiTablesInitializer.RT_TABLE_NAME, CommitTimeOrderingHudiTablesInitializer.RT_TABLE_NAME)) {
            long trinoRowSplits = trinoRowMergeSplits(getQueryRunner(), session(schemaColumnTypes), "SELECT key, name, value FROM " + table);
            if (schemaColumnTypes) {
                assertThat(trinoRowSplits).as(table).isPositive();
            }
            else {
                assertThat(trinoRowSplits).as(table).isZero();
            }
        }
    }

    /**
     * The splits of {@code sql} merged as {@link io.trino.plugin.hudi.reader.TrinoRow}s, from the connector metric
     * {@link HudiPageSource#TRINO_ROW_MERGE_SPLITS}.
     */
    static long trinoRowMergeSplits(QueryRunner queryRunner, Session session, @Language("SQL") String sql)
    {
        QueryId queryId = queryRunner.executeWithPlan(session, sql).queryId();
        return queryRunner.getCoordinator().getQueryManager().getFullQueryInfo(queryId).getQueryStats().getOperatorSummaries().stream()
                .map(operator -> operator.getConnectorMetrics().getMetrics().get(HudiPageSource.TRINO_ROW_MERGE_SPLITS))
                .filter(Objects::nonNull)
                .mapToLong(metric -> ((Count<?>) metric).getTotal())
                .sum();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testCommitTimeOrderingKeepsLatestWrite(boolean schemaColumnTypes)
    {
        // k1's update carries ts=50 < base ts=100. Under COMMIT_TIME_ORDERING the LATEST WRITE wins
        // regardless of the ordering value -- the mirror of the event-time k6 case, where the same
        // shape keeps the BASE row. Together they discriminate the two merger dispatches.
        assertQuery(
                session(schemaColumnTypes),
                "SELECT key, name, value FROM " + CommitTimeOrderingHudiTablesInitializer.RT_TABLE_NAME + " ORDER BY key",
                "VALUES ('k1', 'k1_updated', CAST(11 AS BIGINT)), ('k3', 'k3_base', 30)");
    }

    @Test
    public void testCharMatchesBaseReaderOnUntouchedRow()
    {
        String query = "SELECT code FROM %s WHERE key = 'k3'";
        Object readOptimized = computeScalar(query.formatted(CommitTimeOrderingHudiTablesInitializer.TABLE_NAME));
        assertThat(readOptimized).isEqualTo(" ab  ");
        assertThat(computeScalar(query.formatted(CommitTimeOrderingHudiTablesInitializer.RT_TABLE_NAME))).isEqualTo(readOptimized);
    }

    @Test
    public void testCharFilterMatchesLeadingSpaceOnSnapshotRead()
    {
        assertQuery(
                "SELECT key FROM " + CommitTimeOrderingHudiTablesInitializer.RT_TABLE_NAME + " WHERE code = CAST(' ab' AS CHAR(5))",
                "VALUES 'k3'");
    }

    @Test
    public void testCharKeepsTabsOnUpdatedRow()
    {
        assertThat(computeScalar("SELECT code FROM " + CommitTimeOrderingHudiTablesInitializer.RT_TABLE_NAME + " WHERE key = 'k1'"))
                .isEqualTo("\tx\t  ");
    }

    @Test
    public void testSpilledMergeMatchesInMemoryMerge()
    {
        // The spilling tables hold the same commits with a one-byte merge budget, so every log record and delete
        // goes through HudiTrinoReaderContext.getRecordSerializer before it is merged
        assertQuery(
                "SELECT key, name, value FROM " + EventTimeDeletesHudiTablesInitializer.SPILLING_RT_TABLE_NAME + " ORDER BY key",
                "VALUES ('k1', 'k1_updated', CAST(11 AS BIGINT)), ('k4', 'k4_base', 40),"
                        + " ('k5', 'k5_base', 50), ('k6', 'k6_base', 60)");
        assertQuery(
                "SELECT key, name, value FROM " + CommitTimeOrderingHudiTablesInitializer.SPILLING_RT_TABLE_NAME + " ORDER BY key",
                "VALUES ('k1', 'k1_updated', CAST(11 AS BIGINT)), ('k3', 'k3_base', 30)");
        assertThat(query("SELECT key, name, value, ts, _hoodie_is_deleted FROM " + EventTimeDeletesHudiTablesInitializer.SPILLING_RT_TABLE_NAME))
                .matches("SELECT key, name, value, ts, _hoodie_is_deleted FROM " + EventTimeDeletesHudiTablesInitializer.RT_TABLE_NAME);
        assertThat(query("SELECT key, name, value, ts FROM " + CommitTimeOrderingHudiTablesInitializer.SPILLING_RT_TABLE_NAME))
                .matches("SELECT key, name, value, ts FROM " + CommitTimeOrderingHudiTablesInitializer.RT_TABLE_NAME);
    }

    @Test
    public void testSpilledMergeKeepsEveryColumnType()
    {
        String columns = "key, ts, c_tinyint, c_smallint, c_decimal, c_date, c_timestamp, c_double, c_binary, c_array, c_map, c_struct";
        // k1 is updated and k2 is nulled in the log file; k3 comes from the base file alone
        assertThat(query("SELECT key, ts, c_tinyint, c_smallint, c_decimal, c_date, c_double, c_array, c_struct.name FROM "
                + ColumnTypesHudiTablesInitializer.SPILLING_RT_TABLE_NAME))
                .matches("VALUES"
                        + " (VARCHAR 'k1', BIGINT '200', TINYINT '7', SMALLINT '1007', CAST(7.25 AS decimal(10, 2)), DATE '2022-01-15', DOUBLE '7.5', ARRAY[BIGINT '7', BIGINT '8'], VARCHAR 'struct-7'),"
                        + " (VARCHAR 'k2', BIGINT '200', NULL, NULL, NULL, NULL, NULL, NULL, NULL),"
                        + " (VARCHAR 'k3', BIGINT '100', TINYINT '3', SMALLINT '1003', CAST(3.25 AS decimal(10, 2)), DATE '2022-01-11', DOUBLE '3.5', ARRAY[BIGINT '3', BIGINT '4'], VARCHAR 'struct-3')");
        assertThat(query("SELECT " + columns + " FROM " + ColumnTypesHudiTablesInitializer.SPILLING_RT_TABLE_NAME))
                .matches("SELECT " + columns + " FROM " + ColumnTypesHudiTablesInitializer.RT_TABLE_NAME);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testCountAfterDeletes(boolean schemaColumnTypes)
    {
        assertThat(computeScalar(session(schemaColumnTypes), "SELECT count(*) FROM " + EventTimeDeletesHudiTablesInitializer.RT_TABLE_NAME)).isEqualTo(4L);
        assertThat(computeScalar(session(schemaColumnTypes), "SELECT count(*) FROM " + CommitTimeOrderingHudiTablesInitializer.RT_TABLE_NAME)).isEqualTo(2L);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testNarrowProjectionMergesCorrectly(boolean schemaColumnTypes)
    {
        // Neither the ordering field nor _hoodie_is_deleted is projected; the connector must still read
        // them on both the base and log sides for the merge to resolve updates and deletes correctly
        assertQuery(
                session(schemaColumnTypes),
                "SELECT key, value FROM " + EventTimeDeletesHudiTablesInitializer.RT_TABLE_NAME + " ORDER BY key",
                "VALUES ('k1', CAST(11 AS BIGINT)), ('k4', 40), ('k5', 50), ('k6', 60)");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testPredicateIsNotPushedIntoTheBaseReadOfAMergedSplit(boolean schemaColumnTypes)
    {
        // HudiPageSourceProvider enables parquet predicate pushdown for base-file-only splits ONLY; the merge
        // path builds its base page source with it off. That invariant is load-bearing and nothing else pins it:
        // pruning happens on the BASE row group's statistics, before the log records the merge needs are seen.
        //
        // 65 is above every base value (10..60) and below the obsolete k6 update (66), which loses on event time
        // and must not surface. With pushdown enabled on this path the whole row group is pruned, the base side
        // comes back empty, and every log record is then emitted as an insert -- so k6 appears with 66 and the
        // merge is silently skipped. Note the naive shape does NOT discriminate: for a row whose log record wins
        // and carries the full record, dropping the base row still yields the right answer.
        assertQueryReturnsEmptyResult(
                session(schemaColumnTypes),
                "SELECT key, value FROM " + EventTimeDeletesHudiTablesInitializer.RT_TABLE_NAME + " WHERE value > 65");
        // Anchor: the same predicate one step lower does return the rows it should, so the query above is empty
        // because of the merge, not because nothing ever matches.
        assertQuery(
                session(schemaColumnTypes),
                "SELECT key, value FROM " + EventTimeDeletesHudiTablesInitializer.RT_TABLE_NAME + " WHERE value > 45 ORDER BY key",
                "VALUES ('k5', CAST(50 AS BIGINT)), ('k6', 60)");
    }

    @Test
    public void testSchemaColumnTypesFollowTheTypeMapper()
    {
        TrinoTypeMapper typeMapper = new TrinoTypeMapper(new TypeOperators());
        Map<String, String> expectedTypes = TABLE_SCHEMA.getFields().stream()
                .collect(toImmutableMap(HoodieSchemaField::name, field -> typeMapper.toTypeSystem(field.schema()).getDisplayName()));
        for (String table : List.of(SchemaColumnTypesHudiTablesInitializer.TABLE_NAME, SchemaColumnTypesHudiTablesInitializer.RT_TABLE_NAME)) {
            // DESCRIBE resolves the schema through the table handle, information_schema from the metastore entry alone
            assertThat(columnTypes(computeActual(schemaColumnTypesSession(), "DESCRIBE " + table)))
                    .containsAllEntriesOf(expectedTypes);
            assertThat(columnTypes(computeActual(schemaColumnTypesSession(), "SELECT column_name, data_type FROM information_schema.columns"
                    + " WHERE table_schema = '" + getSession().getSchema().orElseThrow() + "' AND table_name = '" + table + "'")))
                    .containsAllEntriesOf(expectedTypes);
            // With the property off the metastore's Hive sync types stand, and differ from the mapper's for these
            assertThat(columnTypes(computeActual("DESCRIBE " + table)))
                    .containsAllEntriesOf(ImmutableMap.of(
                            "c_uuid", "varbinary",
                            "c_time_millis", "integer",
                            "c_timestamp_micros", "bigint",
                            "c_local_timestamp_micros", "bigint",
                            "c_vector_float", "varbinary"));
        }
    }

    @Test
    public void testSchemaColumnTypesSnapshotReadMergesEveryType()
    {
        // k1 is rewritten with seed 8 and k2 nulled in the log file; k3 comes from the base file alone, copied
        // position by position
        assertThat(query(schemaColumnTypesSession(), "SELECT " + schemaColumnList() + " FROM " + SchemaColumnTypesHudiTablesInitializer.RT_TABLE_NAME))
                .matches("VALUES " + join(", ",
                        populatedRow("k1", 200, 8),
                        nullRow("k2", 200),
                        populatedRow("k3", 100, 3)));
    }

    @Test
    public void testSchemaColumnTypesReadOptimizedReadsBaseValues()
    {
        assertThat(query(schemaColumnTypesSession(), "SELECT " + schemaColumnList() + " FROM " + SchemaColumnTypesHudiTablesInitializer.TABLE_NAME))
                .matches("VALUES " + join(", ",
                        populatedRow("k1", 100, 1),
                        populatedRow("k2", 100, 2),
                        populatedRow("k3", 100, 3)));
    }

    @Test
    public void testSchemaColumnTypesPredicates()
    {
        Session session = schemaColumnTypesSession();
        String rtTable = SchemaColumnTypesHudiTablesInitializer.RT_TABLE_NAME;
        String roTable = SchemaColumnTypesHudiTablesInitializer.TABLE_NAME;

        // timestamp(6) with time zone is an instant: the same moment written in another zone matches too
        assertThat(query(session, "SELECT key FROM " + rtTable + " WHERE c_timestamp_micros = TIMESTAMP '" + timestampText(8) + ".123456 UTC'"))
                .matches("VALUES VARCHAR 'k1'");
        assertThat(query(session, "SELECT key FROM " + rtTable + " WHERE c_timestamp_micros = TIMESTAMP '"
                + timestampOf(8).plusHours(8).format(ofPattern("yyyy-MM-dd HH:mm:ss")) + ".123456 +08:00'"))
                .matches("VALUES VARCHAR 'k1'");
        // k1's base value was replaced in the log file: only the read-optimized table still holds it
        assertQueryReturnsEmptyResult(session, "SELECT key FROM " + rtTable + " WHERE c_timestamp_micros = TIMESTAMP '" + timestampText(1) + ".123456 UTC'");
        assertThat(query(session, "SELECT key FROM " + roTable + " WHERE c_timestamp_micros = TIMESTAMP '" + timestampText(1) + ".123456 UTC'"))
                .matches("VALUES VARCHAR 'k1'");
        // A range over timestamp(3) with time zone; k2 is null in the snapshot
        assertThat(query(session, "SELECT key FROM " + rtTable + " WHERE c_timestamp_millis > TIMESTAMP '" + timestampText(2) + ".123 UTC'"))
                .matches("VALUES VARCHAR 'k1', VARCHAR 'k3'");
        assertThat(query(session, "SELECT key FROM " + roTable + " WHERE c_timestamp_millis > TIMESTAMP '" + timestampText(2) + ".123 UTC'"))
                .matches("VALUES VARCHAR 'k3'");

        // The uuid column is stored as its text form; the read converts it, so the predicate compares uuid values
        assertThat(query(session, "SELECT key FROM " + rtTable + " WHERE c_uuid = UUID '" + uuid(3, 0) + "'"))
                .matches("VALUES VARCHAR 'k3'");
        assertQueryReturnsEmptyResult(session, "SELECT key FROM " + rtTable + " WHERE c_uuid = UUID '" + uuid(1, 0) + "'");
        assertThat(query(session, "SELECT key FROM " + roTable + " WHERE c_uuid = UUID '" + uuid(1, 0) + "'"))
                .matches("VALUES VARCHAR 'k1'");
        assertThat(query(session, "SELECT key FROM " + rtTable + " WHERE c_record.id = UUID '" + uuid(8, 1) + "'"))
                .matches("VALUES VARCHAR 'k1'");
    }

    @Test
    public void testSchemaColumnTypesTableReadsWithThePropertyOff()
    {
        // Default session: metastore column types and the legacy HudiAvroSerializer
        assertThat(computeScalar("SELECT count(*) FROM " + SchemaColumnTypesHudiTablesInitializer.RT_TABLE_NAME)).isEqualTo(3L);
        assertThat(computeScalar("SELECT count(*) FROM " + SchemaColumnTypesHudiTablesInitializer.TABLE_NAME)).isEqualTo(3L);
        assertThat(query("SELECT key, c_int, c_string FROM " + SchemaColumnTypesHudiTablesInitializer.RT_TABLE_NAME))
                .matches("VALUES (VARCHAR 'k1', 800, VARCHAR 'string-8'), (VARCHAR 'k2', NULL, NULL), (VARCHAR 'k3', 300, VARCHAR 'string-3')");
        assertThat(query("SELECT key, c_int, c_string FROM " + SchemaColumnTypesHudiTablesInitializer.TABLE_NAME))
                .matches("VALUES (VARCHAR 'k1', 100, VARCHAR 'string-1'), (VARCHAR 'k2', 200, VARCHAR 'string-2'), (VARCHAR 'k3', 300, VARCHAR 'string-3')");
    }

    private Session schemaColumnTypesSession()
    {
        return session(true);
    }

    private Session session(boolean schemaColumnTypes)
    {
        return SessionBuilder.from(getSession())
                .withSchemaColumnTypesEnabled(schemaColumnTypes)
                .build();
    }

    private static Map<String, String> columnTypes(MaterializedResult result)
    {
        return result.getMaterializedRows().stream()
                .collect(toImmutableMap(row -> (String) row.getField(0), row -> (String) row.getField(1)));
    }

    private static String schemaColumnList()
    {
        return TABLE_SCHEMA.getFields().stream()
                .map(HoodieSchemaField::name)
                .collect(joining(", "));
    }

    /**
     * The SQL literals for the values SchemaColumnTypesHudiTablesInitializer writes from {@code seed}, in
     * {@link #schemaColumnList()} order, each typed exactly as its column so the result types are compared too.
     */
    private static String populatedRow(String key, long ts, int seed)
    {
        String timestamp = timestampText(seed);
        String blob = seed % 2 == 1
                ? "ROW(VARCHAR 'INLINE', X'B10B%02X', NULL)".formatted(seed)
                : "ROW(VARCHAR 'OUT_OF_LINE', NULL, ROW(VARCHAR 'file:///blobs/blob-%s.bin', BIGINT '%s', NULL, true))".formatted(seed, 16 * seed);
        return "(" + join(", ",
                "VARCHAR '%s'".formatted(key),
                "BIGINT '%s'".formatted(ts),
                String.valueOf(seed % 2 == 0),
                "INTEGER '%s'".formatted(100 * seed),
                "BIGINT '%s'".formatted(10_000_000_000L * seed),
                "REAL '%s'".formatted(seed + 0.5f),
                "DOUBLE '%s'".formatted(seed + 0.25),
                "VARCHAR 'string-%s'".formatted(seed),
                "X'CAFE%02X'".formatted(seed),
                "VARCHAR '%s'".formatted(List.of("RED", "GREEN", "BLUE").get(seed % 3)),
                "X'DEADBE%02X'".formatted(seed),
                "UUID '%s'".formatted(uuid(seed, 0)),
                "CAST(DECIMAL '-%s.25' AS decimal(10, 2))".formatted(seed),
                "CAST(DECIMAL '1234567890123456.789%s' AS decimal(20, 4))".formatted(seed),
                "DATE '%s'".formatted(LocalDate.ofEpochDay(19_000 + seed)),
                "TIME '%02d:02:03.456'".formatted(seed),
                "TIME '%02d:02:03.456789'".formatted(seed),
                "TIMESTAMP '%s.123 UTC'".formatted(timestamp),
                "TIMESTAMP '%s.123456 UTC'".formatted(timestamp),
                "TIMESTAMP '%s.123'".formatted(timestamp),
                "TIMESTAMP '%s.123456'".formatted(timestamp),
                "ARRAY[BIGINT '%s', BIGINT '%s']".formatted(seed, seed + 1),
                "MAP(ARRAY[VARCHAR 'a', VARCHAR 'b'], ARRAY[BIGINT '%s', BIGINT '%s'])".formatted(seed, 10 * seed),
                "CAST(ROW(VARCHAR 'record-%s', UUID '%s') AS ROW(\"name\" VARCHAR, \"id\" UUID))".formatted(seed, uuid(seed, 1)),
                "ARRAY[REAL '%s', REAL '%s', REAL '%s']".formatted((float) seed, seed + 0.5f, (float) -seed),
                "ARRAY[DOUBLE '%s', DOUBLE '%s']".formatted(seed + 0.25, -(seed + 0.25)),
                "ARRAY[TINYINT '%s', TINYINT '%s', TINYINT '127', TINYINT '-128']".formatted(seed, -seed),
                ("CAST(%s AS ROW(\"type\" VARCHAR, \"data\" VARBINARY,"
                        + " \"reference\" ROW(\"external_path\" VARCHAR, \"offset\" BIGINT, \"length\" BIGINT, \"managed\" BOOLEAN)))").formatted(blob),
                "CAST(ROW(X'010000', X'0C%02X') AS ROW(\"metadata\" VARBINARY, \"value\" VARBINARY))".formatted(seed)) + ")";
    }

    private static String nullRow(String key, long ts)
    {
        // Every column but the record key and the ordering field
        int typedColumns = TABLE_SCHEMA.getFields().size() - 2;
        return "(VARCHAR '%s', BIGINT '%s', %s)".formatted(key, ts, join(", ", Collections.nCopies(typedColumns, "NULL")));
    }

    /**
     * Seed 0 is 2023-11-14 22:13:20 UTC, epoch second 1_700_000_000; each seed adds a day.
     */
    private static LocalDateTime timestampOf(int seed)
    {
        return LocalDateTime.of(2023, 11, 14, 22, 13, 20).plusDays(seed);
    }

    private static String timestampText(int seed)
    {
        return timestampOf(seed).format(ofPattern("yyyy-MM-dd HH:mm:ss"));
    }

    private static String uuid(int seed, int discriminator)
    {
        return "%08x-%04x-4000-8000-%012x".formatted(seed, discriminator, seed);
    }
}
