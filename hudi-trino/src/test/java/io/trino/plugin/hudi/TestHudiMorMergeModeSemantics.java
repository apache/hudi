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
import io.trino.plugin.hudi.testing.CommitTimeOrderingHudiTablesInitializer;
import io.trino.plugin.hudi.testing.CompositeHudiTablesInitializer;
import io.trino.plugin.hudi.testing.EventTimeDeletesHudiTablesInitializer;
import io.trino.plugin.hudi.testing.EventTimeDeletesHudiTablesInitializer.OrderingType;
import io.trino.plugin.hudi.testing.HoodieSchemaTypesHudiTablesInitializer;
import io.trino.plugin.hudi.testing.MislabeledTimestampHudiTablesInitializer;
import io.trino.plugin.hudi.testing.NestedNullFieldHudiTablesInitializer;
import io.trino.plugin.hudi.util.TrinoTypeMapper;
import io.trino.spi.type.TypeOperators;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.plugin.hudi.testing.HoodieSchemaTypesHudiTablesInitializer.ENUM_SYMBOLS;
import static io.trino.plugin.hudi.testing.HoodieSchemaTypesHudiTablesInitializer.TABLE_SCHEMA;
import static io.trino.plugin.hudi.testing.HoodieSchemaTypesHudiTablesInitializer.uuid;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
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
 * The table written by {@link HoodieSchemaTypesHudiTablesInitializer} holds one column per HoodieSchema type, so its
 * merge carries every type the connector maps through the HoodieSchema-driven serializer.
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
                        new EventTimeDeletesHudiTablesInitializer(OrderingType.DECIMAL),
                        new EventTimeDeletesHudiTablesInitializer(OrderingType.TIMESTAMP_MILLIS),
                        new CommitTimeOrderingHudiTablesInitializer(),
                        new HoodieSchemaTypesHudiTablesInitializer(),
                        HoodieSchemaTypesHudiTablesInitializer.withPayloadMerge(),
                        new NestedNullFieldHudiTablesInitializer(),
                        new MislabeledTimestampHudiTablesInitializer()))
                .build();
    }

    @Test
    public void testReadOptimizedShowsAllBaseRows()
    {
        // Deletes and updates live in log files only; the read-optimized tables reflect the base commit
        assertQuery(
                "SELECT key, name, value FROM " + EventTimeDeletesHudiTablesInitializer.TABLE_NAME + " ORDER BY key",
                "VALUES ('k1', 'k1_base', CAST(10 AS BIGINT)), ('k2', 'k2_base', 20), ('k3', 'k3_base', 30),"
                        + " ('k4', 'k4_base', 40), ('k5', 'k5_base', 50), ('k6', 'k6_base', 60)");
        assertQuery(
                "SELECT key, name, value FROM " + CommitTimeOrderingHudiTablesInitializer.TABLE_NAME + " ORDER BY key",
                "VALUES ('k1', 'k1_base', CAST(10 AS BIGINT)), ('k2', 'k2_base', 20), ('k3', 'k3_base', 30)");
    }

    @ParameterizedTest
    @EnumSource(OrderingType.class)
    public void testEventTimeMergeWithDeletes(OrderingType orderingType)
    {
        // k1: higher-ts update wins; k2: hard-deleted; k3: soft-deleted (higher ts);
        // k4: OBSOLETE soft delete (lower ts) -> base row survives;
        // k5: untouched; k6: OBSOLETE update (lower ts) -> base row survives.
        // A decimal or timestamp ordering field compares the base row's converted value against the log's
        assertQuery(
                "SELECT key, name, value FROM " + orderingType.rtTableName() + " ORDER BY key",
                "VALUES ('k1', 'k1_updated', CAST(11 AS BIGINT)), ('k4', 'k4_base', 40),"
                        + " ('k5', 'k5_base', 50), ('k6', 'k6_base', 60)");
    }

    @Test
    public void testHardDeleteRemovesRowOnSnapshotRead()
    {
        // The hard delete is a native delete log file, resolved through the connector's
        // getFileRecordIterator with the synthetic delete-log schema (record key + ordering field)
        assertThat(computeScalar("SELECT count(*) FROM " + EventTimeDeletesHudiTablesInitializer.RT_TABLE_NAME + " WHERE key = 'k2'"))
                .isEqualTo(0L);
        assertThat(computeScalar("SELECT count(*) FROM " + EventTimeDeletesHudiTablesInitializer.TABLE_NAME + " WHERE key = 'k2'"))
                .isEqualTo(1L);
    }

    @Test
    public void testSoftDeleteRemovesRowOnSnapshotRead()
    {
        // _hoodie_is_deleted=true log record with a winning (higher) ordering value
        assertThat(computeScalar("SELECT count(*) FROM " + EventTimeDeletesHudiTablesInitializer.RT_TABLE_NAME + " WHERE key = 'k3'"))
                .isEqualTo(0L);
        assertThat(computeScalar("SELECT count(*) FROM " + EventTimeDeletesHudiTablesInitializer.TABLE_NAME + " WHERE key = 'k3'"))
                .isEqualTo(1L);
    }

    @Test
    public void testObsoleteSoftDeleteLosesUnderEventTimeOrdering()
    {
        // The k4 soft delete carries ts=50 < base ts=100: event-time merging must keep the base row
        assertQuery(
                "SELECT key, name, value FROM " + EventTimeDeletesHudiTablesInitializer.RT_TABLE_NAME + " WHERE key = 'k4'",
                "VALUES ('k4', 'k4_base', CAST(40 AS BIGINT))");
    }

    @Test
    public void testCommitTimeOrderingKeepsLatestWrite()
    {
        // k1's update carries ts=50 < base ts=100. Under COMMIT_TIME_ORDERING the LATEST WRITE wins
        // regardless of the ordering value -- the mirror of the event-time k6 case, where the same
        // shape keeps the BASE row. Together they discriminate the two merger dispatches.
        assertQuery(
                "SELECT key, name, value FROM " + CommitTimeOrderingHudiTablesInitializer.RT_TABLE_NAME + " ORDER BY key",
                "VALUES ('k1', 'k1_updated', CAST(11 AS BIGINT)), ('k3', 'k3_base', 30)");
    }

    @Test
    public void testSchemaStringOverridesMetastoreChar()
    {
        // The metastore declares code char(5) while the Hudi table schema holds a string: the column reads as
        // varchar, so values come back unpadded
        assertThat(columnTypes(computeActual("DESCRIBE " + CommitTimeOrderingHudiTablesInitializer.RT_TABLE_NAME)))
                .containsEntry("code", "varchar");
        assertThat(columnTypes(computeActual("DESCRIBE " + CommitTimeOrderingHudiTablesInitializer.TABLE_NAME)))
                .containsEntry("code", "varchar");
    }

    @Test
    public void testStringMatchesBaseReaderOnUntouchedRow()
    {
        String query = "SELECT code FROM %s WHERE key = 'k3'";
        Object readOptimized = computeScalar(query.formatted(CommitTimeOrderingHudiTablesInitializer.TABLE_NAME));
        assertThat(readOptimized).isEqualTo(" ab");
        assertThat(computeScalar(query.formatted(CommitTimeOrderingHudiTablesInitializer.RT_TABLE_NAME))).isEqualTo(readOptimized);
    }

    @Test
    public void testStringFilterMatchesLeadingSpaceOnSnapshotRead()
    {
        assertQuery(
                "SELECT key FROM " + CommitTimeOrderingHudiTablesInitializer.RT_TABLE_NAME + " WHERE code = ' ab'",
                "VALUES 'k3'");
        assertQueryReturnsEmptyResult(
                "SELECT key FROM " + CommitTimeOrderingHudiTablesInitializer.RT_TABLE_NAME + " WHERE code = 'ab'");
    }

    @Test
    public void testStringKeepsTabsOnUpdatedRow()
    {
        assertThat(computeScalar("SELECT code FROM " + CommitTimeOrderingHudiTablesInitializer.RT_TABLE_NAME + " WHERE key = 'k1'"))
                .isEqualTo("\tx\t");
    }

    @Test
    public void testCountAfterDeletes()
    {
        assertThat(computeScalar("SELECT count(*) FROM " + EventTimeDeletesHudiTablesInitializer.RT_TABLE_NAME)).isEqualTo(4L);
        assertThat(computeScalar("SELECT count(*) FROM " + CommitTimeOrderingHudiTablesInitializer.RT_TABLE_NAME)).isEqualTo(2L);
    }

    @Test
    public void testNarrowProjectionMergesCorrectly()
    {
        // Neither the ordering field nor _hoodie_is_deleted is projected; the connector must still read
        // them on both the base and log sides for the merge to resolve updates and deletes correctly
        assertQuery(
                "SELECT key, value FROM " + EventTimeDeletesHudiTablesInitializer.RT_TABLE_NAME + " ORDER BY key",
                "VALUES ('k1', CAST(11 AS BIGINT)), ('k4', 40), ('k5', 50), ('k6', 60)");
    }

    @Test
    public void testPredicateIsNotPushedIntoTheBaseReadOfAMergedSplit()
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
                "SELECT key, value FROM " + EventTimeDeletesHudiTablesInitializer.RT_TABLE_NAME + " WHERE value > 65");
        // Anchor: the same predicate one step lower does return the rows it should, so the query above is empty
        // because of the merge, not because nothing ever matches.
        assertQuery(
                "SELECT key, value FROM " + EventTimeDeletesHudiTablesInitializer.RT_TABLE_NAME + " WHERE value > 45 ORDER BY key",
                "VALUES ('k5', CAST(50 AS BIGINT)), ('k6', 60)");
    }

    @Test
    public void testColumnTypesFollowTheTypeMapper()
    {
        TrinoTypeMapper typeMapper = new TrinoTypeMapper(new TypeOperators());
        Map<String, String> expectedTypes = TABLE_SCHEMA.getFields().stream()
                .collect(toImmutableMap(HoodieSchemaField::name, field -> typeMapper.toTypeSystem(field.schema()).getDisplayName()));
        // Unfiltered by table name, information_schema goes through the column listing (streamRelationColumns), which
        // has no table handle; DESCRIBE resolves the schema through the table handle
        MaterializedResult listing = computeActual("SELECT table_name, column_name, data_type FROM information_schema.columns"
                + " WHERE table_schema = '" + getSession().getSchema().orElseThrow() + "'");
        for (String table : List.of(HoodieSchemaTypesHudiTablesInitializer.TABLE_NAME, HoodieSchemaTypesHudiTablesInitializer.RT_TABLE_NAME)) {
            Map<String, String> describedTypes = columnTypes(computeActual("DESCRIBE " + table));
            assertThat(describedTypes).containsAllEntriesOf(expectedTypes);
            // The metastore declares these the way Hive sync does (binary, int, bigint); the schema types replace them
            assertThat(describedTypes).containsAllEntriesOf(ImmutableMap.of(
                    "c_uuid", "uuid",
                    "c_time_millis", "time(3)",
                    "c_timestamp_micros", "timestamp(6) with time zone",
                    "c_local_timestamp_micros", "timestamp(6)",
                    "c_vector_float", "array(real)"));
            Map<String, String> listedTypes = listing.getMaterializedRows().stream()
                    .filter(row -> row.getField(0).equals(table))
                    .collect(toImmutableMap(row -> (String) row.getField(1), row -> (String) row.getField(2)));
            assertThat(listedTypes).containsAllEntriesOf(expectedTypes);
        }
    }

    @Test
    public void testUtcAdjustedTimestampIsAnInstantInEverySessionZone()
    {
        // A UTC-adjusted timestamp reads as an instant, as Spark writes it. Flink stores its wall-clock TIMESTAMP(p)
        // UTC-adjusted too, so a Flink table's values read as that wall-clock time at UTC: a plain timestamp literal is
        // taken at the session zone and matches only in a UTC session
        Session losAngeles = Session.builder(getSession())
                .setTimeZoneKey(getTimeZoneKey("America/Los_Angeles"))
                .build();
        Session utc = Session.builder(getSession())
                .setTimeZoneKey(UTC_KEY)
                .build();
        String rtTable = HoodieSchemaTypesHudiTablesInitializer.RT_TABLE_NAME;
        String instant = "TIMESTAMP '" + timestampText(3) + ".123 UTC'";
        String wallClock = "TIMESTAMP '" + timestampText(3) + ".123'";
        assertThat(query(losAngeles, "SELECT key FROM " + rtTable + " WHERE c_timestamp_millis = " + instant))
                .matches("VALUES VARCHAR 'k3'");
        assertQueryReturnsEmptyResult(losAngeles, "SELECT key FROM " + rtTable + " WHERE c_timestamp_millis = " + wallClock);
        assertThat(query(utc, "SELECT key FROM " + rtTable + " WHERE c_timestamp_millis = " + wallClock))
                .matches("VALUES VARCHAR 'k3'");
        // The value keeps its zone whatever the session's
        assertThat(query(losAngeles, "SELECT c_timestamp_millis FROM " + rtTable + " WHERE key = 'k3'"))
                .matches("VALUES " + instant);
    }

    @Test
    public void testMislabeledTimestampsReadAsTheTableSchemaSays()
    {
        // k1's base and log files label c_ts micros and c_local_ts a plain long, both holding millis; k2's file is
        // written as the table schema says. All read as the table schema's millis, as Spark's readers repair them: the
        // read-optimized table from the base files, the snapshot through the merge of k1's base row and log record
        String roTable = MislabeledTimestampHudiTablesInitializer.TABLE_NAME;
        String rtTable = MislabeledTimestampHudiTablesInitializer.RT_TABLE_NAME;
        String k2 = " (VARCHAR 'k2', TIMESTAMP '2023-11-15 22:13:20.123 UTC', TIMESTAMP '2023-11-15 22:13:20.123')";
        assertThat(query("SELECT key, c_ts, c_local_ts FROM " + roTable))
                .matches("VALUES (VARCHAR 'k1', TIMESTAMP '2023-11-14 22:13:20.123 UTC', TIMESTAMP '2023-11-14 22:13:20.123')," + k2);
        assertThat(query("SELECT key, c_ts, c_local_ts FROM " + rtTable))
                .matches("VALUES (VARCHAR 'k1', TIMESTAMP '2023-11-14 23:13:20.123 UTC', TIMESTAMP '2023-11-14 23:13:20.123')," + k2);

        // The mislabeled base file's statistics read a thousand times small, so a pushed-down predicate would drop k1's
        // row group: this finds k1 only because the domain is not pushed into that file. A split with log files never
        // pushes down, so only the read-optimized table can show it
        assertThat(query("SELECT key FROM " + roTable + " WHERE c_ts = TIMESTAMP '2023-11-14 22:13:20.123 UTC'"))
                .matches("VALUES VARCHAR 'k1'");
    }

    @Test
    public void testSnapshotReadMergesEveryHoodieSchemaType()
    {
        // k1 is rewritten with seed 8 and k2 nulled in the log file; k3 comes from the base file alone, through
        // serialize() and back
        assertThat(query("SELECT " + schemaColumnList() + " FROM " + HoodieSchemaTypesHudiTablesInitializer.RT_TABLE_NAME))
                .matches("VALUES " + join(", ",
                        populatedRow("k1", 200, 8),
                        nullRow("k2", 200),
                        populatedRow("k3", 100, 3)));
    }

    @Test
    public void testPayloadMergeCarriesEveryHoodieSchemaType()
    {
        // The payload serializes the base record the read built for every type, and merges as commit time does: k1's
        // update and k2's nulls win
        assertThat(query("SELECT " + schemaColumnList() + " FROM " + HoodieSchemaTypesHudiTablesInitializer.PAYLOAD_RT_TABLE_NAME))
                .matches("VALUES " + join(", ",
                        populatedRow("k1", 200, 8),
                        nullRow("k2", 200),
                        populatedRow("k3", 100, 3)));
    }

    @Test
    public void testNestedNullFieldIsLeftOutOfBothReads()
    {
        // The base file alone, and the merge of k1's base row with its log record
        assertQuery("SELECT key, c_record.a FROM " + NestedNullFieldHudiTablesInitializer.TABLE_NAME, "VALUES ('k1', 'base')");
        assertQuery("SELECT key, c_record.a FROM " + NestedNullFieldHudiTablesInitializer.RT_TABLE_NAME, "VALUES ('k1', 'updated')");
    }

    @Test
    public void testReadOptimizedReadsEveryHoodieSchemaType()
    {
        assertThat(query("SELECT " + schemaColumnList() + " FROM " + HoodieSchemaTypesHudiTablesInitializer.TABLE_NAME))
                .matches("VALUES " + join(", ",
                        populatedRow("k1", 100, 1),
                        populatedRow("k2", 100, 2),
                        populatedRow("k3", 100, 3)));
    }

    @Test
    public void testPredicatesOnHoodieSchemaTypes()
    {
        Session session = getSession();
        String rtTable = HoodieSchemaTypesHudiTablesInitializer.RT_TABLE_NAME;
        String roTable = HoodieSchemaTypesHudiTablesInitializer.TABLE_NAME;

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
     * The SQL literals for the values HoodieSchemaTypesHudiTablesInitializer writes from {@code seed}, in
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
                "VARCHAR '%s'".formatted(ENUM_SYMBOLS.get(seed % ENUM_SYMBOLS.size())),
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
                "ARRAY[UUID '%s', UUID '%s']".formatted(uuid(seed, 2), uuid(seed, 3)),
                "MAP(ARRAY[VARCHAR 'a'], ARRAY[UUID '%s'])".formatted(uuid(seed, 4)),
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
}
