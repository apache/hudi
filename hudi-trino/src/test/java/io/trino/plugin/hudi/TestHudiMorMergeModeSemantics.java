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

import io.trino.plugin.hudi.testing.ColumnTypesHudiTablesInitializer;
import io.trino.plugin.hudi.testing.CommitTimeOrderingHudiTablesInitializer;
import io.trino.plugin.hudi.testing.CompositeHudiTablesInitializer;
import io.trino.plugin.hudi.testing.EventTimeDeletesHudiTablesInitializer;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import static io.trino.plugin.hudi.testing.AbstractMergerHudiTablesInitializer.spillDirectory;
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
 * Each snapshot-read test runs on a fixture's real-time table and on its spilling twin, whose reads write every
 * log record and delete to disk and read it back before merging it.
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
                        ColumnTypesHudiTablesInitializer.avroLogBlocks(),
                        ColumnTypesHudiTablesInitializer.avroLogBlocksSpillingEveryLogRecord()))
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
    @ValueSource(strings = {EventTimeDeletesHudiTablesInitializer.RT_TABLE_NAME, EventTimeDeletesHudiTablesInitializer.SPILLING_RT_TABLE_NAME})
    public void testEventTimeMergeWithDeletes(String rtTable)
    {
        // k1: higher-ts update wins, and its second, lower-ts update loses to it; k2: hard-deleted;
        // k3: soft-deleted (higher ts); k4: OBSOLETE soft delete (lower ts) -> base row survives;
        // k5: untouched; k6: OBSOLETE update (lower ts) -> base row survives
        assertQuery(
                "SELECT key, name, value FROM " + rtTable + " ORDER BY key",
                "VALUES ('k1', 'k1_updated', CAST(11 AS BIGINT)), ('k4', 'k4_base', 40),"
                        + " ('k5', 'k5_base', 50), ('k6', 'k6_base', 60)");
    }

    @ParameterizedTest
    @ValueSource(strings = {EventTimeDeletesHudiTablesInitializer.RT_TABLE_NAME, EventTimeDeletesHudiTablesInitializer.SPILLING_RT_TABLE_NAME})
    public void testHardDeleteRemovesRowOnSnapshotRead(String rtTable)
    {
        // The hard delete is a native delete log file, resolved through the connector's
        // getFileRecordIterator with the synthetic delete-log schema (record key + ordering field)
        assertThat(computeScalar("SELECT count(*) FROM " + rtTable + " WHERE key = 'k2'"))
                .isEqualTo(0L);
        assertThat(computeScalar("SELECT count(*) FROM " + EventTimeDeletesHudiTablesInitializer.TABLE_NAME + " WHERE key = 'k2'"))
                .isEqualTo(1L);
    }

    @ParameterizedTest
    @ValueSource(strings = {EventTimeDeletesHudiTablesInitializer.RT_TABLE_NAME, EventTimeDeletesHudiTablesInitializer.SPILLING_RT_TABLE_NAME})
    public void testSoftDeleteRemovesRowOnSnapshotRead(String rtTable)
    {
        // _hoodie_is_deleted=true log record with a winning (higher) ordering value
        assertThat(computeScalar("SELECT count(*) FROM " + rtTable + " WHERE key = 'k3'"))
                .isEqualTo(0L);
        assertThat(computeScalar("SELECT count(*) FROM " + EventTimeDeletesHudiTablesInitializer.TABLE_NAME + " WHERE key = 'k3'"))
                .isEqualTo(1L);
    }

    @ParameterizedTest
    @ValueSource(strings = {EventTimeDeletesHudiTablesInitializer.RT_TABLE_NAME, EventTimeDeletesHudiTablesInitializer.SPILLING_RT_TABLE_NAME})
    public void testObsoleteSoftDeleteLosesUnderEventTimeOrdering(String rtTable)
    {
        // The k4 soft delete carries ts=50 < base ts=100: event-time merging must keep the base row
        assertQuery(
                "SELECT key, name, value FROM " + rtTable + " WHERE key = 'k4'",
                "VALUES ('k4', 'k4_base', CAST(40 AS BIGINT))");
    }

    @ParameterizedTest
    @ValueSource(strings = {CommitTimeOrderingHudiTablesInitializer.RT_TABLE_NAME, CommitTimeOrderingHudiTablesInitializer.SPILLING_RT_TABLE_NAME})
    public void testCommitTimeOrderingKeepsLatestWrite(String rtTable)
    {
        // k1's update carries ts=50 < base ts=100. Under COMMIT_TIME_ORDERING the LATEST WRITE wins
        // regardless of the ordering value -- the mirror of the event-time k6 case, where the same
        // shape keeps the BASE row. Together they discriminate the two merger dispatches.
        assertQuery(
                "SELECT key, name, value FROM " + rtTable + " ORDER BY key",
                "VALUES ('k1', 'k1_updated', CAST(11 AS BIGINT)), ('k3', 'k3_base', 30)");
    }

    @ParameterizedTest
    @ValueSource(strings = {CommitTimeOrderingHudiTablesInitializer.RT_TABLE_NAME, CommitTimeOrderingHudiTablesInitializer.SPILLING_RT_TABLE_NAME})
    public void testCharMatchesBaseReaderOnUntouchedRow(String rtTable)
    {
        String query = "SELECT code FROM %s WHERE key = 'k3'";
        Object readOptimized = computeScalar(query.formatted(CommitTimeOrderingHudiTablesInitializer.TABLE_NAME));
        assertThat(readOptimized).isEqualTo(" ab  ");
        assertThat(computeScalar(query.formatted(rtTable))).isEqualTo(readOptimized);
    }

    @ParameterizedTest
    @ValueSource(strings = {CommitTimeOrderingHudiTablesInitializer.RT_TABLE_NAME, CommitTimeOrderingHudiTablesInitializer.SPILLING_RT_TABLE_NAME})
    public void testCharFilterMatchesLeadingSpaceOnSnapshotRead(String rtTable)
    {
        assertQuery(
                "SELECT key FROM " + rtTable + " WHERE code = CAST(' ab' AS CHAR(5))",
                "VALUES 'k3'");
    }

    @ParameterizedTest
    @ValueSource(strings = {CommitTimeOrderingHudiTablesInitializer.RT_TABLE_NAME, CommitTimeOrderingHudiTablesInitializer.SPILLING_RT_TABLE_NAME})
    public void testCharKeepsTabsOnUpdatedRow(String rtTable)
    {
        assertThat(computeScalar("SELECT code FROM " + rtTable + " WHERE key = 'k1'"))
                .isEqualTo("\tx\t  ");
    }

    @Test
    public void testSpilledMergeMatchesInMemoryMerge()
    {
        // Every column, including the ordering and delete marker fields the merge reads but the tests above do not
        // select, against the same commits merged in memory
        assertThat(query("SELECT key, name, value, ts, _hoodie_is_deleted FROM " + EventTimeDeletesHudiTablesInitializer.SPILLING_RT_TABLE_NAME))
                .matches("SELECT key, name, value, ts, _hoodie_is_deleted FROM " + EventTimeDeletesHudiTablesInitializer.RT_TABLE_NAME);
        assertThat(query("SELECT key, name, value, ts, code FROM " + CommitTimeOrderingHudiTablesInitializer.SPILLING_RT_TABLE_NAME))
                .matches("SELECT key, name, value, ts, code FROM " + CommitTimeOrderingHudiTablesInitializer.RT_TABLE_NAME);
        // Only a spill creates the spill directory, so the spilling twins are not merged in memory as well
        assertThat(spillDirectory(EventTimeDeletesHudiTablesInitializer.SPILLING_TABLE_NAME)).isDirectory();
        assertThat(spillDirectory(CommitTimeOrderingHudiTablesInitializer.SPILLING_TABLE_NAME)).isDirectory();
    }

    @ParameterizedTest
    @CsvSource({
            // Native log file: the log rows are Trino objects
            ColumnTypesHudiTablesInitializer.SPILLING_TABLE_NAME + ", " + ColumnTypesHudiTablesInitializer.RT_TABLE_NAME,
            // Avro data blocks of a table version 9 log file: the log rows are Avro values
            ColumnTypesHudiTablesInitializer.AVRO_LOG_SPILLING_TABLE_NAME + ", " + ColumnTypesHudiTablesInitializer.AVRO_LOG_RT_TABLE_NAME})
    public void testSpilledMergeKeepsEveryColumnType(String spillingTable, String inMemoryRtTable)
    {
        String spillingRtTable = spillingTable + "_rt";
        String columns = "key, ts, c_tinyint, c_smallint, c_decimal, c_date, c_timestamp, c_double, c_binary, c_array, c_map, c_struct";
        // k1 is updated and k2 is nulled in the log file; k3 comes from the base file alone
        assertThat(query("SELECT key, ts, c_tinyint, c_smallint, c_decimal, c_date, c_double, c_array, c_struct.name FROM " + spillingRtTable))
                .matches("VALUES"
                        + " (VARCHAR 'k1', BIGINT '200', TINYINT '7', SMALLINT '1007', CAST(7.25 AS decimal(10, 2)), DATE '2022-01-15', DOUBLE '7.5', ARRAY[BIGINT '7', BIGINT '8'], VARCHAR 'struct-7'),"
                        + " (VARCHAR 'k2', BIGINT '200', NULL, NULL, NULL, NULL, NULL, NULL, NULL),"
                        + " (VARCHAR 'k3', BIGINT '100', TINYINT '3', SMALLINT '1003', CAST(3.25 AS decimal(10, 2)), DATE '2022-01-11', DOUBLE '3.5', ARRAY[BIGINT '3', BIGINT '4'], VARCHAR 'struct-3')");
        assertThat(query("SELECT " + columns + " FROM " + spillingRtTable))
                .matches("SELECT " + columns + " FROM " + inMemoryRtTable);
        assertThat(spillDirectory(spillingTable)).isDirectory();
    }

    @ParameterizedTest
    @CsvSource({
            EventTimeDeletesHudiTablesInitializer.RT_TABLE_NAME + ", " + CommitTimeOrderingHudiTablesInitializer.RT_TABLE_NAME,
            EventTimeDeletesHudiTablesInitializer.SPILLING_RT_TABLE_NAME + ", " + CommitTimeOrderingHudiTablesInitializer.SPILLING_RT_TABLE_NAME})
    public void testCountAfterDeletes(String eventTimeRtTable, String commitTimeRtTable)
    {
        assertThat(computeScalar("SELECT count(*) FROM " + eventTimeRtTable)).isEqualTo(4L);
        assertThat(computeScalar("SELECT count(*) FROM " + commitTimeRtTable)).isEqualTo(2L);
    }

    @ParameterizedTest
    @ValueSource(strings = {EventTimeDeletesHudiTablesInitializer.RT_TABLE_NAME, EventTimeDeletesHudiTablesInitializer.SPILLING_RT_TABLE_NAME})
    public void testNarrowProjectionMergesCorrectly(String rtTable)
    {
        // Neither the ordering field nor _hoodie_is_deleted is projected; the connector must still read
        // them on both the base and log sides for the merge to resolve updates and deletes correctly
        assertQuery(
                "SELECT key, value FROM " + rtTable + " ORDER BY key",
                "VALUES ('k1', CAST(11 AS BIGINT)), ('k4', 40), ('k5', 50), ('k6', 60)");
    }

    @ParameterizedTest
    @ValueSource(strings = {EventTimeDeletesHudiTablesInitializer.RT_TABLE_NAME, EventTimeDeletesHudiTablesInitializer.SPILLING_RT_TABLE_NAME})
    public void testPredicateIsNotPushedIntoTheBaseReadOfAMergedSplit(String rtTable)
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
                "SELECT key, value FROM " + rtTable + " WHERE value > 65");
        // Anchor: the same predicate one step lower does return the rows it should, so the query above is empty
        // because of the merge, not because nothing ever matches.
        assertQuery(
                "SELECT key, value FROM " + rtTable + " WHERE value > 45 ORDER BY key",
                "VALUES ('k5', CAST(50 AS BIGINT)), ('k6', 60)");
    }
}
