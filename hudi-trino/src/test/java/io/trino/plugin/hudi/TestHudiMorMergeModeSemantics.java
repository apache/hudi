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

import io.trino.plugin.hudi.testing.CommitTimeOrderingHudiTablesInitializer;
import io.trino.plugin.hudi.testing.CompositeHudiTablesInitializer;
import io.trino.plugin.hudi.testing.EventTimeDeletesHudiTablesInitializer;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

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
                        new CommitTimeOrderingHudiTablesInitializer()))
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

    @Test
    public void testEventTimeMergeWithDeletes()
    {
        // k1: higher-ts update wins; k2: hard-deleted; k3: soft-deleted (higher ts);
        // k4: OBSOLETE soft delete (lower ts) -> base row survives;
        // k5: untouched; k6: OBSOLETE update (lower ts) -> base row survives
        assertQuery(
                "SELECT key, name, value FROM " + EventTimeDeletesHudiTablesInitializer.RT_TABLE_NAME + " ORDER BY key",
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
}
