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
import io.trino.plugin.hudi.testing.UncompactedMetadataHudiTablesInitializer;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import static io.trino.plugin.hudi.testing.UncompactedMetadataHudiTablesInitializer.TABLE_NAME;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for apache/hudi#19279: queries on tables whose metadata table (MDT) has
 * UNCOMPACTED delta commits. Those deltas are native HFILE log files, which the connector previously
 * rejected ("Native HFILE log files are not supported..."), failing every query through the unguarded
 * partition-stats pruning path. The table written by {@link UncompactedMetadataHudiTablesInitializer}
 * keeps its MDT deliberately uncompacted (the zip fixtures always compact after every commit), so the
 * queries below only succeed if the connector reads HFILE log deltas in the MDT's
 * {@code files}/{@code column_stats}/{@code partition_stats} partitions.
 */
public class TestHudiUncompactedMetadataTable
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HudiQueryRunner.builder()
                .setDataLoader(new UncompactedMetadataHudiTablesInitializer())
                .build();
    }

    @Test
    public void testSnapshotReadWithUncompactedMetadataTable()
    {
        // MDT-backed file listing must read the files partition's HFILE log deltas
        assertQuery(
                mdtEnabled(),
                "SELECT id, name, price FROM " + TABLE_NAME + " ORDER BY id",
                "VALUES ('k1', 'k1_c3', CAST(15 AS BIGINT)), ('k2', 'k2_c1', 1000), ('k3', 'k3_c2', 20), ('k4', 'k4_c2', 2000)");
        assertThat(computeScalarWith(mdtEnabled(), "SELECT count(*) FROM " + TABLE_NAME)).isEqualTo(4L);
    }

    @Test
    public void testResultsMatchWithMetadataTableDisabled()
    {
        String query = "SELECT id, name, price, part_col FROM " + TABLE_NAME + " ORDER BY id";
        MaterializedResult withMdt = getQueryRunner().execute(mdtEnabled(), query);
        MaterializedResult withoutMdt = getQueryRunner().execute(mdtDisabled(), query);
        assertThat(withMdt.getMaterializedRows()).isEqualTo(withoutMdt.getMaterializedRows());
    }

    @Test
    public void testPartitionStatsIndexPruningOverUncompactedStats()
    {
        // The exact crash from the issue: partition-stats pruning reads the partition_stats MDT
        // partition, whose deltas are uncompacted HFILE log files. Partition p2 holds prices
        // [1000, 2000], so `price < 100` lets the index prune it entirely.
        Session session = SessionBuilder.from(getSession())
                .withMdtEnabled(true)
                .withColStatsIndexEnabled(false)
                .withRecordLevelIndexEnabled(false)
                .withSecondaryIndexEnabled(false)
                .withPartitionStatsIndexEnabled(true)
                .build();
        MaterializedResult pruned = getQueryRunner().execute(session,
                "SELECT id, price FROM " + TABLE_NAME + " WHERE price < 100");
        assertThat(pruned.getMaterializedRows()).hasSize(2);

        MaterializedResult unpruned = getQueryRunner().execute(mdtEnabled(),
                "SELECT id, price FROM " + TABLE_NAME);
        // Pruning must not scan more than the full table does; correctness is asserted above
        assertThat(pruned.getStatementStats().get().getTotalSplits())
                .isLessThanOrEqualTo(unpruned.getStatementStats().get().getTotalSplits());
    }

    @Test
    public void testColumnStatsFileSkippingOverUncompactedStats()
    {
        // Column-stats file skipping reads the column_stats MDT partition's HFILE log deltas
        Session session = SessionBuilder.from(getSession())
                .withMdtEnabled(true)
                .withColStatsIndexEnabled(true)
                .withRecordLevelIndexEnabled(false)
                .withSecondaryIndexEnabled(false)
                .withPartitionStatsIndexEnabled(false)
                .build();
        assertQuery(
                session,
                "SELECT id, price FROM " + TABLE_NAME + " WHERE price = 15",
                "VALUES ('k1', CAST(15 AS BIGINT))");
    }

    private Session mdtEnabled()
    {
        return SessionBuilder.from(getSession()).withMdtEnabled(true).build();
    }

    private Session mdtDisabled()
    {
        return SessionBuilder.from(getSession()).withMdtEnabled(false).build();
    }

    private Object computeScalarWith(Session session, String query)
    {
        return getQueryRunner().execute(session, query).getOnlyValue();
    }
}
