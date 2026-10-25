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

import static io.trino.plugin.hudi.testing.UncompactedMetadataHudiTablesInitializer.CORRUPTED_TABLE_NAME;
import static io.trino.plugin.hudi.testing.UncompactedMetadataHudiTablesInitializer.TABLE_NAME;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for apache/hudi#19279: queries on tables whose metadata table (MDT) has
 * UNCOMPACTED delta commits. Those deltas are native HFILE log files, which the connector previously
 * rejected ("Native HFILE log files are not supported..."), failing every query through the unguarded
 * partition-stats pruning path. The table written by {@link UncompactedMetadataHudiTablesInitializer}
 * keeps its MDT deliberately uncompacted and its deltas are whole-file native HFILE logs (the zip
 * fixtures predate that write path; their block-format deltas were always readable), so the queries
 * below only succeed if the connector reads native HFILE log files in the MDT's
 * {@code files}/{@code column_stats}/{@code partition_stats} partitions.
 * <p>
 * The initializer also writes {@code CORRUPTED_TABLE_NAME}, an identical table whose MDT log files
 * are corrupted so every MDT read throws; queries on it pin the fallbacks (direct file listing,
 * unpruned split generation) instead of only the clean-read path.
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
        assertThat(computeScalar(mdtEnabled(), "SELECT count(*) FROM " + TABLE_NAME)).isEqualTo(4L);
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
        MaterializedResult pruned = getQueryRunner().execute(partitionStatsPruningOnly(),
                "SELECT id, price FROM " + TABLE_NAME + " WHERE price < 100");
        assertThat(pruned.getMaterializedRows()).hasSize(2);

        // Index pruning must scan exactly p1's file groups: the same split count as a
        // metastore-pruned scan of p1, strictly fewer than the full scan. Split counts are
        // compared instead of hardcoded because the number of file groups per partition depends
        // on the write client's small-file packing.
        int fullScanSplits = totalSplits(mdtEnabled(), "SELECT id, price FROM " + TABLE_NAME);
        int p1ScanSplits = totalSplits(mdtEnabled(), "SELECT id, price FROM " + TABLE_NAME + " WHERE part_col = 'p1'");
        assertThat(p1ScanSplits).isLessThan(fullScanSplits);
        assertThat(pruned.getStatementStats().get().getTotalSplits()).isEqualTo(p1ScanSplits);
    }

    @Test
    public void testReadFallsBackToDirectListingOnUnreadableMetadataTable()
    {
        // The corrupted twin table's MDT log deltas cannot be decoded, so the MDT-backed
        // file-system-view load throws; HudiSnapshotDirectoryLister must fall back to listing
        // files directly from storage and still return complete, correct rows.
        assertQuery(
                mdtEnabled(),
                "SELECT id, name, price FROM " + CORRUPTED_TABLE_NAME + " ORDER BY id",
                "VALUES ('k1', 'k1_c3', CAST(15 AS BIGINT)), ('k2', 'k2_c1', 1000), ('k3', 'k3_c2', 20), ('k4', 'k4_c2', 2000)");
    }

    @Test
    public void testPartitionStatsPruningFallsBackUnprunedOnUnreadableMetadataTable()
    {
        // Same pruning setup as above, but the partition_stats read throws on the corrupted
        // table: prunePartitionsSafely must degrade to "no pruning", so the scan covers the
        // same splits as a full scan instead of failing the query.
        MaterializedResult unpruned = getQueryRunner().execute(partitionStatsPruningOnly(),
                "SELECT id, price FROM " + CORRUPTED_TABLE_NAME + " WHERE price < 100");
        assertThat(unpruned.getMaterializedRows()).hasSize(2);

        int fullScanSplits = totalSplits(mdtDisabled(), "SELECT id, price FROM " + CORRUPTED_TABLE_NAME);
        assertThat(unpruned.getStatementStats().get().getTotalSplits()).isEqualTo(fullScanSplits);
    }

    @Test
    public void testColumnStatsFileSkippingOverUncompactedStats()
    {
        // Column-stats file skipping reads the column_stats MDT partition's HFILE log deltas.
        // The wait timeout must be raised above its 1s default (matching the col-stats tests in
        // TestHudiSmokeTest): shouldSkipFileSlice keeps the file on any failure or timeout, so
        // with the default a broken col-stats read would still return correct rows and a
        // value-only assertion would pass without the deltas ever being read.
        Session session = SessionBuilder.from(getSession())
                .withMdtEnabled(true)
                .withColStatsIndexEnabled(true)
                .withRecordLevelIndexEnabled(false)
                .withSecondaryIndexEnabled(false)
                .withPartitionStatsIndexEnabled(false)
                .withColumnStatsTimeout("10s")
                .build();
        MaterializedResult skipped = getQueryRunner().execute(session,
                "SELECT id, price FROM " + TABLE_NAME + " WHERE price = 15");
        assertThat(skipped.getMaterializedRows()).hasSize(1);
        assertThat(skipped.getMaterializedRows().get(0).getFields()).containsExactly("k1", 15L);

        // File skipping must drop at least p2's file group (its prices [1000, 2000] exclude 15),
        // so the filtered scan uses strictly fewer splits than the unfiltered full scan
        int fullScanSplits = totalSplits(mdtEnabled(), "SELECT id, price FROM " + TABLE_NAME);
        assertThat(skipped.getStatementStats().get().getTotalSplits()).isLessThan(fullScanSplits);
    }

    private Session mdtEnabled()
    {
        return SessionBuilder.from(getSession()).withMdtEnabled(true).build();
    }

    private Session mdtDisabled()
    {
        return SessionBuilder.from(getSession()).withMdtEnabled(false).build();
    }

    /** MDT on with only the partition-stats index enabled, isolating partition pruning. */
    private Session partitionStatsPruningOnly()
    {
        return SessionBuilder.from(getSession())
                .withMdtEnabled(true)
                .withColStatsIndexEnabled(false)
                .withRecordLevelIndexEnabled(false)
                .withSecondaryIndexEnabled(false)
                .withPartitionStatsIndexEnabled(true)
                .build();
    }

    private int totalSplits(Session session, String query)
    {
        return getQueryRunner().execute(session, query).getStatementStats().get().getTotalSplits();
    }
}
