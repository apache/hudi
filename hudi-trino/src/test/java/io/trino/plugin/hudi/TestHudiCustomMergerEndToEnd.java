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
import io.trino.plugin.hudi.testing.IncrementalCustomMergerHudiTablesInitializer;
import io.trino.plugin.hudi.testing.MaxRankRecordMerger;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static io.trino.plugin.hudi.testing.IncrementalCustomMergerHudiTablesInitializer.RT_TABLE_NAME;
import static io.trino.plugin.hudi.testing.IncrementalCustomMergerHudiTablesInitializer.TABLE_NAME;
import static io.trino.plugin.hudi.testing.IncrementalCustomMergerHudiTablesInitializer.TOTAL_COMMITS;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end test of the custom record merger ({@link MaxRankRecordMerger}, configured via
 * {@code hudi.record-merger-impls}) on a 30-column Merge-On-Read table with {@link
 * IncrementalCustomMergerHudiTablesInitializer#NUM_RECORDS} records.
 * <p>
 * The table is written one commit at a time (a bulk insert followed by {@link
 * IncrementalCustomMergerHudiTablesInitializer#TOTAL_COMMITS} - 1 upserts). After every commit the real-time
 * table is read back through Trino and every column of every record is compared against the closed-form expected
 * merge result. The expectation is computed independently of the connector by folding the same max-rank policy
 * over the committed values, so a regression in merger resolution, merge ordering, or column handling fails the
 * assertion.
 */
public class TestHudiCustomMergerEndToEnd
        extends AbstractTestQueryFramework
{
    private IncrementalCustomMergerHudiTablesInitializer writer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        writer = new IncrementalCustomMergerHudiTablesInitializer();
        return HudiQueryRunner.builder()
                .setDataLoader(writer)
                .addConnectorProperties(ImmutableMap.of(
                        "hudi.record-merger-impls", MaxRankRecordMerger.class.getName()))
                .build();
    }

    @AfterAll
    public void tearDown()
            throws Exception
    {
        if (writer != null) {
            writer.close();
            writer = null;
        }
    }

    @Test
    public void testCustomMergerValidatedAfterEveryCommit()
    {
        // The first commit was written and synced by initializeTables; validate it, then drive the remaining commits.
        validateRows(RT_TABLE_NAME, writer.expectedRows());
        for (int commit = 2; commit <= TOTAL_COMMITS; commit++) {
            writer.writeAndSyncNextCommit();
            validateRows(RT_TABLE_NAME, writer.expectedRows());
        }
        // Sanity check that the data actually distinguishes the custom merge from built-in newest-wins:
        // for many keys the winning record is not the most recently committed one.
        assertThat(writer.divergentKeyCount()).isGreaterThan(0);

        // Regression check for projection pushdown: a query that does NOT project the merge column
        // (merge_rank) must still merge correctly, because MaxRankRecordMerger declares merge_rank as a
        // mandatory merge field, so the reader includes it in the read schema even though it is not selected.
        // Without that, the merger would read a pruned (null) merge_rank and fail / mis-merge.
        int s0Index = writer.dataColumnNames().indexOf("s0");
        List<MaterializedRow> projectedRows = computeActual(
                "SELECT key, s0 FROM " + RT_TABLE_NAME + " ORDER BY key").getMaterializedRows();
        Map<String, Object[]> expected = writer.expectedRows();
        assertThat(projectedRows).hasSize(expected.size());
        for (MaterializedRow row : projectedRows) {
            Object[] expectedRow = expected.get((String) row.getField(0));
            assertThat(expectedRow).as("unexpected key %s", row.getField(0)).isNotNull();
            assertThat(row.getField(1)).as("key %s, column s0", row.getField(0)).isEqualTo(expectedRow[s0Index]);
        }
    }

    @Test
    public void testReadOptimizedReflectsBaseFilesOnly()
    {
        // The read-optimized table reads base files only, so it always reflects the first commit regardless of
        // how many delta commits have since been written.
        validateRows(TABLE_NAME, writer.baseRows());
    }

    private void validateRows(String table, Map<String, Object[]> expected)
    {
        List<String> columns = writer.dataColumnNames();
        List<MaterializedRow> rows = computeActual(
                "SELECT " + String.join(", ", columns) + " FROM " + table + " ORDER BY key")
                .getMaterializedRows();

        assertThat(rows).hasSize(expected.size());
        for (MaterializedRow row : rows) {
            String key = (String) row.getField(0);
            Object[] expectedRow = expected.get(key);
            assertThat(expectedRow).as("unexpected key %s in table %s", key, table).isNotNull();
            for (int i = 0; i < expectedRow.length; i++) {
                assertThat(row.getField(i))
                        .as("table %s, key %s, column %s", table, key, columns.get(i))
                        .isEqualTo(expectedRow[i]);
            }
        }
    }
}
