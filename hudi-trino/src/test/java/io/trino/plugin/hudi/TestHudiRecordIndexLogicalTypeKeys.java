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
import io.trino.plugin.hudi.testing.LogicalTypeRecordKeyHudiTablesInitializer;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import static io.trino.plugin.hudi.testing.LogicalTypeRecordKeyHudiTablesInitializer.TABLE_NAME;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Record-level index pruning on a composite key of a date and a local timestamp-micros column, whose key text Hudi's key
 * generator wrote as yyyy-MM-dd and epoch micros. The columns read as date and timestamp(6), so the lookup keys must be
 * rebuilt from those values the writer's way for the index to find them. Key types whose text Spark's row writer writes
 * differently are not looked up at all; {@code TestIndexKeyForm} pins those.
 */
public class TestHudiRecordIndexLogicalTypeKeys
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HudiQueryRunner.builder()
                .setDataLoader(new LogicalTypeRecordKeyHudiTablesInitializer())
                .build();
    }

    @Test
    public void testEqualityOnDateAndTimestampKeyPrunesToOneFileGroup()
    {
        assertThat(totalSplits(recordIndexOnly(false), "SELECT name FROM " + TABLE_NAME)).isEqualTo(3);

        String query = "SELECT name FROM " + TABLE_NAME + " WHERE event_date = DATE '2024-01-02' AND event_ts = TIMESTAMP '2024-01-02 10:00:00.123456'";
        MaterializedResult pruned = getQueryRunner().execute(recordIndexOnly(true), query);
        assertThat(pruned.getMaterializedRows()).extracting(row -> row.getField(0)).containsExactly("b");
        assertThat(pruned.getStatementStats().get().getTotalSplits()).isEqualTo(1);
    }

    @Test
    public void testInListOnDateAndTimestampKeyPrunesToMatchingFileGroups()
    {
        // Four lookup keys, two of which exist
        MaterializedResult pruned = getQueryRunner().execute(recordIndexOnly(true),
                "SELECT name FROM " + TABLE_NAME + " WHERE event_date IN (DATE '2024-01-01', DATE '2024-01-03')"
                        + " AND event_ts IN (TIMESTAMP '2024-01-01 10:00:00.123456', TIMESTAMP '2024-01-03 10:00:00.123456')");
        assertThat(pruned.getMaterializedRows()).extracting(row -> row.getField(0)).containsExactlyInAnyOrder("a", "c");
        assertThat(pruned.getStatementStats().get().getTotalSplits()).isEqualTo(2);
    }

    private Session recordIndexOnly(boolean recordIndexEnabled)
    {
        return SessionBuilder.from(getSession())
                .withMdtEnabled(true)
                .withColStatsIndexEnabled(false)
                .withRecordLevelIndexEnabled(recordIndexEnabled)
                .withRecordIndexTimeout("10s")
                .withSecondaryIndexEnabled(false)
                .withPartitionStatsIndexEnabled(false)
                .build();
    }

    private int totalSplits(Session session, String query)
    {
        return getQueryRunner().execute(session, query).getStatementStats().get().getTotalSplits();
    }
}
