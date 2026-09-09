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

import io.trino.plugin.hudi.testing.CompositeHudiTablesInitializer;
import io.trino.plugin.hudi.testing.DmsPayloadHudiTablesInitializer;
import io.trino.plugin.hudi.testing.OverwriteNonDefaultsPayloadHudiTablesInitializer;
import io.trino.plugin.hudi.testing.SummingPayloadHudiTablesInitializer;
import io.trino.plugin.hudi.testing.SummingTestPayload;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end MoR snapshot-read tests for PAYLOAD-driven merge semantics (issue apache/hudi#18898), on
 * tables written by {@link DmsPayloadHudiTablesInitializer},
 * {@link OverwriteNonDefaultsPayloadHudiTablesInitializer} and {@link SummingPayloadHudiTablesInitializer}.
 * No {@code hudi.record-merger-impls} connector property is set anywhere -- every behavior below must
 * resolve purely from the table config:
 * <ul>
 *   <li>AWSDms: a log record with {@code Op='D'} deletes the row at merge time via the translated
 *       delete-key/marker table properties, while a log record with the non-marker {@code Op='U'} must
 *       apply as an update (the narrow-projection case pins the fix that reads those properties with
 *       their {@code hoodie.record.merge.property.} prefix).</li>
 *   <li>OverwriteNonDefaults: IGNORE_DEFAULTS partial merging keeps the stored value for update columns
 *       equal to the schema default (null).</li>
 *   <li>{@link SummingTestPayload}: a user-defined payload rides the payload-based CUSTOM merge
 *       strategy; the merged value is the SUM of stored and incoming values, which proves the payload's
 *       {@code combineAndGetUpdateValue} executed (overwrite would yield the incoming value), and a hard
 *       delete routed through the same arm must remove its row.</li>
 * </ul>
 */
public class TestHudiMorPayloadSemantics
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HudiQueryRunner.builder()
                .setDataLoader(new CompositeHudiTablesInitializer(
                        new DmsPayloadHudiTablesInitializer(),
                        new OverwriteNonDefaultsPayloadHudiTablesInitializer(),
                        new SummingPayloadHudiTablesInitializer()))
                .build();
    }

    @Test
    public void testDmsDeleteMarkerRemovesRowOnSnapshotRead()
    {
        // Read-optimized: both rows (the log records are not merged)
        assertQuery(
                "SELECT key, name, value, Op FROM " + DmsPayloadHudiTablesInitializer.TABLE_NAME + " ORDER BY key",
                "VALUES ('k1', 'k1_base', CAST(10 AS BIGINT), 'I'), ('k2', 'k2_base', 20, 'I')");
        // Snapshot: k2 is deleted by the Op='D' log record via the delete-key/marker table properties,
        // while k1's NON-marker Op='U' log record must apply as an update -- a marker comparison that
        // fires on any non-null Op would wrongly delete k1 too
        assertQuery(
                "SELECT key, name, value, Op FROM " + DmsPayloadHudiTablesInitializer.RT_TABLE_NAME + " ORDER BY key",
                "VALUES ('k1', 'k1_updated', CAST(11 AS BIGINT), 'U')");
    }

    @Test
    public void testDmsNarrowProjectionMergesCorrectly()
    {
        // The Op column is NOT projected, so the connector must predict it as a merge-required column
        // from the PREFIXED table properties (hoodie.record.merge.property.hoodie.payload.delete.field)
        // for the base read -- the regression this suite pins for HudiUtil.mergeRequiredColumnNames
        assertQuery(
                "SELECT key, value FROM " + DmsPayloadHudiTablesInitializer.RT_TABLE_NAME + " ORDER BY key",
                "VALUES ('k1', CAST(11 AS BIGINT))");
        assertThat(computeScalar("SELECT count(*) FROM " + DmsPayloadHudiTablesInitializer.RT_TABLE_NAME)).isEqualTo(1L);
    }

    @Test
    public void testOverwriteNonDefaultsKeepsStoredValueForDefaultColumns()
    {
        // Read-optimized: base values
        assertQuery(
                "SELECT key, a, b FROM " + OverwriteNonDefaultsPayloadHudiTablesInitializer.TABLE_NAME,
                "VALUES ('k1', 'base_a', 'base_b')");
        // Snapshot: the update carried a='new_a' and b=null (the schema default); IGNORE_DEFAULTS
        // partial merging takes the update's a but keeps the STORED b
        assertQuery(
                "SELECT key, a, b FROM " + OverwriteNonDefaultsPayloadHudiTablesInitializer.RT_TABLE_NAME,
                "VALUES ('k1', 'new_a', 'base_b')");
    }

    @Test
    public void testSummingPayloadRunsCombineAndGetUpdateValueOnRead()
    {
        // Read-optimized: the base values
        assertQuery(
                "SELECT key, value FROM " + SummingPayloadHudiTablesInitializer.TABLE_NAME + " ORDER BY key",
                "VALUES ('k1', CAST(10 AS BIGINT)), ('k2', 20)");
        // Snapshot: 10 + 99 = 109 -- only the payload's combineAndGetUpdateValue can produce this
        // (newest-wins would yield 99, base-only 10), proving the CUSTOM payload-strategy branch ran;
        // k2 is hard-deleted
        assertQuery(
                "SELECT key, value FROM " + SummingPayloadHudiTablesInitializer.RT_TABLE_NAME + " ORDER BY key",
                "VALUES ('k1', CAST(109 AS BIGINT))");
    }

    @Test
    public void testSummingPayloadHardDeleteRemovesRowOnSnapshotRead()
    {
        // The hard delete is a native delete log record routed to the payload-based CUSTOM merge arm,
        // where it wins on HoodieAvroRecordMerger's isCommitTimeOrderingDelete short-circuit (the
        // delete carries the sentinel ordering value) -- the delete path of the user-merger dispatch,
        // which both ordering arms already cover
        assertThat(computeScalar("SELECT count(*) FROM " + SummingPayloadHudiTablesInitializer.RT_TABLE_NAME + " WHERE key = 'k2'"))
                .isEqualTo(0L);
        assertThat(computeScalar("SELECT count(*) FROM " + SummingPayloadHudiTablesInitializer.TABLE_NAME + " WHERE key = 'k2'"))
                .isEqualTo(1L);
    }
}
