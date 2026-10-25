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
import io.trino.plugin.hudi.testing.CompositeHudiTablesInitializer;
import io.trino.plugin.hudi.testing.MaxRankRecordMerger;
import io.trino.plugin.hudi.testing.NonProjectionCompatibleMergerHudiTablesInitializer;
import io.trino.plugin.hudi.testing.NonProjectionCompatibleRankMerger;
import io.trino.plugin.hudi.testing.OmittedOrderingFieldHudiTablesInitializer;
import io.trino.plugin.hudi.testing.OmittedRankFieldHudiTablesInitializer;
import io.trino.plugin.hudi.testing.PayloadOnlyMergerHudiTablesInitializer;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import static io.trino.plugin.hudi.testing.NonProjectionCompatibleMergerHudiTablesInitializer.RT_TABLE_NAME;
import static io.trino.plugin.hudi.testing.NonProjectionCompatibleMergerHudiTablesInitializer.TABLE_NAME;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Acceptance test for full-table-schema merge reads (apache/hudi#19249, issue comment on scope):
 * {@link NonProjectionCompatibleRankMerger} does NOT override {@code isProjectionCompatible()} (default
 * {@code false}) and does NOT declare {@code merge_rank} mandatory, so the file-group reader demands the
 * FULL table schema as its required schema for base and log reads alike, and nothing prepends
 * {@code merge_rank} into the connector's read projection.
 * <p>
 * The queries below never project {@code merge_rank}. The data is laid out so each merge direction is
 * proven independently: {@code k1}'s winning rank is on the LOG record (update wins, value 99) and
 * {@code k2}'s winning rank is on the BASE record (base wins, value 100). A correct result therefore
 * requires the un-projected rank column to be read on BOTH sides of the merge. {@code sum(value)} is a
 * three-way discriminator: merged = 199, base-only = 110, built-in newest-wins = 103.
 * <p>
 * The same full-schema read path is reached without any configured merger at all by a pre-1.0 table that
 * persists only a {@link org.apache.hudi.common.model.HoodieRecordPayload} class
 * ({@link PayloadOnlyMergerHudiTablesInitializer}), covered by the {@code payloadOnly} tests below.
 * <p>
 * A third fixture ({@link OmittedOrderingFieldHudiTablesInitializer}) pins the projection-compatible side
 * end to end: its metastore omits the ordering field its Avro schema carries, so correct event-time results
 * prove the merge path recovers metastore-unknown merge columns from the resolved table schema. A fourth
 * ({@link OmittedRankFieldHudiTablesInitializer}) does the same for a column only the CUSTOM merger itself
 * declares mandatory ({@code merge_rank}), proving the merge path asks the resolved merger too.
 */
public class TestHudiNonProjectionCompatibleMerger
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HudiQueryRunner.builder()
                .setDataLoader(new CompositeHudiTablesInitializer(
                        new NonProjectionCompatibleMergerHudiTablesInitializer(),
                        new PayloadOnlyMergerHudiTablesInitializer(),
                        new OmittedOrderingFieldHudiTablesInitializer(),
                        new OmittedRankFieldHudiTablesInitializer()))
                // Both custom mergers; each table resolves its own by strategy id.
                .addConnectorProperties(ImmutableMap.of(
                        "hudi.record-merger-impls",
                        NonProjectionCompatibleRankMerger.class.getName() + "," + MaxRankRecordMerger.class.getName()))
                .build();
    }

    @Test
    public void testReadOptimizedTableReturnsBaseFileValues()
    {
        // The read-optimized table reads base files only, so it reflects the initial insert.
        assertQuery(
                "SELECT key, name, value FROM " + TABLE_NAME + " ORDER BY key",
                "VALUES ('k1', 'k1_base', CAST(10 AS BIGINT)), ('k2', 'k2_base', CAST(100 AS BIGINT))");
        assertThat(computeScalar("SELECT sum(value) FROM " + TABLE_NAME))
                .isEqualTo(110L);
    }

    @Test
    public void testNarrowProjectionMergesViaFullSchemaRead()
    {
        // Neither query projects merge_rank; the merger can only see it through the full-schema read.
        //  - k1 keeps the update (99): the winning rank (7 > 5) is on the LOG record
        //  - k2 keeps the base (100): the winning rank (9 > 1) is on the BASE record, proving the base
        //    read honors the file-group reader's full-schema required schema
        assertQuery(
                "SELECT key, value FROM " + RT_TABLE_NAME + " ORDER BY key",
                "VALUES ('k1', CAST(99 AS BIGINT)), ('k2', CAST(100 AS BIGINT))");
        // 199 uniquely identifies the rank-based merge: base-only would be 110, newest-wins would be 103.
        assertThat(computeScalar("SELECT sum(value) FROM " + RT_TABLE_NAME))
                .isEqualTo(199L);
    }

    @Test
    public void testSelectStarMergesAllColumns()
    {
        assertQuery(
                "SELECT key, name, value, merge_rank, ts FROM " + RT_TABLE_NAME + " ORDER BY key",
                "VALUES"
                        + " ('k1', 'k1_updated', CAST(99 AS BIGINT), CAST(7 AS BIGINT), CAST(2 AS BIGINT)),"
                        + " ('k2', 'k2_base', CAST(100 AS BIGINT), CAST(9 AS BIGINT), CAST(1 AS BIGINT))");
    }

    @Test
    public void testPayloadOnlyReadOptimizedTableReturnsBaseFileValues()
    {
        assertQuery(
                noRecordMergerImplsSession(),
                "SELECT key, name, value FROM " + PayloadOnlyMergerHudiTablesInitializer.TABLE_NAME + " ORDER BY key",
                "VALUES ('k1', 'k1_base', CAST(10 AS BIGINT)), ('k2', 'k2_base', CAST(100 AS BIGINT))");
    }

    @Test
    public void testPayloadOnlyRealtimeTableAppliesPayloadMerge()
    {
        // Table version 6 with nothing but a payload class in hoodie.properties: the reader infers CUSTOM
        // merge mode with the payload-based strategy, which resolves HoodieAvroRecordMerger and runs
        // RankBasedTestPayload. Both merge directions are exercised, as for the custom merger above.
        assertQuery(
                noRecordMergerImplsSession(),
                "SELECT key, name, value FROM " + PayloadOnlyMergerHudiTablesInitializer.RT_TABLE_NAME + " ORDER BY key",
                "VALUES ('k1', 'k1_updated', CAST(99 AS BIGINT)), ('k2', 'k2_base', CAST(100 AS BIGINT))");
    }

    @Test
    public void testPayloadOnlyNarrowProjectionMergesViaFullSchemaRead()
    {
        // Projects neither key, name nor merge_rank, so 199 proves the full-schema read fed merge_rank to the
        // payload on both sides: base-only would be 110, built-in newest-wins would be 103.
        assertThat(computeScalar(
                noRecordMergerImplsSession(),
                "SELECT sum(value) FROM " + PayloadOnlyMergerHudiTablesInitializer.RT_TABLE_NAME))
                .isEqualTo(199L);
    }

    @Test
    public void testMetastoreOmittedOrderingFieldRealtimeTableMerges()
    {
        // This fixture's metastore does not carry the ordering field ts its Avro schema has (the hive-sync
        // omission shape); event-time merging still needs ts on both sides, so these results only come out
        // when the merge path recovers the column from the resolved table schema.
        assertQuery(
                "SELECT key, name, value FROM " + OmittedOrderingFieldHudiTablesInitializer.RT_TABLE_NAME + " ORDER BY key",
                "VALUES ('k1', 'k1_updated', CAST(99 AS BIGINT)), ('k2', 'k2_base', CAST(100 AS BIGINT))");
    }

    @Test
    public void testMetastoreOmittedOrderingFieldNarrowProjectionSum()
    {
        // 199 discriminates event-time merging from base-only (110) and commit-time newest-wins (103).
        assertThat(computeScalar("SELECT sum(value) FROM " + OmittedOrderingFieldHudiTablesInitializer.RT_TABLE_NAME))
                .isEqualTo(199L);
    }

    @Test
    public void testMetastoreOmittedMergerMandatoryFieldRealtimeTableMerges()
    {
        // This fixture's metastore does not carry merge_rank, the column MaxRankRecordMerger declares
        // mandatory; only asking the resolved merger on the merge path recovers it, so these results only
        // come out when merger-declared columns are recovered from the table schema too.
        assertQuery(
                "SELECT key, name, value FROM " + OmittedRankFieldHudiTablesInitializer.RT_TABLE_NAME + " ORDER BY key",
                "VALUES ('k1', 'k1_updated', CAST(99 AS BIGINT)), ('k2', 'k2_base', CAST(100 AS BIGINT))");
    }

    @Test
    public void testMetastoreOmittedMergerMandatoryFieldNarrowProjectionSum()
    {
        // 199 discriminates the keep-max merge from base-only (110) and newest-wins (103).
        assertThat(computeScalar("SELECT sum(value) FROM " + OmittedRankFieldHudiTablesInitializer.RT_TABLE_NAME))
                .isEqualTo(199L);
    }

    /**
     * The payload-only table must resolve its merger purely from hoodie.properties, so the merger impls the
     * connector is configured with for {@link NonProjectionCompatibleRankMerger} are cleared for its queries.
     */
    private Session noRecordMergerImplsSession()
    {
        return SessionBuilder.from(getSession())
                .withRecordMergerImpls()
                .build();
    }
}
