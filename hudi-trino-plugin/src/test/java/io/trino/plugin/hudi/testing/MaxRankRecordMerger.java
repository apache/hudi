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
package io.trino.plugin.hudi.testing;

import org.apache.avro.Schema;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.RecordContext;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.table.read.BufferedRecord;

import java.io.IOException;

/**
 * Test-only custom {@link HoodieRecordMerger} that keeps, for each record key, the record with the largest value
 * in the {@code merge_rank} column. Ties are broken in favor of the newer (later-committed) record.
 * <p>
 * This policy is deliberately distinct from every built-in merge mode: it depends on an arbitrary data column
 * rather than commit time (newest-wins) or the ordering/precombine field (event-time ordering). Because the
 * winning commit for a key is frequently <em>not</em> the most recent one, the merged result is observably
 * different from both the read-optimized (base-only) view and the built-in newest-wins behavior, which is what
 * {@code TestHudiCustomMergerEndToEnd} validates after every commit.
 * <p>
 * Operates on Avro records ({@link HoodieRecord.HoodieRecordType#AVRO}), the record type the Trino reader
 * context uses ({@code EngineType.JAVA}).
 */
public class MaxRankRecordMerger
        implements HoodieRecordMerger
{
    /**
     * Unique strategy id identifying this custom merger. The test table's
     * {@code hoodie.record.merge.strategy.id} is set to this value so the reader resolves this implementation.
     */
    public static final String MERGE_STRATEGY_ID = "3c2d7e10-9a4b-4f81-bd6e-7f0a1c5e8d24";

    /** Name of the column whose value decides the merge. */
    public static final String RANK_COLUMN = "merge_rank";

    @Override
    public <T> BufferedRecord<T> merge(BufferedRecord<T> older, BufferedRecord<T> newer, RecordContext<T> recordContext, TypedProperties props)
            throws IOException
    {
        // Deletes are passed through so tombstones still win; this test does not exercise deletes.
        if (older == null || older.isDelete() || newer.isDelete()) {
            return newer;
        }
        long olderRank = rankOf(older, recordContext);
        long newerRank = rankOf(newer, recordContext);
        // Keep the newer record on ties so the policy stays deterministic and matches the expected-state fold.
        return newerRank >= olderRank ? newer : older;
    }

    private static <T> long rankOf(BufferedRecord<T> record, RecordContext<T> recordContext)
    {
        Schema schema = recordContext.getSchemaFromBufferRecord(record);
        Object value = recordContext.getValue(record.getRecord(), schema, RANK_COLUMN);
        return ((Number) value).longValue();
    }

    @Override
    public HoodieRecord.HoodieRecordType getRecordType()
    {
        return HoodieRecord.HoodieRecordType.AVRO;
    }

    @Override
    public String getMergingStrategy()
    {
        return MERGE_STRATEGY_ID;
    }
}
