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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.RecordContext;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.read.BufferedRecord;

import java.io.IOException;

/**
 * Test-only custom {@link HoodieRecordMerger} with the same keep-max-{@code merge_rank} policy as
 * {@link MaxRankRecordMerger}, but deliberately WITHOUT the {@code isProjectionCompatible()} and
 * {@code getMandatoryFieldsForMerging()} overrides. It therefore reports the interface default
 * {@code isProjectionCompatible() == false} and never declares {@code merge_rank} as mandatory, so
 * nothing prepends the rank column into the connector's read projection. The file-group reader reacts
 * by demanding the FULL table schema as its required schema for any split with log files -- for the
 * base and log reads alike -- making this merger the acceptance case for full-schema reads
 * (apache/hudi#19249): a query that does not project {@code merge_rank} merges correctly only if both
 * sides of the merge supply it.
 */
public class NonProjectionCompatibleRankMerger
        implements HoodieRecordMerger
{
    /**
     * Unique strategy id identifying this custom merger. The test table's
     * {@code hoodie.record.merge.strategy.id} is set to this value so the reader resolves this implementation.
     */
    public static final String MERGE_STRATEGY_ID = "8e1f4a6b-2c9d-4d35-9a7e-5b0c8f3d1e42";

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
        // Keep the newer record on ties so the policy stays deterministic.
        return newerRank >= olderRank ? newer : older;
    }

    private static <T> long rankOf(BufferedRecord<T> record, RecordContext<T> recordContext)
    {
        HoodieSchema schema = recordContext.getSchemaFromBufferRecord(record);
        Object value = recordContext.getValue(record.getRecord(), schema, RANK_COLUMN);
        // A null here means the reader failed to supply merge_rank on this side of the merge (the very
        // regression this merger exists to catch); fail loudly instead of merging arbitrarily.
        if (value == null) {
            throw new IllegalStateException("merge_rank is missing from a record of schema " + schema);
        }
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
