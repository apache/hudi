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

import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.util.Option;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import java.io.IOException;

/**
 * Test-only {@link org.apache.hudi.common.model.HoodieRecordPayload} that keeps whichever of the two records
 * being merged carries the larger {@code merge_rank}, ties going to the incoming (newer) record.
 * <p>
 * The policy makes payload-based merging distinguishable from both the base-only view (no merging happened at
 * all) and the built-in newest-wins behavior (merging happened, but not through the payload), which is what
 * makes it usable as the read-side acceptance case for a table whose only merge configuration is its payload
 * class. It also forces {@code merge_rank} to be read on BOTH sides of the merge, so a query that does not
 * project the column only produces the right answer over a full-table-schema read.
 * <p>
 * Both constructors are required: Hudi instantiates payloads reflectively through
 * {@code HoodieRecordUtils.loadPayload}, which looks up either {@code (GenericRecord, Comparable)} or
 * {@code (Option)}.
 */
public class RankBasedTestPayload
        extends OverwriteWithLatestAvroPayload
{
    /** Name of the column whose value decides the merge, matching the rank-merger fixtures. */
    public static final String RANK_COLUMN = "merge_rank";

    public RankBasedTestPayload(GenericRecord record, Comparable orderingVal)
    {
        super(record, orderingVal);
    }

    public RankBasedTestPayload(Option<GenericRecord> record)
    {
        super(record);
    }

    @Override
    public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema)
            throws IOException
    {
        // getInsertValue applies the standard empty/delete handling, so an empty result here is a deletion.
        Option<IndexedRecord> incomingRecord = getInsertValue(schema);
        if (incomingRecord.isEmpty()) {
            return incomingRecord;
        }
        return rankOf(incomingRecord.get()) >= rankOf(currentValue) ? incomingRecord : Option.of(currentValue);
    }

    private static long rankOf(IndexedRecord record)
    {
        GenericRecord genericRecord = (GenericRecord) record;
        Schema.Field field = genericRecord.getSchema().getField(RANK_COLUMN);
        Object value = field == null ? null : genericRecord.get(field.pos());
        // A null here means the reader failed to supply merge_rank on this side of the merge (the very
        // regression this payload exists to catch); fail loudly instead of merging arbitrarily.
        if (value == null) {
            throw new IllegalStateException("merge_rank is missing from a record of schema " + genericRecord.getSchema());
        }
        return ((Number) value).longValue();
    }
}
