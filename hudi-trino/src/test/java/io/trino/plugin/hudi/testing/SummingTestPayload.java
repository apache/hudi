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
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.util.Option;

import java.io.IOException;

/**
 * Test-only user-defined {@link org.apache.hudi.common.model.HoodieRecordPayload} whose read-side merge
 * SUMS the {@code value} column of the stored and incoming records. A merged row therefore carries a
 * value no built-in merge policy can produce (overwrite yields the incoming value, base-only the stored
 * value), which proves end-to-end that the connector's CUSTOM merge branch executed this payload's
 * {@code combineAndGetUpdateValue} (issue apache/hudi#18898).
 * <p>
 * Unlike the built-in payloads named in the issue, this class is NOT in hudi's payloads-under-deprecation
 * set, so v9+ table creation persists it as {@code RECORD_MERGE_MODE=CUSTOM} with the payload-based merge
 * strategy id -- the configuration that routes reads through {@code HoodieAvroRecordMerger} and this
 * payload, with no {@code hudi.record-merger-impls} connector property involved.
 */
public class SummingTestPayload
        extends OverwriteWithLatestAvroPayload
{
    /** Name of the column whose stored and incoming values are summed at merge time. */
    public static final String SUM_COLUMN = "value";

    public SummingTestPayload(GenericRecord record, Comparable orderingVal)
    {
        super(record, orderingVal);
    }

    public SummingTestPayload(Option<GenericRecord> record)
    {
        super(record);
    }

    @Override
    public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema)
            throws IOException
    {
        Option<IndexedRecord> incoming = getInsertValue(schema);
        if (incoming.isEmpty()) {
            return Option.empty();
        }
        GenericRecord newer = (GenericRecord) incoming.get();
        GenericRecord older = (GenericRecord) currentValue;

        long sum = ((Number) older.get(SUM_COLUMN)).longValue() + ((Number) newer.get(SUM_COLUMN)).longValue();
        GenericRecord merged = new GenericData.Record(newer.getSchema());
        for (Schema.Field field : newer.getSchema().getFields()) {
            merged.put(field.pos(), newer.get(field.pos()));
        }
        merged.put(SUM_COLUMN, sum);
        return Option.of(merged);
    }
}
