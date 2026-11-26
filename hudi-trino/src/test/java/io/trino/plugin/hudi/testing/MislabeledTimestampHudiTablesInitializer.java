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

import com.google.common.collect.ImmutableList;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;

/**
 * Creates a non-partitioned Merge-On-Read table whose first base file and first log file store two timestamp columns
 * under the wrong logical type, as the writers {@code SchemaRepair} repairs for did (#14161), and whose later commit
 * records the repaired schema:
 * <ul>
 * <li>{@code c_ts}: timestamp-millis in the table schema, but the mislabeled files label it timestamp-micros while
 * holding millis</li>
 * <li>{@code c_local_ts}: local-timestamp-millis in the table schema, but a bare long in the mislabeled files</li>
 * </ul>
 * The first commit writes k1's base file under the mislabeled schema, and the second updates k1 the same way into a log
 * file, so the snapshot read merges a mislabeled base row with a mislabeled log record. The third writes k2, in a file
 * group of its own, under the table schema:
 * <pre>
 * k1 base: c_ts 2023-11-14 22:13:20.123 UTC, c_local_ts 2023-11-14 22:13:20.123
 * k1 log:  c_ts 2023-11-14 23:13:20.123 UTC, c_local_ts 2023-11-14 23:13:20.123
 * k2:      c_ts 2023-11-15 22:13:20.123 UTC, c_local_ts 2023-11-15 22:13:20.123
 * </pre>
 */
public class MislabeledTimestampHudiTablesInitializer
        extends AbstractMergerHudiTablesInitializer
{
    public static final String TABLE_NAME = "mislabeled_timestamp_mor";
    public static final String RT_TABLE_NAME = TABLE_NAME + "_rt";

    // 1_700_000_000_123 ms is 2023-11-14 22:13:20.123 UTC
    private static final long FIRST_EPOCH_MILLIS = 1_700_000_000_123L;
    private static final long MILLIS_PER_HOUR = 3_600_000L;
    private static final long MILLIS_PER_DAY = 86_400_000L;

    private static final Schema TABLE_SCHEMA = schema(HoodieSchema.createTimestampMillis(), HoodieSchema.createLocalTimestampMillis());
    private static final Schema MISLABELED_SCHEMA = schema(HoodieSchema.createTimestampMicros(), HoodieSchema.create(HoodieSchemaType.LONG));

    public MislabeledTimestampHudiTablesInitializer()
    {
        super(TABLE_NAME);
    }

    private static Schema schema(HoodieSchema timestamp, HoodieSchema localTimestamp)
    {
        return HoodieSchema.createRecord(TABLE_NAME, null, null, ImmutableList.of(
                HoodieSchemaField.of(RECORD_KEY_FIELD, HoodieSchema.create(HoodieSchemaType.STRING)),
                HoodieSchemaField.of(ORDERING_FIELD, HoodieSchema.create(HoodieSchemaType.LONG)),
                HoodieSchemaField.of("c_ts", timestamp),
                HoodieSchemaField.of("c_local_ts", localTimestamp))).toAvroSchema();
    }

    @Override
    protected Schema avroSchema()
    {
        return TABLE_SCHEMA;
    }


    @Override
    protected void writeInitialCommits(HoodieJavaWriteClient<HoodieAvroPayload> client)
    {
        try (HoodieJavaWriteClient<HoodieAvroPayload> mislabeledClient = createWriteClient(MISLABELED_SCHEMA)) {
            String baseCommit = mislabeledClient.startCommit();
            mislabeledClient.commit(baseCommit, mislabeledClient.bulkInsert(ImmutableList.of(record(MISLABELED_SCHEMA, "k1", 100L, 0)), baseCommit));
            String logCommit = mislabeledClient.startCommit();
            mislabeledClient.commit(logCommit, mislabeledClient.upsert(ImmutableList.of(record(MISLABELED_SCHEMA, "k1", 200L, MILLIS_PER_HOUR)), logCommit));
        }
        String repairedCommit = client.startCommit();
        client.commit(repairedCommit, client.bulkInsert(ImmutableList.of(record(TABLE_SCHEMA, "k2", 100L, MILLIS_PER_DAY)), repairedCommit));
    }

    private static HoodieRecord<HoodieAvroPayload> record(Schema schema, String key, long ts, long millisAfterFirst)
    {
        GenericRecord record = new GenericData.Record(schema);
        record.put(RECORD_KEY_FIELD, key);
        record.put(ORDERING_FIELD, ts);
        // Millis in both schemas: the mislabeled one calls them micros, or a plain long
        record.put("c_ts", FIRST_EPOCH_MILLIS + millisAfterFirst);
        record.put("c_local_ts", FIRST_EPOCH_MILLIS + millisAfterFirst);
        return avroRecord(record, key);
    }
}
