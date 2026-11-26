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
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.config.HoodieWriteConfig;

import static org.apache.hudi.common.avro.HoodieAvroUtils.getNonNullTypeFromUnion;

/**
 * Creates a non-partitioned Merge-On-Read table whose record column carries a field of type {@code "null"}, which the
 * Parquet writer does not write and Hive sync leaves out of the struct (#20354). k1 is written to a base file and then
 * updated in a log file, so both reads, and the merge between them, carry the record without that field:
 * <pre>
 * k1 base: c_record.a = 'base'
 * k1 log:  c_record.a = 'updated'
 * </pre>
 */
public class NestedNullFieldHudiTablesInitializer
        extends AbstractMergerHudiTablesInitializer
{
    public static final String TABLE_NAME = "nested_null_field_mor";
    public static final String RT_TABLE_NAME = TABLE_NAME + "_rt";

    private static final Schema AVRO_SCHEMA = HoodieSchema.createRecord(TABLE_NAME, null, null, ImmutableList.of(
            HoodieSchemaField.of(RECORD_KEY_FIELD, HoodieSchema.create(HoodieSchemaType.STRING)),
            HoodieSchemaField.of(ORDERING_FIELD, HoodieSchema.create(HoodieSchemaType.LONG)),
            HoodieSchemaField.of("c_record", HoodieSchema.createNullable(HoodieSchema.createRecord("c_record", null, null, ImmutableList.of(
                    HoodieSchemaField.of("a", HoodieSchema.createNullable(HoodieSchema.create(HoodieSchemaType.STRING)), null, HoodieSchema.NULL_VALUE),
                    HoodieSchemaField.of("b", HoodieSchema.create(HoodieSchemaType.NULL), null, HoodieSchema.NULL_VALUE)))), null, HoodieSchema.NULL_VALUE)))
            .toAvroSchema();

    public NestedNullFieldHudiTablesInitializer()
    {
        super(TABLE_NAME);
    }

    @Override
    protected Schema avroSchema()
    {
        return AVRO_SCHEMA;
    }

    @Override
    protected void configureTableConfig(HoodieTableMetaClient.TableBuilder tableBuilder)
    {
        tableBuilder.setRecordMergeMode(RecordMergeMode.COMMIT_TIME_ORDERING);
    }

    @Override
    protected void configureWriteConfig(HoodieWriteConfig.Builder writeConfigBuilder)
    {
        writeConfigBuilder.withRecordMergeMode(RecordMergeMode.COMMIT_TIME_ORDERING);
    }

    @Override
    protected void writeInitialCommits(HoodieJavaWriteClient<HoodieAvroPayload> client)
    {
        String baseCommit = client.startCommit();
        client.commit(baseCommit, client.bulkInsert(ImmutableList.of(record("base", 100L)), baseCommit));
        String logCommit = client.startCommit();
        client.commit(logCommit, client.upsert(ImmutableList.of(record("updated", 200L)), logCommit));
    }

    private static HoodieRecord<HoodieAvroPayload> record(String value, long ts)
    {
        GenericRecord record = new GenericData.Record(AVRO_SCHEMA);
        record.put(RECORD_KEY_FIELD, "k1");
        record.put(ORDERING_FIELD, ts);
        GenericRecord nested = new GenericData.Record(getNonNullTypeFromUnion(AVRO_SCHEMA.getField("c_record").schema()));
        nested.put("a", value);
        record.put("c_record", nested);
        return avroRecord(record, "k1");
    }
}
