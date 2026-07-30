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
import io.trino.metastore.Column;
import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.OverwriteNonDefaultsWithLatestAvroPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.config.HoodieWriteConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.metastore.HiveType.HIVE_LONG;
import static io.trino.metastore.HiveType.HIVE_STRING;

/**
 * Creates a non-partitioned Merge-On-Read table whose merge semantics come from the
 * {@link OverwriteNonDefaultsWithLatestAvroPayload} class persisted in the table config (issue
 * apache/hudi#18898). ONLY the payload class is set (no merge mode / strategy id), so table creation
 * translates it exactly as a real writer would: into COMMIT_TIME_ORDERING plus
 * {@code PARTIAL_UPDATE_MODE=IGNORE_DEFAULTS}.
 * <p>
 * A base commit is followed by an update whose column is null (the schema default), which must keep the
 * STORED value for that column at merge time.
 * <p>
 * Records are wrapped in {@link HoodieAvroPayload} (a pass-through that is NOT a {@code BaseAvroPayload}),
 * so every merge decision happens at read time from the table config. See
 * {@code TestHudiMorPayloadSemantics}.
 */
public class OverwriteNonDefaultsPayloadHudiTablesInitializer
        extends AbstractMergerHudiTablesInitializer
{
    public static final String TABLE_NAME = "overwrite_non_defaults_mor";
    public static final String RT_TABLE_NAME = TABLE_NAME + "_rt";

    public OverwriteNonDefaultsPayloadHudiTablesInitializer()
    {
        super(TABLE_NAME);
    }

    @Override
    protected List<Column> dataColumns()
    {
        return ImmutableList.of(
                new Column(RECORD_KEY_FIELD, HIVE_STRING, Optional.empty(), Map.of()),
                new Column("a", HIVE_STRING, Optional.empty(), Map.of()),
                new Column("b", HIVE_STRING, Optional.empty(), Map.of()),
                new Column(ORDERING_FIELD, HIVE_LONG, Optional.empty(), Map.of()));
    }

    @Override
    protected Schema avroSchema()
    {
        Schema nullableString = Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING));
        List<Schema.Field> fields = ImmutableList.of(
                new Schema.Field(RECORD_KEY_FIELD, Schema.create(Schema.Type.STRING)),
                new Schema.Field("a", nullableString, null, JsonProperties.NULL_VALUE),
                new Schema.Field("b", nullableString, null, JsonProperties.NULL_VALUE),
                new Schema.Field(ORDERING_FIELD, Schema.create(Schema.Type.LONG)));
        return Schema.createRecord(TABLE_NAME, null, null, false, new ArrayList<>(fields));
    }

    @Override
    protected void configureTableConfig(HoodieTableMetaClient.TableBuilder tableBuilder)
    {
        tableBuilder.setPayloadClassName(OverwriteNonDefaultsWithLatestAvroPayload.class.getName());
    }

    @Override
    protected void configureWriteConfig(HoodieWriteConfig.Builder writeConfigBuilder)
    {
        writeConfigBuilder.withWritePayLoad(OverwriteNonDefaultsWithLatestAvroPayload.class.getName());
    }

    @Override
    protected void writeInitialCommits(HoodieJavaWriteClient<HoodieAvroPayload> client)
    {
        Schema schema = avroSchema();
        String firstCommit = client.startCommit();
        List<WriteStatus> firstStatuses = client.bulkInsert(ImmutableList.of(
                record(schema, "k1", "base_a", "base_b", 100L)), firstCommit);
        client.commit(firstCommit, firstStatuses);

        // Update with b=null (the schema default): IGNORE_DEFAULTS partial merging must keep the
        // stored 'base_b' while taking the updated 'new_a'.
        String secondCommit = client.startCommit();
        List<WriteStatus> secondStatuses = client.upsert(ImmutableList.of(
                record(schema, "k1", "new_a", null, 200L)), secondCommit);
        client.commit(secondCommit, secondStatuses);
    }

    private static HoodieRecord<HoodieAvroPayload> record(Schema schema, String key, String a, String b, long ts)
    {
        GenericRecord record = new GenericData.Record(schema);
        record.put(RECORD_KEY_FIELD, key);
        record.put("a", a);
        record.put("b", b);
        record.put(ORDERING_FIELD, ts);
        return avroRecord(record, key);
    }
}
