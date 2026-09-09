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
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.AWSDmsAvroPayload;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.config.HoodieWriteConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static io.trino.metastore.HiveType.HIVE_LONG;
import static io.trino.metastore.HiveType.HIVE_STRING;
import static org.apache.hudi.common.model.AWSDmsAvroPayload.DELETE_OPERATION_VALUE;
import static org.apache.hudi.common.model.AWSDmsAvroPayload.OP_FIELD;

/**
 * Creates a non-partitioned Merge-On-Read table whose merge semantics come from the
 * {@link AWSDmsAvroPayload} class persisted in the table config (issue apache/hudi#18898). ONLY the payload
 * class is set (no merge mode / strategy id), so table creation translates it exactly as a real writer
 * would: at the current table version this "deprecated" payload becomes COMMIT_TIME_ORDERING plus PREFIXED
 * delete-key props ({@code hoodie.record.merge.property.hoodie.payload.delete.field=Op}, marker {@code D}).
 * <p>
 * A base commit is followed by a log record with {@code Op='D'}, which deletes the row at merge time via
 * {@code DeleteContext}, with the payload never executing at read, plus a log record with the non-marker
 * {@code Op='U'} whose update must APPLY rather than delete -- pinning the marker-value comparison itself.
 * <p>
 * Records are wrapped in {@link HoodieAvroPayload} (a pass-through that is NOT a {@code BaseAvroPayload}),
 * so rows a semantic payload would drop at write time land as DATA records and every merge decision happens
 * at read time. See {@code TestHudiMorPayloadSemantics}.
 */
public class DmsPayloadHudiTablesInitializer
        extends AbstractMergerHudiTablesInitializer
{
    public static final String TABLE_NAME = "dms_mor";
    public static final String RT_TABLE_NAME = TABLE_NAME + "_rt";

    public DmsPayloadHudiTablesInitializer()
    {
        super(TABLE_NAME);
    }

    @Override
    protected List<Column> dataColumns()
    {
        return ImmutableList.of(
                new Column(RECORD_KEY_FIELD, HIVE_STRING, Optional.empty(), Map.of()),
                new Column("name", HIVE_STRING, Optional.empty(), Map.of()),
                new Column("value", HIVE_LONG, Optional.empty(), Map.of()),
                // The Avro/parquet field is 'Op' (AWSDms hardcodes that casing), but a real Hive
                // metastore lowercases column names on DDL -- exactly the case mismatch the connector's
                // merge-column matching must bridge
                new Column(OP_FIELD.toLowerCase(Locale.ROOT), HIVE_STRING, Optional.empty(), Map.of()),
                new Column(ORDERING_FIELD, HIVE_LONG, Optional.empty(), Map.of()));
    }

    @Override
    protected Schema avroSchema()
    {
        List<Schema.Field> fields = ImmutableList.of(
                new Schema.Field(RECORD_KEY_FIELD, Schema.create(Schema.Type.STRING)),
                new Schema.Field("name", Schema.create(Schema.Type.STRING)),
                new Schema.Field("value", Schema.create(Schema.Type.LONG)),
                new Schema.Field(OP_FIELD, Schema.create(Schema.Type.STRING)),
                new Schema.Field(ORDERING_FIELD, Schema.create(Schema.Type.LONG)));
        return Schema.createRecord(TABLE_NAME, null, null, false, new ArrayList<>(fields));
    }

    @Override
    protected void configureTableConfig(HoodieTableMetaClient.TableBuilder tableBuilder)
    {
        tableBuilder.setPayloadClassName(AWSDmsAvroPayload.class.getName());
    }

    @Override
    protected void configureWriteConfig(HoodieWriteConfig.Builder writeConfigBuilder)
    {
        writeConfigBuilder.withWritePayLoad(AWSDmsAvroPayload.class.getName());
    }

    @Override
    protected void writeInitialCommits(HoodieJavaWriteClient<HoodieAvroPayload> client)
    {
        Schema schema = avroSchema();
        String firstCommit = client.startCommit();
        List<WriteStatus> firstStatuses = client.bulkInsert(ImmutableList.of(
                record(schema, "k1", "k1_base", 10L, "I", 100L),
                record(schema, "k2", "k2_base", 20L, "I", 100L)), firstCommit);
        client.commit(firstCommit, firstStatuses);

        // Log records, both written as DATA records by the pass-through HoodieAvroPayload. Only k2's
        // marker value deletes at merge time via DeleteContext (delete key Op, marker D from the
        // translated table config); k1 carries the NON-marker Op='U' and its update must apply, so a
        // marker comparison that fires on any non-null Op fails the suite.
        String secondCommit = client.startCommit();
        List<WriteStatus> secondStatuses = client.upsert(ImmutableList.of(
                record(schema, "k1", "k1_updated", 11L, "U", 200L),
                record(schema, "k2", "k2_deleted", 22L, DELETE_OPERATION_VALUE, 200L)), secondCommit);
        client.commit(secondCommit, secondStatuses);
    }

    private static HoodieRecord<HoodieAvroPayload> record(Schema schema, String key, String name, long value, String op, long ts)
    {
        GenericRecord record = new GenericData.Record(schema);
        record.put(RECORD_KEY_FIELD, key);
        record.put("name", name);
        record.put("value", value);
        record.put(OP_FIELD, op);
        record.put(ORDERING_FIELD, ts);
        return avroRecord(record, key);
    }
}
