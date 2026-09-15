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
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.config.HoodieWriteConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.metastore.HiveType.HIVE_BOOLEAN;
import static io.trino.metastore.HiveType.HIVE_LONG;
import static io.trino.metastore.HiveType.HIVE_STRING;
import static org.apache.hudi.common.model.HoodieRecord.HOODIE_IS_DELETED_FIELD;

/**
 * Creates a non-partitioned Merge-On-Read table in {@link RecordMergeMode#EVENT_TIME_ORDERING} that
 * exercises the read-side merge-mode dispatch with deletes (issue apache/hudi#18898). ONLY a record merge
 * mode is set (no payload class), so table creation persists the mode as-is, which is exactly the dispatch
 * input {@code HudiTrinoReaderContext.getRecordMerger} switches on.
 * <p>
 * A base commit is followed by a log commit carrying an update, a soft delete
 * ({@code _hoodie_is_deleted=true}), an OBSOLETE soft delete and an OBSOLETE update (both with an ordering
 * value LOWER than the base row's, so event-time merging must keep the base row), and then a hard-delete
 * commit ({@code writeClient.delete}) that produces a native delete log file read back through the
 * connector's {@code getFileRecordIterator}.
 * <p>
 * Records are wrapped in {@link HoodieAvroPayload}, which implements {@code HoodieRecordPayload} directly
 * (NOT {@code BaseAvroPayload}), so rows with {@code _hoodie_is_deleted=true} are written as DATA records
 * and delete semantics are evaluated at READ time. See {@code TestHudiMorMergeModeSemantics}.
 */
public class EventTimeDeletesHudiTablesInitializer
        extends AbstractMergerHudiTablesInitializer
{
    public static final String TABLE_NAME = "deletes_mor";
    public static final String RT_TABLE_NAME = TABLE_NAME + "_rt";

    public EventTimeDeletesHudiTablesInitializer()
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
                new Column(ORDERING_FIELD, HIVE_LONG, Optional.empty(), Map.of()),
                new Column(HOODIE_IS_DELETED_FIELD, HIVE_BOOLEAN, Optional.empty(), Map.of()));
    }

    @Override
    protected Schema avroSchema()
    {
        List<Schema.Field> fields = ImmutableList.of(
                new Schema.Field(RECORD_KEY_FIELD, Schema.create(Schema.Type.STRING)),
                new Schema.Field("name", Schema.create(Schema.Type.STRING)),
                new Schema.Field("value", Schema.create(Schema.Type.LONG)),
                new Schema.Field(ORDERING_FIELD, Schema.create(Schema.Type.LONG)),
                new Schema.Field(
                        HOODIE_IS_DELETED_FIELD,
                        Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.BOOLEAN)),
                        null,
                        JsonProperties.NULL_VALUE));
        return Schema.createRecord(TABLE_NAME, null, null, false, new ArrayList<>(fields));
    }

    @Override
    protected void configureTableConfig(HoodieTableMetaClient.TableBuilder tableBuilder)
    {
        tableBuilder.setRecordMergeMode(RecordMergeMode.EVENT_TIME_ORDERING);
    }

    @Override
    protected void configureWriteConfig(HoodieWriteConfig.Builder writeConfigBuilder)
    {
        writeConfigBuilder.withRecordMergeMode(RecordMergeMode.EVENT_TIME_ORDERING);
    }

    @Override
    protected void writeInitialCommits(HoodieJavaWriteClient<HoodieAvroPayload> client)
    {
        Schema schema = avroSchema();
        // First commit: base parquet file with 6 keys, all at ordering value (ts) 100.
        String firstCommit = client.startCommit();
        List<WriteStatus> firstStatuses = client.bulkInsert(ImmutableList.of(
                record(schema, "k1", "k1_base", 10L, 100L, false),
                record(schema, "k2", "k2_base", 20L, 100L, false),
                record(schema, "k3", "k3_base", 30L, 100L, false),
                record(schema, "k4", "k4_base", 40L, 100L, false),
                record(schema, "k5", "k5_base", 50L, 100L, false),
                record(schema, "k6", "k6_base", 60L, 100L, false)), firstCommit);
        client.commit(firstCommit, firstStatuses);

        // Second commit (log file). Event-time merging must resolve each key by ordering value:
        //  - k1: update with HIGHER ts (200) -> update wins
        //  - k3: soft delete with HIGHER ts (200) -> row deleted at read time
        //  - k4: soft delete with LOWER ts (50) -> OBSOLETE delete, base row survives
        //  - k6: update with LOWER ts (50) -> OBSOLETE update, base row survives
        String secondCommit = client.startCommit();
        List<WriteStatus> secondStatuses = client.upsert(ImmutableList.of(
                record(schema, "k1", "k1_updated", 11L, 200L, false),
                record(schema, "k3", "k3_deleted", 33L, 200L, true),
                record(schema, "k4", "k4_deleted", 44L, 50L, true),
                record(schema, "k6", "k6_updated", 66L, 50L, false)), secondCommit);
        client.commit(secondCommit, secondStatuses);

        // Third commit: HARD delete of k2. At the current table version this produces a native
        // delete log file, which the file-group reader reads back through the connector's
        // getFileRecordIterator with the synthetic delete-log schema (record key + ordering).
        // Hard deletes carry the sentinel ordering value and win regardless of merge mode.
        String deleteCommit = client.startCommit();
        List<WriteStatus> deleteStatuses = client.delete(
                ImmutableList.of(hoodieKey("k2")), deleteCommit);
        client.commit(deleteCommit, deleteStatuses);
    }

    private static HoodieRecord<HoodieAvroPayload> record(Schema schema, String key, String name, long value, long ts, boolean deleted)
    {
        GenericRecord record = new GenericData.Record(schema);
        record.put(RECORD_KEY_FIELD, key);
        record.put("name", name);
        record.put("value", value);
        record.put(ORDERING_FIELD, ts);
        record.put(HOODIE_IS_DELETED_FIELD, deleted);
        // HoodieAvroPayload passes the record through untouched (it is not a BaseAvroPayload), so a row
        // with _hoodie_is_deleted=true is WRITTEN as a data record and only deleted at merge/read time.
        return avroRecord(record, key);
    }
}
