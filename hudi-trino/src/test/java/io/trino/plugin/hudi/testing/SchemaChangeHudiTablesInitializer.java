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

import static com.google.common.base.Preconditions.checkState;
import static io.trino.metastore.HiveType.HIVE_LONG;
import static io.trino.metastore.HiveType.HIVE_STRING;

/**
 * Creates a non-partitioned Merge-On-Read table whose schema gains a column between two queries, the way a
 * schema-on-write append followed by hive sync evolves it: {@link #addColumnAndUpsert()} writes a delta commit with
 * the column added and then adds it to the metastore. The merge read path takes the table schema from the handle the
 * coordinator builds for each query, so a query after the evolution must see the new column; a schema cached across
 * queries would not.
 * <p>
 * The initial commits leave a log file next to the base file, so the real-time table goes through the file group
 * reader both before and after the evolution. Keys carry a table prefix because the in-memory index the write client
 * tags upserts with is static, and therefore shared with the other fixtures of the same JVM.
 */
public class SchemaChangeHudiTablesInitializer
        extends AbstractMergerHudiTablesInitializer
{
    public static final String TABLE_NAME = "schema_change_mor";
    public static final String RT_TABLE_NAME = TABLE_NAME + "_rt";
    public static final String ADDED_COLUMN = "added_value";

    private boolean columnAdded;

    public SchemaChangeHudiTablesInitializer()
    {
        super(TABLE_NAME);
    }

    @Override
    protected List<Column> dataColumns()
    {
        // The metastore definition registered at initialization; the added column reaches it through addMetastoreColumn
        return ImmutableList.of(
                new Column(RECORD_KEY_FIELD, HIVE_STRING, Optional.empty(), Map.of()),
                new Column("name", HIVE_STRING, Optional.empty(), Map.of()),
                new Column(ORDERING_FIELD, HIVE_LONG, Optional.empty(), Map.of()));
    }

    @Override
    protected Schema avroSchema()
    {
        List<Schema.Field> fields = new ArrayList<>();
        fields.add(new Schema.Field(RECORD_KEY_FIELD, Schema.create(Schema.Type.STRING)));
        fields.add(new Schema.Field("name", Schema.create(Schema.Type.STRING)));
        fields.add(new Schema.Field(ORDERING_FIELD, Schema.create(Schema.Type.LONG)));
        if (columnAdded) {
            // A backward-compatible addition: nullable, defaulting to null for the records written before it
            Schema nullableLong = Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.LONG));
            fields.add(new Schema.Field(ADDED_COLUMN, nullableLong, null, JsonProperties.NULL_VALUE));
        }
        return Schema.createRecord(TABLE_NAME, null, null, false, fields);
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
    protected boolean keepsWriterOpen()
    {
        return true;
    }

    @Override
    protected void writeInitialCommits(HoodieJavaWriteClient<HoodieAvroPayload> client)
    {
        Schema schema = avroSchema();
        // Base file with two keys
        String firstCommit = client.startCommit();
        List<WriteStatus> firstStatuses = client.bulkInsert(ImmutableList.of(
                record(schema, "sc_k1", "k1_base", 1L, null),
                record(schema, "sc_k2", "k2_base", 1L, null)), firstCommit);
        client.commit(firstCommit, firstStatuses);

        // Log file updating sc_k1, before the evolution
        String secondCommit = client.startCommit();
        List<WriteStatus> secondStatuses = client.upsert(ImmutableList.of(
                record(schema, "sc_k1", "k1_updated", 2L, null)), secondCommit);
        client.commit(secondCommit, secondStatuses);
    }

    /**
     * Adds {@link #ADDED_COLUMN} to the table schema with a delta commit updating {@code sc_k2} to carry a value for
     * it, mirrors the commit into the Trino filesystem, and adds the column to the metastore.
     */
    public void addColumnAndUpsert()
    {
        checkState(!columnAdded, "column already added");
        columnAdded = true;
        reopenWriteClient();
        Schema schema = avroSchema();
        String commitTime = writeClient().startCommit();
        List<WriteStatus> statuses = writeClient().upsert(ImmutableList.of(
                record(schema, "sc_k2", "k2_updated", 3L, 42L)), commitTime);
        writeClient().commit(commitTime, statuses);
        syncToTrino();
        addMetastoreColumn(ADDED_COLUMN, HIVE_LONG);
    }

    private static HoodieRecord<HoodieAvroPayload> record(Schema schema, String key, String name, long ts, Long addedValue)
    {
        GenericRecord record = new GenericData.Record(schema);
        record.put(RECORD_KEY_FIELD, key);
        record.put("name", name);
        record.put(ORDERING_FIELD, ts);
        if (schema.getField(ADDED_COLUMN) != null) {
            record.put(ADDED_COLUMN, addedValue);
        }
        return avroRecord(record, key);
    }
}
