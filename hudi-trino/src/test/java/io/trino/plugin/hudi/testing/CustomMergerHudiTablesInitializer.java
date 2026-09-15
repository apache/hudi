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
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.config.HoodieWriteConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.metastore.HiveType.HIVE_LONG;
import static io.trino.metastore.HiveType.HIVE_STRING;

/**
 * Creates a non-partitioned Merge-On-Read table at test runtime that is configured to use a custom record
 * merger ({@link KeyBasedTestRecordMerger}) via {@link RecordMergeMode#CUSTOM}. The table is written with one
 * {@code insert} (producing base files) followed by one {@code upsert} of the same keys (producing log files),
 * with inline compaction disabled so the log files survive and must be merged at read time.
 * <p>
 * Two tables are registered in the metastore: a read-optimized table (base files only) and a real-time table
 * (suffix {@code _rt}) that merges base + log files through the file group reader.
 * <p>
 * Data is laid out so the key-based merge result is distinguishable from both the base-only view and the
 * built-in newest-wins behavior (see {@code TestHudiCustomMerger}).
 */
public class CustomMergerHudiTablesInitializer
        extends AbstractMergerHudiTablesInitializer
{
    public static final String TABLE_NAME = "custom_merger_mor";
    public static final String RT_TABLE_NAME = TABLE_NAME + "_rt";

    public CustomMergerHudiTablesInitializer()
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
                new Column(ORDERING_FIELD, HIVE_LONG, Optional.empty(), Map.of()));
    }

    @Override
    protected Schema avroSchema()
    {
        List<Schema.Field> fields = ImmutableList.of(
                new Schema.Field(RECORD_KEY_FIELD, Schema.create(Schema.Type.STRING)),
                new Schema.Field("name", Schema.create(Schema.Type.STRING)),
                new Schema.Field("value", Schema.create(Schema.Type.LONG)),
                new Schema.Field(ORDERING_FIELD, Schema.create(Schema.Type.LONG)));
        return Schema.createRecord(TABLE_NAME, null, null, false, new ArrayList<>(fields));
    }

    @Override
    protected void configureTableConfig(HoodieTableMetaClient.TableBuilder tableBuilder)
    {
        tableBuilder
                .setPayloadClassName(HoodieAvroPayload.class.getName())
                .setRecordMergeMode(RecordMergeMode.CUSTOM)
                .setRecordMergeStrategyId(KeyBasedTestRecordMerger.MERGE_STRATEGY_ID);
    }

    @Override
    protected void configureWriteConfig(HoodieWriteConfig.Builder writeConfigBuilder)
    {
        writeConfigBuilder
                .withRecordMergeMode(RecordMergeMode.CUSTOM)
                .withRecordMergeStrategyId(KeyBasedTestRecordMerger.MERGE_STRATEGY_ID)
                .withRecordMergeImplClasses(KeyBasedTestRecordMerger.class.getName());
    }

    @Override
    protected void writeInitialCommits(HoodieJavaWriteClient<HoodieAvroPayload> client)
    {
        Schema schema = avroSchema();
        // First commit: bulk insert base records (produces base parquet files).
        String firstCommit = client.startCommit();
        List<WriteStatus> firstStatuses = client.bulkInsert(ImmutableList.of(
                record(schema, "k1", "k1_base", 10L, 1L),
                record(schema, "k2", "k2_base", 100L, 1L)), firstCommit);
        client.commit(firstCommit, firstStatuses);

        // Second commit: upserts the same keys (produces log files since inline compaction is disabled).
        // k1 update has a larger value (99 > 10) -> keep-max keeps the update.
        // k2 update has a smaller value (5 < 100) -> keep-max keeps the original base record.
        String secondCommit = client.startCommit();
        List<WriteStatus> secondStatuses = client.upsert(ImmutableList.of(
                record(schema, "k1", "k1_updated", 99L, 2L),
                record(schema, "k2", "k2_updated", 5L, 2L)), secondCommit);
        client.commit(secondCommit, secondStatuses);
    }

    private static HoodieRecord<HoodieAvroPayload> record(Schema schema, String key, String name, long value, long ts)
    {
        GenericRecord record = new GenericData.Record(schema);
        record.put(RECORD_KEY_FIELD, key);
        record.put("name", name);
        record.put("value", value);
        record.put(ORDERING_FIELD, ts);
        return avroRecord(record, key);
    }
}
