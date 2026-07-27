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
 * Creates a non-partitioned Merge-On-Read table at test runtime configured with the
 * NON-projection-compatible {@link NonProjectionCompatibleRankMerger} via {@link RecordMergeMode#CUSTOM}.
 * One {@code bulkInsert} (base files) is followed by one {@code upsert} of the same keys (log files), with
 * inline compaction disabled so the merger runs at read time over a full-table-schema read.
 * <p>
 * Data is laid out so each merge direction is distinguishable: one key's winning rank comes from the LOG
 * record and the other's from the BASE record, so a correct result proves both sides of the merge supplied
 * the (never projected) {@code merge_rank} column. See {@code TestHudiNonProjectionCompatibleMerger}.
 */
public class NonProjectionCompatibleMergerHudiTablesInitializer
        extends AbstractMergerHudiTablesInitializer
{
    public static final String TABLE_NAME = "non_projection_merger_mor";
    public static final String RT_TABLE_NAME = TABLE_NAME + "_rt";

    private static final String RANK_FIELD = NonProjectionCompatibleRankMerger.RANK_COLUMN;

    public NonProjectionCompatibleMergerHudiTablesInitializer()
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
                new Column(RANK_FIELD, HIVE_LONG, Optional.empty(), Map.of()),
                new Column(ORDERING_FIELD, HIVE_LONG, Optional.empty(), Map.of()));
    }

    @Override
    protected Schema avroSchema()
    {
        List<Schema.Field> fields = ImmutableList.of(
                new Schema.Field(RECORD_KEY_FIELD, Schema.create(Schema.Type.STRING)),
                new Schema.Field("name", Schema.create(Schema.Type.STRING)),
                new Schema.Field("value", Schema.create(Schema.Type.LONG)),
                new Schema.Field(RANK_FIELD, Schema.create(Schema.Type.LONG)),
                new Schema.Field(ORDERING_FIELD, Schema.create(Schema.Type.LONG)));
        return Schema.createRecord(TABLE_NAME, null, null, false, new ArrayList<>(fields));
    }

    @Override
    protected void configureTableConfig(HoodieTableMetaClient.TableBuilder tableBuilder)
    {
        tableBuilder
                .setPayloadClassName(HoodieAvroPayload.class.getName())
                .setRecordMergeMode(RecordMergeMode.CUSTOM)
                .setRecordMergeStrategyId(NonProjectionCompatibleRankMerger.MERGE_STRATEGY_ID);
    }

    @Override
    protected void configureWriteConfig(HoodieWriteConfig.Builder writeConfigBuilder)
    {
        writeConfigBuilder
                .withRecordMergeMode(RecordMergeMode.CUSTOM)
                .withRecordMergeStrategyId(NonProjectionCompatibleRankMerger.MERGE_STRATEGY_ID)
                .withRecordMergeImplClasses(NonProjectionCompatibleRankMerger.class.getName());
    }

    @Override
    protected void writeInitialCommits(HoodieJavaWriteClient<HoodieAvroPayload> client)
    {
        Schema schema = avroSchema();
        // First commit: bulk insert base records (produces base parquet files).
        String firstCommit = client.startCommit();
        List<WriteStatus> firstStatuses = client.bulkInsert(ImmutableList.of(
                record(schema, "k1", "k1_base", 10L, 5L, 1L),
                record(schema, "k2", "k2_base", 100L, 9L, 1L)), firstCommit);
        client.commit(firstCommit, firstStatuses);

        // Second commit: upserts the same keys (produces log files since inline compaction is disabled).
        // k1 update has a HIGHER rank (7 > 5) -> merge keeps the update (99): the LOG record's rank decides.
        // k2 update has a LOWER rank (1 < 9) -> merge keeps the base record (100): the BASE record's rank
        // decides, which only works when the base read carries merge_rank despite it never being projected.
        String secondCommit = client.startCommit();
        List<WriteStatus> secondStatuses = client.upsert(ImmutableList.of(
                record(schema, "k1", "k1_updated", 99L, 7L, 2L),
                record(schema, "k2", "k2_updated", 4L, 1L, 2L)), secondCommit);
        client.commit(secondCommit, secondStatuses);
    }

    private static HoodieRecord<HoodieAvroPayload> record(Schema schema, String key, String name, long value, long rank, long ts)
    {
        GenericRecord record = new GenericData.Record(schema);
        record.put(RECORD_KEY_FIELD, key);
        record.put("name", name);
        record.put("value", value);
        record.put(RANK_FIELD, rank);
        record.put(ORDERING_FIELD, ts);
        return avroRecord(record, key);
    }
}
