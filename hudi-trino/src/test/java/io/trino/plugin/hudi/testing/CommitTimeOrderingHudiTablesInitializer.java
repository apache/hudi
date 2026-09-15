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
 * Creates a non-partitioned Merge-On-Read table in {@link RecordMergeMode#COMMIT_TIME_ORDERING} that
 * exercises the read-side merge-mode dispatch (issue apache/hudi#18898). ONLY a record merge mode is set
 * (no payload class), so table creation persists the mode as-is, which is exactly the dispatch input
 * {@code HudiTrinoReaderContext.getRecordMerger} switches on.
 * <p>
 * A base commit is followed by a log commit whose update carries an ordering value LOWER than the base
 * row's: latest-write-wins must KEEP the update, the exact mirror of the event-time obsolete-update case
 * in {@link EventTimeDeletesHudiTablesInitializer}, which is what discriminates the two merger dispatches.
 * A final commit hard-deletes a key ({@code writeClient.delete}); commit-time deletes always win. See
 * {@code TestHudiMorMergeModeSemantics}.
 */
public class CommitTimeOrderingHudiTablesInitializer
        extends AbstractMergerHudiTablesInitializer
{
    public static final String TABLE_NAME = "commit_time_mor";
    public static final String RT_TABLE_NAME = TABLE_NAME + "_rt";

    public CommitTimeOrderingHudiTablesInitializer()
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
        Schema schema = avroSchema();
        // First commit: base parquet file with 3 keys at ts 100.
        String firstCommit = client.startCommit();
        List<WriteStatus> firstStatuses = client.bulkInsert(ImmutableList.of(
                record(schema, "k1", "k1_base", 10L, 100L),
                record(schema, "k2", "k2_base", 20L, 100L),
                record(schema, "k3", "k3_base", 30L, 100L)), firstCommit);
        client.commit(firstCommit, firstStatuses);

        // Second commit (log file): update k1 with a LOWER ts (50). Commit-time ordering keeps the
        // LATEST WRITE regardless of the ordering value -- the exact mirror of the event-time k6
        // case, which discriminates OverwriteWithLatestMerger from event-time merging.
        String secondCommit = client.startCommit();
        List<WriteStatus> secondStatuses = client.upsert(ImmutableList.of(
                record(schema, "k1", "k1_updated", 11L, 50L)), secondCommit);
        client.commit(secondCommit, secondStatuses);

        // Third commit: hard delete of k2 (commit-time deletes always win).
        String deleteCommit = client.startCommit();
        List<WriteStatus> deleteStatuses = client.delete(
                ImmutableList.of(hoodieKey("k2")), deleteCommit);
        client.commit(deleteCommit, deleteStatuses);
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
