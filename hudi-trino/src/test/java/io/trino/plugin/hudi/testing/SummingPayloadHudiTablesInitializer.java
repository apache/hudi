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
 * Creates a non-partitioned Merge-On-Read table whose merge semantics come from the {@link SummingTestPayload}
 * class persisted in the table config (issue apache/hudi#18898). ONLY the payload class is set (no merge mode
 * / strategy id), so table creation translates it exactly as a real writer would: this user-defined payload is
 * NOT in the deprecation set, so it is persisted as RECORD_MERGE_MODE=CUSTOM with the payload-based merge
 * strategy id. Reads resolve {@code HoodieAvroRecordMerger} (no {@code hudi.record-merger-impls} needed) and
 * run the payload's {@code combineAndGetUpdateValue}, observable as SUMMED values.
 * <p>
 * A final commit hard-deletes a key ({@code writeClient.delete}): the native delete log record routes to
 * {@code HoodieAvroRecordMerger} but wins on its {@code isCommitTimeOrderingDelete} short-circuit (the
 * delete carries the sentinel ordering value), before any payload is constructed -- the delete coverage
 * both ordering arms already have.
 * <p>
 * Records are wrapped in {@link HoodieAvroPayload} (a pass-through that is NOT a {@code BaseAvroPayload}), so
 * every merge decision happens at read time from the table config. See {@code TestHudiMorPayloadSemantics}.
 */
public class SummingPayloadHudiTablesInitializer
        extends AbstractMergerHudiTablesInitializer
{
    public static final String TABLE_NAME = "summing_mor";
    public static final String RT_TABLE_NAME = TABLE_NAME + "_rt";

    private static final String SUM_FIELD = SummingTestPayload.SUM_COLUMN;

    public SummingPayloadHudiTablesInitializer()
    {
        super(TABLE_NAME);
    }

    @Override
    protected List<Column> dataColumns()
    {
        return ImmutableList.of(
                new Column(RECORD_KEY_FIELD, HIVE_STRING, Optional.empty(), Map.of()),
                new Column(SUM_FIELD, HIVE_LONG, Optional.empty(), Map.of()),
                new Column(ORDERING_FIELD, HIVE_LONG, Optional.empty(), Map.of()));
    }

    @Override
    protected Schema avroSchema()
    {
        List<Schema.Field> fields = ImmutableList.of(
                new Schema.Field(RECORD_KEY_FIELD, Schema.create(Schema.Type.STRING)),
                new Schema.Field(SUM_FIELD, Schema.create(Schema.Type.LONG)),
                new Schema.Field(ORDERING_FIELD, Schema.create(Schema.Type.LONG)));
        return Schema.createRecord(TABLE_NAME, null, null, false, new ArrayList<>(fields));
    }

    @Override
    protected void configureTableConfig(HoodieTableMetaClient.TableBuilder tableBuilder)
    {
        tableBuilder.setPayloadClassName(SummingTestPayload.class.getName());
    }

    @Override
    protected void configureWriteConfig(HoodieWriteConfig.Builder writeConfigBuilder)
    {
        writeConfigBuilder.withWritePayLoad(SummingTestPayload.class.getName());
    }

    @Override
    protected void writeInitialCommits(HoodieJavaWriteClient<HoodieAvroPayload> client)
    {
        Schema schema = avroSchema();
        String firstCommit = client.startCommit();
        List<WriteStatus> firstStatuses = client.bulkInsert(ImmutableList.of(
                record(schema, "k1", 10L, 100L),
                record(schema, "k2", 20L, 100L)), firstCommit);
        client.commit(firstCommit, firstStatuses);

        // The payload's combineAndGetUpdateValue SUMS stored and incoming values: 10 + 99 = 109 --
        // a result neither overwrite (99) nor base-only (10) can produce.
        String secondCommit = client.startCommit();
        List<WriteStatus> secondStatuses = client.upsert(ImmutableList.of(
                record(schema, "k1", 99L, 200L)), secondCommit);
        client.commit(secondCommit, secondStatuses);

        // Third commit: hard delete of k2. The native delete log record reaches the payload-based
        // CUSTOM merge arm, where it wins on HoodieAvroRecordMerger's isCommitTimeOrderingDelete
        // short-circuit (writeClient.delete records carry the sentinel ordering value), before any
        // payload is constructed -- the delete path of the user-merger dispatch.
        String deleteCommit = client.startCommit();
        List<WriteStatus> deleteStatuses = client.delete(
                ImmutableList.of(hoodieKey("k2")), deleteCommit);
        client.commit(deleteCommit, deleteStatuses);
    }

    private static HoodieRecord<HoodieAvroPayload> record(Schema schema, String key, long value, long ts)
    {
        GenericRecord record = new GenericData.Record(schema);
        record.put(RECORD_KEY_FIELD, key);
        record.put(SUM_FIELD, value);
        record.put(ORDERING_FIELD, ts);
        return avroRecord(record, key);
    }
}
