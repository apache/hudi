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
 * Creates a non-partitioned Merge-On-Read table in {@link RecordMergeMode#EVENT_TIME_ORDERING} whose
 * ordering field is a STRING rather than a long.
 * <p>
 * This is the shape that broke on this release line. Base-file records reach the file-group reader through
 * {@code HudiAvroSerializer.serialize}, which builds them from Trino blocks, while classic Avro log blocks
 * deserialize inline and yield Avro's own {@code Utf8}. The two meet in
 * {@code BufferedRecordMergerFactory#shouldKeepNewerRecord}, which compares their ordering values directly,
 * and {@code Utf8.compareTo} casts its argument to {@code Utf8} -- so a String base ordering value against a
 * Utf8 log ordering value threw ClassCastException and failed the query.
 * <p>
 * A long ordering field cannot catch this: Long is the same class on both sides. The other MoR fixtures all
 * order on a long, which is why only the Trino E2E stock-ticks table (string {@code ts}) caught it.
 * <p>
 * Ordering values are written zero-padded so that lexicographic string ordering matches the intended
 * event-time ordering.
 */
public class StringOrderingHudiTablesInitializer
        extends AbstractMergerHudiTablesInitializer
{
    public static final String TABLE_NAME = "string_ordering_mor";
    public static final String RT_TABLE_NAME = TABLE_NAME + "_rt";

    public StringOrderingHudiTablesInitializer()
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
                new Column(ORDERING_FIELD, HIVE_STRING, Optional.empty(), Map.of()));
    }

    @Override
    protected Schema avroSchema()
    {
        List<Schema.Field> fields = ImmutableList.of(
                new Schema.Field(RECORD_KEY_FIELD, Schema.create(Schema.Type.STRING)),
                new Schema.Field("name", Schema.create(Schema.Type.STRING)),
                new Schema.Field("value", Schema.create(Schema.Type.LONG)),
                new Schema.Field(ORDERING_FIELD, Schema.create(Schema.Type.STRING)));
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
        // Base parquet file: both keys at ts "2018-08-31 10:00:00".
        String firstCommit = client.startCommit();
        List<WriteStatus> firstStatuses = client.bulkInsert(ImmutableList.of(
                record(schema, "k1", "k1_base", 10L, "2018-08-31 10:00:00"),
                record(schema, "k2", "k2_base", 20L, "2018-08-31 10:00:00")), firstCommit);
        client.commit(firstCommit, firstStatuses);

        // Log commit. Both keys also exist in the base file, so the merge path compares a base ordering
        // value against a log one for each -- which is what reproduces the type mismatch.
        //  - k1: update with a LATER ts -> update wins
        //  - k2: update with an EARLIER ts -> OBSOLETE, base row survives
        String secondCommit = client.startCommit();
        List<WriteStatus> secondStatuses = client.upsert(ImmutableList.of(
                record(schema, "k1", "k1_updated", 11L, "2018-08-31 11:00:00"),
                record(schema, "k2", "k2_updated", 22L, "2018-08-31 09:00:00")), secondCommit);
        client.commit(secondCommit, secondStatuses);
    }

    private static HoodieRecord<HoodieAvroPayload> record(Schema schema, String key, String name, long value, String ts)
    {
        GenericRecord record = new GenericData.Record(schema);
        record.put(RECORD_KEY_FIELD, key);
        record.put("name", name);
        record.put("value", value);
        record.put(ORDERING_FIELD, ts);
        return avroRecord(record, key);
    }
}
