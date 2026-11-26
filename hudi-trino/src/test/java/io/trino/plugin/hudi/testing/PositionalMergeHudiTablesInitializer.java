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
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

import static io.trino.metastore.HiveType.HIVE_BOOLEAN;
import static io.trino.metastore.HiveType.HIVE_LONG;
import static io.trino.metastore.HiveType.HIVE_STRING;
import static java.util.Objects.requireNonNull;
import static org.apache.hudi.common.model.HoodieRecord.HOODIE_IS_DELETED_FIELD;

/**
 * A non-partitioned Merge-On-Read table whose log blocks carry record positions, for the positional merge of the
 * vectorized MOR read. The SIMPLE index tags each update and delete with the row it replaces in the base file, so the
 * log blocks are written with positions.
 * <p>
 * The base commit holds {@link #ROWS} keys {@code k0000..}, key {@code i} with value {@code i}, in small row groups
 * whose value ranges do not overlap, so a predicate on {@code value} prunes row groups. A log commit then moves
 * {@code k0010} out of the low value range and {@code k0900} into it, rewrites a run of keys in one page, and, under
 * event time ordering, adds updates and soft deletes that lose to the base row. A last commit hard deletes two keys.
 * {@link #expectedValues} is the resulting snapshot, computed from the same definitions.
 */
public class PositionalMergeHudiTablesInitializer
        extends AbstractMergerHudiTablesInitializer
{
    public static final int ROWS = 1000;
    public static final String COMMIT_TIME_TABLE_NAME = "positional_commit_time_mor";
    public static final String EVENT_TIME_TABLE_NAME = "positional_event_time_mor";

    private static final long BASE_TS = 100;
    private static final int UPDATED_RUN_START = 500;
    private static final int UPDATED_RUN_LENGTH = 10;
    private static final List<Integer> HARD_DELETES = List.of(20, 950);

    private final RecordMergeMode mergeMode;

    private PositionalMergeHudiTablesInitializer(String tableName, RecordMergeMode mergeMode)
    {
        super(tableName);
        this.mergeMode = requireNonNull(mergeMode, "mergeMode is null");
    }

    public static PositionalMergeHudiTablesInitializer commitTimeOrdering()
    {
        return new PositionalMergeHudiTablesInitializer(COMMIT_TIME_TABLE_NAME, RecordMergeMode.COMMIT_TIME_ORDERING);
    }

    public static PositionalMergeHudiTablesInitializer eventTimeOrdering()
    {
        return new PositionalMergeHudiTablesInitializer(EVENT_TIME_TABLE_NAME, RecordMergeMode.EVENT_TIME_ORDERING);
    }

    public static String key(int index)
    {
        return "k%04d".formatted(index);
    }

    /**
     * The value of every key in the snapshot of the table merged in {@code mergeMode}, by key.
     */
    public static Map<String, Long> expectedValues(RecordMergeMode mergeMode)
    {
        Map<String, Long> values = new TreeMap<>();
        for (int i = 0; i < ROWS; i++) {
            values.put(key(i), (long) i);
        }
        for (LogRecord update : updates(mergeMode)) {
            if (mergeMode == RecordMergeMode.EVENT_TIME_ORDERING && update.ts() < BASE_TS) {
                continue;
            }
            if (update.deleted()) {
                values.remove(update.key());
            }
            else {
                values.put(update.key(), update.value());
            }
        }
        HARD_DELETES.forEach(index -> values.remove(key(index)));
        return values;
    }

    private record LogRecord(String key, long value, long ts, boolean deleted) {}

    private static List<LogRecord> updates(RecordMergeMode mergeMode)
    {
        ImmutableList.Builder<LogRecord> updates = ImmutableList.builder();
        // Under commit time ordering the latest write wins even with a lower ordering value
        long ts = mergeMode == RecordMergeMode.COMMIT_TIME_ORDERING ? 50 : 200;
        updates.add(new LogRecord(key(10), 5000, ts, false));
        updates.add(new LogRecord(key(900), 1, ts, false));
        for (int i = UPDATED_RUN_START; i < UPDATED_RUN_START + UPDATED_RUN_LENGTH; i++) {
            updates.add(new LogRecord(key(i), i + 10000, ts, false));
        }
        updates.add(new LogRecord(key(12), 12, 200, true));
        if (mergeMode == RecordMergeMode.EVENT_TIME_ORDERING) {
            // Lose to their base rows on event time
            updates.add(new LogRecord(key(11), 9999, 50, false));
            updates.add(new LogRecord(key(13), 13, 50, true));
            // A tie on the ordering value goes to the log record
            updates.add(new LogRecord(key(14), 7777, BASE_TS, false));
        }
        return updates.build();
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
        return Schema.createRecord("positional_mor", null, null, false, new ArrayList<>(fields));
    }

    @Override
    protected void configureTableConfig(HoodieTableMetaClient.TableBuilder tableBuilder)
    {
        tableBuilder.setRecordMergeMode(mergeMode);
    }

    @Override
    protected void configureWriteConfig(HoodieWriteConfig.Builder writeConfigBuilder)
    {
        writeConfigBuilder
                .withRecordMergeMode(mergeMode)
                .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.SIMPLE).build())
                .withWriteRecordPositionsEnabled(true)
                // Row groups of about a hundred rows, so a predicate on value prunes most of them
                .withStorageConfig(HoodieStorageConfig.newBuilder().parquetBlockSize(2048).parquetPageSize(1024).build());
    }

    @Override
    protected void writeInitialCommits(HoodieJavaWriteClient<HoodieAvroPayload> client)
    {
        Schema schema = avroSchema();
        List<HoodieRecord<HoodieAvroPayload>> baseRecords = new ArrayList<>(ROWS);
        for (int i = 0; i < ROWS; i++) {
            baseRecords.add(record(schema, new LogRecord(key(i), i, BASE_TS, false)));
        }
        String baseCommit = client.startCommit();
        List<WriteStatus> baseStatuses = client.bulkInsert(baseRecords, baseCommit);
        client.commit(baseCommit, baseStatuses);

        String updateCommit = client.startCommit();
        List<WriteStatus> updateStatuses = client.upsert(
                new ArrayList<>(updates(mergeMode).stream().map(update -> record(schema, update)).toList()),
                updateCommit);
        client.commit(updateCommit, updateStatuses);

        String deleteCommit = client.startCommit();
        List<WriteStatus> deleteStatuses = client.delete(new ArrayList<>(HARD_DELETES.stream().map(index -> hoodieKey(key(index))).toList()), deleteCommit);
        client.commit(deleteCommit, deleteStatuses);
    }

    private static HoodieRecord<HoodieAvroPayload> record(Schema schema, LogRecord logRecord)
    {
        GenericRecord record = new GenericData.Record(schema);
        record.put(RECORD_KEY_FIELD, logRecord.key());
        record.put("name", "name_" + logRecord.key() + "_" + logRecord.value());
        record.put("value", logRecord.value());
        record.put(ORDERING_FIELD, logRecord.ts());
        record.put(HOODIE_IS_DELETED_FIELD, logRecord.deleted());
        return avroRecord(record, logRecord.key());
    }
}
