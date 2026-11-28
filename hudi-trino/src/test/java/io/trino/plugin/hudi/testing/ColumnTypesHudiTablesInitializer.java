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
import io.trino.metastore.HiveType;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.config.HoodieWriteConfig;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.metastore.HiveType.HIVE_LONG;
import static io.trino.metastore.HiveType.HIVE_STRING;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hudi.common.config.HoodieMemoryConfig.MAX_MEMORY_FOR_MERGE;

/**
 * Creates a non-partitioned Merge-On-Read table with one column per kind of value the merge buffer holds: tinyint,
 * smallint, decimal, date, timestamp, double, binary, array, map and struct. A base commit of three rows is followed
 * by a log commit that updates one of them and nulls every typed column of another, so the read-time merge carries
 * each kind, and a null of each kind, through the merge buffer. See {@code TestHudiMorMergeModeSemantics}.
 */
public class ColumnTypesHudiTablesInitializer
        extends AbstractMergerHudiTablesInitializer
{
    public static final String TABLE_NAME = "column_types_mor";
    public static final String RT_TABLE_NAME = TABLE_NAME + "_rt";
    public static final String SPILLING_TABLE_NAME = TABLE_NAME + "_spilling";
    public static final String SPILLING_RT_TABLE_NAME = SPILLING_TABLE_NAME + "_rt";

    private static final Schema DECIMAL_SCHEMA = LogicalTypes.decimal(10, 2).addToSchema(Schema.createFixed("c_decimal_fixed", null, null, 5));
    private static final Schema STRUCT_SCHEMA = SchemaBuilder.record("c_struct_record").fields()
            .optionalString("name")
            .optionalInt("count")
            .endRecord();

    public ColumnTypesHudiTablesInitializer()
    {
        super(TABLE_NAME);
    }

    private ColumnTypesHudiTablesInitializer(String tableName, Map<String, String> extraTableProperties)
    {
        super(tableName, extraTableProperties);
    }

    /**
     * The same commits under {@link #SPILLING_TABLE_NAME}, with a one-byte merge memory budget, so every log record
     * is spilled to disk.
     */
    public static ColumnTypesHudiTablesInitializer spillingEveryLogRecord()
    {
        return new ColumnTypesHudiTablesInitializer(SPILLING_TABLE_NAME, Map.of(MAX_MEMORY_FOR_MERGE.key(), "1"));
    }

    @Override
    protected List<Column> dataColumns()
    {
        return ImmutableList.of(
                new Column(RECORD_KEY_FIELD, HIVE_STRING, Optional.empty(), Map.of()),
                new Column(ORDERING_FIELD, HIVE_LONG, Optional.empty(), Map.of()),
                column("c_tinyint", "tinyint"),
                column("c_smallint", "smallint"),
                column("c_decimal", "decimal(10,2)"),
                column("c_date", "date"),
                column("c_timestamp", "timestamp"),
                column("c_double", "double"),
                column("c_binary", "binary"),
                column("c_array", "array<bigint>"),
                column("c_map", "map<string,bigint>"),
                column("c_struct", "struct<name:string,count:int>"));
    }

    @Override
    protected Schema avroSchema()
    {
        return SchemaBuilder.record(TABLE_NAME).fields()
                .requiredString(RECORD_KEY_FIELD)
                .requiredLong(ORDERING_FIELD)
                // The metastore narrows these two; the table schema has no type narrower than int
                .optionalInt("c_tinyint")
                .optionalInt("c_smallint")
                .name("c_decimal").type(nullable(DECIMAL_SCHEMA)).withDefault(null)
                .name("c_date").type(nullable(LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT)))).withDefault(null)
                .name("c_timestamp").type(nullable(LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG)))).withDefault(null)
                .optionalDouble("c_double")
                .optionalBytes("c_binary")
                .name("c_array").type(nullable(Schema.createArray(Schema.create(Schema.Type.LONG)))).withDefault(null)
                .name("c_map").type(nullable(Schema.createMap(Schema.create(Schema.Type.LONG)))).withDefault(null)
                .name("c_struct").type(nullable(STRUCT_SCHEMA)).withDefault(null)
                .endRecord();
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
        // First commit: base parquet file with three fully populated rows
        String firstCommit = client.startCommit();
        client.commit(firstCommit, client.bulkInsert(ImmutableList.of(
                populated(schema, "k1", 100L, 1),
                populated(schema, "k2", 100L, 2),
                populated(schema, "k3", 100L, 3)), firstCommit));

        // Second commit (log file): k1 gets new values of every kind, k2 gets a null of every kind
        String secondCommit = client.startCommit();
        client.commit(secondCommit, client.upsert(ImmutableList.of(
                populated(schema, "k1", 200L, 7),
                empty(schema, "k2", 200L)), secondCommit));
    }

    private static HoodieRecord<HoodieAvroPayload> populated(Schema schema, String key, long ts, int seed)
    {
        GenericRecord record = new GenericData.Record(schema);
        record.put(RECORD_KEY_FIELD, key);
        record.put(ORDERING_FIELD, ts);
        record.put("c_tinyint", seed);
        record.put("c_smallint", 1000 + seed);
        record.put("c_decimal", new Conversions.DecimalConversion().toFixed(new BigDecimal(seed + ".25"), DECIMAL_SCHEMA, DECIMAL_SCHEMA.getLogicalType()));
        record.put("c_date", 19_000 + seed);
        record.put("c_timestamp", 1_700_000_000_000_000L + seed);
        record.put("c_double", seed + 0.5);
        record.put("c_binary", ByteBuffer.wrap(("binary-" + seed).getBytes(UTF_8)));
        record.put("c_array", ImmutableList.of((long) seed, seed + 1L));
        record.put("c_map", Map.of("a", (long) seed));
        GenericRecord struct = new GenericData.Record(STRUCT_SCHEMA);
        struct.put("name", "struct-" + seed);
        struct.put("count", seed);
        record.put("c_struct", struct);
        return avroRecord(record, key);
    }

    private static HoodieRecord<HoodieAvroPayload> empty(Schema schema, String key, long ts)
    {
        GenericRecord record = new GenericData.Record(schema);
        record.put(RECORD_KEY_FIELD, key);
        record.put(ORDERING_FIELD, ts);
        return avroRecord(record, key);
    }

    private static Column column(String name, String hiveType)
    {
        return new Column(name, HiveType.valueOf(hiveType), Optional.empty(), Map.of());
    }

    private static Schema nullable(Schema schema)
    {
        return Schema.createUnion(Schema.create(Schema.Type.NULL), schema);
    }
}
