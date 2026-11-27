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
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.config.HoodieWriteConfig;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.metastore.HiveType.HIVE_LONG;
import static io.trino.metastore.HiveType.HIVE_STRING;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Creates a non-partitioned Merge-On-Read table with one column per kind of value the merge buffer holds: tinyint,
 * smallint, decimal, date, timestamp, double, binary, array, map and struct. A base commit of three rows is followed
 * by a log commit that updates one of them and nulls every typed column of another, so the read-time merge carries
 * each kind, and a null of each kind, through the merge buffer. See {@code TestHudiMorMergeModeSemantics}.
 * <p>
 * At the current table version the log commit writes a native log file, whose rows the connector reads as Trino
 * objects. {@link #avroLogBlocks} writes the same commits at table version 9, where the log file holds Avro data
 * blocks and its rows are Avro values, the other representation the merge buffer spills.
 */
public class ColumnTypesHudiTablesInitializer
        extends AbstractMergerHudiTablesInitializer
{
    public static final String TABLE_NAME = "column_types_mor";
    public static final String RT_TABLE_NAME = TABLE_NAME + "_rt";
    public static final String SPILLING_TABLE_NAME = TABLE_NAME + SPILLING_SUFFIX;
    public static final String SPILLING_RT_TABLE_NAME = SPILLING_TABLE_NAME + "_rt";
    public static final String AVRO_LOG_TABLE_NAME = "column_types_v9_mor";
    public static final String AVRO_LOG_RT_TABLE_NAME = AVRO_LOG_TABLE_NAME + "_rt";
    public static final String AVRO_LOG_SPILLING_TABLE_NAME = AVRO_LOG_TABLE_NAME + SPILLING_SUFFIX;
    public static final String AVRO_LOG_SPILLING_RT_TABLE_NAME = AVRO_LOG_SPILLING_TABLE_NAME + "_rt";

    // The last version that writes log files as Avro data blocks
    private static final HoodieTableVersion AVRO_LOG_TABLE_VERSION = HoodieTableVersion.NINE;
    private static final Schema DECIMAL_SCHEMA = LogicalTypes.decimal(10, 2).addToSchema(Schema.createFixed("c_decimal_fixed", null, null, 5));
    private static final Schema STRUCT_SCHEMA = SchemaBuilder.record("c_struct_record").fields()
            .optionalString("name")
            .optionalInt("count")
            .endRecord();

    private final Optional<HoodieTableVersion> tableVersion;

    public ColumnTypesHudiTablesInitializer()
    {
        this(TABLE_NAME, Optional.empty(), Map.of());
    }

    private ColumnTypesHudiTablesInitializer(String tableName, Optional<HoodieTableVersion> tableVersion, Map<String, String> extraTableProperties)
    {
        super(tableName, extraTableProperties);
        this.tableVersion = tableVersion;
    }

    /** The same commits under {@link #SPILLING_TABLE_NAME}, read with {@link #spillEveryLogRecord}. */
    public static ColumnTypesHudiTablesInitializer spillingEveryLogRecord()
    {
        return new ColumnTypesHudiTablesInitializer(SPILLING_TABLE_NAME, Optional.empty(), spillEveryLogRecord(SPILLING_TABLE_NAME));
    }

    /** The same commits under {@link #AVRO_LOG_TABLE_NAME}, written at table version 9 so the log file holds Avro data blocks. */
    public static ColumnTypesHudiTablesInitializer avroLogBlocks()
    {
        return new ColumnTypesHudiTablesInitializer(AVRO_LOG_TABLE_NAME, Optional.of(AVRO_LOG_TABLE_VERSION), Map.of());
    }

    /** {@link #avroLogBlocks} under {@link #AVRO_LOG_SPILLING_TABLE_NAME}, read with {@link #spillEveryLogRecord}. */
    public static ColumnTypesHudiTablesInitializer avroLogBlocksSpillingEveryLogRecord()
    {
        return new ColumnTypesHudiTablesInitializer(AVRO_LOG_SPILLING_TABLE_NAME, Optional.of(AVRO_LOG_TABLE_VERSION), spillEveryLogRecord(AVRO_LOG_SPILLING_TABLE_NAME));
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
                .name("c_decimal").type().optional().type(DECIMAL_SCHEMA)
                .name("c_date").type().optional().type(LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT)))
                .name("c_timestamp").type().optional().type(LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG)))
                .optionalDouble("c_double")
                .optionalBytes("c_binary")
                .name("c_array").type().optional().type(Schema.createArray(Schema.create(Schema.Type.LONG)))
                .name("c_map").type().optional().type(Schema.createMap(Schema.create(Schema.Type.LONG)))
                .name("c_struct").type().optional().type(STRUCT_SCHEMA)
                .endRecord();
    }

    @Override
    protected void configureTableConfig(HoodieTableMetaClient.TableBuilder tableBuilder)
    {
        tableBuilder.setRecordMergeMode(RecordMergeMode.COMMIT_TIME_ORDERING);
        tableVersion.ifPresent(tableBuilder::setTableVersion);
    }

    @Override
    protected void configureWriteConfig(HoodieWriteConfig.Builder writeConfigBuilder)
    {
        writeConfigBuilder.withRecordMergeMode(RecordMergeMode.COMMIT_TIME_ORDERING);
        tableVersion.ifPresent(version -> writeConfigBuilder
                .withWriteTableVersion(version.versionCode())
                // Without this the write client upgrades the table to the current version on the first commit
                .withAutoUpgradeVersion(false));
    }

    @Override
    protected void writeInitialCommits(HoodieJavaWriteClient<HoodieAvroPayload> client)
            throws IOException
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

        // The log format follows the table version, so check the writer kept the version it was given
        if (tableVersion.isPresent()) {
            Properties properties = new Properties();
            try (InputStream input = Files.newInputStream(stagingTableDirectory().resolve(".hoodie").resolve("hoodie.properties"))) {
                properties.load(input);
            }
            String version = properties.getProperty(HoodieTableConfig.VERSION.key());
            checkState(String.valueOf(tableVersion.get().versionCode()).equals(version), "Expected table version %s but found %s", tableVersion.get(), version);
        }
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
}
