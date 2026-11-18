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
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.config.HoodieWriteConfig;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.hudi.common.schema.HoodieSchema.Vector.VectorElementType.DOUBLE;
import static org.apache.hudi.common.schema.HoodieSchema.Vector.VectorElementType.FLOAT;
import static org.apache.hudi.common.schema.HoodieSchema.Vector.VectorElementType.INT8;
import static org.apache.hudi.common.schema.HoodieSchema.VectorLogicalType.VECTOR_BYTE_ORDER;

/**
 * Creates a non-partitioned Merge-On-Read table with one column per {@link HoodieSchemaType} that
 * {@code TrinoTypeMapper} maps, built from the {@link HoodieSchema} factory methods so the logical types and custom
 * properties are exactly what Hudi writes. The metastore declares each column the way Hive sync does
 * ({@code HiveSchemaUtil.convertField}, with {@code support_timestamp} off), so several metastore types differ from the
 * mapper's: uuid, fixed and vector are binary, time is int or bigint, timestamps are bigint.
 * <p>
 * Rows are filled from a seed: the base commit writes k1, k2 and k3 with seeds 1, 2 and 3 at ts 100; the log commit
 * rewrites k1 with seed 8 and k2 with a null in every typed column, at ts 200, so the read-time merge carries each type,
 * and a null of each type, in both directions. See {@code TestHudiMorMergeModeSemantics}.
 */
public class SchemaColumnTypesHudiTablesInitializer
        extends AbstractMergerHudiTablesInitializer
{
    public static final String TABLE_NAME = "schema_column_types_mor";
    public static final String RT_TABLE_NAME = TABLE_NAME + "_rt";

    private static final List<String> ENUM_SYMBOLS = ImmutableList.of("RED", "GREEN", "BLUE");

    // A converted type nested in a record: the read path converts the uuid inside the row
    private static final HoodieSchema RECORD_SCHEMA = HoodieSchema.createRecord("c_record", null, null, ImmutableList.of(
            nullableField("name", HoodieSchema.create(HoodieSchemaType.STRING)),
            nullableField("id", HoodieSchema.createUUID())));

    /** The data columns, in metastore order; every typed column is nullable. */
    public static final HoodieSchema TABLE_SCHEMA = HoodieSchema.createRecord(TABLE_NAME, null, null, ImmutableList.of(
            HoodieSchemaField.of(RECORD_KEY_FIELD, HoodieSchema.create(HoodieSchemaType.STRING)),
            HoodieSchemaField.of(ORDERING_FIELD, HoodieSchema.create(HoodieSchemaType.LONG)),
            nullableField("c_boolean", HoodieSchema.create(HoodieSchemaType.BOOLEAN)),
            nullableField("c_int", HoodieSchema.create(HoodieSchemaType.INT)),
            nullableField("c_long", HoodieSchema.create(HoodieSchemaType.LONG)),
            nullableField("c_float", HoodieSchema.create(HoodieSchemaType.FLOAT)),
            nullableField("c_double", HoodieSchema.create(HoodieSchemaType.DOUBLE)),
            nullableField("c_string", HoodieSchema.create(HoodieSchemaType.STRING)),
            nullableField("c_bytes", HoodieSchema.create(HoodieSchemaType.BYTES)),
            nullableField("c_enum", HoodieSchema.createEnum("c_enum", null, null, ENUM_SYMBOLS)),
            nullableField("c_fixed", HoodieSchema.createFixed("c_fixed", null, null, 4)),
            nullableField("c_uuid", HoodieSchema.createUUID()),
            nullableField("c_decimal_fixed", HoodieSchema.createDecimal("c_decimal_fixed", null, null, 10, 2, 5)),
            // Precision above 18: a long decimal in Trino
            nullableField("c_decimal_bytes", HoodieSchema.createDecimal(20, 4)),
            nullableField("c_date", HoodieSchema.createDate()),
            nullableField("c_time_millis", HoodieSchema.createTimeMillis()),
            nullableField("c_time_micros", HoodieSchema.createTimeMicros()),
            nullableField("c_timestamp_millis", HoodieSchema.createTimestampMillis()),
            nullableField("c_timestamp_micros", HoodieSchema.createTimestampMicros()),
            nullableField("c_local_timestamp_millis", HoodieSchema.createLocalTimestampMillis()),
            nullableField("c_local_timestamp_micros", HoodieSchema.createLocalTimestampMicros()),
            // Required elements: parquet-avro's default two-level list (write-old-list-structure) rejects a null element
            nullableField("c_array", HoodieSchema.createArray(HoodieSchema.create(HoodieSchemaType.LONG))),
            nullableField("c_map", HoodieSchema.createMap(HoodieSchema.create(HoodieSchemaType.LONG))),
            nullableField("c_record", RECORD_SCHEMA),
            nullableField("c_vector_float", HoodieSchema.createVector(3, FLOAT)),
            nullableField("c_vector_double", HoodieSchema.createVector(2, DOUBLE)),
            nullableField("c_vector_int8", HoodieSchema.createVector(4, INT8)),
            nullableField("c_blob", HoodieSchema.createBlob()),
            nullableField("c_variant", HoodieSchema.createVariant())));

    private static final Schema AVRO_SCHEMA = TABLE_SCHEMA.toAvroSchema();
    private static final Conversions.DecimalConversion DECIMAL_CONVERSION = new Conversions.DecimalConversion();

    public SchemaColumnTypesHudiTablesInitializer()
    {
        super(TABLE_NAME);
    }

    @Override
    protected List<Column> dataColumns()
    {
        return ImmutableList.of(
                column(RECORD_KEY_FIELD, "string"),
                column(ORDERING_FIELD, "bigint"),
                column("c_boolean", "boolean"),
                column("c_int", "int"),
                column("c_long", "bigint"),
                column("c_float", "float"),
                column("c_double", "double"),
                column("c_string", "string"),
                column("c_bytes", "binary"),
                column("c_enum", "string"),
                column("c_fixed", "binary"),
                column("c_uuid", "binary"),
                column("c_decimal_fixed", "decimal(10,2)"),
                column("c_decimal_bytes", "decimal(20,4)"),
                column("c_date", "date"),
                column("c_time_millis", "int"),
                column("c_time_micros", "bigint"),
                column("c_timestamp_millis", "bigint"),
                column("c_timestamp_micros", "bigint"),
                column("c_local_timestamp_millis", "bigint"),
                column("c_local_timestamp_micros", "bigint"),
                column("c_array", "array<bigint>"),
                column("c_map", "map<string,bigint>"),
                column("c_record", "struct<name:string,id:binary>"),
                column("c_vector_float", "binary"),
                column("c_vector_double", "binary"),
                column("c_vector_int8", "binary"),
                column("c_blob", "struct<type:string,data:binary,reference:struct<external_path:string,offset:bigint,length:bigint,managed:boolean>>"),
                column("c_variant", "struct<metadata:binary,value:binary>"));
    }

    @Override
    protected Schema avroSchema()
    {
        return AVRO_SCHEMA;
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
        // First commit: base parquet file with three fully populated rows
        String firstCommit = client.startCommit();
        client.commit(firstCommit, client.bulkInsert(ImmutableList.of(
                populated("k1", 100L, 1),
                populated("k2", 100L, 2),
                populated("k3", 100L, 3)), firstCommit));

        // Second commit (log file): k1 gets new values of every type, k2 gets a null of every type
        String secondCommit = client.startCommit();
        client.commit(secondCommit, client.upsert(ImmutableList.of(
                populated("k1", 200L, 8),
                empty("k2", 200L)), secondCommit));
    }

    private static HoodieRecord<HoodieAvroPayload> populated(String key, long ts, int seed)
    {
        GenericRecord record = new GenericData.Record(AVRO_SCHEMA);
        record.put(RECORD_KEY_FIELD, key);
        record.put(ORDERING_FIELD, ts);
        record.put("c_boolean", seed % 2 == 0);
        record.put("c_int", 100 * seed);
        record.put("c_long", 10_000_000_000L * seed);
        record.put("c_float", seed + 0.5f);
        record.put("c_double", seed + 0.25);
        record.put("c_string", "string-" + seed);
        record.put("c_bytes", ByteBuffer.wrap(new byte[] {(byte) 0xCA, (byte) 0xFE, (byte) seed}));
        record.put("c_enum", new GenericData.EnumSymbol(valueSchema("c_enum"), ENUM_SYMBOLS.get(seed % ENUM_SYMBOLS.size())));
        record.put("c_fixed", new GenericData.Fixed(valueSchema("c_fixed"), new byte[] {(byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) seed}));
        record.put("c_uuid", uuid(seed, 0));
        // Negative, so the fixed-backed value is sign-extended
        Schema decimalFixed = valueSchema("c_decimal_fixed");
        record.put("c_decimal_fixed", DECIMAL_CONVERSION.toFixed(BigDecimal.valueOf(-(seed * 100L + 25), 2), decimalFixed, decimalFixed.getLogicalType()));
        Schema decimalBytes = valueSchema("c_decimal_bytes");
        record.put("c_decimal_bytes", DECIMAL_CONVERSION.toBytes(new BigDecimal("1234567890123456.789" + seed), decimalBytes, decimalBytes.getLogicalType()));
        // 19000 is 2022-01-08
        record.put("c_date", 19_000 + seed);
        // <seed>:02:03.456 and <seed>:02:03.456789
        record.put("c_time_millis", seed * 3_600_000 + 123_456);
        record.put("c_time_micros", seed * 3_600_000_000L + 123_456_789L);
        // 1_700_000_000_000 ms is 2023-11-14 22:13:20 UTC; each seed adds a day
        record.put("c_timestamp_millis", 1_700_000_000_123L + seed * 86_400_000L);
        record.put("c_timestamp_micros", 1_700_000_000_123_456L + seed * 86_400_000_000L);
        record.put("c_local_timestamp_millis", 1_700_000_000_123L + seed * 86_400_000L);
        record.put("c_local_timestamp_micros", 1_700_000_000_123_456L + seed * 86_400_000_000L);
        record.put("c_array", ImmutableList.of((long) seed, seed + 1L));
        record.put("c_map", Map.of("a", (long) seed, "b", 10L * seed));
        GenericRecord nested = new GenericData.Record(valueSchema("c_record"));
        nested.put("name", "record-" + seed);
        nested.put("id", uuid(seed, 1));
        record.put("c_record", nested);
        record.put("c_vector_float", vector(valueSchema("c_vector_float"), ByteBuffer.allocate(12).order(VECTOR_BYTE_ORDER)
                .putFloat(seed)
                .putFloat(seed + 0.5f)
                .putFloat(-seed)));
        record.put("c_vector_double", vector(valueSchema("c_vector_double"), ByteBuffer.allocate(16).order(VECTOR_BYTE_ORDER)
                .putDouble(seed + 0.25)
                .putDouble(-(seed + 0.25))));
        record.put("c_vector_int8", vector(valueSchema("c_vector_int8"), ByteBuffer.allocate(4)
                .put((byte) seed)
                .put((byte) -seed)
                .put(Byte.MAX_VALUE)
                .put(Byte.MIN_VALUE)));
        record.put("c_blob", blob(seed));
        // Unshredded variant: an empty metadata dictionary and an int8 primitive holding the seed
        GenericRecord variant = new GenericData.Record(valueSchema("c_variant"));
        variant.put(HoodieSchema.Variant.VARIANT_METADATA_FIELD, ByteBuffer.wrap(new byte[] {0x01, 0x00, 0x00}));
        variant.put(HoodieSchema.Variant.VARIANT_VALUE_FIELD, ByteBuffer.wrap(new byte[] {0x0C, (byte) seed}));
        record.put("c_variant", variant);
        return avroRecord(record, key);
    }

    /**
     * Odd seeds hold the bytes inline; even seeds point at an external file, with a null length (the rest of the file).
     */
    private static GenericRecord blob(int seed)
    {
        Schema blobSchema = valueSchema("c_blob");
        GenericRecord blob = new GenericData.Record(blobSchema);
        Schema typeSchema = blobSchema.getField(HoodieSchema.Blob.TYPE).schema();
        if (seed % 2 == 1) {
            blob.put(HoodieSchema.Blob.TYPE, new GenericData.EnumSymbol(typeSchema, HoodieSchema.Blob.INLINE));
            blob.put(HoodieSchema.Blob.INLINE_DATA_FIELD, ByteBuffer.wrap(new byte[] {(byte) 0xB1, 0x0B, (byte) seed}));
            return blob;
        }
        blob.put(HoodieSchema.Blob.TYPE, new GenericData.EnumSymbol(typeSchema, HoodieSchema.Blob.OUT_OF_LINE));
        GenericRecord reference = new GenericData.Record(nonNull(blobSchema.getField(HoodieSchema.Blob.EXTERNAL_REFERENCE).schema()));
        reference.put(HoodieSchema.Blob.EXTERNAL_REFERENCE_PATH, "file:///blobs/blob-" + seed + ".bin");
        reference.put(HoodieSchema.Blob.EXTERNAL_REFERENCE_OFFSET, 16L * seed);
        reference.put(HoodieSchema.Blob.EXTERNAL_REFERENCE_IS_MANAGED, true);
        blob.put(HoodieSchema.Blob.EXTERNAL_REFERENCE, reference);
        return blob;
    }

    private static HoodieRecord<HoodieAvroPayload> empty(String key, long ts)
    {
        GenericRecord record = new GenericData.Record(AVRO_SCHEMA);
        record.put(RECORD_KEY_FIELD, key);
        record.put(ORDERING_FIELD, ts);
        return avroRecord(record, key);
    }

    private static String uuid(int seed, int discriminator)
    {
        return "%08x-%04x-4000-8000-%012x".formatted(seed, discriminator, seed);
    }

    private static GenericData.Fixed vector(Schema vectorSchema, ByteBuffer littleEndianElements)
    {
        return new GenericData.Fixed(vectorSchema, littleEndianElements.array());
    }

    /** The non-null branch of a nullable column, which typed values must carry for the union to resolve. */
    private static Schema valueSchema(String fieldName)
    {
        return nonNull(AVRO_SCHEMA.getField(fieldName).schema());
    }

    private static Schema nonNull(Schema nullableSchema)
    {
        return nullableSchema.getTypes().stream()
                .filter(branch -> branch.getType() != Schema.Type.NULL)
                .findFirst()
                .orElseThrow();
    }

    private static HoodieSchemaField nullableField(String name, HoodieSchema schema)
    {
        return HoodieSchemaField.of(name, HoodieSchema.createNullable(schema), null, HoodieSchema.NULL_VALUE);
    }

    private static Column column(String name, String hiveType)
    {
        return new Column(name, HiveType.valueOf(hiveType), Optional.empty(), Map.of());
    }
}
