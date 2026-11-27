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
package io.trino.plugin.hudi.reader;

import io.airlift.slice.Slices;
import io.trino.plugin.hudi.storage.TrinoStorageConfiguration;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.connector.EmptyPageSource;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.SqlDate;
import io.trino.spi.type.SqlDecimal;
import io.trino.spi.type.SqlTimestamp;
import io.trino.spi.type.SqlVarbinary;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.util.Utf8;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.log.block.HoodieDeleteBlock;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.table.read.BufferedRecordMerger;
import org.apache.hudi.common.table.read.BufferedRecordMergerFactory;
import org.apache.hudi.common.table.read.BufferedRecords;
import org.apache.hudi.common.table.read.FileGroupReaderSchemaHandler;
import org.apache.hudi.common.table.read.TableState;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.DefaultSizeEstimator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.SizeEstimator;
import org.apache.hudi.common.util.collection.ArrayComparable;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.storage.StoragePath;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.Decimals.writeBigDecimal;
import static io.trino.spi.type.Decimals.writeShortDecimal;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.RowType.field;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.TIME_MILLIS;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_NANOS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hudi.common.config.HoodieCommonConfig.DISK_MAP_BITCASK_COMPRESSION_ENABLED;
import static org.apache.hudi.common.config.RecordMergeMode.EVENT_TIME_ORDERING;
import static org.apache.hudi.common.table.HoodieTableConfig.RECORD_MERGE_PROPERTY_PREFIX;
import static org.apache.hudi.keygen.constant.KeyGeneratorOptions.KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Pins the merge buffer size estimator and spill serializer of {@link HudiTrinoReaderContext} through the same
 * {@link ExternalSpillableMap} that {@code FileGroupRecordBuffer} builds.
 */
final class TestHudiTrinoReaderContext
{
    private static final DecimalType SHORT_DECIMAL = createDecimalType(7, 2);
    private static final DecimalType LONG_DECIMAL = createDecimalType(20, 2);

    @TempDir
    Path spillDirectory;

    @Test
    void testSizeEstimatorExcludesTheSchema()
    {
        // A wide schema with three populated fields: a graph walk of the record would count the schema's 300 fields
        SchemaBuilder.FieldAssembler<Schema> fields = SchemaBuilder.record("wide").fields();
        for (int i = 0; i < 300; i++) {
            fields = fields.optionalString("column_with_a_long_name_" + i);
        }
        HoodieSchema schema = HoodieSchema.fromAvroSchema(fields.endRecord());
        HudiTrinoReaderContext readerContext = readerContext();
        GenericData.Record record = new GenericData.Record(schema.toAvroSchema());
        record.put(0, "value");
        record.put(1, 1L);
        record.put(2, new Utf8("utf8"));
        BufferedRecord<IndexedRecord> buffered = BufferedRecords.fromEngineRecord(record, schema, readerContext.getRecordContext(), 1L, "key", false);

        // Key, ordering value, three values and the record's 300-slot value array, but not the 300-field schema
        assertThat(readerContext.getRecordSizeEstimator().sizeEstimate(buffered)).isBetween(1_000L, 2_000L);
    }

    @Test
    void testSizeEstimatorCountsTheBytesOfEveryValueKind()
    {
        // An undercount keeps records in memory past the merge budget, so each kind must count what it holds
        int bytes = 100_000;
        byte[] payload = new byte[bytes];
        Arrays.fill(payload, (byte) 1);
        Schema fixedSchema = Schema.createFixed("payload", null, "test", bytes);
        Schema nestedSchema = SchemaBuilder.record("nested_payload").fields().optionalString("payload").endRecord();
        GenericData.Record nested = new GenericData.Record(nestedSchema);
        nested.put(0, "x".repeat(bytes));
        Map<String, Object> valuesByKind = Map.ofEntries(
                Map.entry("String", "x".repeat(bytes)),
                Map.entry("Utf8", new Utf8(payload)),
                Map.entry("ByteBuffer", ByteBuffer.wrap(payload)),
                Map.entry("GenericFixed", new GenericData.Fixed(fixedSchema, payload)),
                Map.entry("IndexedRecord", nested),
                Map.entry("SqlDecimal", new SqlDecimal(new BigInteger(1, payload), 38, 0)),
                Map.entry("SqlVarbinary", new SqlVarbinary(payload)),
                // Boxed values, sized as fixed-width values
                Map.entry("List", Collections.nCopies(bytes / Long.BYTES, 1L)),
                Map.entry("Map", IntStream.range(0, bytes / Long.BYTES).boxed().collect(toImmutableMap(i -> "key-" + i, i -> (long) i))));

        HoodieSchema schema = HoodieSchema.fromAvroSchema(SchemaBuilder.record("one_value").fields().optionalString("value").endRecord());
        HudiTrinoReaderContext readerContext = readerContext();
        SizeEstimator<BufferedRecord<IndexedRecord>> estimator = readerContext.getRecordSizeEstimator();
        GenericData.Record empty = new GenericData.Record(schema.toAvroSchema());
        long emptySize = estimator.sizeEstimate(BufferedRecords.fromEngineRecord(empty, schema, readerContext.getRecordContext(), 1L, "key", false));
        assertThat(emptySize).isLessThan(1_000L);
        valuesByKind.forEach((kind, value) -> {
            GenericData.Record record = new GenericData.Record(schema.toAvroSchema());
            record.put(0, value);
            long size = estimator.sizeEstimate(BufferedRecords.fromEngineRecord(record, schema, readerContext.getRecordContext(), 1L, "key", false));
            assertThat(size - emptySize).as(kind).isGreaterThanOrEqualTo(bytes);
        });
    }

    // BITCASK is the only disk map the connector can spill to: hudi-trino excludes RocksDB from its dependencies
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testSpillRoundTripsEveryValueKind(boolean compressionEnabled)
    {
        Schema shortDecimalSchema = LogicalTypes.decimal(7, 2).addToSchema(Schema.createFixed("decimal_7_2", null, "test", 4));
        Schema longDecimalSchema = LogicalTypes.decimal(20, 2).addToSchema(Schema.createFixed("decimal_20_2", null, "test", 9));
        Schema timestampSchema = LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG));
        Schema rowSchema = SchemaBuilder.record("price_row").fields()
                .optionalString("name")
                .name("price").type(nullable(shortDecimalSchema)).withDefault(null)
                .endRecord();
        // Trino objects, as Type.getObjectValue returns them for rows read from native log files
        List<Map.Entry<Schema, Object>> trinoKinds = List.of(
                kind(Schema.create(Schema.Type.STRING), objectValue(VARCHAR, builder -> VARCHAR.writeString(builder, "trino"))),
                // A CHAR's object value is space-padded to its length; the leading space and tab must survive the spill
                kind(Schema.create(Schema.Type.STRING), objectValue(createCharType(5), builder -> createCharType(5).writeSlice(builder, Slices.utf8Slice(" a\t")))),
                kind(Schema.create(Schema.Type.BOOLEAN), objectValue(BOOLEAN, builder -> BOOLEAN.writeBoolean(builder, true))),
                kind(Schema.create(Schema.Type.INT), objectValue(TINYINT, builder -> TINYINT.writeLong(builder, 7))),
                kind(Schema.create(Schema.Type.INT), objectValue(SMALLINT, builder -> SMALLINT.writeLong(builder, 300))),
                kind(Schema.create(Schema.Type.INT), objectValue(INTEGER, builder -> INTEGER.writeLong(builder, 70_000))),
                kind(Schema.create(Schema.Type.LONG), objectValue(BIGINT, builder -> BIGINT.writeLong(builder, 5_000_000_000L))),
                kind(Schema.create(Schema.Type.FLOAT), objectValue(REAL, builder -> REAL.writeLong(builder, Float.floatToIntBits(1.5f)))),
                kind(Schema.create(Schema.Type.DOUBLE), objectValue(DOUBLE, builder -> DOUBLE.writeDouble(builder, 2.25))),
                kind(shortDecimalSchema, objectValue(SHORT_DECIMAL, builder -> writeShortDecimal(builder, -1_234_567))),
                kind(longDecimalSchema, objectValue(LONG_DECIMAL, builder -> writeBigDecimal(LONG_DECIMAL, builder, new BigDecimal("123456789012345678.90")))),
                kind(LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT)), objectValue(DATE, builder -> DATE.writeLong(builder, 19_000))),
                kind(timestampSchema, objectValue(TIMESTAMP_MICROS, builder -> TIMESTAMP_MICROS.writeLong(builder, 1_700_000_000_123_456L))),
                kind(timestampSchema, objectValue(TIMESTAMP_NANOS, builder -> TIMESTAMP_NANOS.writeObject(builder, new LongTimestamp(1_700_000_000_123_456L, 789_000)))),
                kind(Schema.create(Schema.Type.BYTES), objectValue(VARBINARY, builder -> VARBINARY.writeSlice(builder, Slices.wrappedBuffer(new byte[] {1, 2, 3})))),
                kind(Schema.createArray(nullable(Schema.create(Schema.Type.LONG))), objectValue(new ArrayType(BIGINT), builder -> ((ArrayBlockBuilder) builder).buildEntry(elements -> {
                    BIGINT.writeLong(elements, 1);
                    elements.appendNull();
                }))),
                kind(Schema.createMap(Schema.create(Schema.Type.LONG)), objectValue(new MapType(VARCHAR, BIGINT, new TypeOperators()), builder -> ((MapBlockBuilder) builder).buildEntry((keys, values) -> {
                    VARCHAR.writeString(keys, "a");
                    BIGINT.writeLong(values, 1);
                }))),
                // Multi-byte UTF-8, including a supplementary character, in a value and in a map key
                kind(Schema.create(Schema.Type.STRING), objectValue(VARCHAR, builder -> VARCHAR.writeString(builder, "café 😀"))),
                // Values that carry their own precision, nested in an array and a map, next to a null
                kind(Schema.createArray(nullable(longDecimalSchema)), objectValue(new ArrayType(LONG_DECIMAL), builder -> ((ArrayBlockBuilder) builder).buildEntry(elements -> {
                    writeBigDecimal(LONG_DECIMAL, elements, new BigDecimal("-98765432109876543.21"));
                    elements.appendNull();
                }))),
                kind(Schema.createMap(nullable(timestampSchema)), objectValue(new MapType(VARCHAR, TIMESTAMP_MICROS, new TypeOperators()), builder -> ((MapBlockBuilder) builder).buildEntry((keys, values) -> {
                    VARCHAR.writeString(keys, "été 😀");
                    TIMESTAMP_MICROS.writeLong(values, 1_700_000_000_123_456L);
                    VARCHAR.writeString(keys, "none");
                    values.appendNull();
                }))),
                // An empty map has no key to tell the representations apart, and takes the Avro one
                kind(Schema.createMap(Schema.create(Schema.Type.LONG)), objectValue(new MapType(VARCHAR, BIGINT, new TypeOperators()), builder -> ((MapBlockBuilder) builder).buildEntry((keys, values) -> {}))),
                kind(rowSchema, objectValue(RowType.from(List.of(field("name", VARCHAR), field("price", SHORT_DECIMAL))), builder -> ((RowBlockBuilder) builder).buildEntry(rowFields -> {
                    VARCHAR.writeString(rowFields.get(0), "nested");
                    writeShortDecimal(rowFields.get(1), 999);
                }))),
                // Hive sync declares UUID and VECTOR as binary, and BLOB and VARIANT as structs
                kind(HoodieSchema.createUUID().toAvroSchema(), new SqlVarbinary(new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})),
                kind(HoodieSchema.createVector(2).toAvroSchema(), new SqlVarbinary(new byte[] {0, 0, -128, 63, 0, 0, 0, 64})),
                kind(HoodieSchema.createBlob().toAvroSchema(), structOfNulls(HoodieSchema.createBlob(), "INLINE")),
                kind(HoodieSchema.createVariant().toAvroSchema(), structOfNulls(HoodieSchema.createVariant(), new SqlVarbinary(new byte[] {1}))));
        assertThat(trinoKinds).extracting(kind -> (Object) kind.getValue().getClass())
                .contains(String.class, SqlDecimal.class, SqlDate.class, SqlTimestamp.class, SqlVarbinary.class);

        // Avro values, as GenericDatumReader returns them for rows decoded from Avro log blocks
        Schema nestedSchema = SchemaBuilder.record("nested").fields().requiredString("name").optionalInt("count").endRecord();
        Schema enumSchema = Schema.createEnum("color", null, "test", List.of("RED", "GREEN"));
        Schema stringArraySchema = Schema.createArray(Schema.create(Schema.Type.STRING));
        GenericRecord nested = new GenericData.Record(nestedSchema);
        nested.put("name", new Utf8("nested"));
        nested.put("count", 3);
        Map<Utf8, Object> avroMap = new HashMap<>();
        avroMap.put(new Utf8("first"), nested);
        List<Map.Entry<Schema, Object>> avroKinds = List.of(
                kind(Schema.create(Schema.Type.STRING), new Utf8("avro")),
                kind(Schema.create(Schema.Type.STRING), new Utf8("über 😀")),
                // A CHAR column from an Avro log block, as written: unpadded, leading whitespace kept
                kind(Schema.create(Schema.Type.STRING), new Utf8(" ab\t")),
                kind(Schema.create(Schema.Type.BYTES), ByteBuffer.wrap("bytes".getBytes(UTF_8))),
                kind(shortDecimalSchema, new Conversions.DecimalConversion().toFixed(new BigDecimal("12345.67"), shortDecimalSchema, shortDecimalSchema.getLogicalType())),
                kind(nestedSchema, nested),
                kind(stringArraySchema, new GenericData.Array<>(stringArraySchema, List.of(new Utf8("a"), new Utf8("b")))),
                kind(Schema.createMap(nestedSchema), avroMap),
                kind(enumSchema, new GenericData.EnumSymbol(enumSchema, "GREEN")));

        List<Map.Entry<Schema, Object>> allKindsAndValues = new ArrayList<>(trinoKinds);
        allKindsAndValues.addAll(avroKinds);
        SchemaBuilder.FieldAssembler<Schema> fields = SchemaBuilder.record("all_kinds").fields();
        for (int i = 0; i < allKindsAndValues.size(); i++) {
            fields = fields.name("field_" + i).type(nullable(allKindsAndValues.get(i).getKey())).withDefault(null);
        }
        HoodieSchema schema = HoodieSchema.fromAvroSchema(fields.endRecord());
        List<Object> allValues = allKindsAndValues.stream().map(Map.Entry::getValue).toList();
        HudiTrinoReaderContext readerContext = readerContext();
        GenericData.Record allKinds = new GenericData.Record(schema.toAvroSchema());
        for (int i = 0; i < allValues.size(); i++) {
            allKinds.put(i, allValues.get(i));
        }
        GenericData.Record allNulls = new GenericData.Record(schema.toAvroSchema());
        List<BufferedRecord<IndexedRecord>> records = List.of(
                BufferedRecords.fromEngineRecord(allKinds, schema, readerContext.getRecordContext(), 10L, "all-kinds", false),
                BufferedRecords.fromEngineRecord(allNulls, schema, readerContext.getRecordContext(), "ordering", "all-nulls", false),
                // Ordering values of the other kinds the record context converts to, on records and on deletes
                BufferedRecords.fromEngineRecord(allNulls, schema, readerContext.getRecordContext(), LocalDate.of(2024, 2, 29), "date-ordered", false),
                BufferedRecords.fromEngineRecord(allNulls, schema, readerContext.getRecordContext(), Timestamp.valueOf("2024-02-29 01:02:03.456"), "timestamp-ordered", false),
                BufferedRecords.fromEngineRecord(allNulls, schema, readerContext.getRecordContext(), new BigDecimal("-12.340"), "decimal-ordered", false),
                BufferedRecords.fromEngineRecord(allNulls, schema, readerContext.getRecordContext(), new Utf8("utf8"), "utf8-ordered", false),
                BufferedRecords.fromEngineRecord(allNulls, schema, readerContext.getRecordContext(),
                        new ArrayComparable(new Comparable[] {5L, LocalDate.of(2024, 3, 1)}), "array-ordered", false),
                BufferedRecords.createDelete("deleted", 30L),
                BufferedRecords.createDelete("date-deleted", LocalDate.of(2024, 3, 1)));

        // A one-byte budget leaves no room in memory, so every put goes through the spill serializer
        try (ExternalSpillableMap<Serializable, BufferedRecord<IndexedRecord>> map = spillableMap(readerContext, 1, readerContext.getRecordSizeEstimator(), compressionEnabled)) {
            for (BufferedRecord<IndexedRecord> record : records) {
                map.put(record.getRecordKey(), record);
            }
            assertThat(map.getInMemoryMapNumEntries()).isZero();
            assertThat(map.getDiskBasedMapNumEntries()).isEqualTo(records.size());

            for (BufferedRecord<IndexedRecord> expected : records) {
                BufferedRecord<IndexedRecord> actual = map.get(expected.getRecordKey());
                assertSpilledRecord(actual, expected);
                // A merge that keeps a buffered record writes it back, so the record read back is spilled again: its
                // narrowed integers are Integer now, and take the Avro representation
                map.put(actual.getRecordKey(), actual);
                assertSpilledRecord(map.get(expected.getRecordKey()), expected);
            }
        }
    }

    private static void assertSpilledRecord(BufferedRecord<IndexedRecord> actual, BufferedRecord<IndexedRecord> expected)
    {
        assertThat(actual.getRecordKey()).isEqualTo(expected.getRecordKey());
        assertThat(actual.getOrderingValue()).isEqualTo(expected.getOrderingValue());
        assertThat(actual.getOrderingValue()).hasSameClassAs(expected.getOrderingValue());
        assertThat(actual.getSchemaId()).isEqualTo(expected.getSchemaId());
        assertThat(actual.getHoodieOperation()).isEqualTo(expected.getHoodieOperation());
        assertThat(actual.isDelete()).isEqualTo(expected.isDelete());
        if (expected.getRecord() == null) {
            assertThat(actual.getRecord()).isNull();
            return;
        }
        assertThat(actual.getRecord().getSchema()).isEqualTo(expected.getRecord().getSchema());
        for (int i = 0; i < expected.getRecord().getSchema().getFields().size(); i++) {
            Object expectedValue = expected.getRecord().get(i);
            Object actualValue = actual.getRecord().get(i);
            if (expectedValue instanceof Byte || expectedValue instanceof Short) {
                // HoodieSchema has no type narrower than INT, so tinyint and smallint come back as its Integer
                assertThat(actualValue).as("field %s", i).isEqualTo(((Number) expectedValue).intValue());
                continue;
            }
            assertThat(actualValue).as("field %s", i).isEqualTo(expectedValue);
            if (expectedValue instanceof List<?> || expectedValue instanceof Map<?, ?>) {
                // Collection equality ignores whether a collection is an Avro or a Trino one, the spill marker does not
                assertThat(isAvroCollection(actualValue)).as("field %s", i).isEqualTo(isAvroCollection(expectedValue));
            }
            else if (expectedValue != null) {
                assertThat(actualValue).as("field %s", i).hasSameClassAs(expectedValue);
            }
        }
    }

    private static boolean isAvroCollection(Object collection)
    {
        return collection instanceof GenericContainer
                || (collection instanceof Map<?, ?> map && !map.isEmpty() && map.keySet().iterator().next() instanceof Utf8);
    }

    @Test
    void testSpillRejectsAValueItsFieldTypeDoesNotMap()
    {
        HoodieSchema schema = HoodieSchema.fromAvroSchema(SchemaBuilder.record("mismatch").fields()
                .requiredInt("count")
                .name("time").type(LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT))).noDefault()
                .name("instant").type(LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG))).noDefault()
                .endRecord());
        HudiTrinoReaderContext readerContext = readerContext();
        // Trino values with no spilled form fail with the field type, not with a ClassCastException from the Avro writer
        Map<Integer, Object> valuesByField = Map.of(
                0, "not an int",
                1, objectValue(TIME_MILLIS, builder -> TIME_MILLIS.writeLong(builder, 1_000_000_000_000L)),
                2, objectValue(TIMESTAMP_TZ_MILLIS, builder -> TIMESTAMP_TZ_MILLIS.writeLong(builder, packDateTimeWithZone(1_700_000_000_000L, UTC_KEY))));
        Map<Integer, String> messages = Map.of(
                0, "Cannot spill a merge buffer value of class java.lang.String for a field of type INT",
                1, "Cannot spill a merge buffer value of class io.trino.spi.type.SqlTime for a field of type TIME",
                2, "Cannot spill a merge buffer value of class io.trino.spi.type.SqlTimestampWithTimeZone for a field of type TIMESTAMP");

        try (ExternalSpillableMap<Serializable, BufferedRecord<IndexedRecord>> map = spillableMap(readerContext, 1, readerContext.getRecordSizeEstimator())) {
            valuesByField.forEach((field, value) -> {
                GenericData.Record record = new GenericData.Record(schema.toAvroSchema());
                record.put(field, value);
                BufferedRecord<IndexedRecord> buffered = BufferedRecords.fromEngineRecord(record, schema, readerContext.getRecordContext(), 1L, "key-" + field, false);
                assertThatThrownBy(() -> map.put(buffered.getRecordKey(), buffered))
                        .hasStackTraceContaining(messages.get(field));
            });
        }
    }

    // A Trino row for the struct: its first field set, the others null
    private static List<Object> structOfNulls(HoodieSchema struct, Object firstField)
    {
        List<Object> row = new ArrayList<>(Collections.nCopies(struct.getFields().size(), null));
        row.set(0, firstField);
        return row;
    }

    private static Map.Entry<Schema, Object> kind(Schema avroType, Object value)
    {
        return Map.entry(avroType, value);
    }

    private static Schema nullable(Schema schema)
    {
        return Schema.createUnion(Schema.create(Schema.Type.NULL), schema);
    }

    private static Object objectValue(Type type, Consumer<BlockBuilder> writer)
    {
        BlockBuilder builder = type.createBlockBuilder(null, 1);
        writer.accept(builder);
        return type.getObjectValue(builder.build(), 0);
    }

    private static HudiTrinoReaderContext readerContext()
    {
        return new HudiTrinoReaderContext(new TrinoStorageConfiguration(), new HoodieTableConfig(), null, List.of(), null, null, new TypedProperties());
    }

    private ExternalSpillableMap<Serializable, BufferedRecord<IndexedRecord>> spillableMap(
            HudiTrinoReaderContext readerContext,
            long maxMemoryBytes,
            SizeEstimator<BufferedRecord<IndexedRecord>> estimator)
    {
        return spillableMap(readerContext, maxMemoryBytes, estimator, DISK_MAP_BITCASK_COMPRESSION_ENABLED.defaultValue());
    }

    private ExternalSpillableMap<Serializable, BufferedRecord<IndexedRecord>> spillableMap(
            HudiTrinoReaderContext readerContext,
            long maxMemoryBytes,
            SizeEstimator<BufferedRecord<IndexedRecord>> estimator,
            boolean compressionEnabled)
    {
        try {
            return new ExternalSpillableMap<>(
                    maxMemoryBytes,
                    spillDirectory.toString(),
                    new DefaultSizeEstimator<>(),
                    estimator,
                    ExternalSpillableMap.DiskMapType.BITCASK,
                    readerContext.getRecordSerializer(),
                    compressionEnabled,
                    "TestHudiTrinoReaderContext");
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @ParameterizedTest
    @CsvSource({"9, false", "11, true"})
    void testDateOrderingWithSerializedDelete(int deleteEpochDay, boolean expectDeleted)
            throws IOException
    {
        HoodieSchema schema = schemaWithOrdering(LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT)));
        TypedProperties props = new TypedProperties();
        HudiTrinoReaderContext readerContext = createReaderContext(schema, tableConfig(), props);
        GenericRecord record = new GenericData.Record(schema.toAvroSchema());
        record.put("id", "key");
        record.put("ordering", 10);
        BufferedRecord<IndexedRecord> data = BufferedRecords.fromEngineRecord(
                record, "key", schema, readerContext.getRecordContext(), List.of("ordering"), null);
        BufferedRecord<IndexedRecord> delete = readDelete(readerContext, deleteEpochDay);
        BufferedRecordMerger<IndexedRecord> merger = BufferedRecordMergerFactory.create(
                readerContext, EVENT_TIME_ORDERING, false, Option.empty(), schema, Option.empty(), props, Option.empty());

        assertThat(merger.finalMerge(data, delete)).isSameAs(expectDeleted ? delete : data);
    }

    @ParameterizedTest
    @CsvSource({"false, false", "true, false", "false, true", "true, true"})
    void testTimestampOrderingConfiguration(boolean consistentTimestamp, boolean persistedProperty)
            throws IOException
    {
        HoodieSchema schema = schemaWithOrdering(LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG)));
        HoodieTableConfig tableConfig = tableConfig();
        TypedProperties props = new TypedProperties();
        String key = KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED.key();
        if (persistedProperty) {
            tableConfig.setValue(RECORD_MERGE_PROPERTY_PREFIX + key, Boolean.toString(consistentTimestamp));
            // Persisted merge properties take precedence over the reader's value.
            props.setProperty(key, Boolean.toString(!consistentTimestamp));
        }
        else {
            props.setProperty(key, Boolean.toString(consistentTimestamp));
        }
        HudiTrinoReaderContext readerContext = createReaderContext(schema, tableConfig, props);
        GenericRecord record = new GenericData.Record(schema.toAvroSchema());
        record.put("id", "key");
        record.put("ordering", 1_234_567L);
        Object expected = consistentTimestamp ? new Timestamp(1234) : 1_234_567L;

        assertThat(readerContext.getRecordContext().getOrderingValue(record, schema, List.of("ordering"))).isEqualTo(expected);
        assertThat(readDelete(readerContext, 1_234_567L).getOrderingValue()).isEqualTo(expected);
    }

    private static HoodieSchema schemaWithOrdering(Schema orderingSchema)
    {
        return HoodieSchema.fromAvroSchema(SchemaBuilder.record("test_record").fields()
                .requiredString("id")
                .name("ordering").type(orderingSchema).noDefault()
                .endRecord());
    }

    private static HoodieTableConfig tableConfig()
    {
        HoodieTableConfig tableConfig = new HoodieTableConfig();
        tableConfig.setValue(HoodieTableConfig.VERSION, "9");
        tableConfig.setValue(HoodieTableConfig.RECORD_MERGE_MODE, EVENT_TIME_ORDERING.name());
        tableConfig.setValue(HoodieTableConfig.ORDERING_FIELDS, "ordering");
        return tableConfig;
    }

    private static HudiTrinoReaderContext createReaderContext(HoodieSchema schema, HoodieTableConfig tableConfig, TypedProperties props)
    {
        // These tests consume classic log records directly; no page source is read.
        HudiTrinoReaderContext readerContext = new HudiTrinoReaderContext(
                new TrinoStorageConfiguration(), tableConfig, new EmptyPageSource(), List.of(), null,
                (path, start, length, projection) -> {
                    throw new AssertionError("Unexpected parquet read");
                }, props);
        TypedProperties mergeProps = ConfigUtils.getMergeProps(props, tableConfig);
        readerContext.initRecordMerger(mergeProps);
        readerContext.setHasLogFiles(false);
        readerContext.setHasBootstrapBaseFile(false);
        HoodieSchema projectedSchema = HoodieSchema.fromAvroSchema(SchemaBuilder.record("test_record").fields().requiredString("id").endRecord());
        // The ordering field can be absent from the projection, so initialization must use the table schema.
        readerContext.setSchemaHandler(new FileGroupReaderSchemaHandler<>(
                readerContext, schema, projectedSchema, Option.empty(), mergeProps,
                TableState.of(new StoragePath("/test-table"), tableConfig, Option.empty(), Option.empty())));
        return readerContext;
    }

    private static BufferedRecord<IndexedRecord> readDelete(HudiTrinoReaderContext readerContext, Comparable<?> orderingValue)
            throws IOException
    {
        HoodieDeleteBlock written = new HoodieDeleteBlock(List.of(Pair.of(DeleteRecord.create("key", "", orderingValue), -1L)), Map.of());
        HoodieDeleteBlock read = new HoodieDeleteBlock(Option.of(written.getContentBytes(null).toByteArray()),
                null, false, Option.empty(), Map.of(), Map.of());
        return read.getRecordsToDelete(readerContext).getFirst();
    }
}
