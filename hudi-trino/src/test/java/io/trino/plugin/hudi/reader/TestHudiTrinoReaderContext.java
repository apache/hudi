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
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.util.Utf8;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.table.read.BufferedRecords;
import org.apache.hudi.common.util.DefaultSizeEstimator;
import org.apache.hudi.common.util.SizeEstimator;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.Decimals.writeBigDecimal;
import static io.trino.spi.type.Decimals.writeShortDecimal;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.RowType.field;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_NANOS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hudi.common.config.HoodieMemoryConfig.MAX_MEMORY_FOR_MERGE;
import static org.assertj.core.api.Assertions.assertThat;

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
    void testSizeEstimatorExcludesTheSchemaAndSamplesEveryHundredthPut()
    {
        // A wide schema with three populated fields: a graph walk of the record would count the schema's 300 fields
        SchemaBuilder.FieldAssembler<Schema> fields = SchemaBuilder.record("wide").fields();
        for (int i = 0; i < 300; i++) {
            fields = fields.optionalString("column_with_a_long_name_" + i);
        }
        HoodieSchema schema = HoodieSchema.fromAvroSchema(fields.endRecord());
        HudiTrinoReaderContext readerContext = readerContext();
        int recordCount = 5_000;
        List<BufferedRecord<IndexedRecord>> records = new ArrayList<>(recordCount);
        for (int i = 0; i < recordCount; i++) {
            GenericData.Record record = new GenericData.Record(schema.toAvroSchema());
            record.put(0, "value-" + i);
            record.put(1, (long) i);
            record.put(2, new Utf8("utf8-" + i));
            records.add(BufferedRecords.fromEngineRecord(record, schema, readerContext.getRecordContext(), (long) i, "key-" + i, false));
        }

        SizeEstimator<BufferedRecord<IndexedRecord>> estimator = readerContext.getRecordSizeEstimator();
        // Key, ordering value, three values and the record's 300-slot value array, but not the 300-field schema
        assertThat(estimator.sizeEstimate(records.getFirst())).isBetween(1_000L, 2_000L);

        AtomicInteger estimatorCalls = new AtomicInteger();
        SizeEstimator<BufferedRecord<IndexedRecord>> countingEstimator = record -> {
            estimatorCalls.incrementAndGet();
            return estimator.sizeEstimate(record);
        };
        try (ExternalSpillableMap<Serializable, BufferedRecord<IndexedRecord>> map = spillableMap(readerContext, MAX_MEMORY_FOR_MERGE.defaultValue(), countingEstimator)) {
            for (BufferedRecord<IndexedRecord> record : records) {
                map.put(record.getRecordKey(), record);
            }
            assertThat(map.getDiskBasedMapNumEntries()).isZero();
        }
        // The first put and every 100th one after it: 50 calls for 5,000 records, each sizing only the record's values
        assertThat(estimatorCalls.get()).isEqualTo(recordCount / 100);
    }

    @Test
    void testSpillRoundTripsEveryValueKind()
    {
        // Trino objects, as Type.getObjectValue returns them for rows read from native log files
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
                kind(rowSchema, objectValue(RowType.from(List.of(field("name", VARCHAR), field("price", SHORT_DECIMAL))), builder -> ((RowBlockBuilder) builder).buildEntry(rowFields -> {
                    VARCHAR.writeString(rowFields.get(0), "nested");
                    writeShortDecimal(rowFields.get(1), 999);
                }))));
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
                BufferedRecords.createDelete("deleted", 30L));

        // A one-byte budget leaves no room in memory, so every put goes through the spill serializer
        try (ExternalSpillableMap<Serializable, BufferedRecord<IndexedRecord>> map = spillableMap(readerContext, 1, readerContext.getRecordSizeEstimator())) {
            for (BufferedRecord<IndexedRecord> record : records) {
                map.put(record.getRecordKey(), record);
            }
            assertThat(map.getInMemoryMapNumEntries()).isZero();
            assertThat(map.getDiskBasedMapNumEntries()).isEqualTo(records.size());

            for (BufferedRecord<IndexedRecord> expected : records) {
                BufferedRecord<IndexedRecord> actual = map.get(expected.getRecordKey());
                assertThat(actual.getRecordKey()).isEqualTo(expected.getRecordKey());
                assertThat(actual.getOrderingValue()).isEqualTo(expected.getOrderingValue());
                assertThat(actual.getSchemaId()).isEqualTo(expected.getSchemaId());
                assertThat(actual.getHoodieOperation()).isEqualTo(expected.getHoodieOperation());
                if (expected.getRecord() == null) {
                    assertThat(actual.getRecord()).isNull();
                    continue;
                }
                assertThat(actual.getRecord().getSchema()).isEqualTo(expected.getRecord().getSchema());
                for (int i = 0; i < allValues.size(); i++) {
                    Object expectedValue = expected.getRecord().get(i);
                    Object actualValue = actual.getRecord().get(i);
                    assertThat(actualValue).as("field %s", i).isEqualTo(expectedValue);
                    if (expectedValue != null && !(expectedValue instanceof List<?>) && !(expectedValue instanceof Map<?, ?>)) {
                        assertThat(actualValue).as("field %s", i).hasSameClassAs(expectedValue);
                    }
                }
            }
        }
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
        return new HudiTrinoReaderContext(new TrinoStorageConfiguration(), new HoodieTableConfig(), null, List.of(), null, null);
    }

    private ExternalSpillableMap<Serializable, BufferedRecord<IndexedRecord>> spillableMap(
            HudiTrinoReaderContext readerContext,
            long maxMemoryBytes,
            SizeEstimator<BufferedRecord<IndexedRecord>> estimator)
    {
        try {
            return new ExternalSpillableMap<>(
                    maxMemoryBytes,
                    spillDirectory.toString(),
                    new DefaultSizeEstimator<>(),
                    estimator,
                    ExternalSpillableMap.DiskMapType.BITCASK,
                    readerContext.getRecordSerializer(),
                    false,
                    "TestHudiTrinoReaderContext");
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
