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
package io.trino.plugin.hudi.util;

import io.trino.metastore.HiveType;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HivePartitionKey;
import io.trino.plugin.hudi.HudiSplit;
import io.trino.plugin.hudi.file.HudiBaseFile;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.SplitWeight;
import io.trino.spi.TrinoException;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Int128;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.UuidType;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.util.Utf8;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.TimeType.TIME_MICROS;
import static io.trino.spi.type.TimeType.TIME_MILLIS;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.UuidType.javaUuidToTrinoUuid;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Float.floatToRawIntBits;
import static java.time.ZoneOffset.UTC;
import static org.apache.hudi.common.avro.HoodieAvroUtils.avroToBytes;
import static org.apache.hudi.common.avro.HoodieAvroUtils.bytesToAvro;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestHoodieSchemaAvroSerializer
{
    private static final TrinoTypeMapper TYPE_MAPPER = new TrinoTypeMapper(new TypeOperators());

    private static final UUID SAMPLE_UUID = UUID.fromString("123e4567-e89b-12d3-a456-426614174000");
    // 2023-11-14T22:13:20.123456Z
    private static final long EPOCH_MILLIS = 1_700_000_000_123L;
    private static final long EPOCH_MICROS = 1_700_000_000_123_456L;
    // 12:34:56.789123
    private static final long MICROS_OF_DAY = 45_296_789_123L;
    private static final int MILLIS_OF_DAY = 45_296_789;

    /**
     * One case per Hudi type: how to write a value of the mapper's Trino type, and the Avro-native value serialize()
     * must turn it into.
     */
    record TypeCase(String description, HoodieSchema fieldSchema, Consumer<BlockBuilder> writer, Object expectedAvro)
    {
        @Override
        public String toString()
        {
            return description;
        }
    }

    static Stream<TypeCase> typeCases()
    {
        HoodieSchema enumSchema = HoodieSchema.createEnum("color", null, null, List.of("RED", "GREEN"));
        HoodieSchema fixedSchema = HoodieSchema.createFixed("md5", null, null, 4);
        HoodieSchema fixedUuidSchema = HoodieSchema.fromAvroSchema(LogicalTypes.uuid().addToSchema(Schema.createFixed("uuid_fixed", null, null, 16)));
        HoodieSchema bytesDecimalSchema = HoodieSchema.createDecimal(10, 2);
        HoodieSchema fixedDecimalSchema = HoodieSchema.createDecimal("amount", null, null, 20, 4, 9);
        HoodieSchema pointSchema = HoodieSchema.createRecord("point", null, null, List.of(
                HoodieSchemaField.of("name", HoodieSchema.create(HoodieSchemaType.STRING)),
                HoodieSchemaField.of("at", HoodieSchema.createNullable(HoodieSchema.createLocalTimestampMicros()), null, HoodieSchema.NULL_VALUE)));
        HoodieSchema keyedSchema = HoodieSchema.createRecord("keyed", null, null, List.of(
                HoodieSchemaField.of("id", HoodieSchema.createUUID()),
                HoodieSchemaField.of("tags", HoodieSchema.createArray(HoodieSchema.create(HoodieSchemaType.STRING)))));
        HoodieSchema blobSchema = HoodieSchema.createBlob();
        HoodieSchema variantSchema = HoodieSchema.createVariant();

        return Stream.of(
                new TypeCase("boolean", HoodieSchema.create(HoodieSchemaType.BOOLEAN), output -> BOOLEAN.writeBoolean(output, true), true),
                new TypeCase("int", HoodieSchema.create(HoodieSchemaType.INT), output -> INTEGER.writeLong(output, -42), -42),
                new TypeCase("long", HoodieSchema.create(HoodieSchemaType.LONG), output -> BIGINT.writeLong(output, 1L << 40), 1L << 40),
                new TypeCase("float", HoodieSchema.create(HoodieSchemaType.FLOAT), output -> REAL.writeLong(output, floatToRawIntBits(1.5f)), 1.5f),
                new TypeCase("double", HoodieSchema.create(HoodieSchemaType.DOUBLE), output -> DOUBLE.writeDouble(output, 2.25), 2.25),
                new TypeCase("string", HoodieSchema.create(HoodieSchemaType.STRING), output -> VARCHAR.writeSlice(output, utf8Slice("h\u00e9llo")), "h\u00e9llo"),
                new TypeCase("enum", enumSchema, output -> VARCHAR.writeSlice(output, utf8Slice("GREEN")),
                        new GenericData.EnumSymbol(enumSchema.toAvroSchema(), "GREEN")),
                new TypeCase("bytes", HoodieSchema.create(HoodieSchemaType.BYTES), output -> VARBINARY.writeSlice(output, wrappedBuffer(new byte[] {1, 2, 3})),
                        ByteBuffer.wrap(new byte[] {1, 2, 3})),
                new TypeCase("fixed", fixedSchema, output -> VARBINARY.writeSlice(output, wrappedBuffer(new byte[] {1, 2, 3, 4})),
                        new GenericData.Fixed(fixedSchema.toAvroSchema(), new byte[] {1, 2, 3, 4})),
                new TypeCase("uuid over string", HoodieSchema.createUUID(), output -> UuidType.UUID.writeSlice(output, javaUuidToTrinoUuid(SAMPLE_UUID)),
                        SAMPLE_UUID.toString()),
                new TypeCase("uuid over fixed(16)", fixedUuidSchema, output -> UuidType.UUID.writeSlice(output, javaUuidToTrinoUuid(SAMPLE_UUID)),
                        new GenericData.Fixed(fixedUuidSchema.toAvroSchema(), uuidBytes(SAMPLE_UUID))),
                new TypeCase("decimal over bytes", bytesDecimalSchema, output -> TYPE_MAPPER.toTypeSystem(bytesDecimalSchema).writeLong(output, -12345),
                        ByteBuffer.wrap(BigInteger.valueOf(-12345).toByteArray())),
                // A long decimal, padded to its fixed size with the sign byte the way Avro's own conversion pads it
                new TypeCase("decimal over fixed", fixedDecimalSchema,
                        output -> TYPE_MAPPER.toTypeSystem(fixedDecimalSchema).writeObject(output, Int128.valueOf(-123_456_789L)),
                        new Conversions.DecimalConversion().toFixed(new BigDecimal("-12345.6789"), fixedDecimalSchema.toAvroSchema(), fixedDecimalSchema.toAvroSchema().getLogicalType())),
                new TypeCase("date", HoodieSchema.createDate(), output -> DATE.writeLong(output, 19_000), 19_000),
                new TypeCase("time-millis", HoodieSchema.createTimeMillis(), output -> TIME_MILLIS.writeLong(output, MILLIS_OF_DAY * 1_000_000_000L), MILLIS_OF_DAY),
                new TypeCase("time-micros", HoodieSchema.createTimeMicros(), output -> TIME_MICROS.writeLong(output, MICROS_OF_DAY * 1_000_000L), MICROS_OF_DAY),
                new TypeCase("timestamp-millis", HoodieSchema.createTimestampMillis(),
                        output -> TIMESTAMP_TZ_MILLIS.writeLong(output, packDateTimeWithZone(EPOCH_MILLIS, UTC_KEY)), EPOCH_MILLIS),
                new TypeCase("timestamp-micros", HoodieSchema.createTimestampMicros(),
                        output -> TIMESTAMP_TZ_MICROS.writeObject(output, LongTimestampWithTimeZone.fromEpochMillisAndFraction(EPOCH_MILLIS, 456_000_000, UTC_KEY)), EPOCH_MICROS),
                new TypeCase("local-timestamp-millis", HoodieSchema.createLocalTimestampMillis(), output -> TIMESTAMP_MILLIS.writeLong(output, EPOCH_MILLIS * 1000), EPOCH_MILLIS),
                new TypeCase("local-timestamp-micros", HoodieSchema.createLocalTimestampMicros(), output -> TIMESTAMP_MICROS.writeLong(output, EPOCH_MICROS), EPOCH_MICROS),
                new TypeCase("array", HoodieSchema.createArray(HoodieSchema.createNullable(HoodieSchema.create(HoodieSchemaType.LONG))),
                        output -> ((ArrayBlockBuilder) output).buildEntry(elements -> {
                            BIGINT.writeLong(elements, 1);
                            elements.appendNull();
                            BIGINT.writeLong(elements, 3);
                        }),
                        Arrays.asList(1L, null, 3L)),
                new TypeCase("map", HoodieSchema.createMap(HoodieSchema.createNullable(HoodieSchema.createDate())),
                        output -> ((MapBlockBuilder) output).buildEntry((keys, values) -> {
                            VARCHAR.writeSlice(keys, utf8Slice("a"));
                            DATE.writeLong(values, 19_000);
                            VARCHAR.writeSlice(keys, utf8Slice("b"));
                            values.appendNull();
                        }),
                        mapOf("a", 19_000, "b", null)),
                new TypeCase("record", pointSchema,
                        output -> ((RowBlockBuilder) output).buildEntry(fields -> {
                            VARCHAR.writeSlice(fields.get(0), utf8Slice("p"));
                            fields.get(1).appendNull();
                        }),
                        record(pointSchema, "p", null)),
                new TypeCase("array of records nesting a uuid and an array", HoodieSchema.createArray(keyedSchema),
                        output -> ((ArrayBlockBuilder) output).buildEntry(elements -> ((RowBlockBuilder) elements).buildEntry(fields -> {
                            UuidType.UUID.writeSlice(fields.get(0), javaUuidToTrinoUuid(SAMPLE_UUID));
                            ((ArrayBlockBuilder) fields.get(1)).buildEntry(tags -> VARCHAR.writeSlice(tags, utf8Slice("t")));
                        })),
                        List.of(record(keyedSchema, SAMPLE_UUID.toString(), List.of("t")))),
                new TypeCase("blob", blobSchema,
                        output -> ((RowBlockBuilder) output).buildEntry(fields -> {
                            VARCHAR.writeSlice(fields.get(0), utf8Slice("OUT_OF_LINE"));
                            fields.get(1).appendNull();
                            ((RowBlockBuilder) fields.get(2)).buildEntry(reference -> {
                                VARCHAR.writeSlice(reference.get(0), utf8Slice("s3://bucket/blob"));
                                BIGINT.writeLong(reference.get(1), 10);
                                reference.get(2).appendNull();
                                BOOLEAN.writeBoolean(reference.get(3), true);
                            });
                        }),
                        blobRecord(blobSchema)),
                new TypeCase("variant", variantSchema,
                        output -> ((RowBlockBuilder) output).buildEntry(fields -> {
                            VARBINARY.writeSlice(fields.get(0), wrappedBuffer(new byte[] {1}));
                            VARBINARY.writeSlice(fields.get(1), wrappedBuffer(new byte[] {12, 5}));
                        }),
                        record(variantSchema, ByteBuffer.wrap(new byte[] {1}), ByteBuffer.wrap(new byte[] {12, 5}))),
                vectorCase(HoodieSchema.Vector.VectorElementType.FLOAT, output -> {
                    REAL.writeLong(output, floatToRawIntBits(1.0f));
                    REAL.writeLong(output, floatToRawIntBits(-2.5f));
                    REAL.writeLong(output, floatToRawIntBits(3.25f));
                }, ByteBuffer.allocate(12).order(ByteOrder.LITTLE_ENDIAN).putFloat(1.0f).putFloat(-2.5f).putFloat(3.25f).array()),
                vectorCase(HoodieSchema.Vector.VectorElementType.DOUBLE, output -> {
                    DOUBLE.writeDouble(output, 1.0);
                    DOUBLE.writeDouble(output, -2.5);
                    DOUBLE.writeDouble(output, 3.25);
                }, ByteBuffer.allocate(24).order(ByteOrder.LITTLE_ENDIAN).putDouble(1.0).putDouble(-2.5).putDouble(3.25).array()),
                vectorCase(HoodieSchema.Vector.VectorElementType.INT8, output -> {
                    TINYINT.writeLong(output, 1);
                    TINYINT.writeLong(output, -2);
                    TINYINT.writeLong(output, 127);
                }, new byte[] {1, -2, 127}));
    }

    private static TypeCase vectorCase(HoodieSchema.Vector.VectorElementType elementType, Consumer<BlockBuilder> elementWriter, byte[] littleEndianBytes)
    {
        HoodieSchema vectorSchema = HoodieSchema.createVector(3, elementType);
        return new TypeCase(
                "vector of " + elementType,
                vectorSchema,
                output -> ((ArrayBlockBuilder) output).buildEntry(elementWriter::accept),
                new GenericData.Fixed(vectorSchema.toAvroSchema(), littleEndianBytes));
    }

    /**
     * Pins every type through the full merge-path cycle: page to an Avro-native record, through the Avro binary
     * round trip payload-based merging applies, and back to a page equal to the input. The nullable wrapper is how
     * nearly every Hudi column is declared, and is where a null value travels.
     */
    @ParameterizedTest
    @MethodSource("typeCases")
    void testRoundTrip(TypeCase typeCase)
            throws IOException
    {
        assertRoundTrip(typeCase, typeCase.fieldSchema(), false);
        assertRoundTrip(typeCase, HoodieSchema.createNullable(typeCase.fieldSchema()), true);
    }

    private static void assertRoundTrip(TypeCase typeCase, HoodieSchema fieldSchema, boolean withNullRow)
            throws IOException
    {
        Schema recordSchema = HoodieSchema.createRecord("rec", null, null, List.of(
                HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.LONG)),
                HoodieSchemaField.of("value", fieldSchema))).toAvroSchema();
        Type type = TYPE_MAPPER.toTypeSystem(fieldSchema);
        List<Type> types = List.of(BIGINT, type);
        List<HiveColumnHandle> columns = List.of(dataColumn("id", BIGINT), dataColumn("value", type));

        PageBuilder inputBuilder = new PageBuilder(types);
        inputBuilder.declarePosition();
        BIGINT.writeLong(inputBuilder.getBlockBuilder(0), 1);
        typeCase.writer().accept(inputBuilder.getBlockBuilder(1));
        if (withNullRow) {
            inputBuilder.declarePosition();
            BIGINT.writeLong(inputBuilder.getBlockBuilder(0), 2);
            inputBuilder.getBlockBuilder(1).appendNull();
        }
        Page input = inputBuilder.build();

        HoodieSchemaAvroSerializer serializer = new HoodieSchemaAvroSerializer(columns, prefilledValues(), recordSchema);
        PageBuilder fromRecords = new PageBuilder(types);
        PageBuilder fromDecodedRecords = new PageBuilder(types);
        for (int position = 0; position < input.getPositionCount(); position++) {
            IndexedRecord record = serializer.serialize(input, position);
            assertThat(record.getSchema()).isSameAs(recordSchema);
            assertThat(record.get(1)).isEqualTo(position == 0 ? typeCase.expectedAvro() : null);

            byte[] bytes = avroToBytes(record);
            GenericRecord decoded = bytesToAvro(bytes, recordSchema);
            // Re-encoding what was decoded gives the same bytes: the record survived the binary round trip intact
            assertThat(avroToBytes(decoded)).isEqualTo(bytes);

            serializer.buildRecordInPage(fromRecords, record);
            serializer.buildRecordInPage(fromDecodedRecords, decoded);
        }
        assertPageEquals(types, fromRecords.build(), input);
        assertPageEquals(types, fromDecodedRecords.build(), input);
    }

    /**
     * Records that come out of a classic Avro log block carry Avro-native values: Utf8 strings, raw logical-type
     * numbers, ByteBuffer and GenericFixed binaries. A record decoded with logical-type conversions carries
     * java.time, BigDecimal and UUID values instead. Both must build the same page, and the record may be wider than
     * the projection and in a different field order.
     */
    @Test
    void testBuildRecordInPageFromAvroLogBlockShapes()
    {
        HoodieSchema decimalSchema = HoodieSchema.createDecimal(10, 2);
        HoodieSchema enumSchema = HoodieSchema.createEnum("color", null, null, List.of("RED", "GREEN"));
        Schema recordSchema = HoodieSchema.createRecord("log_record", null, null, List.of(
                HoodieSchemaField.of("unprojected", HoodieSchema.create(HoodieSchemaType.STRING)),
                HoodieSchemaField.of("ts", HoodieSchema.createNullable(HoodieSchema.createTimestampMicros()), null, HoodieSchema.NULL_VALUE),
                HoodieSchemaField.of("local_ts", HoodieSchema.createNullable(HoodieSchema.createLocalTimestampMillis()), null, HoodieSchema.NULL_VALUE),
                HoodieSchemaField.of("day", HoodieSchema.createNullable(HoodieSchema.createDate()), null, HoodieSchema.NULL_VALUE),
                HoodieSchemaField.of("t", HoodieSchema.createNullable(HoodieSchema.createTimeMicros()), null, HoodieSchema.NULL_VALUE),
                HoodieSchemaField.of("amount", HoodieSchema.createNullable(decimalSchema), null, HoodieSchema.NULL_VALUE),
                HoodieSchemaField.of("id", HoodieSchema.createNullable(HoodieSchema.createUUID()), null, HoodieSchema.NULL_VALUE),
                HoodieSchemaField.of("name", HoodieSchema.createNullable(HoodieSchemaType.STRING), null, HoodieSchema.NULL_VALUE),
                HoodieSchemaField.of("color", HoodieSchema.createNullable(enumSchema), null, HoodieSchema.NULL_VALUE),
                HoodieSchemaField.of("tags", HoodieSchema.createNullable(HoodieSchema.createMap(HoodieSchema.create(HoodieSchemaType.STRING))), null, HoodieSchema.NULL_VALUE)))
                .toAvroSchema();
        List<HiveColumnHandle> columns = List.of(
                dataColumn("name", VARCHAR),
                dataColumn("ts", TIMESTAMP_TZ_MICROS),
                dataColumn("local_ts", TIMESTAMP_MILLIS),
                dataColumn("day", DATE),
                dataColumn("t", TIME_MICROS),
                dataColumn("amount", TYPE_MAPPER.toTypeSystem(decimalSchema)),
                dataColumn("id", UuidType.UUID),
                dataColumn("color", VARCHAR),
                dataColumn("tags", TYPE_MAPPER.toTypeSystem(HoodieSchema.createMap(HoodieSchema.create(HoodieSchemaType.STRING)))),
                HiveColumnHandle.createBaseColumn("pk_int", -1, HiveType.HIVE_INT, INTEGER, HiveColumnHandle.ColumnType.PARTITION_KEY, Optional.empty()));
        List<Type> types = columns.stream().map(HiveColumnHandle::getType).toList();

        GenericData.Record avroNative = new GenericData.Record(recordSchema);
        avroNative.put("unprojected", new Utf8("ignored"));
        avroNative.put("ts", EPOCH_MICROS);
        avroNative.put("local_ts", EPOCH_MILLIS);
        avroNative.put("day", 19_000);
        avroNative.put("t", MICROS_OF_DAY);
        avroNative.put("amount", ByteBuffer.wrap(BigInteger.valueOf(12_345).toByteArray()));
        avroNative.put("id", new Utf8(SAMPLE_UUID.toString()));
        avroNative.put("name", new Utf8("n\u00e4me"));
        avroNative.put("color", new GenericData.EnumSymbol(enumSchema.toAvroSchema(), "RED"));
        avroNative.put("tags", mapOf(new Utf8("k"), new Utf8("v")));

        GenericData.Record converted = new GenericData.Record(recordSchema);
        converted.put("unprojected", "ignored");
        converted.put("ts", Instant.ofEpochSecond(0, EPOCH_MICROS * 1000));
        converted.put("local_ts", LocalDateTime.ofEpochSecond(0, 0, UTC).plusNanos(EPOCH_MILLIS * 1_000_000));
        converted.put("day", LocalDate.ofEpochDay(19_000));
        converted.put("t", LocalTime.ofNanoOfDay(MICROS_OF_DAY * 1000));
        converted.put("amount", new BigDecimal("123.45"));
        converted.put("id", SAMPLE_UUID);
        converted.put("name", "n\u00e4me");
        converted.put("color", "RED");
        converted.put("tags", mapOf("k", "v"));

        HoodieSchemaAvroSerializer serializer = new HoodieSchemaAvroSerializer(columns, prefilledValues());
        PageBuilder pageBuilder = new PageBuilder(types);
        serializer.buildRecordInPage(pageBuilder, avroNative);
        serializer.buildRecordInPage(pageBuilder, converted);
        Page page = pageBuilder.build();

        for (int position = 0; position < 2; position++) {
            assertThat(VARCHAR.getSlice(page.getBlock(0), position).toStringUtf8()).isEqualTo("n\u00e4me");
            LongTimestampWithTimeZone timestamp = (LongTimestampWithTimeZone) TIMESTAMP_TZ_MICROS.getObject(page.getBlock(1), position);
            assertThat(timestamp.getEpochMillis()).isEqualTo(EPOCH_MILLIS);
            assertThat(timestamp.getPicosOfMilli()).isEqualTo(456_000_000);
            assertThat(timestamp.getTimeZoneKey()).isEqualTo(UTC_KEY.getKey());
            assertThat(TIMESTAMP_MILLIS.getLong(page.getBlock(2), position)).isEqualTo(EPOCH_MILLIS * 1000);
            assertThat(DATE.getLong(page.getBlock(3), position)).isEqualTo(19_000);
            assertThat(TIME_MICROS.getLong(page.getBlock(4), position)).isEqualTo(MICROS_OF_DAY * 1_000_000L);
            assertThat(types.get(5).getLong(page.getBlock(5), position)).isEqualTo(12_345);
            assertThat(UuidType.UUID.getObjectValue(page.getBlock(6), position)).isEqualTo(SAMPLE_UUID.toString());
            assertThat(VARCHAR.getSlice(page.getBlock(7), position).toStringUtf8()).isEqualTo("RED");
            assertThat(types.get(8).getObjectValue(page.getBlock(8), position)).isEqualTo(Map.of("k", "v"));
            assertThat(INTEGER.getInt(page.getBlock(9), position)).isEqualTo(42);
        }
    }

    @Test
    void testUnionFieldIsRejected()
    {
        Schema recordSchema = HoodieSchema.createRecord("rec", null, null, List.of(
                HoodieSchemaField.of("u", HoodieSchema.createUnion(HoodieSchema.create(HoodieSchemaType.INT), HoodieSchema.create(HoodieSchemaType.STRING)))))
                .toAvroSchema();
        assertThatThrownBy(() -> new HoodieSchemaAvroSerializer(List.of(dataColumn("u", VARCHAR)), prefilledValues(), recordSchema))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Trino has no union type");
    }

    @Test
    void testUnexpectedValueShapeNamesTheField()
    {
        Schema recordSchema = HoodieSchema.createRecord("rec", null, null, List.of(
                HoodieSchemaField.of("day", HoodieSchema.createDate()))).toAvroSchema();
        GenericData.Record record = new GenericData.Record(recordSchema);
        record.put("day", new Utf8("2024-01-01"));

        HoodieSchemaAvroSerializer serializer = new HoodieSchemaAvroSerializer(List.of(dataColumn("day", DATE)), prefilledValues());
        assertThatThrownBy(() -> serializer.buildRecordInPage(new PageBuilder(List.of(DATE)), record))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("day")
                .hasMessageContaining("DATE")
                .hasMessageContaining(Utf8.class.getName());
    }

    @Test
    void testMalformedVectorIsRejected()
    {
        HoodieSchema vectorSchema = HoodieSchema.createVector(2);
        Schema recordSchema = HoodieSchema.createRecord("rec", null, null, List.of(HoodieSchemaField.of("v", vectorSchema))).toAvroSchema();
        Type type = TYPE_MAPPER.toTypeSystem(vectorSchema);
        GenericData.Record record = new GenericData.Record(recordSchema);
        record.put("v", ByteBuffer.wrap(new byte[7]));

        HoodieSchemaAvroSerializer serializer = new HoodieSchemaAvroSerializer(List.of(dataColumn("v", type)), prefilledValues(), recordSchema);
        assertThatThrownBy(() -> serializer.buildRecordInPage(new PageBuilder(List.of(type)), record))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("multiple of 4 bytes");

        // A vector whose array does not hold exactly its dimension cannot be written as its fixed
        PageBuilder pageBuilder = new PageBuilder(List.of(type));
        pageBuilder.declarePosition();
        ((ArrayBlockBuilder) pageBuilder.getBlockBuilder(0)).buildEntry(elements -> REAL.writeLong(elements, floatToRawIntBits(1.0f)));
        Page page = pageBuilder.build();
        assertThatThrownBy(() -> serializer.serialize(page, 0))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("dimension 2");
    }

    private static void assertPageEquals(List<Type> types, Page actual, Page expected)
    {
        assertThat(actual.getPositionCount()).isEqualTo(expected.getPositionCount());
        for (int channel = 0; channel < types.size(); channel++) {
            Type type = types.get(channel);
            for (int position = 0; position < expected.getPositionCount(); position++) {
                assertThat(type.getObjectValue(actual.getBlock(channel), position))
                        .as("channel %s, position %s", channel, position)
                        .isEqualTo(type.getObjectValue(expected.getBlock(channel), position));
            }
        }
    }

    private static GenericData.Record record(HoodieSchema schema, Object... values)
    {
        GenericData.Record record = new GenericData.Record(schema.toAvroSchema());
        for (int field = 0; field < values.length; field++) {
            record.put(field, values[field]);
        }
        return record;
    }

    private static GenericData.Record blobRecord(HoodieSchema blobSchema)
    {
        Schema avroSchema = blobSchema.toAvroSchema();
        Schema typeSchema = avroSchema.getField("type").schema();
        Schema referenceSchema = HoodieSchema.fromAvroSchema(avroSchema.getField("reference").schema()).getNonNullType().toAvroSchema();
        GenericData.Record reference = new GenericData.Record(referenceSchema);
        reference.put("external_path", "s3://bucket/blob");
        reference.put("offset", 10L);
        reference.put("length", null);
        reference.put("managed", true);
        GenericData.Record blob = new GenericData.Record(avroSchema);
        blob.put("type", new GenericData.EnumSymbol(typeSchema, "OUT_OF_LINE"));
        blob.put("data", null);
        blob.put("reference", reference);
        return blob;
    }

    private static byte[] uuidBytes(UUID uuid)
    {
        return ByteBuffer.allocate(16).putLong(uuid.getMostSignificantBits()).putLong(uuid.getLeastSignificantBits()).array();
    }

    private static <K, V> Map<K, V> mapOf(K firstKey, V firstValue)
    {
        Map<K, V> map = new HashMap<>();
        map.put(firstKey, firstValue);
        return map;
    }

    private static Map<String, Object> mapOf(String firstKey, Object firstValue, String secondKey, Object secondValue)
    {
        Map<String, Object> map = new HashMap<>();
        map.put(firstKey, firstValue);
        map.put(secondKey, secondValue);
        return map;
    }

    private static HiveColumnHandle dataColumn(String name, Type type)
    {
        // The Hive type is metadata only; the serializer reads the Trino type
        return HiveColumnHandle.createBaseColumn(name, 0, HiveType.HIVE_STRING, type, HiveColumnHandle.ColumnType.REGULAR, Optional.empty());
    }

    private static PrefilledColumnValues prefilledValues()
    {
        HudiBaseFile baseFile = new HudiBaseFile("s3://bucket/table/file1.parquet", "file1.parquet", 1234, 1700000000123L, 0, 1234);
        HudiSplit split = new HudiSplit(
                baseFile,
                List.of(),
                "001",
                TupleDomain.all(),
                List.of(new HivePartitionKey("pk_int", "42")),
                SplitWeight.standard());
        return PrefilledColumnValues.create(split);
    }
}
