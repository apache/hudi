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

import io.trino.plugin.hudi.util.ParquetReadConversions.ReadConversion;
import io.trino.spi.TrinoException;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.ColumnarArray;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.block.RowBlock;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.UuidType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;

import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.apache.parquet.schema.LogicalTypeAnnotation.listType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.mapType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.stringType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.timestampType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.uuidType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestParquetReadConversions
{
    private static final TypeOperators TYPE_OPERATORS = new TypeOperators();
    private static final UUID SAMPLE_UUID = UUID.fromString("123e4567-e89b-12d3-a456-426614174000");

    private static final org.apache.parquet.schema.Type STRING_UUID = Types.primitive(BINARY, OPTIONAL).as(stringType()).named("id");
    private static final org.apache.parquet.schema.Type FIXED_UUID = Types.primitive(FIXED_LEN_BYTE_ARRAY, OPTIONAL).length(16).as(uuidType()).named("id");

    @Test
    void testUuidFromBothLayouts()
    {
        for (org.apache.parquet.schema.Type parquetType : List.of(STRING_UUID, FIXED_UUID)) {
            ReadConversion conversion = ParquetReadConversions.forColumn(UuidType.UUID, parquetType).orElseThrow();
            assertThat(conversion.readType()).isEqualTo(VARBINARY);
        }

        // Hudi's default string layout holds the canonical text, write-parquet-uuid's fixed layout the raw bytes
        Block read = varbinary(SAMPLE_UUID.toString().getBytes(), null, uuidBytes(SAMPLE_UUID));
        Block converted = ParquetReadConversions.forColumn(UuidType.UUID, STRING_UUID).orElseThrow().toColumnType().apply(read);
        assertThat(objectValues(UuidType.UUID, converted)).containsExactly(SAMPLE_UUID.toString(), null, SAMPLE_UUID.toString());
    }

    @Test
    void testMalformedUuidIsRejected()
    {
        ReadConversion conversion = ParquetReadConversions.forColumn(UuidType.UUID, STRING_UUID).orElseThrow();
        assertThatThrownBy(() -> conversion.toColumnType().apply(varbinary(new byte[10])))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("found 10 bytes");
        byte[] notAUuid = "123e4567-e89b-12d3-a456-42661417400z".getBytes();
        assertThatThrownBy(() -> conversion.toColumnType().apply(varbinary(notAUuid)))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Invalid UUID");
    }

    @Test
    void testVectors()
    {
        org.apache.parquet.schema.Type floats = Types.primitive(FIXED_LEN_BYTE_ARRAY, OPTIONAL).length(8).named("v");
        assertVector(REAL, floats,
                ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putFloat(1.5f).putFloat(-2.0f).array(),
                List.of(1.5f, -2.0f));
        org.apache.parquet.schema.Type doubles = Types.primitive(FIXED_LEN_BYTE_ARRAY, REQUIRED).length(16).named("v");
        assertVector(DOUBLE, doubles,
                ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN).putDouble(0.25).putDouble(-8.0).array(),
                List.of(0.25, -8.0));
        org.apache.parquet.schema.Type bytes = Types.primitive(FIXED_LEN_BYTE_ARRAY, OPTIONAL).length(3).named("v");
        assertVector(TINYINT, bytes, new byte[] {1, -2, 127}, List.of((byte) 1, (byte) -2, (byte) 127));
    }

    @Test
    void testMalformedVectorIsRejected()
    {
        org.apache.parquet.schema.Type floats = Types.primitive(FIXED_LEN_BYTE_ARRAY, OPTIONAL).length(8).named("v");
        ReadConversion conversion = ParquetReadConversions.forColumn(new ArrayType(REAL), floats).orElseThrow();
        assertThatThrownBy(() -> conversion.toColumnType().apply(varbinary(new byte[6])))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("needs a multiple of 4 bytes");
    }

    @Test
    void testColumnsTheReaderDecodesNeedNoConversion()
    {
        // A real parquet list of floats, as a metastore array<float> column has, and a uuid column the reader cannot
        // decode anyway: both are left to the reader as they are
        org.apache.parquet.schema.Type floatList = Types.optionalGroup().as(listType())
                .addField(Types.repeatedGroup().addField(Types.primitive(FLOAT, OPTIONAL).named("element")).named("list"))
                .named("v");
        assertThat(ParquetReadConversions.forColumn(new ArrayType(REAL), floatList)).isEmpty();
        assertThat(ParquetReadConversions.forColumn(new ArrayType(REAL), Types.primitive(FLOAT, REPEATED).named("v"))).isEmpty();
        assertThat(ParquetReadConversions.forColumn(BIGINT, Types.primitive(INT64, OPTIONAL).named("v"))).isEmpty();
        assertThat(ParquetReadConversions.forColumn(UuidType.UUID, Types.optionalGroup().addField(STRING_UUID).named("v"))).isEmpty();
        // A row without UUID or VECTOR fields passes through untouched
        RowType plainRow = RowType.from(List.of(RowType.field("a", BIGINT), RowType.field("b", VARCHAR)));
        org.apache.parquet.schema.Type plainGroup = Types.optionalGroup()
                .addField(Types.primitive(INT64, OPTIONAL).named("a"))
                .addField(Types.primitive(BINARY, OPTIONAL).as(stringType()).named("b"))
                .named("r");
        assertThat(ParquetReadConversions.forColumn(plainRow, plainGroup)).isEmpty();
    }

    @Test
    void testRowWithAUuidField()
    {
        RowType rowType = RowType.from(List.of(RowType.field("id", UuidType.UUID), RowType.field("name", VARCHAR)));
        // Parquet field names resolve case-insensitively, as the parquet reader resolves them
        org.apache.parquet.schema.Type group = Types.optionalGroup()
                .addField(Types.primitive(BINARY, OPTIONAL).as(stringType()).named("ID"))
                .addField(Types.primitive(BINARY, OPTIONAL).as(stringType()).named("name"))
                .named("r");

        ReadConversion conversion = ParquetReadConversions.forColumn(rowType, group).orElseThrow();
        assertThat(conversion.readType()).isEqualTo(RowType.from(List.of(RowType.field("id", VARBINARY), RowType.field("name", VARCHAR))));

        RowType readType = (RowType) conversion.readType();
        Block read = build(readType, 3, builder -> {
            RowBlockBuilder rows = (RowBlockBuilder) builder;
            rows.buildEntry(fields -> {
                VARBINARY.writeSlice(fields.get(0), utf8Slice(SAMPLE_UUID.toString()));
                VARCHAR.writeSlice(fields.get(1), utf8Slice("a"));
            });
            rows.appendNull();
            rows.buildEntry(fields -> {
                fields.get(0).appendNull();
                VARCHAR.writeSlice(fields.get(1), utf8Slice("c"));
            });
        });
        Block converted = conversion.toColumnType().apply(read);
        assertThat(objectValues(rowType, converted)).containsExactly(
                List.of(SAMPLE_UUID.toString(), "a"),
                null,
                Arrays.asList(null, "c"));
    }

    @Test
    void testArrayOfVectors()
    {
        ArrayType columnType = new ArrayType(new ArrayType(REAL));
        org.apache.parquet.schema.Type list = Types.optionalGroup().as(listType())
                .addField(Types.repeatedGroup().addField(Types.primitive(FIXED_LEN_BYTE_ARRAY, OPTIONAL).length(4).named("element")).named("list"))
                .named("vs");

        ReadConversion conversion = ParquetReadConversions.forColumn(columnType, list).orElseThrow();
        assertThat(conversion.readType()).isEqualTo(new ArrayType(VARBINARY));

        Block read = build(conversion.readType(), 2, builder -> {
            ArrayBlockBuilder arrays = (ArrayBlockBuilder) builder;
            arrays.buildEntry(elements -> {
                VARBINARY.writeSlice(elements, wrappedBuffer(ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putFloat(1.0f).array()));
                elements.appendNull();
            });
            arrays.appendNull();
        });
        Block converted = conversion.toColumnType().apply(read);
        assertThat(objectValues(columnType, converted)).containsExactly(Arrays.asList(List.of(1.0f), null), null);
    }

    @Test
    void testMapOfUuids()
    {
        MapType columnType = new MapType(VARCHAR, UuidType.UUID, TYPE_OPERATORS);
        org.apache.parquet.schema.Type map = Types.optionalGroup().as(mapType())
                .addField(Types.repeatedGroup()
                        .addField(Types.primitive(BINARY, REQUIRED).as(stringType()).named("key"))
                        .addField(Types.primitive(FIXED_LEN_BYTE_ARRAY, OPTIONAL).length(16).as(LogicalTypeAnnotation.uuidType()).named("value"))
                        .named("key_value"))
                .named("m");

        ReadConversion conversion = ParquetReadConversions.forColumn(columnType, map).orElseThrow();
        assertThat(conversion.readType()).isEqualTo(new MapType(VARCHAR, VARBINARY, TYPE_OPERATORS));

        Block read = build(conversion.readType(), 2, builder -> {
            MapBlockBuilder maps = (MapBlockBuilder) builder;
            maps.buildEntry((keys, values) -> {
                VARCHAR.writeSlice(keys, utf8Slice("k"));
                VARBINARY.writeSlice(values, wrappedBuffer(uuidBytes(SAMPLE_UUID)));
            });
            maps.appendNull();
        });
        Block converted = conversion.toColumnType().apply(read);
        assertThat(objectValues(columnType, converted)).containsExactly(Map.of("k", SAMPLE_UUID.toString()), null);
    }

    @Test
    void testTwoLevelListOfUuids()
    {
        // parquet-avro's default (write-old-list-structure) list: the repeated field named "array" is the element
        ArrayType columnType = new ArrayType(UuidType.UUID);
        org.apache.parquet.schema.Type list = Types.optionalGroup().as(listType())
                .addField(Types.primitive(BINARY, REPEATED).as(stringType()).named("array"))
                .named("ids");

        ReadConversion conversion = ParquetReadConversions.forColumn(columnType, list).orElseThrow();
        assertThat(conversion.readType()).isEqualTo(new ArrayType(VARBINARY));

        Block read = build(conversion.readType(), 2, builder -> {
            ((ArrayBlockBuilder) builder).buildEntry(elements -> VARBINARY.writeSlice(elements, utf8Slice(SAMPLE_UUID.toString())));
            builder.appendNull();
        });
        Block converted = conversion.toColumnType().apply(read);
        assertThat(objectValues(columnType, converted)).containsExactly(List.of(SAMPLE_UUID.toString()), null);
    }

    @Test
    void testMisstatedTimestampsReadInTheTableUnit()
    {
        long epochMillis = 1_700_000_000_123L;
        org.apache.parquet.schema.Type microsUtc = Types.primitive(INT64, OPTIONAL).as(timestampType(true, TimeUnit.MICROS)).named("ts");
        org.apache.parquet.schema.Type bareLong = Types.primitive(INT64, OPTIONAL).named("ts");
        HoodieSchema millisField = HoodieSchema.createNullable(HoodieSchema.createTimestampMillis());

        // A field the table declares millis whose file says MICROS holds millis (#14161)
        ReadConversion utcMillis = ParquetReadConversions.forColumn(TIMESTAMP_TZ_MILLIS, microsUtc, millisField).orElseThrow();
        assertThat(utcMillis.readType()).isEqualTo(BIGINT);
        Block converted = utcMillis.toColumnType().apply(bigints(epochMillis, null));
        assertThat(TIMESTAMP_TZ_MILLIS.getLong(converted, 0)).isEqualTo(packDateTimeWithZone(epochMillis, UTC_KEY));
        assertThat(converted.isNull(1)).isTrue();

        // A bare long under a local timestamp holds the table's unit; Trino holds a short timestamp as micros
        Block localMillis = ParquetReadConversions.forColumn(TIMESTAMP_MILLIS, bareLong, HoodieSchema.createLocalTimestampMillis()).orElseThrow()
                .toColumnType().apply(bigints(epochMillis));
        assertThat(TIMESTAMP_MILLIS.getLong(localMillis, 0)).isEqualTo(epochMillis * 1000);
        Block localMicros = ParquetReadConversions.forColumn(TIMESTAMP_MICROS, bareLong, HoodieSchema.createLocalTimestampMicros()).orElseThrow()
                .toColumnType().apply(bigints(epochMillis * 1000 + 456));
        assertThat(TIMESTAMP_MICROS.getLong(localMicros, 0)).isEqualTo(epochMillis * 1000 + 456);

        // A bare long under a UTC timestamp: a long column promoted under timestamp.logical.type.overrides (#19384)
        Block promotedMillis = ParquetReadConversions.forColumn(TIMESTAMP_TZ_MILLIS, bareLong, HoodieSchema.createTimestampMillis()).orElseThrow()
                .toColumnType().apply(bigints(epochMillis));
        assertThat(TIMESTAMP_TZ_MILLIS.getLong(promotedMillis, 0)).isEqualTo(packDateTimeWithZone(epochMillis, UTC_KEY));
        Block promotedMicros = ParquetReadConversions.forColumn(TIMESTAMP_TZ_MICROS, bareLong, HoodieSchema.createTimestampMicros()).orElseThrow()
                .toColumnType().apply(bigints(epochMillis * 1000 + 456));
        assertThat(TIMESTAMP_TZ_MICROS.getObject(promotedMicros, 0)).isEqualTo(LongTimestampWithTimeZone.fromEpochMillisAndFraction(epochMillis, 456_000_000, UTC_KEY));

        // Nested, as a list element and as a row field, the field matched case-insensitively
        org.apache.parquet.schema.Type list = Types.optionalGroup().as(listType())
                .addField(Types.repeatedGroup().addField(microsUtc).named("list"))
                .named("tss");
        ReadConversion listConversion = ParquetReadConversions.forColumn(new ArrayType(TIMESTAMP_TZ_MILLIS), list, HoodieSchema.createArray(millisField)).orElseThrow();
        assertThat(listConversion.readType()).isEqualTo(new ArrayType(BIGINT));
        Block readList = build(listConversion.readType(), 1, builder -> ((ArrayBlockBuilder) builder).buildEntry(elements -> BIGINT.writeLong(elements, epochMillis)));
        Block convertedElements = ColumnarArray.toColumnarArray(listConversion.toColumnType().apply(readList)).getElementsBlock();
        assertThat(TIMESTAMP_TZ_MILLIS.getLong(convertedElements, 0)).isEqualTo(packDateTimeWithZone(epochMillis, UTC_KEY));
        RowType rowType = RowType.from(List.of(RowType.field("ts", TIMESTAMP_TZ_MILLIS)));
        HoodieSchema record = HoodieSchema.createRecord("r", null, null, List.of(HoodieSchemaField.of("TS", millisField, null, HoodieSchema.NULL_VALUE)));
        ReadConversion rowConversion = ParquetReadConversions.forColumn(rowType, Types.optionalGroup().addField(microsUtc).named("r"), record).orElseThrow();
        assertThat(rowConversion.readType()).isEqualTo(RowType.from(List.of(RowType.field("ts", BIGINT))));
        Block readRow = build(rowConversion.readType(), 1, builder -> ((RowBlockBuilder) builder).buildEntry(fields -> BIGINT.writeLong(fields.getFirst(), epochMillis)));
        Block convertedField = RowBlock.getRowFieldsFromBlock(rowConversion.toColumnType().apply(readRow)).getFirst();
        assertThat(TIMESTAMP_TZ_MILLIS.getLong(convertedField, 0)).isEqualTo(packDateTimeWithZone(epochMillis, UTC_KEY));
    }

    @Test
    void testTrustedTimestampAnnotationsNeedNoConversion()
    {
        org.apache.parquet.schema.Type millisUtc = Types.primitive(INT64, OPTIONAL).as(timestampType(true, TimeUnit.MILLIS)).named("ts");
        org.apache.parquet.schema.Type microsUtc = Types.primitive(INT64, OPTIONAL).as(timestampType(true, TimeUnit.MICROS)).named("ts");
        org.apache.parquet.schema.Type bareLong = Types.primitive(INT64, OPTIONAL).named("ts");
        assertThat(ParquetReadConversions.forColumn(TIMESTAMP_TZ_MILLIS, millisUtc, HoodieSchema.createTimestampMillis())).isEmpty();
        assertThat(ParquetReadConversions.forColumn(TIMESTAMP_TZ_MICROS, microsUtc, HoodieSchema.createTimestampMicros())).isEmpty();
        // A millis file under a micros field is a lossless widening the reader handles
        assertThat(ParquetReadConversions.forColumn(TIMESTAMP_TZ_MICROS, millisUtc, HoodieSchema.createTimestampMicros())).isEmpty();
        // A local MICROS under a local millis field holds real micros when the row writer annotated a timestamp_ntz,
        // so the base read follows the parquet readers' SchemaRepair, which leaves it alone
        org.apache.parquet.schema.Type microsLocal = Types.primitive(INT64, OPTIONAL).as(timestampType(false, TimeUnit.MICROS)).named("ts");
        assertThat(ParquetReadConversions.forColumn(TIMESTAMP_MILLIS, microsLocal, HoodieSchema.createLocalTimestampMillis())).isEmpty();
        // Nor a UTC MICROS under a local millis field, or the reverse
        assertThat(ParquetReadConversions.forColumn(TIMESTAMP_MILLIS, microsUtc, HoodieSchema.createLocalTimestampMillis())).isEmpty();
        assertThat(ParquetReadConversions.forColumn(TIMESTAMP_TZ_MILLIS, microsLocal, HoodieSchema.createTimestampMillis())).isEmpty();
        // Without the field's schema the precision alone says nothing: a timestamp(3) read of a micros file is a
        // rounding the reader does itself, as for a metastore-typed column
        assertThat(ParquetReadConversions.forColumn(TIMESTAMP_MILLIS, microsUtc)).isEmpty();
    }

    private static Block bigints(Long... values)
    {
        return build(BIGINT, values.length, builder -> {
            for (Long value : values) {
                if (value == null) {
                    builder.appendNull();
                }
                else {
                    BIGINT.writeLong(builder, value);
                }
            }
        });
    }

    private static void assertVector(Type elementType, org.apache.parquet.schema.Type parquetType, byte[] littleEndianBytes, List<Object> expectedElements)
    {
        ArrayType columnType = new ArrayType(elementType);
        ReadConversion conversion = ParquetReadConversions.forColumn(columnType, parquetType).orElseThrow();
        assertThat(conversion.readType()).isEqualTo(VARBINARY);
        Block converted = conversion.toColumnType().apply(varbinary(littleEndianBytes, null));
        assertThat(objectValues(columnType, converted)).containsExactly(expectedElements, null);
    }

    private static Block varbinary(byte[]... values)
    {
        return build(VARBINARY, values.length, builder -> {
            for (byte[] value : values) {
                if (value == null) {
                    builder.appendNull();
                }
                else {
                    VARBINARY.writeSlice(builder, wrappedBuffer(value));
                }
            }
        });
    }

    private static Block build(Type type, int expectedEntries, Consumer<BlockBuilder> writer)
    {
        BlockBuilder builder = type.createBlockBuilder(null, expectedEntries);
        writer.accept(builder);
        return builder.build();
    }

    private static List<Object> objectValues(Type type, Block block)
    {
        List<Object> values = new ArrayList<>(block.getPositionCount());
        for (int position = 0; position < block.getPositionCount(); position++) {
            values.add(type.getObjectValue(block, position));
        }
        return values;
    }

    private static byte[] uuidBytes(UUID uuid)
    {
        return ByteBuffer.allocate(16).putLong(uuid.getMostSignificantBits()).putLong(uuid.getLeastSignificantBits()).array();
    }
}
