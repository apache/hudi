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

import io.trino.spi.TrinoException;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.RowType.field;
import static io.trino.spi.type.TimeType.TIME_MICROS;
import static io.trino.spi.type.TimeType.TIME_MILLIS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.UuidType.UUID;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestTrinoTypeMapper
{
    private static final TypeOperators TYPE_OPERATORS = new TypeOperators();
    private static final TrinoTypeMapper MAPPER = new TrinoTypeMapper(TYPE_OPERATORS);

    @ParameterizedTest
    @MethodSource("mappings")
    void testToTypeSystem(HoodieSchema schema, Type expected)
    {
        assertThat(MAPPER.toTypeSystem(schema)).isEqualTo(expected);
        // Trino carries nullability on the column, not in the type
        assertThat(MAPPER.toTypeSystem(HoodieSchema.createNullable(schema))).isEqualTo(expected);
    }

    static Stream<Arguments> mappings()
    {
        HoodieSchema struct = HoodieSchema.createRecord("point", null, null, List.of(
                HoodieSchemaField.of("name", HoodieSchema.create(HoodieSchemaType.STRING), null, null),
                HoodieSchemaField.of("at", HoodieSchema.createNullable(HoodieSchema.createLocalTimestampMicros()), null, HoodieSchema.NULL_VALUE)));
        return Stream.of(
                Arguments.of(HoodieSchema.create(HoodieSchemaType.BOOLEAN), BOOLEAN),
                Arguments.of(HoodieSchema.create(HoodieSchemaType.INT), INTEGER),
                Arguments.of(HoodieSchema.create(HoodieSchemaType.LONG), BIGINT),
                Arguments.of(HoodieSchema.create(HoodieSchemaType.FLOAT), REAL),
                Arguments.of(HoodieSchema.create(HoodieSchemaType.DOUBLE), DOUBLE),
                Arguments.of(HoodieSchema.create(HoodieSchemaType.STRING), VARCHAR),
                Arguments.of(HoodieSchema.create(HoodieSchemaType.BYTES), VARBINARY),
                Arguments.of(HoodieSchema.createEnum("color", null, null, List.of("RED", "GREEN")), VARCHAR),
                Arguments.of(HoodieSchema.createFixed("md5", null, null, 16), VARBINARY),
                Arguments.of(HoodieSchema.createUUID(), UUID),
                Arguments.of(HoodieSchema.createDate(), DATE),
                Arguments.of(HoodieSchema.createDecimal(10, 2), createDecimalType(10, 2)),
                Arguments.of(HoodieSchema.createDecimal("amount", null, null, 20, 4, 9), createDecimalType(20, 4)),
                Arguments.of(HoodieSchema.createTimeMillis(), TIME_MILLIS),
                Arguments.of(HoodieSchema.createTimeMicros(), TIME_MICROS),
                Arguments.of(HoodieSchema.createTimestampMillis(), TIMESTAMP_TZ_MILLIS),
                Arguments.of(HoodieSchema.createTimestampMicros(), TIMESTAMP_TZ_MICROS),
                Arguments.of(HoodieSchema.createLocalTimestampMillis(), TIMESTAMP_MILLIS),
                Arguments.of(HoodieSchema.createLocalTimestampMicros(), TIMESTAMP_MICROS),
                Arguments.of(HoodieSchema.createArray(HoodieSchema.createNullable(HoodieSchema.create(HoodieSchemaType.LONG))), new ArrayType(BIGINT)),
                Arguments.of(HoodieSchema.createMap(HoodieSchema.createDate()), new MapType(VARCHAR, DATE, TYPE_OPERATORS)),
                Arguments.of(struct, RowType.from(List.of(field("name", VARCHAR), field("at", TIMESTAMP_MICROS)))),
                Arguments.of(HoodieSchema.createVector(4), new ArrayType(REAL)),
                Arguments.of(HoodieSchema.createVector(4, HoodieSchema.Vector.VectorElementType.DOUBLE), new ArrayType(DOUBLE)),
                Arguments.of(HoodieSchema.createVector(4, HoodieSchema.Vector.VectorElementType.INT8), new ArrayType(TINYINT)),
                // BLOB's reference is a nested record, so this also covers a row inside a row
                Arguments.of(HoodieSchema.createBlob(), RowType.from(List.of(
                        field("type", VARCHAR),
                        field("data", VARBINARY),
                        field("reference", RowType.from(List.of(
                                field("external_path", VARCHAR),
                                field("offset", BIGINT),
                                field("length", BIGINT),
                                field("managed", BOOLEAN))))))),
                Arguments.of(HoodieSchema.createVariant(), RowType.from(List.of(field("metadata", VARBINARY), field("value", VARBINARY)))));
    }

    @Test
    void testNullOnlyRecordFieldsAreLeftOut()
    {
        // Hive sync leaves a "null" field out of the struct at any depth (#20354), and the Parquet writer does not write it
        HoodieSchema nested = HoodieSchema.createRecord("inner", null, null, List.of(
                HoodieSchemaField.of("b", HoodieSchema.create(HoodieSchemaType.NULL)),
                HoodieSchemaField.of("c", HoodieSchema.create(HoodieSchemaType.INT))));
        HoodieSchema record = HoodieSchema.createRecord("r", null, null, List.of(
                HoodieSchemaField.of("a", HoodieSchema.create(HoodieSchemaType.STRING)),
                HoodieSchemaField.of("b", HoodieSchema.create(HoodieSchemaType.NULL)),
                HoodieSchemaField.of("inner", HoodieSchema.createNullable(nested))));
        assertThat(MAPPER.toTypeSystem(record)).isEqualTo(RowType.from(List.of(
                field("a", VARCHAR),
                field("inner", RowType.from(List.of(field("c", INTEGER)))))));

        // A row needs a field, so a record of nothing but null fields still has no Trino type
        HoodieSchema allNull = HoodieSchema.createRecord("n", null, null, List.of(HoodieSchemaField.of("b", HoodieSchema.create(HoodieSchemaType.NULL))));
        assertThatThrownBy(() -> MAPPER.toTypeSystem(allNull))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("every field of this record can only hold null");
    }

    @Test
    void testTypesWithoutATrinoType()
    {
        assertThatThrownBy(() -> MAPPER.toTypeSystem(HoodieSchema.createDecimal(39, 2)))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Trino decimals stop at precision 38");
        assertThatThrownBy(() -> MAPPER.toTypeSystem(HoodieSchema.createUnion(HoodieSchema.create(HoodieSchemaType.INT), HoodieSchema.create(HoodieSchemaType.STRING))))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Trino has no union type");
        assertThatThrownBy(() -> MAPPER.toTypeSystem(HoodieSchema.create(HoodieSchemaType.NULL)))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Trino has no null type");
        assertThatThrownBy(() -> MAPPER.toTypeSystem(HoodieSchema.createUnion(HoodieSchema.create(HoodieSchemaType.NULL))))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Trino has no union type");
    }
}
