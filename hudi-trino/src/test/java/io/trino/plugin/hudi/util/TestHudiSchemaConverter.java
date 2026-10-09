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

import com.google.common.collect.ImmutableList;
import io.trino.hive.formats.avro.NativeLogicalTypesAvroTypeBlockHandler;
import io.trino.plugin.hive.util.HiveTypeTranslator;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.UuidType.UUID;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static org.apache.hudi.common.model.HoodieRecord.COMMIT_SEQNO_METADATA_FIELD;
import static org.apache.hudi.common.model.HoodieRecord.COMMIT_TIME_METADATA_FIELD;
import static org.apache.hudi.common.model.HoodieRecord.FILENAME_METADATA_FIELD;
import static org.apache.hudi.common.model.HoodieRecord.PARTITION_PATH_METADATA_FIELD;
import static org.apache.hudi.common.model.HoodieRecord.RECORD_KEY_METADATA_FIELD;
import static org.apache.hudi.common.schema.HoodieSchemaUtils.getRecordQualifiedName;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Pins the Trino type -> Avro decisions documented on {@link HudiSchemaConverter}. These are
 * write-time choices that only surface as a problem much later, when another engine writes into the
 * table, so they are asserted directly rather than inferred from a round trip.
 */
final class TestHudiSchemaConverter
{
    private static final TypeOperators TYPE_OPERATORS = new TypeOperators();

    @Test
    void testMetaFieldsArePrependedInHudiOrder()
    {
        Schema schema = tableSchema(column("id", BIGINT), column("city", VARCHAR));

        assertThat(schema.getFields().stream().map(Schema.Field::name).toList())
                .containsExactly(
                        COMMIT_TIME_METADATA_FIELD,
                        COMMIT_SEQNO_METADATA_FIELD,
                        RECORD_KEY_METADATA_FIELD,
                        PARTITION_PATH_METADATA_FIELD,
                        FILENAME_METADATA_FIELD,
                        "id",
                        "city");
        // Every meta field is a nullable string, matching what hive sync registers for tables
        // created by Spark or Flink.
        for (String metaField : List.of(
                COMMIT_TIME_METADATA_FIELD,
                COMMIT_SEQNO_METADATA_FIELD,
                RECORD_KEY_METADATA_FIELD,
                PARTITION_PATH_METADATA_FIELD,
                FILENAME_METADATA_FIELD)) {
            assertThat(unwrapNullable(schema.getField(metaField).schema()).getType()).isEqualTo(Schema.Type.STRING);
        }
    }

    @Test
    void testNullabilityFollowsColumnMetadata()
    {
        Schema schema = tableSchema(
                ColumnMetadata.builder().setName("nullable_col").setType(BIGINT).setNullable(true).build(),
                ColumnMetadata.builder().setName("required_col").setType(BIGINT).setNullable(false).build());

        assertThat(schema.getField("nullable_col").schema().getType()).isEqualTo(Schema.Type.UNION);
        assertThat(schema.getField("required_col").schema().getType()).isEqualTo(Schema.Type.LONG);
    }

    @Test
    void testPrimitiveMappings()
    {
        assertThat(avroTypeOf(BOOLEAN)).isEqualTo(Schema.Type.BOOLEAN);
        assertThat(avroTypeOf(BIGINT)).isEqualTo(Schema.Type.LONG);
        assertThat(avroTypeOf(REAL)).isEqualTo(Schema.Type.FLOAT);
        assertThat(avroTypeOf(DOUBLE)).isEqualTo(Schema.Type.DOUBLE);
        assertThat(avroTypeOf(VARBINARY)).isEqualTo(Schema.Type.BYTES);
        assertThat(avroTypeOf(VARCHAR)).isEqualTo(Schema.Type.STRING);
    }

    @Test
    void testNarrowIntegersWidenToAvroInt()
    {
        // Avro has no int8/int16. Documented widening: these read back as INTEGER.
        assertThat(avroTypeOf(TINYINT)).isEqualTo(Schema.Type.INT);
        assertThat(avroTypeOf(SMALLINT)).isEqualTo(Schema.Type.INT);
        assertThat(avroTypeOf(INTEGER)).isEqualTo(Schema.Type.INT);
    }

    @Test
    void testBoundedVarcharLosesItsLengthBound()
    {
        // Documented widening: nothing carries the bound once another engine writes.
        assertThat(avroTypeOf(createVarcharType(10))).isEqualTo(Schema.Type.STRING);
    }

    @Test
    void testDateCarriesLogicalType()
    {
        assertThat(logicalTypeOf(DATE)).isEqualTo(LogicalTypes.date());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("unsupportedTypes")
    void testRejectsUnsupportedType(UnsupportedType unsupportedType)
    {
        assertThatThrownBy(() -> tableSchema(unsupportedType.column()))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining(unsupportedType.expectedMessage());
    }

    @Test
    void testEveryAcceptedTypeIsRegisterableInTheMetastore()
    {
        // The guard behind the two rejections above: anything this converter accepts must survive
        // HiveTypeTranslator, since that is what builds the metastore descriptor.
        List<Type> accepted = List.of(
                BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT, REAL, DOUBLE, VARBINARY,
                VARCHAR, createVarcharType(10), DATE,
                DecimalType.createDecimalType(18, 4),
                TimestampType.createTimestampType(3),
                TimestampType.createTimestampType(6),
                new ArrayType(BIGINT),
                new MapType(VARCHAR, BIGINT, TYPE_OPERATORS),
                RowType.rowType(RowType.field("nested", BIGINT)));
        for (Type type : accepted) {
            Schema fieldSchema = fieldSchema(type);
            assertThatCode(() -> HiveTypeTranslator.toHiveType(
                    new NativeLogicalTypesAvroTypeBlockHandler().typeFor(fieldSchema)))
                    .describedAs("%s maps to Avro %s, which must be registerable in the metastore", type, fieldSchema)
                    .doesNotThrowAnyException();
        }
    }

    @Test
    void testDecimalUsesBytesNotFixed()
    {
        // bytes avoids having to invent a unique Avro name per decimal column; the read path
        // decodes both representations.
        assertThat(avroTypeOf(DecimalType.createDecimalType(18, 4))).isEqualTo(Schema.Type.BYTES);
        assertThat(logicalTypeOf(DecimalType.createDecimalType(18, 4))).isEqualTo(LogicalTypes.decimal(18, 4));
    }

    @Test
    void testTimestampPrecisionMapping()
    {
        assertThat(logicalTypeOf(TimestampType.createTimestampType(3))).isEqualTo(LogicalTypes.timestampMillis());
        assertThat(logicalTypeOf(TimestampType.createTimestampType(6))).isEqualTo(LogicalTypes.timestampMicros());
        // Precisions between the two representable ones round up, losslessly.
        assertThat(logicalTypeOf(TimestampType.createTimestampType(0))).isEqualTo(LogicalTypes.timestampMillis());
        assertThat(logicalTypeOf(TimestampType.createTimestampType(4))).isEqualTo(LogicalTypes.timestampMicros());
    }

    @Test
    void testNestedTypesAreAlwaysNullableInside()
    {
        Schema array = fieldSchema(new ArrayType(BIGINT));
        assertThat(array.getElementType().getType()).isEqualTo(Schema.Type.UNION);

        Schema map = fieldSchema(new MapType(VARCHAR, BIGINT, TYPE_OPERATORS));
        assertThat(map.getValueType().getType()).isEqualTo(Schema.Type.UNION);

        Schema row = fieldSchema(RowType.rowType(RowType.field("nested", BIGINT)));
        assertThat(row.getType()).isEqualTo(Schema.Type.RECORD);
        assertThat(row.getField("nested").schema().getType()).isEqualTo(Schema.Type.UNION);
    }

    @Test
    void testNestedRecordNameDoesNotCollideWithTableName()
    {
        Schema tableSchema = HudiSchemaConverter.toTableSchema(
                ImmutableList.of(column("trips", RowType.rowType(RowType.field("id", BIGINT)))),
                "trips").toAvroSchema();
        Schema nestedRecord = unwrapNullable(tableSchema.getField("trips").schema());

        assertThat(nestedRecord.getFullName()).isNotEqualTo(tableSchema.getFullName());
    }

    @Test
    void testTableRecordNameUsesHudiConvention()
    {
        Schema schema = HudiSchemaConverter.toTableSchema(
                ImmutableList.of(column("id", BIGINT)),
                "test-table").toAvroSchema();

        assertThat(schema.getFullName()).isEqualTo(getRecordQualifiedName("test-table"));
    }

    @Test
    void testNestedRecordNamesRemainUniqueWhenSanitizedPathsCollide()
    {
        Type nestedRow = RowType.rowType(RowType.field(
                "b",
                RowType.rowType(RowType.field("id", BIGINT))));
        Schema tableSchema = HudiSchemaConverter.toTableSchema(
                ImmutableList.of(
                        column("a", nestedRow),
                        column("a_b", RowType.rowType(RowType.field("id", BIGINT)))),
                "test_table").toAvroSchema();
        Schema nestedPathRecord = unwrapNullable(
                unwrapNullable(tableSchema.getField("a").schema()).getField("b").schema());
        Schema topLevelPathRecord = unwrapNullable(tableSchema.getField("a_b").schema());

        assertThat(nestedPathRecord.getFullName()).isNotEqualTo(topLevelPathRecord.getFullName());
    }

    private static Stream<UnsupportedType> unsupportedTypes()
    {
        return Stream.of(
                new UnsupportedType("UUID", column("id", UUID), "column 'id' of type uuid"),
                new UnsupportedType("TIME", column("t", TimeType.createTimeType(6)), "no TIME type"),
                new UnsupportedType("CHAR", column("c", CharType.createCharType(5)), "column 'c' of type char(5)"),
                new UnsupportedType("TIMESTAMP WITH TIME ZONE", column("ts", createTimestampWithTimeZoneType(6)), "time zone would be lost"),
                new UnsupportedType("TIMESTAMP precision", column("ts", TimestampType.createTimestampType(9)), "TIMESTAMP(6) or narrower"),
                new UnsupportedType("non-VARCHAR map key", column("m", new MapType(BIGINT, BIGINT, TYPE_OPERATORS)), "Avro map keys are always strings"),
                new UnsupportedType(
                        "nested field",
                        column("outer", RowType.rowType(RowType.field("inner", new ArrayType(CharType.createCharType(3))))),
                        "outer_inner_element"));
    }

    private static ColumnMetadata column(String name, Type type)
    {
        return ColumnMetadata.builder().setName(name).setType(type).build();
    }

    private static Schema tableSchema(ColumnMetadata... columns)
    {
        return HudiSchemaConverter.toTableSchema(ImmutableList.copyOf(columns), "test_table").toAvroSchema();
    }

    /**
     * The Avro schema of a single column, with the nullable union unwrapped.
     */
    private static Schema fieldSchema(Type type)
    {
        return unwrapNullable(tableSchema(column("col", type)).getField("col").schema());
    }

    private static Schema.Type avroTypeOf(Type type)
    {
        return fieldSchema(type).getType();
    }

    private static LogicalType logicalTypeOf(Type type)
    {
        return fieldSchema(type).getLogicalType();
    }

    private record UnsupportedType(String description, ColumnMetadata column, String expectedMessage)
    {
        @Override
        public String toString()
        {
            return description;
        }
    }

    private static Schema unwrapNullable(Schema schema)
    {
        if (schema.getType() != Schema.Type.UNION) {
            return schema;
        }
        return schema.getTypes().stream()
                .filter(candidate -> candidate.getType() != Schema.Type.NULL)
                .reduce((first, second) -> {
                    throw new AssertionError("expected a single non-null branch, got " + schema);
                })
                .orElseThrow(() -> new AssertionError("union has no non-null branch: " + schema));
    }
}
