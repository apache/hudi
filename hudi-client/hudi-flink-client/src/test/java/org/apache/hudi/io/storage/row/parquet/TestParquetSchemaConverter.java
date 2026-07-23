/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.io.storage.row.parquet;

import org.apache.hudi.adapter.DataTypeAdapter;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.io.storage.row.HoodieRowDataParquetWriteSupport;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for {@link ParquetSchemaConverter}.
 */
public class TestParquetSchemaConverter {
  private static final RowType ROW_TYPE =
      RowType.of(
          new VarCharType(VarCharType.MAX_LENGTH),
          new BooleanType(),
          new TinyIntType(),
          new SmallIntType(),
          new IntType(),
          new BigIntType(),
          new FloatType(),
          new DoubleType(),
          new TimestampType(6),
          new DecimalType(5, 0),
          new ArrayType(new VarCharType(VarCharType.MAX_LENGTH)),
          new ArrayType(new BooleanType()),
          new ArrayType(new TinyIntType()),
          new ArrayType(new SmallIntType()),
          new ArrayType(new IntType()),
          new ArrayType(new BigIntType()),
          new ArrayType(new FloatType()),
          new ArrayType(new DoubleType()),
          new ArrayType(new TimestampType(6)),
          new ArrayType(new DecimalType(5, 0)),
          new MapType(
              new VarCharType(VarCharType.MAX_LENGTH),
              new VarCharType(VarCharType.MAX_LENGTH)),
          new MapType(new VarCharType(VarCharType.MAX_LENGTH), new BooleanType()),
          RowType.of(new VarCharType(VarCharType.MAX_LENGTH), new IntType()));

  private static final RowType NESTED_ARRAY_MAP_TYPE =
      RowType.of(
          new IntType(),
          new ArrayType(true, new IntType()),
          new ArrayType(true, new ArrayType(true, new IntType())),
          new ArrayType(
              true,
              new MapType(
                  true,
                  new VarCharType(VarCharType.MAX_LENGTH),
                  new VarCharType(VarCharType.MAX_LENGTH))),
          new ArrayType(
              true,
              new RowType(
                  Collections.singletonList(
                      new RowType.RowField("a", new IntType())))),
          RowType.of(
              new IntType(),
              new ArrayType(
                  true,
                  new RowType(
                      Arrays.asList(
                          new RowType.RowField(
                              "b",
                              new ArrayType(
                                  true,
                                  new ArrayType(
                                      true, new IntType()))),
                          new RowType.RowField("c", new IntType()))))));

  @Test
  void testParquetFlinkTypeConverting() {
    MessageType messageType = ParquetSchemaConverter.convertToParquetMessageType("flink_schema", ROW_TYPE);
    RowType rowType = ParquetSchemaConverter.convertToRowType(messageType);
    // The parquet schema is derived from the avro-based HoodieSchema, which has no byte/short
    // types, so TINYINT/SMALLINT (including as array elements) are normalized to INT on the round trip.
    assertThat(rowType, is((RowType) normalizeByteShortToInt(ROW_TYPE)));

    messageType = ParquetSchemaConverter.convertToParquetMessageType("flink_schema", NESTED_ARRAY_MAP_TYPE);
    rowType = ParquetSchemaConverter.convertToRowType(messageType);
    assertThat(rowType, is(NESTED_ARRAY_MAP_TYPE));
  }

  @Test
  void testConvertComplexTypes() {
    DataType dataType = DataTypes.ROW(
        DataTypes.FIELD("f_array",
            DataTypes.ARRAY(DataTypes.CHAR(10).notNull())),
        DataTypes.FIELD("f_map",
            DataTypes.MAP(DataTypes.STRING(), DataTypes.VARCHAR(20))),
        DataTypes.FIELD("f_row",
            DataTypes.ROW(
                DataTypes.FIELD("f_row_f0", DataTypes.INT()),
                DataTypes.FIELD("f_row_f1", DataTypes.VARCHAR(10)),
                DataTypes.FIELD("f_row_f2",
                    DataTypes.ROW(
                        DataTypes.FIELD("f_row_f2_f0", DataTypes.INT()),
                        DataTypes.FIELD("f_row_f2_f1", DataTypes.VARCHAR(10)))))));
    org.apache.parquet.schema.MessageType messageType =
        ParquetSchemaConverter.convertToParquetMessageType("converted", (RowType) dataType.getLogicalType());
    assertThat(messageType.getColumns().size(), is(7));
    final String expected = "message converted {\n"
        + "  optional group f_array (LIST) {\n"
        + "    repeated group list {\n"
        + "      required binary element (STRING);\n"
        + "    }\n"
        + "  }\n"
        + "  optional group f_map (MAP) {\n"
        + "    repeated group key_value {\n"
        + "      required binary key (STRING);\n"
        + "      optional binary value (STRING);\n"
        + "    }\n"
        + "  }\n"
        + "  optional group f_row {\n"
        + "    optional int32 f_row_f0;\n"
        + "    optional binary f_row_f1 (STRING);\n"
        + "    optional group f_row_f2 {\n"
        + "      optional int32 f_row_f2_f0;\n"
        + "      optional binary f_row_f2_f1 (STRING);\n"
        + "    }\n"
        + "  }\n"
        + "}\n";
    assertThat(messageType.toString(), is(expected));
  }

  @Test
  void testConvertNestedComplexTypes() {
    DataType dataType = DataTypes.ROW(
        DataTypes.FIELD("f_array",
            DataTypes.ARRAY(DataTypes.ROW(
                DataTypes.FIELD("f_array_f0", DataTypes.INT()),
                DataTypes.FIELD("f_array_f1", DataTypes.VARCHAR(10).notNull()),
                DataTypes.FIELD("f_array_f3", DataTypes.ARRAY(DataTypes.CHAR(10).notNull()))).notNull())),
        DataTypes.FIELD("f_map",
            DataTypes.MAP(DataTypes.STRING(), DataTypes.ROW(
                DataTypes.FIELD("f_map_f0", DataTypes.INT()),
                DataTypes.FIELD("f_map_f1", DataTypes.VARCHAR(10))).notNull())));

    org.apache.parquet.schema.MessageType messageType =
        ParquetSchemaConverter.convertToParquetMessageType("converted", (RowType) dataType.getLogicalType());

    assertThat(messageType.getColumns().size(), is(6));
    final String expected = "message converted {\n"
        + "  optional group f_array (LIST) {\n"
        + "    repeated group list {\n"
        + "      required group element {\n"
        + "        optional int32 f_array_f0;\n"
        + "        required binary f_array_f1 (STRING);\n"
        + "        optional group f_array_f3 (LIST) {\n"
        + "          repeated group list {\n"
        + "            required binary element (STRING);\n"
        + "          }\n"
        + "        }\n"
        + "      }\n"
        + "    }\n"
        + "  }\n"
        + "  optional group f_map (MAP) {\n"
        + "    repeated group key_value {\n"
        + "      required binary key (STRING);\n"
        + "      required group value {\n"
        + "        optional int32 f_map_f0;\n"
        + "        optional binary f_map_f1 (STRING);\n"
        + "      }\n"
        + "    }\n"
        + "  }\n"
        + "}\n";
    assertThat(messageType.toString(), is(expected));
  }

  @Test
  void testConvertVectorColumnsWithHoodieSchema() {
    HoodieSchema hoodieSchema = HoodieSchema.createRecord(
        "vector_record",
        null,
        null,
        Arrays.asList(
            HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT)),
            HoodieSchemaField.of("embedding", HoodieSchema.createVector(2)),
            HoodieSchemaField.of("features", HoodieSchema.createVector(2, HoodieSchema.Vector.VectorElementType.DOUBLE))));
    MessageType messageType = ParquetSchemaConverter.convertToParquetMessageType("converted", hoodieSchema);

    PrimitiveType embeddingType = messageType.getType("embedding").asPrimitiveType();
    assertEquals(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, embeddingType.getPrimitiveTypeName());
    assertEquals(8, embeddingType.getTypeLength());
    PrimitiveType featuresType = messageType.getType("features").asPrimitiveType();
    assertEquals(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, featuresType.getPrimitiveTypeName());
    assertEquals(16, featuresType.getTypeLength());
  }

  @Test
  void testUnannotatedFixedLenByteArrayConvertsToBytes() {
    MessageType messageType = new MessageType(
        "test",
        Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.REQUIRED).named("id"),
        Types.primitive(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, Type.Repetition.OPTIONAL)
            .length(8)
            .named("embedding"));

    RowType rowType = ParquetSchemaConverter.convertToRowType(messageType);

    assertEquals(DataTypes.BYTES().getLogicalType(), rowType.getTypeAt(1));
  }

  @Test
  void testVectorFooterMetadataComesFromHoodieSchema() {
    HoodieSchema hoodieSchema = HoodieSchema.createRecord(
        "vector_record",
        null,
        null,
        Arrays.asList(
            HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT)),
            HoodieSchemaField.of("embedding", HoodieSchema.createVector(2))));

    HoodieRowDataParquetWriteSupport writeSupport =
        new HoodieRowDataParquetWriteSupport(new Configuration(), hoodieSchema, null);
    Map<String, String> metadata = writeSupport.finalizeWrite().getExtraMetaData();

    assertEquals("embedding:VECTOR(2)", metadata.get(HoodieSchema.VECTOR_COLUMNS_METADATA_KEY));
  }

  @Test
  void testConvertTimestampTypes() {
    DataType dataType = DataTypes.ROW(
        DataTypes.FIELD("ts_3", DataTypes.TIMESTAMP(3)),
        DataTypes.FIELD("ts_6", DataTypes.TIMESTAMP(6)));
    org.apache.parquet.schema.MessageType messageType =
        ParquetSchemaConverter.convertToParquetMessageType("converted", (RowType) dataType.getLogicalType());
    assertThat(messageType.getColumns().size(), is(2));
    final String expected = "message converted {\n"
        + "  optional int64 ts_3 (TIMESTAMP(MILLIS,true));\n"
        + "  optional int64 ts_6 (TIMESTAMP(MICROS,true));\n"
        + "}\n";
    assertThat(messageType.toString(), is(expected));
  }

  @Test
  void testConvertAnnotatedPrimitiveParquetTypes() {
    MessageType parquet = new MessageType("annotated",
        Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.REQUIRED)
            .as(LogicalTypeAnnotation.decimalType(2, 8)).named("decimal32"),
        Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.OPTIONAL)
            .as(LogicalTypeAnnotation.intType(8, true)).named("tiny"),
        Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.OPTIONAL)
            .as(LogicalTypeAnnotation.intType(16, true)).named("small"),
        Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.OPTIONAL)
            .as(LogicalTypeAnnotation.intType(32, true)).named("number"),
        Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.OPTIONAL)
            .as(LogicalTypeAnnotation.dateType()).named("day"),
        Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.OPTIONAL)
            .as(LogicalTypeAnnotation.timeType(true, LogicalTypeAnnotation.TimeUnit.MILLIS)).named("time"),
        Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.OPTIONAL)
            .as(LogicalTypeAnnotation.decimalType(3, 12)).named("decimal64"),
        Types.primitive(PrimitiveType.PrimitiveTypeName.INT96, Type.Repetition.OPTIONAL)
            .named("timestamp96"),
        Types.primitive(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, Type.Repetition.OPTIONAL)
            .length(8).as(LogicalTypeAnnotation.decimalType(4, 16)).named("decimal_fixed"));

    RowType converted = ParquetSchemaConverter.convertToRowType(parquet);
    assertEquals("DECIMAL(8, 2) NOT NULL", converted.getTypeAt(0).asSummaryString());
    assertEquals("TINYINT", converted.getTypeAt(1).asSummaryString());
    assertEquals("SMALLINT", converted.getTypeAt(2).asSummaryString());
    assertEquals("INT", converted.getTypeAt(3).asSummaryString());
    assertEquals("DATE", converted.getTypeAt(4).asSummaryString());
    assertEquals("TIME(0)", converted.getTypeAt(5).asSummaryString());
    assertEquals("DECIMAL(12, 3)", converted.getTypeAt(6).asSummaryString());
    assertEquals("TIMESTAMP(9)", converted.getTypeAt(7).asSummaryString());
    assertEquals("DECIMAL(16, 4)", converted.getTypeAt(8).asSummaryString());
  }

  @Test
  void testConvertLocalZonedTimestampToParquet() {
    RowType rowType = (RowType) DataTypes.ROW(
        DataTypes.FIELD("local_millis", DataTypes.TIMESTAMP_LTZ(3)),
        DataTypes.FIELD("local_micros", DataTypes.TIMESTAMP_LTZ(6))).notNull().getLogicalType();
    MessageType parquet = ParquetSchemaConverter.convertToParquetMessageType("local", rowType);
    assertEquals(LogicalTypeAnnotation.timestampType(
            false, LogicalTypeAnnotation.TimeUnit.MILLIS),
        parquet.getType("local_millis").getLogicalTypeAnnotation());
    assertEquals(LogicalTypeAnnotation.timestampType(
            false, LogicalTypeAnnotation.TimeUnit.MICROS),
        parquet.getType("local_micros").getLogicalTypeAnnotation());
  }

  @Test
  void testConvertBinaryDateAndTimeToParquet() {
    RowType rowType = (RowType) DataTypes.ROW(
        DataTypes.FIELD("payload", DataTypes.BYTES()),
        DataTypes.FIELD("day", DataTypes.DATE()),
        DataTypes.FIELD("time", DataTypes.TIME(3))).notNull().getLogicalType();
    MessageType parquet = ParquetSchemaConverter.convertToParquetMessageType("primitives", rowType);
    assertEquals(PrimitiveType.PrimitiveTypeName.BINARY,
        parquet.getType("payload").asPrimitiveType().getPrimitiveTypeName());
    assertEquals(LogicalTypeAnnotation.dateType(),
        parquet.getType("day").getLogicalTypeAnnotation());
    assertEquals(LogicalTypeAnnotation.timeType(true, LogicalTypeAnnotation.TimeUnit.MILLIS),
        parquet.getType("time").getLogicalTypeAnnotation());
  }

  /**
   * A Parquet group with metadata + value binary fields but NO VARIANT annotation must be
   * treated as a plain ROW. Only the Parquet {@code VARIANT} annotation triggers variant
   * detection in this converter; unannotated groups are never guessed as variant.
   */
  @Test
  void testVariantPhysicalLayoutTreatedAsRow() {
    MessageType variantParquet = new MessageType(
        "test",
        Types.primitive(PrimitiveType.PrimitiveTypeName.INT32,
            Type.Repetition.REQUIRED).named("id"),
        Types.buildGroup(Type.Repetition.REQUIRED)
            .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY,
                Type.Repetition.REQUIRED).named("metadata"))
            .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY,
                Type.Repetition.REQUIRED).named("value"))
            .named("data"));

    RowType rowType = ParquetSchemaConverter.convertToRowType(variantParquet);
    assertEquals(2, rowType.getFieldCount());
    assertEquals("ROW", rowType.getTypeAt(1).getTypeRoot().name());
  }

  /**
   * Unannotated group with metadata + value + typed_value (3 fields) is treated as a generic
   * ROW when no annotation or schema hint is present.
   */
  @Test
  void testUnannotatedShreddedGroupTreatedAsRow() {
    MessageType shreddedNoAnnotation = new MessageType(
        "test",
        Types.primitive(PrimitiveType.PrimitiveTypeName.INT32,
            Type.Repetition.REQUIRED).named("id"),
        Types.buildGroup(Type.Repetition.REQUIRED)
            .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY,
                Type.Repetition.REQUIRED).named("metadata"))
            .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY,
                Type.Repetition.REQUIRED).named("value"))
            .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.INT32,
                Type.Repetition.OPTIONAL).named("typed_value"))
            .named("data"));

    RowType rowType = ParquetSchemaConverter.convertToRowType(shreddedNoAnnotation);
    assertEquals(2, rowType.getFieldCount());
    assertEquals("ROW", rowType.getTypeAt(1).getTypeRoot().name());
  }

  /**
   * On Flink 2.1+ with parquet 1.16.0+, converting a RowType containing a Variant column to a
   * Parquet MessageType should produce a group with the VARIANT annotation and required binary
   * {@code metadata} and {@code value} fields.
   * On pre-2.1 Flink this test is skipped since VariantType does not exist.
   * On parquet < 1.16.0 the write is expected to fail (annotation unavailable).
   */
  @Test
  void testVariantWritePathProducesCorrectLayout() {
    LogicalType variantType;
    try {
      variantType = DataTypeAdapter.createVariantType().getLogicalType();
    } catch (UnsupportedOperationException e) {
      // Pre-2.1 Flink: VariantType doesn't exist, skip
      return;
    }

    RowType rowType = RowType.of(
        new LogicalType[]{new IntType(), variantType},
        new String[]{"id", "data"});

    if (!DataTypeAdapter.variantParquetAnnotation().isPresent()) {
      // parquet < 1.16.0: write must fail because annotation is unavailable
      UnsupportedOperationException ex = org.junit.jupiter.api.Assertions.assertThrows(
          UnsupportedOperationException.class,
          () -> ParquetSchemaConverter.convertToParquetMessageType("test", rowType));
      assertTrue(ex.getMessage().contains("parquet-java 1.16.0+"),
          "Error message should mention parquet version requirement");
      return;
    }

    // parquet 1.16.0+: write succeeds with annotation
    MessageType messageType = ParquetSchemaConverter.convertToParquetMessageType("test", rowType);
    assertEquals(2, messageType.getFieldCount());

    Type variantField = messageType.getType("data");
    assertTrue(variantField instanceof GroupType, "Variant column should be a Parquet group");
    GroupType variantGroup = (GroupType) variantField;
    assertEquals(2, variantGroup.getFieldCount());
    assertEquals(HoodieSchema.Variant.VARIANT_METADATA_FIELD, variantGroup.getType(0).getName());
    assertEquals(HoodieSchema.Variant.VARIANT_VALUE_FIELD, variantGroup.getType(1).getName());
    assertTrue(variantGroup.getType(0).isPrimitive());
    assertTrue(variantGroup.getType(1).isPrimitive());
    assertEquals(PrimitiveType.PrimitiveTypeName.BINARY,
        variantGroup.getType(0).asPrimitiveType().getPrimitiveTypeName());
    assertEquals(PrimitiveType.PrimitiveTypeName.BINARY,
        variantGroup.getType(1).asPrimitiveType().getPrimitiveTypeName());
    assertNotNull(variantGroup.getLogicalTypeAnnotation(),
        "Variant group must carry the VARIANT annotation");
  }

  /**
   * Verifies that writing a Variant column fails with a clear error when parquet-java on the
   * classpath does not support the VARIANT annotation (< 1.16.0). On pre-2.1 Flink the adapter
   * throws directly; on Flink 2.1+ with parquet < 1.16.0 the write path throws.
   */
  @Test
  void testVariantWriteFailsWithoutAnnotation() {
    Option<LogicalTypeAnnotation> annotationOpt;
    try {
      annotationOpt = DataTypeAdapter.variantParquetAnnotation();
    } catch (UnsupportedOperationException e) {
      // Pre-2.1 Flink: expected to throw from the adapter
      assertTrue(e.getMessage().contains("VARIANT type is only supported in Flink 2.1+"));
      return;
    }

    if (annotationOpt.isPresent()) {
      // parquet 1.16.0+: annotation is available, write succeeds — nothing to test here
      return;
    }

    // Flink 2.1 + parquet < 1.16.0: annotation is null, write must fail
    LogicalType variantType = DataTypeAdapter.createVariantType().getLogicalType();
    RowType rowType = RowType.of(
        new LogicalType[]{new IntType(), variantType},
        new String[]{"id", "data"});

    UnsupportedOperationException ex = org.junit.jupiter.api.Assertions.assertThrows(
        UnsupportedOperationException.class,
        () -> ParquetSchemaConverter.convertToParquetMessageType("test", rowType));
    assertTrue(ex.getMessage().contains("parquet-java 1.16.0+"),
        "Error message should mention the parquet version requirement");
    assertTrue(ex.getMessage().contains("VARIANT"),
        "Error message should mention VARIANT");
  }

  /**
   * Replaces TINYINT/SMALLINT with INT recursively, mirroring the lossy conversion that happens
   * when a parquet schema is built from the avro-based {@link HoodieSchema}.
   * The problem is tracked here: <a href="https://github.com/apache/hudi/issues/18974">Issue#18974</a>.
   */
  private static LogicalType normalizeByteShortToInt(LogicalType type) {
    if (type instanceof TinyIntType || type instanceof SmallIntType) {
      return new IntType();
    } else if (type instanceof ArrayType) {
      return new ArrayType(normalizeByteShortToInt(((ArrayType) type).getElementType()));
    } else if (type instanceof RowType) {
      RowType row = (RowType) type;
      LogicalType[] children = new LogicalType[row.getFieldCount()];
      for (int i = 0; i < children.length; i++) {
        children[i] = normalizeByteShortToInt(row.getTypeAt(i));
      }
      return RowType.of(children);
    }
    return type;
  }
}
