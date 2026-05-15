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
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

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
          new MapType(new IntType(), new BooleanType()),
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
    assertThat(rowType, is(ROW_TYPE));

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
            DataTypes.MAP(DataTypes.INT(), DataTypes.VARCHAR(20))),
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
        + "      required int32 key;\n"
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
            DataTypes.MAP(DataTypes.INT(), DataTypes.ROW(
                DataTypes.FIELD("f_map_f0", DataTypes.INT()),
                DataTypes.FIELD("f_map_f1", DataTypes.VARCHAR(10))).notNull())));

    org.apache.parquet.schema.MessageType messageType = ParquetSchemaConverter.convertToParquetMessageType("converted", (RowType) dataType.getLogicalType());

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
        + "      required int32 key;\n"
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
  void testConvertTimestampTypes() {
    DataType dataType = DataTypes.ROW(
        DataTypes.FIELD("ts_3", DataTypes.TIMESTAMP(3)),
        DataTypes.FIELD("ts_6", DataTypes.TIMESTAMP(6)),
        DataTypes.FIELD("ts_9", DataTypes.TIMESTAMP(9)));
    org.apache.parquet.schema.MessageType messageType =
        ParquetSchemaConverter.convertToParquetMessageType("converted", (RowType) dataType.getLogicalType());
    assertThat(messageType.getColumns().size(), is(3));
    final String expected = "message converted {\n"
        + "  optional int64 ts_3 (TIMESTAMP(MILLIS,true));\n"
        + "  optional int64 ts_6 (TIMESTAMP(MICROS,true));\n"
        + "  optional int96 ts_9;\n"
        + "}\n";
    assertThat(messageType.toString(), is(expected));
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
   * On Flink 2.1+, converting a RowType containing a Variant column to a Parquet MessageType
   * should produce a group with the VARIANT annotation and required binary {@code metadata}
   * and {@code value} fields.
   * On pre-2.1 Flink this test is skipped since VariantType does not exist.
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
   * Verifies that on pre-2.1 Flink, calling variantParquetAnnotation() throws
   * UnsupportedOperationException with a clear message.
   */
  @Test
  void testVariantAnnotationThrowsOnPreFlink21() {
    try {
      DataTypeAdapter.variantParquetAnnotation();
    } catch (UnsupportedOperationException e) {
      // Pre-2.1 Flink: expected to throw from the adapter
      assertTrue(e.getMessage().contains("VARIANT type is only supported in Flink 2.1+"));
      return;
    }
    // On Flink 2.1+ this method succeeds — nothing else to verify here
  }

}
