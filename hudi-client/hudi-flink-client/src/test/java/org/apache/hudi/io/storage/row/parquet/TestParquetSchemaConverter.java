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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.parquet.schema.MessageType;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

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
            DataTypes.ARRAY(DataTypes.CHAR(10))),
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
        + "      optional binary element (STRING);\n"
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
}
