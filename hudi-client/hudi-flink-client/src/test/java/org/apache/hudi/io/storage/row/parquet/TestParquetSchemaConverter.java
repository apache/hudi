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
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Test cases for {@link ParquetSchemaConverter}.
 */
public class TestParquetSchemaConverter {
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
        + "      optional binary element (UTF8);\n"
        + "    }\n"
        + "  }\n"
        + "  optional group f_map (MAP) {\n"
        + "    repeated group key_value {\n"
        + "      optional int32 key;\n"
        + "      optional binary value (UTF8);\n"
        + "    }\n"
        + "  }\n"
        + "  optional group f_row {\n"
        + "    optional int32 f_row_f0;\n"
        + "    optional binary f_row_f1 (UTF8);\n"
        + "    optional group f_row_f2 {\n"
        + "      optional int32 f_row_f2_f0;\n"
        + "      optional binary f_row_f2_f1 (UTF8);\n"
        + "    }\n"
        + "  }\n"
        + "}\n";
    assertThat(messageType.toString(), is(expected));
  }
}
