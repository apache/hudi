/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.  
 */

package org.apache.hudi.source.prune;

import org.apache.hudi.source.stats.ColumnStats;
import org.apache.hudi.utils.TestData;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class TestColumnStatsProbe {

  @ParameterizedTest
  @MethodSource("testTypes")
  void testConvertColumnStats(DataType dataType, Object minValue, Object maxValue) {
    DataType rowDataType = getDataRowDataType(dataType);
    DataType indexRowDataType = getIndexRowDataType(dataType);

    Map<String, ColumnStats> stats1 = ColumnStatsProbe.convertColumnStats(
        getIndexRow(indexRowDataType, minValue, maxValue),
        getDataFields(rowDataType)
    );

    assertEquals(minValue, stats1.get("field").getMinVal());
    assertEquals(maxValue, stats1.get("field").getMaxVal());
  }

  @ParameterizedTest
  @MethodSource("columnStatsCompleteness")
  void testProbeWithIncompleteColumnStats(Integer minValue, Integer maxValue, Long nullCount, boolean expected) {
    CallExpression filter = CallExpression.permanent(BuiltInFunctionDefinitions.EQUALS,
        Arrays.asList(new FieldReferenceExpression("field", DataTypes.INT(), 0, 0), new ValueLiteralExpression(3)),
        DataTypes.BOOLEAN());
    ColumnStatsProbe probe = ColumnStatsProbe.newInstance(Collections.singletonList(filter));
    RowData indexRow = GenericRowData.of(StringData.fromString("f1"), 10L, minValue, maxValue, nullCount);
    RowType.RowField[] fields = getDataFields(getDataRowDataType(DataTypes.INT()));
    assertEquals(expected, probe.test(indexRow, fields));
  }

  static Stream<Arguments> columnStatsCompleteness() {
    return Stream.of(
        arguments(null, null, null, true), // Missing statistics must retain the file.
        arguments(null, null, 10L, false), // Known all-null data cannot match equality.
        arguments(1, 5, 0L, true),
        arguments(4, 5, 0L, false)
    );
  }

  static Stream<Arguments> testTypes() {
    return Stream.of(
        arguments(DataTypes.INT(), 1, 5),
        arguments(DataTypes.BIGINT(), 3L, 4L)
    );
  }

  private DataType getIndexRowDataType(DataType dataType) {
    return DataTypes.ROW(
        DataTypes.FIELD("file_name", DataTypes.STRING()),
        DataTypes.FIELD("value_cnt", DataTypes.BIGINT()),
        DataTypes.FIELD("field_min", dataType),
        DataTypes.FIELD("field_max", dataType),
        DataTypes.FIELD("field_null_cnt", DataTypes.BIGINT())
    ).notNull();
  }

  private RowData getIndexRow(DataType indexRowDataType, Object minValue, Object maxValue) {
    return TestData.insertRow((RowType) indexRowDataType.getLogicalType(),
        StringData.fromString("f1"), 10L, minValue, maxValue, 2L);
  }

  private DataType getDataRowDataType(DataType dataType) {
    return DataTypes.ROW(
        DataTypes.FIELD("field", dataType)
    ).notNull();
  }

  private RowType.RowField[] getDataFields(DataType rowDataType) {
    return new RowType.RowField[] {
        ((RowType) rowDataType.getLogicalType()).getFields().get(0)
    };
  }
}
