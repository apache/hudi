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

package org.apache.hudi.source;

import org.apache.hudi.source.stats.ColumnStats;
import org.apache.hudi.utils.TestData;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.hudi.source.ExpressionEvaluators.fromExpression;
import static org.apache.hudi.source.prune.ColumnStatsProbe.convertColumnStats;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for {@link org.apache.hudi.source.ExpressionEvaluators.Evaluator}.
 */
public class TestExpressionEvaluators {
  private static final DataType ROW_DATA_TYPE = DataTypes.ROW(
      DataTypes.FIELD("f_tinyint", DataTypes.TINYINT()),
      DataTypes.FIELD("f_smallint", DataTypes.SMALLINT()),
      DataTypes.FIELD("f_int", DataTypes.INT()),
      DataTypes.FIELD("f_long", DataTypes.BIGINT()),
      DataTypes.FIELD("f_float", DataTypes.FLOAT()),
      DataTypes.FIELD("f_double", DataTypes.DOUBLE()),
      DataTypes.FIELD("f_boolean", DataTypes.BOOLEAN()),
      DataTypes.FIELD("f_decimal", DataTypes.DECIMAL(10, 2)),
      DataTypes.FIELD("f_bytes", DataTypes.VARBINARY(10)),
      DataTypes.FIELD("f_string", DataTypes.VARCHAR(10)),
      DataTypes.FIELD("f_time", DataTypes.TIME(3)),
      DataTypes.FIELD("f_date", DataTypes.DATE()),
      DataTypes.FIELD("f_timestamp", DataTypes.TIMESTAMP(3))
  ).notNull();
  private static final DataType INDEX_ROW_DATA_TYPE = DataTypes.ROW(
      DataTypes.FIELD("file_name", DataTypes.STRING()),
      DataTypes.FIELD("value_cnt", DataTypes.BIGINT()),
      DataTypes.FIELD("f_int_min", DataTypes.INT()),
      DataTypes.FIELD("f_int_max", DataTypes.INT()),
      DataTypes.FIELD("f_int_null_cnt", DataTypes.BIGINT()),
      DataTypes.FIELD("f_string_min", DataTypes.VARCHAR(10)),
      DataTypes.FIELD("f_string_max", DataTypes.VARCHAR(10)),
      DataTypes.FIELD("f_string_null_cnt", DataTypes.BIGINT()),
      DataTypes.FIELD("f_timestamp_min", DataTypes.TIMESTAMP(3)),
      DataTypes.FIELD("f_timestamp_max", DataTypes.TIMESTAMP(3)),
      DataTypes.FIELD("f_timestamp_null_cnt", DataTypes.BIGINT())
  ).notNull();

  private static final RowType INDEX_ROW_TYPE = (RowType) INDEX_ROW_DATA_TYPE.getLogicalType();

  @Test
  void testEqualTo() {
    ExpressionEvaluators.EqualTo equalTo = ExpressionEvaluators.EqualTo.getInstance();
    FieldReferenceExpression rExpr = new FieldReferenceExpression("f_int", DataTypes.INT(), 2, 2);
    ValueLiteralExpression vExpr = new ValueLiteralExpression(12);

    equalTo.bindVal(vExpr)
        .bindFieldReference(rExpr);
    RowData indexRow1 = intIndexRow(11, 13);
    Map<String, ColumnStats> stats1 = convertColumnStats(indexRow1, queryFields(2));
    assertTrue(equalTo.eval(stats1), "11 < 12 < 13");

    RowData indexRow2 = intIndexRow(12, 13);
    Map<String, ColumnStats> stats2 = convertColumnStats(indexRow2, queryFields(2));
    assertTrue(equalTo.eval(stats2), "12 <= 12 < 13");

    RowData indexRow3 = intIndexRow(11, 12);
    Map<String, ColumnStats> stats3 = convertColumnStats(indexRow3, queryFields(2));
    assertTrue(equalTo.eval(stats3), "11 < 12 <= 12");

    RowData indexRow4 = intIndexRow(10, 11);
    Map<String, ColumnStats> stats4 = convertColumnStats(indexRow4, queryFields(2));
    assertFalse(equalTo.eval(stats4), "11 < 12");

    RowData indexRow5 = intIndexRow(13, 14);
    Map<String, ColumnStats> stats5 = convertColumnStats(indexRow5, queryFields(2));
    assertFalse(equalTo.eval(stats5), "12 < 13");

    RowData indexRow6 = intIndexRow(null, null);
    Map<String, ColumnStats> stats6 = convertColumnStats(indexRow6, queryFields(2));
    assertFalse(equalTo.eval(stats6), "12 <> null");

    equalTo.bindVal(new ValueLiteralExpression(null, DataTypes.INT()));
    assertFalse(equalTo.eval(stats1), "It is not possible to test for NULL values with '=' operator");
  }

  @Test
  void testNotEqualTo() {
    ExpressionEvaluators.NotEqualTo notEqualTo = ExpressionEvaluators.NotEqualTo.getInstance();
    FieldReferenceExpression rExpr = new FieldReferenceExpression("f_int", DataTypes.INT(), 2, 2);
    ValueLiteralExpression vExpr = new ValueLiteralExpression(12);

    RowData indexRow1 = intIndexRow(11, 13);
    notEqualTo.bindVal(vExpr)
        .bindFieldReference(rExpr);
    Map<String, ColumnStats> stats1 = convertColumnStats(indexRow1, queryFields(2));
    assertTrue(notEqualTo.eval(stats1), "11 <> 12 && 12 <> 13");

    RowData indexRow2 = intIndexRow(12, 13);
    Map<String, ColumnStats> stats2 = convertColumnStats(indexRow2, queryFields(2));
    assertTrue(notEqualTo.eval(stats2), "12 <> 13");

    RowData indexRow3 = intIndexRow(11, 12);
    Map<String, ColumnStats> stats3 = convertColumnStats(indexRow3, queryFields(2));
    assertTrue(notEqualTo.eval(stats3), "11 <> 12");

    RowData indexRow4 = intIndexRow(10, 11);
    Map<String, ColumnStats> stats4 = convertColumnStats(indexRow4, queryFields(2));
    assertTrue(notEqualTo.eval(stats4), "10 <> 12 and 11 < 12");

    RowData indexRow5 = intIndexRow(13, 14);
    Map<String, ColumnStats> stats5 = convertColumnStats(indexRow5, queryFields(2));
    assertTrue(notEqualTo.eval(stats5), "12 <> 13 and 12 <> 14");

    RowData indexRow6 = intIndexRow(null, null);
    Map<String, ColumnStats> stats6 = convertColumnStats(indexRow6, queryFields(2));
    assertFalse(notEqualTo.eval(stats6), "12 <> null");

    RowData indexRow7 = intIndexRow(12, 12);
    Map<String, ColumnStats> stats7 = convertColumnStats(indexRow7, queryFields(2));
    assertFalse(notEqualTo.eval(stats7), "12 == 12 == 12");

    notEqualTo.bindVal(new ValueLiteralExpression(null, DataTypes.INT()));
    assertFalse(notEqualTo.eval(stats1), "It is not possible to test for NULL values with '<>' operator");
  }

  @Test
  void testIsNull() {
    ExpressionEvaluators.IsNull isNull = ExpressionEvaluators.IsNull.getInstance();
    FieldReferenceExpression rExpr = new FieldReferenceExpression("f_int", DataTypes.INT(), 2, 2);

    RowData indexRow1 = intIndexRow(11, 13);
    isNull.bindFieldReference(rExpr);
    Map<String, ColumnStats> stats1 = convertColumnStats(indexRow1, queryFields(2));
    assertTrue(isNull.eval(stats1), "2 nulls");

    RowData indexRow2 = intIndexRow(12, 13, 0L);
    Map<String, ColumnStats> stats2 = convertColumnStats(indexRow2, queryFields(2));
    assertFalse(isNull.eval(stats2), "0 nulls");
  }

  @Test
  void testIsNotNull() {
    ExpressionEvaluators.IsNotNull isNotNull = ExpressionEvaluators.IsNotNull.getInstance();
    FieldReferenceExpression rExpr = new FieldReferenceExpression("f_int", DataTypes.INT(), 2, 2);

    RowData indexRow1 = intIndexRow(11, 13);
    isNotNull.bindFieldReference(rExpr);
    Map<String, ColumnStats> stats1 = convertColumnStats(indexRow1, queryFields(2));
    assertTrue(isNotNull.eval(stats1), "min 11 is not null");

    RowData indexRow2 = intIndexRow(null, null, 0L);
    Map<String, ColumnStats> stats2 = convertColumnStats(indexRow2, queryFields(2));
    assertTrue(isNotNull.eval(stats2), "min is null and 0 nulls");
  }

  @Test
  void testLessThan() {
    ExpressionEvaluators.LessThan lessThan = ExpressionEvaluators.LessThan.getInstance();
    FieldReferenceExpression rExpr = new FieldReferenceExpression("f_int", DataTypes.INT(), 2, 2);
    ValueLiteralExpression vExpr = new ValueLiteralExpression(12);

    RowData indexRow1 = intIndexRow(11, 13);
    lessThan.bindVal(vExpr)
        .bindFieldReference(rExpr);
    Map<String, ColumnStats> stats1 = convertColumnStats(indexRow1, queryFields(2));
    assertTrue(lessThan.eval(stats1), "12 < 13");

    RowData indexRow2 = intIndexRow(12, 13);
    Map<String, ColumnStats> stats2 = convertColumnStats(indexRow2, queryFields(2));
    assertFalse(lessThan.eval(stats2), "min 12 = 12");

    RowData indexRow3 = intIndexRow(11, 12);
    Map<String, ColumnStats> stats3 = convertColumnStats(indexRow3, queryFields(2));
    assertTrue(lessThan.eval(stats3), "11 < 12");

    RowData indexRow4 = intIndexRow(10, 11);
    Map<String, ColumnStats> stats4 = convertColumnStats(indexRow4, queryFields(2));
    assertTrue(lessThan.eval(stats4), "11 < 12");

    RowData indexRow5 = intIndexRow(13, 14);
    Map<String, ColumnStats> stats5 = convertColumnStats(indexRow5, queryFields(2));
    assertFalse(lessThan.eval(stats5), "12 < min 13");

    RowData indexRow6 = intIndexRow(null, null);
    Map<String, ColumnStats> stats6 = convertColumnStats(indexRow6, queryFields(2));
    assertFalse(lessThan.eval(stats6), "12 <> null");

    lessThan.bindVal(new ValueLiteralExpression(null, DataTypes.INT()));
    assertFalse(lessThan.eval(stats1), "It is not possible to test for NULL values with '<' operator");
  }

  @Test
  void testGreaterThan() {
    ExpressionEvaluators.GreaterThan greaterThan = ExpressionEvaluators.GreaterThan.getInstance();
    FieldReferenceExpression rExpr = new FieldReferenceExpression("f_int", DataTypes.INT(), 2, 2);
    ValueLiteralExpression vExpr = new ValueLiteralExpression(12);

    RowData indexRow1 = intIndexRow(11, 13);
    greaterThan.bindVal(vExpr)
        .bindFieldReference(rExpr);
    Map<String, ColumnStats> stats1 = convertColumnStats(indexRow1, queryFields(2));
    assertTrue(greaterThan.eval(stats1), "12 < 13");

    RowData indexRow2 = intIndexRow(12, 13);
    Map<String, ColumnStats> stats2 = convertColumnStats(indexRow2, queryFields(2));
    assertTrue(greaterThan.eval(stats2), "12 < 13");

    RowData indexRow3 = intIndexRow(11, 12);
    Map<String, ColumnStats> stats3 = convertColumnStats(indexRow3, queryFields(2));
    assertFalse(greaterThan.eval(stats3), "max 12 = 12");

    RowData indexRow4 = intIndexRow(10, 11);
    Map<String, ColumnStats> stats4 = convertColumnStats(indexRow4, queryFields(2));
    assertFalse(greaterThan.eval(stats4), "max 11 < 12");

    RowData indexRow5 = intIndexRow(13, 14);
    Map<String, ColumnStats> stats5 = convertColumnStats(indexRow5, queryFields(2));
    assertTrue(greaterThan.eval(stats5), "12 < 13");

    RowData indexRow6 = intIndexRow(null, null);
    Map<String, ColumnStats> stats6 = convertColumnStats(indexRow6, queryFields(2));
    assertFalse(greaterThan.eval(stats6), "12 <> null");

    greaterThan.bindVal(new ValueLiteralExpression(null, DataTypes.INT()));
    assertFalse(greaterThan.eval(stats1), "It is not possible to test for NULL values with '>' operator");
  }

  @Test
  void testLessThanOrEqual() {
    ExpressionEvaluators.LessThanOrEqual lessThanOrEqual = ExpressionEvaluators.LessThanOrEqual.getInstance();
    FieldReferenceExpression rExpr = new FieldReferenceExpression("f_int", DataTypes.INT(), 2, 2);
    ValueLiteralExpression vExpr = new ValueLiteralExpression(12);

    RowData indexRow1 = intIndexRow(11, 13);
    lessThanOrEqual.bindVal(vExpr)
        .bindFieldReference(rExpr);
    Map<String, ColumnStats> stats1 = convertColumnStats(indexRow1, queryFields(2));
    assertTrue(lessThanOrEqual.eval(stats1), "11 < 12");

    RowData indexRow2 = intIndexRow(12, 13);
    Map<String, ColumnStats> stats2 = convertColumnStats(indexRow2, queryFields(2));
    assertTrue(lessThanOrEqual.eval(stats2), "min 12 = 12");

    RowData indexRow3 = intIndexRow(11, 12);
    Map<String, ColumnStats> stats3 = convertColumnStats(indexRow3, queryFields(2));
    assertTrue(lessThanOrEqual.eval(stats3), "max 12 = 12");

    RowData indexRow4 = intIndexRow(10, 11);
    Map<String, ColumnStats> stats4 = convertColumnStats(indexRow4, queryFields(2));
    assertTrue(lessThanOrEqual.eval(stats4), "max 11 < 12");

    RowData indexRow5 = intIndexRow(13, 14);
    Map<String, ColumnStats> stats5 = convertColumnStats(indexRow5, queryFields(2));
    assertFalse(lessThanOrEqual.eval(stats5), "12 < 13");

    RowData indexRow6 = intIndexRow(null, null);
    Map<String, ColumnStats> stats6 = convertColumnStats(indexRow6, queryFields(2));
    assertFalse(lessThanOrEqual.eval(stats6), "12 <> null");

    lessThanOrEqual.bindVal(new ValueLiteralExpression(null, DataTypes.INT()));
    assertFalse(lessThanOrEqual.eval(stats1), "It is not possible to test for NULL values with '<=' operator");
  }

  @Test
  void testGreaterThanOrEqual() {
    ExpressionEvaluators.GreaterThanOrEqual greaterThanOrEqual = ExpressionEvaluators.GreaterThanOrEqual.getInstance();
    FieldReferenceExpression rExpr = new FieldReferenceExpression("f_int", DataTypes.INT(), 2, 2);
    ValueLiteralExpression vExpr = new ValueLiteralExpression(12);

    RowData indexRow1 = intIndexRow(11, 13);
    greaterThanOrEqual.bindVal(vExpr)
        .bindFieldReference(rExpr);
    Map<String, ColumnStats> stats1 = convertColumnStats(indexRow1, queryFields(2));
    assertTrue(greaterThanOrEqual.eval(stats1), "12 < 13");

    RowData indexRow2 = intIndexRow(12, 13);
    Map<String, ColumnStats> stats2 = convertColumnStats(indexRow2, queryFields(2));
    assertTrue(greaterThanOrEqual.eval(stats2), "min 12 = 12");

    RowData indexRow3 = intIndexRow(11, 12);
    Map<String, ColumnStats> stats3 = convertColumnStats(indexRow3, queryFields(2));
    assertTrue(greaterThanOrEqual.eval(stats3), "max 12 = 12");

    RowData indexRow4 = intIndexRow(10, 11);
    Map<String, ColumnStats> stats4 = convertColumnStats(indexRow4, queryFields(2));
    assertFalse(greaterThanOrEqual.eval(stats4), "max 11 < 12");

    RowData indexRow5 = intIndexRow(13, 14);
    Map<String, ColumnStats> stats5 = convertColumnStats(indexRow5, queryFields(2));
    assertTrue(greaterThanOrEqual.eval(stats5), "12 < 13");

    RowData indexRow6 = intIndexRow(null, null);
    Map<String, ColumnStats> stats6 = convertColumnStats(indexRow6, queryFields(2));
    assertFalse(greaterThanOrEqual.eval(stats6), "12 <> null");

    greaterThanOrEqual.bindVal(new ValueLiteralExpression(null, DataTypes.INT()));
    assertFalse(greaterThanOrEqual.eval(stats1), "It is not possible to test for NULL values with '>=' operator");
  }

  @Test
  void testIn() {
    ExpressionEvaluators.In in = ExpressionEvaluators.In.getInstance();
    FieldReferenceExpression rExpr = new FieldReferenceExpression("f_int", DataTypes.INT(), 2, 2);

    RowData indexRow1 = intIndexRow(11, 13);
    in.bindFieldReference(rExpr);
    in.bindVals(11, 12);
    Map<String, ColumnStats> stats1 = convertColumnStats(indexRow1, queryFields(2));
    assertTrue(in.eval(stats1), "11 < 12 < 13");

    RowData indexRow2 = intIndexRow(12, 13);
    Map<String, ColumnStats> stats2 = convertColumnStats(indexRow2, queryFields(2));
    assertTrue(in.eval(stats2), "min 12 = 12");

    RowData indexRow3 = intIndexRow(11, 12);
    Map<String, ColumnStats> stats3 = convertColumnStats(indexRow3, queryFields(2));
    assertTrue(in.eval(stats3), "max 12 = 12");

    RowData indexRow4 = intIndexRow(10, 11);
    Map<String, ColumnStats> stats4 = convertColumnStats(indexRow4, queryFields(2));
    assertTrue(in.eval(stats4), "max 11 = 11");

    RowData indexRow5 = intIndexRow(13, 14);
    Map<String, ColumnStats> stats5 = convertColumnStats(indexRow5, queryFields(2));
    assertFalse(in.eval(stats5), "12 < 13");

    RowData indexRow6 = intIndexRow(null, null);
    Map<String, ColumnStats> stats6 = convertColumnStats(indexRow6, queryFields(2));
    assertFalse(in.eval(stats6), "12 <> null");

    in.bindVals((Object) null);
    assertFalse(in.eval(stats1), "It is not possible to test for NULL values with 'in' operator");
  }

  @Test
  void testAlwaysFalse() {
    FieldReferenceExpression ref = new FieldReferenceExpression("f_int", DataTypes.INT(), 2, 2);
    ValueLiteralExpression nullLiteral = new ValueLiteralExpression(null, DataTypes.INT());
    RowData indexRow = intIndexRow(11, 13);
    Map<String, ColumnStats> stats = convertColumnStats(indexRow, queryFields(2));
    BuiltInFunctionDefinition[] funDefs = new BuiltInFunctionDefinition[] {
        BuiltInFunctionDefinitions.EQUALS,
        BuiltInFunctionDefinitions.NOT_EQUALS,
        BuiltInFunctionDefinitions.LESS_THAN,
        BuiltInFunctionDefinitions.GREATER_THAN,
        BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL,
        BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL,
        BuiltInFunctionDefinitions.IN};
    for (BuiltInFunctionDefinition funDef : funDefs) {
      CallExpression expr =
          CallExpression.permanent(
              funDef,
              Arrays.asList(ref, nullLiteral),
              DataTypes.BOOLEAN());
      // always return false if the literal value is null
      assertFalse(fromExpression(expr).eval(stats));
    }
  }

  @ParameterizedTest
  @MethodSource("twelveObjects")
  void testAllNumericDataTypes(Object twelve) {
    ExpressionEvaluators.GreaterThan greaterThan = ExpressionEvaluators.GreaterThan.getInstance();
    FieldReferenceExpression rExpr = new FieldReferenceExpression("f_int", DataTypes.INT(), 2, 2);
    ValueLiteralExpression vExpr = new ValueLiteralExpression(twelve);

    RowData indexRow = intIndexRow(11, 13);
    greaterThan.bindVal(vExpr).bindFieldReference(rExpr);
    Map<String, ColumnStats> stats = convertColumnStats(indexRow, queryFields(2));
    assertTrue(greaterThan.eval(stats), "12 < 13");
  }

  public static Stream<Object> twelveObjects() {
    return Stream.of((byte) 12, (short) 12, 12, 12L, new BigDecimal(12), 12f, 12d);
  }

  private static RowData intIndexRow(Integer minVal, Integer maxVal) {
    return intIndexRow(minVal, maxVal, 2L);
  }

  private static RowData intIndexRow(Integer minVal, Integer maxVal, Long nullCnt) {
    return indexRow(StringData.fromString("f1"), 100L,
        minVal, maxVal, nullCnt,
        StringData.fromString("1"), StringData.fromString("100"), 5L,
        TimestampData.fromEpochMillis(1), TimestampData.fromEpochMillis(100), 3L);
  }

  private static RowData indexRow(Object... fields) {
    return TestData.insertRow(INDEX_ROW_TYPE, fields);
  }

  private static RowType.RowField[] queryFields(int... pos) {
    List<RowType.RowField> fields = ((RowType) ROW_DATA_TYPE.getLogicalType()).getFields();
    return Arrays.stream(pos).mapToObj(fields::get).toArray(RowType.RowField[]::new);
  }
}
