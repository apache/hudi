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

package org.apache.hudi.source.stats;

import org.apache.hudi.utils.TestData;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for {@link ExpressionEvaluator}.
 */
public class TestExpressionEvaluator {
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
    ExpressionEvaluator.EqualTo equalTo = ExpressionEvaluator.EqualTo.getInstance();
    FieldReferenceExpression rExpr = new FieldReferenceExpression("f_int", DataTypes.INT(), 2, 2);
    ValueLiteralExpression vExpr = new ValueLiteralExpression(12);

    RowData indexRow1 = intIndexRow(11, 13);
    equalTo.bindFieldReference(rExpr)
        .bindVal(vExpr)
        .bindColStats(indexRow1, queryFields(2), rExpr);
    assertTrue(equalTo.eval(), "11 < 12 < 13");

    RowData indexRow2 = intIndexRow(12, 13);
    equalTo.bindColStats(indexRow2, queryFields(2), rExpr);
    assertTrue(equalTo.eval(), "12 <= 12 < 13");

    RowData indexRow3 = intIndexRow(11, 12);
    equalTo.bindColStats(indexRow3, queryFields(2), rExpr);
    assertTrue(equalTo.eval(), "11 < 12 <= 12");

    RowData indexRow4 = intIndexRow(10, 11);
    equalTo.bindColStats(indexRow4, queryFields(2), rExpr);
    assertFalse(equalTo.eval(), "11 < 12");

    RowData indexRow5 = intIndexRow(13, 14);
    equalTo.bindColStats(indexRow5, queryFields(2), rExpr);
    assertFalse(equalTo.eval(), "12 < 13");

    RowData indexRow6 = intIndexRow(null, null);
    equalTo.bindColStats(indexRow6, queryFields(2), rExpr);
    assertFalse(equalTo.eval(), "12 <> null");

    equalTo.bindVal(new ValueLiteralExpression(null, DataTypes.INT()));
    assertFalse(equalTo.eval(), "null <> null");
  }

  @Test
  void testNotEqualTo() {
    ExpressionEvaluator.NotEqualTo notEqualTo = ExpressionEvaluator.NotEqualTo.getInstance();
    FieldReferenceExpression rExpr = new FieldReferenceExpression("f_int", DataTypes.INT(), 2, 2);
    ValueLiteralExpression vExpr = new ValueLiteralExpression(12);

    RowData indexRow1 = intIndexRow(11, 13);
    notEqualTo.bindFieldReference(rExpr)
        .bindVal(vExpr)
        .bindColStats(indexRow1, queryFields(2), rExpr);
    assertTrue(notEqualTo.eval(), "11 <> 12 && 12 <> 13");

    RowData indexRow2 = intIndexRow(12, 13);
    notEqualTo.bindColStats(indexRow2, queryFields(2), rExpr);
    assertTrue(notEqualTo.eval(), "12 <> 13");

    RowData indexRow3 = intIndexRow(11, 12);
    notEqualTo.bindColStats(indexRow3, queryFields(2), rExpr);
    assertTrue(notEqualTo.eval(), "11 <> 12");

    RowData indexRow4 = intIndexRow(10, 11);
    notEqualTo.bindColStats(indexRow4, queryFields(2), rExpr);
    assertTrue(notEqualTo.eval(), "10 <> 12 and 11 < 12");

    RowData indexRow5 = intIndexRow(13, 14);
    notEqualTo.bindColStats(indexRow5, queryFields(2), rExpr);
    assertTrue(notEqualTo.eval(), "12 <> 13 and 12 <> 14");

    RowData indexRow6 = intIndexRow(null, null);
    notEqualTo.bindColStats(indexRow6, queryFields(2), rExpr);
    assertTrue(notEqualTo.eval(), "12 <> null");

    notEqualTo.bindVal(new ValueLiteralExpression(null, DataTypes.INT()));
    assertTrue(notEqualTo.eval(), "null <> null");
  }

  @Test
  void testIsNull() {
    ExpressionEvaluator.IsNull isNull = ExpressionEvaluator.IsNull.getInstance();
    FieldReferenceExpression rExpr = new FieldReferenceExpression("f_int", DataTypes.INT(), 2, 2);

    RowData indexRow1 = intIndexRow(11, 13);
    isNull.bindFieldReference(rExpr)
        .bindColStats(indexRow1, queryFields(2), rExpr);
    assertTrue(isNull.eval(), "2 nulls");

    RowData indexRow2 = intIndexRow(12, 13, 0L);
    isNull.bindColStats(indexRow2, queryFields(2), rExpr);
    assertFalse(isNull.eval(), "0 nulls");
  }

  @Test
  void testIsNotNull() {
    ExpressionEvaluator.IsNotNull isNotNull = ExpressionEvaluator.IsNotNull.getInstance();
    FieldReferenceExpression rExpr = new FieldReferenceExpression("f_int", DataTypes.INT(), 2, 2);

    RowData indexRow1 = intIndexRow(11, 13);
    isNotNull.bindFieldReference(rExpr)
        .bindColStats(indexRow1, queryFields(2), rExpr);
    assertTrue(isNotNull.eval(), "min 11 is not null");

    RowData indexRow2 = intIndexRow(null, null, 0L);
    isNotNull.bindColStats(indexRow2, queryFields(2), rExpr);
    assertTrue(isNotNull.eval(), "min is null and 0 nulls");
  }

  @Test
  void testLessThan() {
    ExpressionEvaluator.LessThan lessThan = ExpressionEvaluator.LessThan.getInstance();
    FieldReferenceExpression rExpr = new FieldReferenceExpression("f_int", DataTypes.INT(), 2, 2);
    ValueLiteralExpression vExpr = new ValueLiteralExpression(12);

    RowData indexRow1 = intIndexRow(11, 13);
    lessThan.bindFieldReference(rExpr)
        .bindVal(vExpr)
        .bindColStats(indexRow1, queryFields(2), rExpr);
    assertTrue(lessThan.eval(), "12 < 13");

    RowData indexRow2 = intIndexRow(12, 13);
    lessThan.bindColStats(indexRow2, queryFields(2), rExpr);
    assertFalse(lessThan.eval(), "min 12 = 12");

    RowData indexRow3 = intIndexRow(11, 12);
    lessThan.bindColStats(indexRow3, queryFields(2), rExpr);
    assertTrue(lessThan.eval(), "11 < 12");

    RowData indexRow4 = intIndexRow(10, 11);
    lessThan.bindColStats(indexRow4, queryFields(2), rExpr);
    assertTrue(lessThan.eval(), "11 < 12");

    RowData indexRow5 = intIndexRow(13, 14);
    lessThan.bindColStats(indexRow5, queryFields(2), rExpr);
    assertFalse(lessThan.eval(), "12 < min 13");

    RowData indexRow6 = intIndexRow(null, null);
    lessThan.bindColStats(indexRow6, queryFields(2), rExpr);
    assertFalse(lessThan.eval(), "12 <> null");

    lessThan.bindVal(new ValueLiteralExpression(null, DataTypes.INT()));
    assertFalse(lessThan.eval(), "null <> null");
  }

  @Test
  void testGreaterThan() {
    ExpressionEvaluator.GreaterThan greaterThan = ExpressionEvaluator.GreaterThan.getInstance();
    FieldReferenceExpression rExpr = new FieldReferenceExpression("f_int", DataTypes.INT(), 2, 2);
    ValueLiteralExpression vExpr = new ValueLiteralExpression(12);

    RowData indexRow1 = intIndexRow(11, 13);
    greaterThan.bindFieldReference(rExpr)
        .bindVal(vExpr)
        .bindColStats(indexRow1, queryFields(2), rExpr);
    assertTrue(greaterThan.eval(), "12 < 13");

    RowData indexRow2 = intIndexRow(12, 13);
    greaterThan.bindColStats(indexRow2, queryFields(2), rExpr);
    assertTrue(greaterThan.eval(), "12 < 13");

    RowData indexRow3 = intIndexRow(11, 12);
    greaterThan.bindColStats(indexRow3, queryFields(2), rExpr);
    assertFalse(greaterThan.eval(), "max 12 = 12");

    RowData indexRow4 = intIndexRow(10, 11);
    greaterThan.bindColStats(indexRow4, queryFields(2), rExpr);
    assertFalse(greaterThan.eval(), "max 11 < 12");

    RowData indexRow5 = intIndexRow(13, 14);
    greaterThan.bindColStats(indexRow5, queryFields(2), rExpr);
    assertTrue(greaterThan.eval(), "12 < 13");

    RowData indexRow6 = intIndexRow(null, null);
    greaterThan.bindColStats(indexRow6, queryFields(2), rExpr);
    assertFalse(greaterThan.eval(), "12 <> null");

    greaterThan.bindVal(new ValueLiteralExpression(null, DataTypes.INT()));
    assertFalse(greaterThan.eval(), "null <> null");
  }

  @Test
  void testLessThanOrEqual() {
    ExpressionEvaluator.LessThanOrEqual lessThanOrEqual = ExpressionEvaluator.LessThanOrEqual.getInstance();
    FieldReferenceExpression rExpr = new FieldReferenceExpression("f_int", DataTypes.INT(), 2, 2);
    ValueLiteralExpression vExpr = new ValueLiteralExpression(12);

    RowData indexRow1 = intIndexRow(11, 13);
    lessThanOrEqual.bindFieldReference(rExpr)
        .bindVal(vExpr)
        .bindColStats(indexRow1, queryFields(2), rExpr);
    assertTrue(lessThanOrEqual.eval(), "11 < 12");

    RowData indexRow2 = intIndexRow(12, 13);
    lessThanOrEqual.bindColStats(indexRow2, queryFields(2), rExpr);
    assertTrue(lessThanOrEqual.eval(), "min 12 = 12");

    RowData indexRow3 = intIndexRow(11, 12);
    lessThanOrEqual.bindColStats(indexRow3, queryFields(2), rExpr);
    assertTrue(lessThanOrEqual.eval(), "max 12 = 12");

    RowData indexRow4 = intIndexRow(10, 11);
    lessThanOrEqual.bindColStats(indexRow4, queryFields(2), rExpr);
    assertTrue(lessThanOrEqual.eval(), "max 11 < 12");

    RowData indexRow5 = intIndexRow(13, 14);
    lessThanOrEqual.bindColStats(indexRow5, queryFields(2), rExpr);
    assertFalse(lessThanOrEqual.eval(), "12 < 13");

    RowData indexRow6 = intIndexRow(null, null);
    lessThanOrEqual.bindColStats(indexRow6, queryFields(2), rExpr);
    assertFalse(lessThanOrEqual.eval(), "12 <> null");

    lessThanOrEqual.bindVal(new ValueLiteralExpression(null, DataTypes.INT()));
    assertFalse(lessThanOrEqual.eval(), "null <> null");
  }

  @Test
  void testGreaterThanOrEqual() {
    ExpressionEvaluator.GreaterThanOrEqual greaterThanOrEqual = ExpressionEvaluator.GreaterThanOrEqual.getInstance();
    FieldReferenceExpression rExpr = new FieldReferenceExpression("f_int", DataTypes.INT(), 2, 2);
    ValueLiteralExpression vExpr = new ValueLiteralExpression(12);

    RowData indexRow1 = intIndexRow(11, 13);
    greaterThanOrEqual.bindFieldReference(rExpr)
        .bindVal(vExpr)
        .bindColStats(indexRow1, queryFields(2), rExpr);
    assertTrue(greaterThanOrEqual.eval(), "12 < 13");

    RowData indexRow2 = intIndexRow(12, 13);
    greaterThanOrEqual.bindColStats(indexRow2, queryFields(2), rExpr);
    assertTrue(greaterThanOrEqual.eval(), "min 12 = 12");

    RowData indexRow3 = intIndexRow(11, 12);
    greaterThanOrEqual.bindColStats(indexRow3, queryFields(2), rExpr);
    assertTrue(greaterThanOrEqual.eval(), "max 12 = 12");

    RowData indexRow4 = intIndexRow(10, 11);
    greaterThanOrEqual.bindColStats(indexRow4, queryFields(2), rExpr);
    assertFalse(greaterThanOrEqual.eval(), "max 11 < 12");

    RowData indexRow5 = intIndexRow(13, 14);
    greaterThanOrEqual.bindColStats(indexRow5, queryFields(2), rExpr);
    assertTrue(greaterThanOrEqual.eval(), "12 < 13");

    RowData indexRow6 = intIndexRow(null, null);
    greaterThanOrEqual.bindColStats(indexRow6, queryFields(2), rExpr);
    assertFalse(greaterThanOrEqual.eval(), "12 <> null");

    greaterThanOrEqual.bindVal(new ValueLiteralExpression(null, DataTypes.INT()));
    assertFalse(greaterThanOrEqual.eval(), "null <> null");
  }

  @Test
  void testIn() {
    ExpressionEvaluator.In in = ExpressionEvaluator.In.getInstance();
    FieldReferenceExpression rExpr = new FieldReferenceExpression("f_int", DataTypes.INT(), 2, 2);

    RowData indexRow1 = intIndexRow(11, 13);
    in.bindFieldReference(rExpr)
        .bindColStats(indexRow1, queryFields(2), rExpr);
    in.bindVals(12);
    assertTrue(in.eval(), "11 < 12 < 13");

    RowData indexRow2 = intIndexRow(12, 13);
    in.bindColStats(indexRow2, queryFields(2), rExpr);
    assertTrue(in.eval(), "min 12 = 12");

    RowData indexRow3 = intIndexRow(11, 12);
    in.bindColStats(indexRow3, queryFields(2), rExpr);
    assertTrue(in.eval(), "max 12 = 12");

    RowData indexRow4 = intIndexRow(10, 11);
    in.bindColStats(indexRow4, queryFields(2), rExpr);
    assertFalse(in.eval(), "max 11 < 12");

    RowData indexRow5 = intIndexRow(13, 14);
    in.bindColStats(indexRow5, queryFields(2), rExpr);
    assertFalse(in.eval(), "12 < 13");

    RowData indexRow6 = intIndexRow(null, null);
    in.bindColStats(indexRow6, queryFields(2), rExpr);
    assertFalse(in.eval(), "12 <> null");

    in.bindVals((Object) null);
    assertFalse(in.eval(), "null <> null");
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
