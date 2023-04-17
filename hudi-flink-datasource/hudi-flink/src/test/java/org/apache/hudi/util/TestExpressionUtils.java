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

package org.apache.hudi.util;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestExpressionUtils {

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


  private static final DataType ROW_DATA_TYPE_FIELD_NON_NULL = DataTypes.ROW(
      DataTypes.FIELD("f_tinyint", new AtomicDataType(new TinyIntType(false))),
      DataTypes.FIELD("f_smallint", new AtomicDataType(new SmallIntType(false))),
      DataTypes.FIELD("f_int", new AtomicDataType(new IntType(false))),
      DataTypes.FIELD("f_long", new AtomicDataType(new BigIntType(false))),
      DataTypes.FIELD("f_float", new AtomicDataType(new FloatType(false))),
      DataTypes.FIELD("f_double", new AtomicDataType(new DoubleType(false))),
      DataTypes.FIELD("f_boolean", new AtomicDataType(new BooleanType(false))),
      DataTypes.FIELD("f_decimal", new AtomicDataType(new DecimalType(false, 10, 2))),
      DataTypes.FIELD("f_bytes", new AtomicDataType(new VarBinaryType(false, 10))),
      DataTypes.FIELD("f_string", new AtomicDataType(new VarCharType(false, 10))),
      DataTypes.FIELD("f_time", new AtomicDataType(new TimeType(false, 3))),
      DataTypes.FIELD("f_date", new AtomicDataType(new DateType(false))),
      DataTypes.FIELD("f_timestamp", new AtomicDataType(new TimestampType(false, 3)))
  ).notNull();

  @Test
  void getValueFromLiteralForNull() {
    List<RowType.RowField> fields = ((RowType) ROW_DATA_TYPE.getLogicalType()).getFields();
    List<DataType> dataTypes = ROW_DATA_TYPE.getChildren();
    CallExpression callExpression;
    for (int i = 0; i < fields.size(); i++) {
      // 1. Build all types
      callExpression = new CallExpression(
          BuiltInFunctionDefinitions.IS_NOT_NULL,
          Arrays.asList(new FieldReferenceExpression(fields.get(i).getName(),
              dataTypes.get(i),
              2,
              2), new ValueLiteralExpression(null, dataTypes.get(i))),
          DataTypes.BOOLEAN());
      List<Expression> childExprs = callExpression.getChildren();

      // 2. Parse each type
      boolean hasNullLiteral =
          childExprs.stream().anyMatch(e ->
              e instanceof ValueLiteralExpression
                  && ExpressionUtils.getValueFromLiteral((ValueLiteralExpression) e) == null);
      assertTrue(hasNullLiteral);
    }

  }

  @Test
  void getValueFromLiteralForNonNull() {
    List<RowType.RowField> fields = ((RowType) ROW_DATA_TYPE_FIELD_NON_NULL.getLogicalType()).getFields();
    List<DataType> dataTypes = ROW_DATA_TYPE_FIELD_NON_NULL.getChildren();
    // tests for non-null literals
    List<Object> dataList = new ArrayList<>(fields.size());
    dataList.add(new Byte("1")); // f_tinyint
    dataList.add(new Short("2")); // f_smallint
    dataList.add(new Integer("3")); // f_int
    dataList.add(new Long("4")); // f_long
    dataList.add(new Float(5.0)); // f_float
    dataList.add(new Double(6.0)); // f_double
    dataList.add(new Boolean(true)); // f_boolean
    dataList.add(new BigDecimal(3.0)); // f_decimal
    dataList.add("hudi".getBytes(StandardCharsets.UTF_8)); // f_bytes
    dataList.add("hudi ok"); // f_string
    dataList.add(LocalTime.of(1, 11, 11)); // f_time
    dataList.add(LocalDate.of(2023, 1, 2)); // f_date
    dataList.add(LocalDateTime.of(2023, 1, 2, 3, 4)); // f_timestamp
    CallExpression callExpression;
    for (int i = 0; i < fields.size(); i++) {
      // 1. Build all types
      callExpression = new CallExpression(
          BuiltInFunctionDefinitions.IS_NOT_NULL,
          Arrays.asList(
              new FieldReferenceExpression(
                  fields.get(i).getName(),
                  dataTypes.get(i),
                  i,
                  i),
              new ValueLiteralExpression(dataList.get(i), dataTypes.get(i))),
          DataTypes.BOOLEAN());
      List<Expression> childExprs = callExpression.getChildren();
      // 2. Parse each type
      if (dataList.get(i) instanceof LocalTime) {
        assertEquals(((LocalTime) dataList.get(i)).get(ChronoField.MILLI_OF_DAY), ExpressionUtils.getValueFromLiteral((ValueLiteralExpression) childExprs.get(1)));
      } else if (dataList.get(i) instanceof LocalDate) {
        assertEquals(((LocalDate) dataList.get(i)).toEpochDay(), ExpressionUtils.getValueFromLiteral((ValueLiteralExpression) childExprs.get(1)));
      } else if (dataList.get(i) instanceof LocalDateTime) {
        assertEquals(((LocalDateTime) dataList.get(i)).toInstant(ZoneOffset.UTC).toEpochMilli(), ExpressionUtils.getValueFromLiteral((ValueLiteralExpression) childExprs.get(1)));
      } else {
        assertEquals(dataList.get(i), ExpressionUtils.getValueFromLiteral((ValueLiteralExpression) childExprs.get(1)));
      }
    }
  }
}