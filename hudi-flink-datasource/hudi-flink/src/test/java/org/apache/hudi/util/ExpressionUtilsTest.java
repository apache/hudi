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
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

class ExpressionUtilsTest {

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

  @Test
  void getValueFromLiteral() {
    List<RowType.RowField> fields = ((RowType) ROW_DATA_TYPE.getLogicalType()).getFields();
    List<DataType> dataTypes = ROW_DATA_TYPE.getChildren();
    for (int i = 0; i < fields.size(); i++) {
      // 1. Build all types
      CallExpression callExpression = new CallExpression(
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
}