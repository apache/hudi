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

package org.apache.hudi.util;

import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.logical.LogicalType;

import javax.annotation.Nullable;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoField;
import java.util.Arrays;
import java.util.List;

/**
 * Utilities for expression resolving.
 */
public class ExpressionUtils {

  /**
   * Collect the referenced columns with given expressions,
   * only simple call expression is supported.
   */
  public static String[] referencedColumns(List<ResolvedExpression> exprs) {
    return exprs.stream()
        .map(ExpressionUtils::getReferencedColumns)
        .filter(columns -> columns.length > 0)
        .flatMap(Arrays::stream)
        .distinct() // deduplication
        .toArray(String[]::new);
  }

  /**
   * Returns whether the given expression is simple call expression:
   * a binary call with one operand as field reference and another operand
   * as literal.
   */
  public static boolean isSimpleCallExpression(Expression expr) {
    if (!(expr instanceof CallExpression)) {
      return false;
    }
    CallExpression callExpression = (CallExpression) expr;
    FunctionDefinition funcDef = callExpression.getFunctionDefinition();
    // simple call list:
    // NOT AND OR IN EQUALS NOT_EQUALS IS_NULL IS_NOT_NULL LESS_THAN GREATER_THAN
    // LESS_THAN_OR_EQUAL GREATER_THAN_OR_EQUAL

    if (funcDef == BuiltInFunctionDefinitions.NOT
        || funcDef == BuiltInFunctionDefinitions.AND
        || funcDef == BuiltInFunctionDefinitions.OR) {
      return callExpression.getChildren().stream()
          .allMatch(ExpressionUtils::isSimpleCallExpression);
    }
    if (!(funcDef == BuiltInFunctionDefinitions.IN
        || funcDef == BuiltInFunctionDefinitions.EQUALS
        || funcDef == BuiltInFunctionDefinitions.NOT_EQUALS
        || funcDef == BuiltInFunctionDefinitions.IS_NULL
        || funcDef == BuiltInFunctionDefinitions.IS_NOT_NULL
        || funcDef == BuiltInFunctionDefinitions.LESS_THAN
        || funcDef == BuiltInFunctionDefinitions.GREATER_THAN
        || funcDef == BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL
        || funcDef == BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL)) {
      return false;
    }
    // handle IN
    if (funcDef == BuiltInFunctionDefinitions.IN) {
      // In expression RHS operands are always literals
      return true;
    }
    // handle unary operator
    if (funcDef == BuiltInFunctionDefinitions.IS_NULL
        || funcDef == BuiltInFunctionDefinitions.IS_NOT_NULL) {
      return callExpression.getChildren().stream()
          .allMatch(e -> e instanceof FieldReferenceExpression);
    }
    // handle binary operator
    return isFieldReferenceAndLiteral(callExpression.getChildren());
  }

  private static boolean isFieldReferenceAndLiteral(List<Expression> exprs) {
    if (exprs.size() != 2) {
      return false;
    }
    final Expression expr0 = exprs.get(0);
    final Expression expr1 = exprs.get(1);
    return expr0 instanceof FieldReferenceExpression && expr1 instanceof ValueLiteralExpression
        || expr0 instanceof ValueLiteralExpression && expr1 instanceof FieldReferenceExpression;
  }

  private static String[] getReferencedColumns(ResolvedExpression expression) {
    CallExpression callExpr = (CallExpression) expression;
    FunctionDefinition funcDef = callExpr.getFunctionDefinition();
    if (funcDef == BuiltInFunctionDefinitions.NOT
        || funcDef == BuiltInFunctionDefinitions.AND
        || funcDef == BuiltInFunctionDefinitions.OR) {
      return callExpr.getChildren().stream()
          .map(e -> getReferencedColumns((ResolvedExpression) e))
          .flatMap(Arrays::stream)
          .toArray(String[]::new);
    }

    return expression.getChildren().stream()
        .filter(expr -> expr instanceof FieldReferenceExpression)
        .map(expr -> ((FieldReferenceExpression) expr).getName())
        .toArray(String[]::new);
  }

  /**
   * Returns the value with given value literal expression.
   *
   * <p>Returns null if the value can not parse as the output data type correctly,
   * should call {@code ValueLiteralExpression.isNull} first to decide whether
   * the literal is NULL.
   */
  @Nullable
  public static Object getValueFromLiteral(ValueLiteralExpression expr) {
    LogicalType logicalType = expr.getOutputDataType().getLogicalType();
    switch (logicalType.getTypeRoot()) {
      case TIMESTAMP_WITHOUT_TIME_ZONE:
        return expr.getValueAs(LocalDateTime.class)
            .map(ldt -> ldt.toInstant(ZoneOffset.UTC).toEpochMilli())
            .orElse(null);
      case TIME_WITHOUT_TIME_ZONE:
        return expr.getValueAs(LocalTime.class)
            .map(lt -> lt.get(ChronoField.MILLI_OF_DAY))
            .orElse(null);
      case DATE:
        return expr.getValueAs(LocalDate.class)
            .map(LocalDate::toEpochDay)
            .orElse(null);
      // NOTE: All integral types of size less than Int are encoded as Ints in MT
      case BOOLEAN:
        return expr.getValueAs(Boolean.class).orElse(null);
      case TINYINT:
      case SMALLINT:
      case INTEGER:
        return expr.getValueAs(Integer.class).orElse(null);
      case FLOAT:
        return expr.getValueAs(Float.class).orElse(null);
      case DOUBLE:
        return expr.getValueAs(Double.class).orElse(null);
      case BINARY:
      case VARBINARY:
        return expr.getValueAs(byte[].class).orElse(null);
      case CHAR:
      case VARCHAR:
        return expr.getValueAs(String.class).orElse(null);
      case DECIMAL:
        return expr.getValueAs(BigDecimal.class).orElse(null);
      default:
        throw new UnsupportedOperationException("Unsupported type: " + logicalType);
    }
  }
}
