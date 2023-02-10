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

import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.source.evaluator.AlwaysFalse;
import org.apache.hudi.source.evaluator.And;
import org.apache.hudi.source.evaluator.EqualTo;
import org.apache.hudi.source.evaluator.Evaluator;
import org.apache.hudi.source.evaluator.GreaterThan;
import org.apache.hudi.source.evaluator.GreaterThanOrEqual;
import org.apache.hudi.source.evaluator.In;
import org.apache.hudi.source.evaluator.IsNotNull;
import org.apache.hudi.source.evaluator.IsNull;
import org.apache.hudi.source.evaluator.LessThan;
import org.apache.hudi.source.evaluator.LessThanOrEqual;
import org.apache.hudi.source.evaluator.Not;
import org.apache.hudi.source.evaluator.NotEqualTo;
import org.apache.hudi.source.evaluator.NullFalseEvaluator;
import org.apache.hudi.source.evaluator.Or;

import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.logical.LogicalType;

import javax.validation.constraints.NotNull;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Utilities for evaluators.
 */
public class EvaluatorUtils {

  /**
   * Converts specific call expression list to the evaluator list.
   */
  public static List<Evaluator> fromExpression(List<ResolvedExpression> exprs) {
    return exprs.stream()
        .map(e -> EvaluatorUtils.fromExpression((CallExpression) e))
        .collect(Collectors.toList());
  }

  /**
   * Converts specific call expression to the evaluator.
   *
   * <p>Two steps to bind the call:
   * 1. map the evaluator instance;
   * 2. bind the field reference;
   *
   * <p>Normalize the expression to simplify the subsequent decision logic:
   * always put the literal expression in the RHS.
   */
  public static Evaluator fromExpression(CallExpression expr) {
    FunctionDefinition funDef = expr.getFunctionDefinition();
    List<Expression> childExprs = expr.getChildren();

    boolean normalized = childExprs.get(0) instanceof FieldReferenceExpression;

    if (BuiltInFunctionDefinitions.NOT.equals(funDef)) {
      Not evaluator = Not.getInstance();
      Evaluator childEvaluator = fromExpression((CallExpression) childExprs.get(0));
      return evaluator.bindEvaluator(childEvaluator);
    }

    if (BuiltInFunctionDefinitions.AND.equals(funDef)) {
      And evaluator = And.getInstance();
      Evaluator evaluator1 = fromExpression((CallExpression) childExprs.get(0));
      Evaluator evaluator2 = fromExpression((CallExpression) childExprs.get(1));
      return evaluator.bindEvaluator(evaluator1, evaluator2);
    }

    if (BuiltInFunctionDefinitions.OR.equals(funDef)) {
      Or evaluator = Or.getInstance();
      Evaluator evaluator1 = fromExpression((CallExpression) childExprs.get(0));
      Evaluator evaluator2 = fromExpression((CallExpression) childExprs.get(1));
      return evaluator.bindEvaluator(evaluator1, evaluator2);
    }

    // handle unary operators
    if (BuiltInFunctionDefinitions.IS_NULL.equals(funDef)) {
      FieldReferenceExpression rExpr = (FieldReferenceExpression) childExprs.get(0);
      return IsNull.getInstance()
          .bindFieldReference(rExpr);
    } else if (BuiltInFunctionDefinitions.IS_NOT_NULL.equals(funDef)) {
      FieldReferenceExpression rExpr = (FieldReferenceExpression) childExprs.get(0);
      return IsNotNull.getInstance()
          .bindFieldReference(rExpr);
    }

    boolean hasNullLiteral =
        childExprs.stream().anyMatch(e ->
            e instanceof ValueLiteralExpression
                && ExpressionUtils.getValueFromLiteral((ValueLiteralExpression) e) == null);
    if (hasNullLiteral) {
      return AlwaysFalse.getInstance();
    }

    // handle IN specifically
    if (BuiltInFunctionDefinitions.IN.equals(funDef)) {
      ValidationUtils.checkState(normalized, "The IN expression expects to be normalized");
      In in = In.getInstance();
      FieldReferenceExpression rExpr = (FieldReferenceExpression) childExprs.get(0);
      in.bindFieldReference(rExpr);
      in.bindVals(getInLiteralVals(childExprs));
      return in;
    }

    NullFalseEvaluator evaluator;
    // handle binary operators
    if (BuiltInFunctionDefinitions.EQUALS.equals(funDef)) {
      evaluator = EqualTo.getInstance();
    } else if (BuiltInFunctionDefinitions.NOT_EQUALS.equals(funDef)) {
      evaluator = NotEqualTo.getInstance();
    } else if (BuiltInFunctionDefinitions.LESS_THAN.equals(funDef)) {
      evaluator = normalized ? LessThan.getInstance() : GreaterThan.getInstance();
    } else if (BuiltInFunctionDefinitions.GREATER_THAN.equals(funDef)) {
      evaluator = normalized ? GreaterThan.getInstance() : LessThan.getInstance();
    } else if (BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL.equals(funDef)) {
      evaluator = normalized ? LessThanOrEqual.getInstance() : GreaterThanOrEqual.getInstance();
    } else if (BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL.equals(funDef)) {
      evaluator = normalized ? GreaterThanOrEqual.getInstance() : LessThanOrEqual.getInstance();
    } else {
      throw new AssertionError("Unexpected function definition " + funDef);
    }
    FieldReferenceExpression rExpr = normalized
        ? (FieldReferenceExpression) childExprs.get(0)
        : (FieldReferenceExpression) childExprs.get(1);
    ValueLiteralExpression vExpr = normalized
        ? (ValueLiteralExpression) childExprs.get(1)
        : (ValueLiteralExpression) childExprs.get(0);
    evaluator
        .bindVal(vExpr)
        .bindFieldReference(rExpr);
    return evaluator;
  }

  /**
   * Returns the IN expression literal values.
   */
  private static Object[] getInLiteralVals(List<Expression> childExprs) {
    List<Object> vals = new ArrayList<>();
    for (int i = 1; i < childExprs.size(); i++) {
      vals.add(ExpressionUtils.getValueFromLiteral((ValueLiteralExpression) childExprs.get(i)));
    }
    return vals.toArray();
  }

  public static int compare(@NotNull Object val1, @NotNull Object val2, LogicalType logicalType) {
    switch (logicalType.getTypeRoot()) {
      case TIMESTAMP_WITHOUT_TIME_ZONE:
      case TIME_WITHOUT_TIME_ZONE:
      case DATE:
        return ((Long) val1).compareTo((Long) val2);
      case BOOLEAN:
        return ((Boolean) val1).compareTo((Boolean) val2);
      case TINYINT:
      case SMALLINT:
      case INTEGER:
        return ((Integer) val1).compareTo((Integer) val2);
      case FLOAT:
        return ((Float) val1).compareTo((Float) val2);
      case DOUBLE:
        return ((Double) val1).compareTo((Double) val2);
      case BINARY:
      case VARBINARY:
        return compareBytes((byte[]) val1, (byte[]) val2);
      case CHAR:
      case VARCHAR:
        return ((String) val1).compareTo((String) val2);
      case DECIMAL:
        return ((BigDecimal) val1).compareTo((BigDecimal) val2);
      default:
        throw new UnsupportedOperationException("Unsupported type: " + logicalType);
    }
  }

  private static int compareBytes(byte[] v1, byte[] v2) {
    int len1 = v1.length;
    int len2 = v2.length;
    int lim = Math.min(len1, len2);

    int k = 0;
    while (k < lim) {
      byte c1 = v1[k];
      byte c2 = v2[k];
      if (c1 != c2) {
        return c1 - c2;
      }
      k++;
    }
    return len1 - len2;
  }
}
