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

package org.apache.hudi.hive.util;

import org.apache.hudi.hive.expression.AttributeReferenceExpression;
import org.apache.hudi.hive.expression.BinaryOperator;
import org.apache.hudi.hive.expression.Expression;
import org.apache.hudi.hive.expression.ExpressionVisitor;
import org.apache.hudi.hive.expression.Literal;

import java.util.Locale;

public class FilterGenVisitor implements ExpressionVisitor<String> {

  private String makeBinaryOperatorString(String left,  Expression.Operator operator, String right) {
    return String.format("%s %s %s", left, operator.sqlOperator, right);
  }

  private String quoteStringLiteral(String value) {
    if (!value.contains("\"")) {
      return "\"" + value + "\"";
    } else if (!value.contains("'")) {
      return "'" + value + "'";
    } else {
      throw new UnsupportedOperationException("Cannot pushdown filters if \" and ' both exist");
    }
  }

  private String visitAnd(Expression left, Expression right) {
    String leftResult = left.accept(this);
    String rightResult = right.accept(this);

    if (leftResult.isEmpty()) {
      if (rightResult.isEmpty()) {
        return "";
      }

      return rightResult;
    } else if (rightResult.isEmpty()) {
      return leftResult;
    }

    return "(" + makeBinaryOperatorString(leftResult, Expression.Operator.AND, rightResult) + ")";
  }

  private String visitOr(Expression left, Expression right) {
    String leftResult = left.accept(this);
    String rightResult = right.accept(this);

    if (!leftResult.isEmpty() && !rightResult.isEmpty()) {
      return "(" + makeBinaryOperatorString(leftResult, Expression.Operator.OR, rightResult) + ")";
    }
    return "";
  }

  private String visitBinaryComparator(Expression left, Expression.Operator operator, Expression right) {
    String leftResult = left.accept(this);
    String rightResult = right.accept(this);

    if (!leftResult.isEmpty() && !rightResult.isEmpty()) {
      return makeBinaryOperatorString(leftResult, operator, rightResult);
    }

    return "";
  }

  @Override
  public String visitBinaryOperator(BinaryOperator expr) {
    switch (expr.getOperator()) {
      case AND:
        return visitAnd(expr.getLeft(), expr.getRight());
      case OR:
        return visitOr(expr.getLeft(), expr.getRight());
      case EQ:
      case GT:
      case LT:
      case GT_EQ:
      case LT_EQ:
        return visitBinaryComparator(expr.getLeft(), expr.getOperator(), expr.getRight());
      default:
        return "";
    }
  }

  @Override
  public String visitLiteral(Literal literalExpr) {
    switch (literalExpr.getType().toLowerCase(Locale.ROOT)) {
      case HiveSchemaUtil.STRING_TYPE_NAME:
        return quoteStringLiteral(literalExpr.getValue());
      case HiveSchemaUtil.INT_TYPE_NAME:
      case HiveSchemaUtil.BIGINT_TYPE_NAME:
      case HiveSchemaUtil.DATE_TYPE_NAME:
        return literalExpr.getValue();
      default:
        return "";
    }
  }

  @Override
  public String visitAttribute(AttributeReferenceExpression attribute) {
    return attribute.getName();
  }
}
