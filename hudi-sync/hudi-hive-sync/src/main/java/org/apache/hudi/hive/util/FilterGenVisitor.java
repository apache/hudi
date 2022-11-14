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

import org.apache.hudi.hive.expression.Expression;
import org.apache.hudi.hive.expression.ExpressionVisitor;
import org.apache.hudi.hive.expression.LeafExpression;

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

  @Override
  public String visitAnd(Expression left, Expression right) {
    String leftResult = left.accept(this);
    String rightResult = right.accept(this);

    if (leftResult.isEmpty() && rightResult.isEmpty()) {
      return "";
    }

    if (!leftResult.isEmpty() && rightResult.isEmpty()) {
      return leftResult;
    }

    if (leftResult.isEmpty()) {
      return rightResult;
    }

    return "(" + makeBinaryOperatorString(leftResult, Expression.Operator.AND, rightResult) + ")";
  }

  @Override
  public String visitOr(Expression left, Expression right) {
    String leftResult = left.accept(this);
    String rightResult = right.accept(this);

    if (!leftResult.isEmpty() && !rightResult.isEmpty()) {
      return "(" + makeBinaryOperatorString(leftResult, Expression.Operator.OR, rightResult) + ")";
    }
    return "";
  }

  @Override
  public String visitBinaryComparator(Expression left, Expression.Operator operator, Expression right) {
    String leftResult = left.accept(this);
    String rightResult = right.accept(this);

    if (!leftResult.isEmpty() && !rightResult.isEmpty()) {
      return makeBinaryOperatorString(leftResult, operator, rightResult);
    }

    return "";
  }

  @Override
  public String visitLiteral(LeafExpression.Literal literalExpr) {
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
  public String visitAttribute(LeafExpression.AttributeReferenceExpression attribute) {
    return attribute.getName();
  }
}
