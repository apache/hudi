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

import org.apache.hudi.expression.NameReference;
import org.apache.hudi.expression.BoundReference;
import org.apache.hudi.expression.Expression;
import org.apache.hudi.expression.ExpressionVisitor;
import org.apache.hudi.expression.Literal;
import org.apache.hudi.expression.Predicate;
import org.apache.hudi.expression.Predicates;
import org.apache.hudi.internal.schema.Types;

public class FilterGenVisitor implements ExpressionVisitor<String> {

  private String makeBinaryOperatorString(String left,  Expression.Operator operator, String right) {
    return String.format("%s %s %s", left, operator.sqlOperator, right);
  }

  protected String quoteStringLiteral(String value) {
    if (!value.contains("\"")) {
      return "\"" + value + "\"";
    } else if (!value.contains("'")) {
      return "'" + value + "'";
    } else {
      throw new UnsupportedOperationException("Cannot pushdown filters if \" and ' both exist");
    }
  }

  @Override
  public String visitAnd(Predicates.And and) {
    String leftResult = and.getLeft().accept(this);
    String rightResult = and.getRight().accept(this);

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

  @Override
  public String visitOr(Predicates.Or or) {
    String leftResult = or.getLeft().accept(this);
    String rightResult = or.getRight().accept(this);

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
  public String visitPredicate(Predicate predicate) {
    if (predicate instanceof Predicates.BinaryComparison) {
      Predicates.BinaryComparison expr = (Predicates.BinaryComparison) predicate;
      return visitBinaryComparator(expr.getLeft(), expr.getOperator(), expr.getRight());
    }

    return "";
  }

  @Override
  public String alwaysTrue() {
    return "";
  }

  @Override
  public String alwaysFalse() {
    return "";
  }

  @Override
  public String visitLiteral(Literal literalExpr) {
    if (literalExpr.getDataType() instanceof Types.StringType) {
      return quoteStringLiteral(((Literal<String>)literalExpr).getValue());
    }

    if (literalExpr.getDataType() instanceof Types.IntType || literalExpr.getDataType() instanceof Types.LongType
        || literalExpr.getDataType() instanceof Types.DateType) {
      return literalExpr.getValue().toString();
    }

    return "";
  }

  @Override
  public String visitNameReference(NameReference attribute) {
    return attribute.getName();
  }

  @Override
  public String visitBoundReference(BoundReference boundReference) {
    throw new UnsupportedOperationException("BoundReference cannot be used to build filter string");
  }
}
