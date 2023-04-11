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

package org.apache.hudi.expression;

import org.apache.hudi.internal.schema.Type;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class Predicates {

  public static And and(Expression left, Expression right) {
    return new And(left, right);
  }

  public static Or or(Expression left, Expression right) {
    return new Or(left, right);
  }

  public static BinaryComparison gt(Expression left, Expression right) {
    return new BinaryComparison(left, Expression.Operator.GT, right);
  }

  public static BinaryComparison lt(Expression left, Expression right) {
    return new BinaryComparison(left, Expression.Operator.LT, right);
  }

  public static BinaryComparison eq(Expression left, Expression right) {
    return new BinaryComparison(left, Expression.Operator.EQ, right);
  }

  public static BinaryComparison gteq(Expression left, Expression right) {
    return new BinaryComparison(left, Expression.Operator.GT_EQ, right);
  }

  public static BinaryComparison lteq(Expression left, Expression right) {
    return new BinaryComparison(left, Expression.Operator.LT_EQ, right);
  }

  public static BinaryComparison startsWith(Expression left, Expression right) {
    return new BinaryComparison(left, Expression.Operator.STARTS_WITH, right);
  }

  public static In in(Expression left, List<Expression> validExpressions) {
    return new In(left, validExpressions);
  }

  public static Not not(Expression expr) {
    return new Not(expr);
  }

  public static class True extends LeafExpression implements Predicate {

    private static final True INSTANCE = new True();

    public static True get() {
      return INSTANCE;
    }

    @Override
    public Boolean eval(StructLike data) {
      return true;
    }

    @Override
    public Operator getOperator() {
      return Operator.TRUE;
    }

    @Override
    public <T> T accept(ExpressionVisitor<T> exprVisitor) {
      return exprVisitor.alwaysTrue();
    }

    @Override
    public String toString() {
      return "TRUE";
    }
  }

  public static class False extends LeafExpression implements Predicate {

    private static final False INSTANCE = new False();

    public static False get() {
      return INSTANCE;
    }

    @Override
    public Boolean eval(StructLike data) {
      return false;
    }

    @Override
    public Operator getOperator() {
      return Operator.FALSE;
    }

    @Override
    public <T> T accept(ExpressionVisitor<T> exprVisitor) {
      return exprVisitor.alwaysFalse();
    }

    @Override
    public String toString() {
      return "FALSE";
    }
  }

  public static class And extends BinaryLike implements Predicate {

    public And(Expression left, Expression right) {
      super(left, Operator.AND, right);
    }

    @Override
    public Boolean eval(StructLike data) {
      if (getLeft() instanceof False || getRight() instanceof False) {
        return false;
      }
      Object left = getLeft().eval(data);
      if (left != null && !(Boolean) left) {
        return false;
      } else {
        Object right = getRight().eval(data);
        if (right != null && !(Boolean) right) {
          return false;
        } else {
          if (left != null && right != null) {
            return true;
          } else {
            return false;
          }
        }
      }
    }

    @Override
    public <T> T accept(ExpressionVisitor<T> exprVisitor) {
      return exprVisitor.visitAnd(this);
    }

    @Override
    public String toString() {
      return "(" + getLeft() + " " + getOperator().symbol + " " + getRight() + ")";
    }
  }

  public static class Or extends BinaryLike implements Predicate {

    public Or(Expression left, Expression right) {
      super(left, Operator.OR, right);
    }

    @Override
    public Boolean eval(StructLike data) {
      if (getLeft() instanceof True || getRight() instanceof True) {
        return true;
      }

      Object left = getLeft().eval(data);

      if (left == null) {
        return false;
      }

      if ((Boolean) left) {
        return true;
      } else {
        Object right = getRight().eval(data);
        return right != null && (Boolean) right;
      }
    }

    @Override
    public <T> T accept(ExpressionVisitor<T> exprVisitor) {
      return exprVisitor.visitOr(this);
    }

    @Override
    public String toString() {
      return "(" + getLeft() + " " + getOperator().symbol + " " + getRight() + ")";
    }
  }

  public static class In implements Predicate {

    protected final Expression value;
    protected final List<Expression> validValues;

    public In(Expression value, List<Expression> validValues) {
      this.value = value;
      this.validValues = validValues;
    }

    @Override
    public List<Expression> getChildren() {
      ArrayList<Expression> children = new ArrayList<>(validValues.size() + 1);
      children.add(value);
      children.addAll(validValues);
      return children;
    }

    @Override
    public Boolean eval(StructLike data) {
      Set<Object> values = validValues.stream()
          .map(validValue -> validValue.eval(data))
          .collect(Collectors.toSet());
      return values.contains(value.eval(data));
    }

    @Override
    public Operator getOperator() {
      return Operator.IN;
    }

    @Override
    public String toString() {
      return value.toString() + " " + getOperator().symbol + " "
          + validValues.stream().map(Expression::toString).collect(Collectors.joining(",", "(", ")"));
    }
  }

  public static class Not implements Predicate {

    Expression child;

    public Not(Expression child) {
      this.child = child;
    }

    @Override
    public List<Expression> getChildren() {
      return Collections.singletonList(child);
    }

    @Override
    public Boolean eval(StructLike data) {
      return ! (Boolean) child.eval(data);
    }

    @Override
    public Operator getOperator() {
      return Operator.NOT;
    }

    @Override
    public String toString() {
      return "NOT " + child;
    }
  }

  public static class BinaryComparison extends BinaryLike implements Predicate {

    public BinaryComparison(Expression left, Operator operator, Expression right) {
      super(left, operator, right);
    }

    @Override
    public Boolean eval(StructLike data) {
      if (getLeft().getDataType().isNestedType()) {
        throw new IllegalArgumentException("The nested type doesn't support binary comparison");
      }
      Comparator<Object> comparator = Comparators.forType((Type.PrimitiveType) getLeft().getDataType());
      switch (getOperator()) {
        case EQ:
          return comparator.compare(getLeft().eval(data), getRight().eval(data)) == 0;
        case GT:
          return comparator.compare(getLeft().eval(data), getRight().eval(data)) > 0;
        case GT_EQ:
          return comparator.compare(getLeft().eval(data), getRight().eval(data)) >= 0;
        case LT:
          return comparator.compare(getLeft().eval(data), getRight().eval(data)) < 0;
        case LT_EQ:
          return comparator.compare(getLeft().eval(data), getRight().eval(data)) <= 0;
        case STARTS_WITH:
          return String.valueOf(getLeft().eval(data)).startsWith(String.valueOf(getRight().eval(data)));
        default:
          throw new IllegalArgumentException("The operation " + getOperator() + " doesn't support binary comparison");
      }
    }
  }
}
