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
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class Predicates {

  public static TrueExpression alwaysTrue() {
    return TrueExpression.get();
  }

  public static FalseExpression alwaysFalse() {
    return FalseExpression.get();
  }

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

  public static StringStartsWith startsWith(Expression left, Expression right) {
    return new StringStartsWith(left, right);
  }

  public static StringContains contains(Expression left, Expression right) {
    return new StringContains(left, right);
  }

  public static In in(Expression left, List<Expression> validExpressions) {
    return new In(left, validExpressions);
  }

  public static IsNull isNull(Expression child) {
    return new IsNull(child);
  }

  public static IsNotNull isNotNull(Expression child) {
    return new IsNotNull(child);
  }

  public static Not not(Expression expr) {
    return new Not(expr);
  }

  public static StringStartsWithAny startsWithAny(Expression left, List<Expression> right) {
    return new StringStartsWithAny(left, right);
  }

  public static class TrueExpression extends LeafExpression implements Predicate {

    private static final TrueExpression INSTANCE = new TrueExpression();

    public static TrueExpression get() {
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

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      return o != null && getClass() == o.getClass();
    }

    @Override
    public int hashCode() {
      return getClass().hashCode();
    }
  }

  public static class FalseExpression extends LeafExpression implements Predicate {

    private static final FalseExpression INSTANCE = new FalseExpression();

    public static FalseExpression get() {
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

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      return o != null && getClass() == o.getClass();
    }

    @Override
    public int hashCode() {
      return getClass().hashCode();
    }
  }

  public static class And extends BinaryExpression implements Predicate {

    public And(Expression left, Expression right) {
      super(left, Operator.AND, right);
    }

    @Override
    public Boolean eval(StructLike data) {
      if (getLeft() instanceof FalseExpression || getRight() instanceof FalseExpression) {
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
          return left != null && right != null;
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

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      And and = (And) o;
      return Objects.equals(getLeft(), and.getLeft()) && Objects.equals(getRight(), and.getRight());
    }

    @Override
    public int hashCode() {
      return Objects.hash(getLeft(), getRight(), getOperator());
    }
  }

  public static class Or extends BinaryExpression implements Predicate {

    public Or(Expression left, Expression right) {
      super(left, Operator.OR, right);
    }

    @Override
    public Boolean eval(StructLike data) {
      if (getLeft() instanceof TrueExpression || getRight() instanceof TrueExpression) {
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

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Or or = (Or) o;
      return Objects.equals(getLeft(), or.getLeft()) && Objects.equals(getRight(), or.getRight());
    }

    @Override
    public int hashCode() {
      return Objects.hash(getLeft(), getRight(), getOperator());
    }
  }

  public static class StringStartsWith extends BinaryExpression implements Predicate {

    StringStartsWith(Expression left, Expression right) {
      super(left, Operator.STARTS_WITH, right);
    }

    @Override
    public String toString() {
      return getLeft().toString() + ".startWith(" + getRight().toString() + ")";
    }

    @Override
    public Object eval(StructLike data) {
      return getLeft().eval(data).toString().startsWith(getRight().eval(data).toString());
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      StringStartsWith that = (StringStartsWith) o;
      return Objects.equals(getLeft(), that.getLeft()) && Objects.equals(getRight(),
          that.getRight());
    }

    @Override
    public int hashCode() {
      return Objects.hash(getLeft(), getRight(), getOperator());
    }
  }

  public static class StringContains extends BinaryExpression implements Predicate {

    StringContains(Expression left, Expression right) {
      super(left, Operator.CONTAINS, right);
    }

    @Override
    public String toString() {
      return getLeft().toString() + ".contains(" + getRight().toString() + ")";
    }

    @Override
    public Object eval(StructLike data) {
      return getLeft().eval(data).toString().contains(getRight().eval(data).toString());
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      StringContains that = (StringContains) o;
      return Objects.equals(getLeft(), that.getLeft()) && Objects.equals(getRight(),
          that.getRight());
    }

    @Override
    public int hashCode() {
      return Objects.hash(getLeft(), getRight(), getOperator());
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

    public List<Expression> getRightChildren() {
      return validValues;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      In in = (In) o;
      return Objects.equals(value, in.value) && Objects.equals(validValues, in.validValues);
    }

    @Override
    public int hashCode() {
      return Objects.hash(value, validValues);
    }
  }

  public static class IsNull implements Predicate {

    protected final Expression child;

    public IsNull(Expression child) {
      this.child = child;
    }

    @Override
    public List<Expression> getChildren() {
      return Collections.singletonList(child);
    }

    @Override
    public Boolean eval(StructLike data) {
      return child.eval(data) == null;
    }

    @Override
    public Operator getOperator() {
      return Operator.IS_NULL;
    }

    @Override
    public String toString() {
      return child.toString() + " IS NULL";
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      IsNull isNull = (IsNull) o;
      return Objects.equals(child, isNull.child);
    }

    @Override
    public int hashCode() {
      return Objects.hash(child);
    }
  }

  public static class IsNotNull implements Predicate {

    protected final Expression child;

    public IsNotNull(Expression child) {
      this.child = child;
    }

    @Override
    public List<Expression> getChildren() {
      return Collections.singletonList(child);
    }

    @Override
    public Boolean eval(StructLike data) {
      return child.eval(data) != null;
    }

    @Override
    public Operator getOperator() {
      return Operator.IS_NOT_NULL;
    }

    @Override
    public String toString() {
      return child.toString() + " IS NOT NULL";
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      IsNotNull isNotNull = (IsNotNull) o;
      return Objects.equals(child, isNotNull.child);
    }

    @Override
    public int hashCode() {
      return Objects.hash(child);
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
      return !(Boolean) child.eval(data);
    }

    @Override
    public Operator getOperator() {
      return Operator.NOT;
    }

    @Override
    public String toString() {
      return "NOT " + child;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Not not = (Not) o;
      return Objects.equals(child, not.child);
    }

    @Override
    public int hashCode() {
      return Objects.hash(child);
    }
  }

  public static class BinaryComparison extends BinaryExpression implements Predicate {

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
        default:
          throw new IllegalArgumentException("The operation " + getOperator() + " doesn't support binary comparison");
      }
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      BinaryComparison that = (BinaryComparison) o;
      return getOperator() == that.getOperator() && Objects.equals(getLeft(), that.getLeft())
          && Objects.equals(getRight(), that.getRight());
    }

    @Override
    public int hashCode() {
      return Objects.hash(getLeft(), getRight(), getOperator());
    }
  }

  public static class StringStartsWithAny implements Predicate {

    private final Operator operator;
    private final Expression left;
    private final List<Expression> right;

    public StringStartsWithAny(Expression left, List<Expression> right) {
      this.left = left;
      this.operator = Operator.STARTS_WITH;
      this.right = right;
    }

    @Override
    public List<Expression> getChildren() {
      List<Expression> children = new ArrayList<>();
      children.add(left);
      children.addAll(right);
      return children;
    }

    @Override
    public Operator getOperator() {
      return operator;
    }

    @Override
    public Object eval(StructLike data) {
      for (Expression e : right) {
        Expression exp = new StringStartsWith(left, e);
        if ((boolean) exp.eval(data)) {
          return true;
        }
      }
      return false;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      StringStartsWithAny that = (StringStartsWithAny) o;
      return operator == that.operator && Objects.equals(left, that.left)
          && Objects.equals(right, that.right);
    }

    public List<Expression> getRightChildren() {
      return right;
    }

    @Override
    public int hashCode() {
      return Objects.hash(operator, left, right);
    }
  }
}
