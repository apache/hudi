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

import org.apache.hudi.internal.schema.Types;

import java.util.List;
import java.util.stream.Collectors;

public class BindVisitor implements ExpressionVisitor<Expression>  {

  protected final Types.RecordType recordType;
  protected final boolean caseSensitive;

  public BindVisitor(Types.RecordType recordType, boolean caseSensitive) {
    this.recordType = recordType;
    this.caseSensitive = caseSensitive;
  }

  @Override
  public Expression alwaysTrue() {
    return Predicates.TrueExpression.get();
  }

  @Override
  public Expression alwaysFalse() {
    return Predicates.FalseExpression.get();
  }

  @Override
  public Expression visitAnd(Predicates.And and) {
    if (and.getLeft() instanceof Predicates.FalseExpression
        || and.getRight() instanceof Predicates.FalseExpression) {
      return alwaysFalse();
    }

    Expression left = and.getLeft().accept(this);
    Expression right = and.getRight().accept(this);
    if (left instanceof Predicates.FalseExpression
        || right instanceof Predicates.FalseExpression) {
      return alwaysFalse();
    }

    if (left instanceof Predicates.TrueExpression
        && right instanceof Predicates.TrueExpression) {
      return alwaysTrue();
    }

    if (left instanceof Predicates.TrueExpression) {
      return right;
    }

    if (right instanceof Predicates.TrueExpression) {
      return left;
    }

    return Predicates.and(left, right);
  }

  @Override
  public Expression visitOr(Predicates.Or or) {
    if (or.getLeft() instanceof Predicates.TrueExpression
        || or.getRight() instanceof Predicates.TrueExpression) {
      return alwaysTrue();
    }

    Expression left = or.getLeft().accept(this);
    Expression right = or.getRight().accept(this);
    if (left instanceof Predicates.TrueExpression
        || right instanceof Predicates.TrueExpression) {
      return alwaysTrue();
    }

    if (left instanceof Predicates.FalseExpression
        && right instanceof Predicates.FalseExpression) {
      return alwaysFalse();
    }

    if (left instanceof Predicates.FalseExpression) {
      return right;
    }

    if (right instanceof Predicates.FalseExpression) {
      return left;
    }

    return Predicates.or(left, right);
  }

  @Override
  public Expression visitLiteral(Literal literal) {
    return literal;
  }

  @Override
  public Expression visitNameReference(NameReference attribute) {
    Types.Field field = caseSensitive
        ? recordType.fieldByName(attribute.getName())
        : recordType.fieldByNameCaseInsensitive(attribute.getName());

    if (field == null) {
      throw new IllegalArgumentException("The attribute " + attribute
          + " cannot be bound from schema " + recordType);
    }

    return new BoundReference(field.fieldId(), field.type());
  }

  @Override
  public Expression visitBoundReference(BoundReference boundReference) {
    return boundReference;
  }

  @Override
  public Expression visitPredicate(Predicate predicate) {
    if (predicate instanceof Predicates.Not) {
      Expression expr = ((Predicates.Not) predicate).child.accept(this);
      if (expr instanceof Predicates.TrueExpression) {
        return alwaysFalse();
      }
      if (expr instanceof Predicates.FalseExpression) {
        return alwaysTrue();
      }

      return Predicates.not(expr);
    }

    if (predicate instanceof Predicates.BinaryComparison) {
      Predicates.BinaryComparison binaryExp = (Predicates.BinaryComparison) predicate;
      Expression left = binaryExp.getLeft().accept(this);
      Expression right = binaryExp.getRight().accept(this);
      return new Predicates.BinaryComparison(left, binaryExp.getOperator(), right);
    }

    if (predicate instanceof Predicates.In) {
      Predicates.In in = ((Predicates.In) predicate);
      Expression valueExpression = in.value.accept(this);
      List<Expression> validValues = in.validValues.stream()
          .map(validValue -> validValue.accept(this))
          .collect(Collectors.toList());

      return Predicates.in(valueExpression, validValues);
    }

    if (predicate instanceof Predicates.IsNull) {
      Predicates.IsNull isNull = (Predicates.IsNull) predicate;
      return Predicates.isNull(isNull.child.accept(this));
    }

    if (predicate instanceof Predicates.IsNotNull) {
      Predicates.IsNotNull isNotNull = (Predicates.IsNotNull) predicate;
      return Predicates.isNotNull(isNotNull.child.accept(this));
    }

    if (predicate instanceof Predicates.StringStartsWith) {
      Predicates.StringStartsWith contains = (Predicates.StringStartsWith) predicate;
      Expression left = contains.getLeft().accept(this);
      Expression right = contains.getRight().accept(this);
      return Predicates.startsWith(left, right);
    }

    if (predicate instanceof Predicates.StringContains) {
      Predicates.StringContains contains = (Predicates.StringContains) predicate;
      Expression left = contains.getLeft().accept(this);
      Expression right = contains.getRight().accept(this);
      return Predicates.contains(left, right);
    }

    throw new IllegalArgumentException("The expression " + this + "cannot be visited as predicate");
  }
}
