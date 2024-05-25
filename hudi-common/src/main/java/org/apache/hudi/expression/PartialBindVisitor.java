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

import org.apache.hudi.common.util.Option;
import org.apache.hudi.internal.schema.Types;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Will try to bind all references, and convert unresolved references to AlwaysTrue.
 *
 * e.g. `year=2023 AND day=12`, if year and day both are provided to `recordType`,
 * then the expression won't change, if day is not provided, the expression will be
 * transformed to `year=2023 AND True`, which will be optimized to `year=2023`.
 */
public class PartialBindVisitor extends BindVisitor {

  public PartialBindVisitor(Types.RecordType recordType, boolean caseSensitive) {
    super(recordType, caseSensitive);
  }

  /**
   * If the attribute cannot find from the schema, directly return null, visitPredicate
   * will handle it.
   */
  @Override
  public Expression visitNameReference(NameReference attribute) {
    Types.Field field = caseSensitive
        ? recordType.fieldByName(attribute.getName())
        : recordType.fieldByNameCaseInsensitive(attribute.getName());

    if (field == null) {
      return null;
    }

    return new BoundReference(field.fieldId(), field.type());
  }

  /**
   * If an expression is null after accept method, which means it cannot be bounded from
   * schema, we'll directly return {@link Predicates.TrueExpression}.
   */
  @Override
  public Expression visitPredicate(Predicate predicate) {

    if (predicate instanceof Predicates.BinaryComparison) {
      Predicates.BinaryComparison binaryExp = (Predicates.BinaryComparison) predicate;
      Expression left = binaryExp.getLeft().accept(this);
      if (left == null) {
        return alwaysTrue();
      } else {
        Expression right = binaryExp.getRight().accept(this);
        if (right == null) {
          return alwaysTrue();
        }

        return new Predicates.BinaryComparison(left, binaryExp.getOperator(), right);
      }
    }

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

    if (predicate instanceof Predicates.In) {
      Predicates.In in = ((Predicates.In) predicate);
      Expression valueExpression = in.value.accept(this);
      if (valueExpression == null) {
        return alwaysTrue();
      }
      List<Expression> validValues = in.validValues.stream()
          .map(validValue -> validValue.accept(this))
          .collect(Collectors.toList());
      if (validValues.stream().anyMatch(Objects::isNull)) {
        return alwaysTrue();
      }
      return Predicates.in(valueExpression, validValues);
    }

    if (predicate instanceof Predicates.IsNull) {
      Predicates.IsNull isNull = (Predicates.IsNull) predicate;
      return Option.ofNullable(isNull.child.accept(this))
          .map(expr -> (Expression)Predicates.isNull(expr))
          .orElseGet(this::alwaysTrue);
    }

    if (predicate instanceof Predicates.IsNotNull) {
      Predicates.IsNotNull isNotNull = (Predicates.IsNotNull) predicate;
      return Option.ofNullable(isNotNull.child.accept(this))
          .map(expr -> (Expression)Predicates.isNotNull(expr))
          .orElseGet(this::alwaysTrue);
    }

    if (predicate instanceof Predicates.StringStartsWith) {
      Predicates.StringStartsWith startsWith = (Predicates.StringStartsWith) predicate;
      Expression left = startsWith.getLeft().accept(this);
      if (left == null) {
        return alwaysTrue();
      } else {
        Expression right = startsWith.getRight().accept(this);
        if (right == null) {
          return alwaysTrue();
        }

        return Predicates.startsWith(left, right);
      }
    }

    if (predicate instanceof Predicates.StringContains) {
      Predicates.StringContains contains = (Predicates.StringContains) predicate;
      Expression left = contains.getLeft().accept(this);
      if (left == null) {
        return alwaysTrue();
      } else {
        Expression right = contains.getRight().accept(this);
        if (right == null) {
          return alwaysTrue();
        }

        return Predicates.contains(left, right);
      }
    }

    throw new IllegalArgumentException("The expression " + predicate + " cannot be visited as predicate");
  }
}
