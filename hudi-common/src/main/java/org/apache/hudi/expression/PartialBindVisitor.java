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
import java.util.Objects;
import java.util.stream.Collectors;

public class PartialBindVisitor extends BindVisitor {

  public PartialBindVisitor(Types.RecordType recordType, boolean caseSensitive) {
    super(recordType, caseSensitive);
  }

  /**
   * If the attribute cannot find from the schema, directly return null, visitPredicate
   * will handle it.
   */
  @Override
  public Expression visitAttribute(AttributeReference attribute) {
    // TODO Should consider caseSensitive?
    Types.Field field = recordType.field(attribute.getName());

    if (field == null) {
      return null;
    }

    return new BoundReference(field.fieldId(), field.type(), attribute.isNullable());
  }

  /**
   * If an expression is null after accept method, which means it cannot be bounded from
   * schema, we'll directly return {@link Predicates.True}.
   */
  @Override
  public Expression visitPredicate(Predicate predicate) {
    if (predicate instanceof Predicates.Not) {
      Expression expr = ((Predicates.Not) predicate).child.accept(this);
      if (expr instanceof Predicates.True) {
        return alwaysFalse();
      }
      if (expr instanceof Predicates.False) {
        return alwaysTrue();
      }

      return Predicates.not(expr);
    }

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

    throw new IllegalArgumentException("The expression " + this + "cannot be visited as predicate");
  }
}
