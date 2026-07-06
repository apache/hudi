/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.common.expression;

import org.apache.hudi.common.schema.internal.Types;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestPredicates {
  @Test
  void testStringStartsWithToString() {
    Predicates.StringStartsWith predicate = Predicates.startsWith(Literal.from("key"), Literal.from("k1"));
    assertEquals("key.startsWith(k1)", predicate.toString());
  }

  @Test
  void testStringStartsWithAnyWhenMatched() {
    Expression left = Literal.from("key2_any");
    List<Expression> right = Arrays.asList(
        Literal.from("key1"),
        Literal.from("key2"),
        Literal.from("key3"));
    Predicates.StringStartsWithAny predicate = Predicates.startsWithAny(left, right);
    assertEquals(Expression.Operator.STARTS_WITH, predicate.getOperator());
    assertTrue((boolean) predicate.eval(null));
  }

  @Test
  void testStringStartsWithAnyWhenNotMatched() {
    Expression left = Literal.from("key4_any");
    List<Expression> right = Arrays.asList(
        Literal.from("key1"),
        Literal.from("key2"),
        Literal.from("key3"));
    Predicates.StringStartsWithAny predicate = Predicates.startsWithAny(left, right);
    assertEquals(Expression.Operator.STARTS_WITH, predicate.getOperator());
    assertFalse((boolean) predicate.eval(null));
  }

  @Test
  void testStringStartsWithAnyToString() {
    Predicates.StringStartsWithAny predicate =
        Predicates.startsWithAny(Literal.from("key"), Arrays.asList(Literal.from("k1"), Literal.from("k2")));
    assertEquals("key.startsWithAny(k1,k2)", predicate.toString());
  }

  @Test
  void testStringStartsWithAnyToStringIsNullSafeForAbsentLeft() {
    Predicates.StringStartsWithAny predicate =
        Predicates.startsWithAny(null, Collections.singletonList(Literal.from("key1")));
    assertEquals("null.startsWithAny(key1)", predicate.toString());
  }

  @Test
  void testStringStartsWithAnyGetChildrenIncludesLeftAndAllRightValues() {
    Expression left = Literal.from("key");
    List<Expression> right = Arrays.asList(Literal.from("k1"), Literal.from("k2"));
    Predicates.StringStartsWithAny predicate = Predicates.startsWithAny(left, right);

    assertEquals(Arrays.asList(left, right.get(0), right.get(1)), predicate.getChildren());
    assertEquals(right, predicate.getRightChildren());
  }

  @Test
  void testTrueExpressionEvalAndDispatch() {
    Predicates.TrueExpression trueExpr = Predicates.alwaysTrue();
    assertTrue(trueExpr.eval(null));
    assertEquals(Expression.Operator.TRUE, trueExpr.getOperator());
    assertEquals("TRUE", trueExpr.toString());
    assertEquals("visitedAlwaysTrue", trueExpr.accept(new TaggingVisitor()));
  }

  @Test
  void testFalseExpressionEvalAndDispatch() {
    Predicates.FalseExpression falseExpr = Predicates.alwaysFalse();
    assertFalse(falseExpr.eval(null));
    assertEquals(Expression.Operator.FALSE, falseExpr.getOperator());
    assertEquals("FALSE", falseExpr.toString());
    assertEquals("visitedAlwaysFalse", falseExpr.accept(new TaggingVisitor()));
  }

  @Test
  void testAndEvalBothTrue() {
    assertTrue(Predicates.and(Literal.from(true), Literal.from(true)).eval(null));
  }

  @Test
  void testAndEvalLeftFalse() {
    assertFalse(Predicates.and(Literal.from(false), Literal.from(true)).eval(null));
  }

  @Test
  void testAndEvalRightFalse() {
    assertFalse(Predicates.and(Literal.from(true), Literal.from(false)).eval(null));
  }

  @Test
  void testAndEvalShortCircuitsWhenLeftIsFalseExpression() {
    assertFalse(Predicates.and(Predicates.alwaysFalse(), new NameReference("unbound")).eval(null));
  }

  @Test
  void testAndEvalShortCircuitsWhenRightIsFalseExpression() {
    assertFalse(Predicates.and(new NameReference("unbound"), Predicates.alwaysFalse()).eval(null));
  }

  @Test
  void testAndEvalWithNullLeftIsFalse() {
    Literal<Boolean> nullBool = new Literal<>(null, Types.BooleanType.get());
    assertFalse(Predicates.and(nullBool, Literal.from(true)).eval(null));
  }

  @Test
  void testAndEvalWithNullRightIsFalse() {
    Literal<Boolean> nullBool = new Literal<>(null, Types.BooleanType.get());
    assertFalse(Predicates.and(Literal.from(true), nullBool).eval(null));
  }

  @Test
  void testOrEvalShortCircuitsWhenLeftIsTrueExpression() {
    assertTrue(Predicates.or(Predicates.alwaysTrue(), new NameReference("unbound")).eval(null));
  }

  @Test
  void testOrEvalShortCircuitsWhenRightIsTrueExpression() {
    assertTrue(Predicates.or(new NameReference("unbound"), Predicates.alwaysTrue()).eval(null));
  }

  @Test
  void testOrEvalShortCircuitsWhenLeftIsTrueLiteral() {
    // once left evaluates to true, right should never be evaluated
    assertTrue(Predicates.or(Literal.from(true), new NameReference("unbound")).eval(null));
  }

  @Test
  void testOrEvalWithNullLeftIsFalseEvenIfRightIsTrue() {
    Literal<Boolean> nullBool = new Literal<>(null, Types.BooleanType.get());
    assertFalse(Predicates.or(nullBool, Literal.from(true)).eval(null));
  }

  @Test
  void testOrEvalWithFalseLeftAndNullRightIsFalse() {
    Literal<Boolean> nullBool = new Literal<>(null, Types.BooleanType.get());
    assertFalse(Predicates.or(Literal.from(false), nullBool).eval(null));
  }

  @Test
  void testOrEvalWithFalseLeftAndTrueRight() {
    assertTrue(Predicates.or(Literal.from(false), Literal.from(true)).eval(null));
  }

  @Test
  void testNotEval() {
    assertFalse(Predicates.not(Literal.from(true)).eval(null));
    assertTrue(Predicates.not(Literal.from(false)).eval(null));
  }

  @Test
  void testNotGetChildren() {
    Expression child = Literal.from(true);
    assertEquals(java.util.Collections.singletonList(child), Predicates.not(child).getChildren());
  }

  @Test
  void testIsNullEvalTrueForNullValue() {
    Literal<String> nullValue = new Literal<>(null, Types.StringType.get());
    assertTrue(Predicates.isNull(nullValue).eval(null));
  }

  @Test
  void testIsNullEvalFalseForNonNullValue() {
    assertFalse(Predicates.isNull(Literal.from("value")).eval(null));
  }

  @Test
  void testIsNotNullEvalFalseForNullValue() {
    Literal<String> nullValue = new Literal<>(null, Types.StringType.get());
    assertFalse(Predicates.isNotNull(nullValue).eval(null));
  }

  @Test
  void testIsNotNullEvalTrueForNonNullValue() {
    assertTrue(Predicates.isNotNull(Literal.from("value")).eval(null));
  }

  @Test
  void testInEvalTrueWhenValueMatches() {
    Predicates.In in = Predicates.in(Literal.from("b"),
        Arrays.asList(Literal.from("a"), Literal.from("b"), Literal.from("c")));
    assertTrue(in.eval(null));
  }

  @Test
  void testInEvalFalseWhenValueDoesNotMatch() {
    Predicates.In in = Predicates.in(Literal.from("z"),
        Arrays.asList(Literal.from("a"), Literal.from("b"), Literal.from("c")));
    assertFalse(in.eval(null));
  }

  @Test
  void testInGetChildrenIncludesValueAndValidValues() {
    Expression value = Literal.from("b");
    List<Expression> validValues = Arrays.asList(Literal.from("a"), Literal.from("b"));
    Predicates.In in = Predicates.in(value, validValues);

    List<Expression> expected = new ArrayList<>();
    expected.add(value);
    expected.addAll(validValues);
    assertEquals(expected, in.getChildren());
    assertEquals(validValues, in.getRightChildren());
  }

  @Test
  void testStringStartsWithEval() {
    Predicates.StringStartsWith startsWith = Predicates.startsWith(Literal.from("hoodie"), Literal.from("hoo"));
    assertTrue((boolean) startsWith.eval(null));

    Predicates.StringStartsWith noMatch = Predicates.startsWith(Literal.from("hoodie"), Literal.from("bar"));
    assertFalse((boolean) noMatch.eval(null));
  }

  @Test
  void testStringContainsEval() {
    Predicates.StringContains contains = Predicates.contains(Literal.from("hoodie"), Literal.from("ood"));
    assertTrue((boolean) contains.eval(null));

    Predicates.StringContains noMatch = Predicates.contains(Literal.from("hoodie"), Literal.from("xyz"));
    assertFalse((boolean) noMatch.eval(null));
  }

  @Test
  void testBinaryComparisonEvalForEachOperator() {
    assertTrue(Predicates.eq(Literal.from(5), Literal.from(5)).eval(null));
    assertTrue(Predicates.gt(Literal.from(5), Literal.from(3)).eval(null));
    assertTrue(Predicates.gteq(Literal.from(5), Literal.from(5)).eval(null));
    assertTrue(Predicates.lt(Literal.from(3), Literal.from(5)).eval(null));
    assertTrue(Predicates.lteq(Literal.from(5), Literal.from(5)).eval(null));

    assertFalse(Predicates.eq(Literal.from(5), Literal.from(6)).eval(null));
    assertFalse(Predicates.gt(Literal.from(3), Literal.from(5)).eval(null));
    assertFalse(Predicates.lt(Literal.from(5), Literal.from(3)).eval(null));
  }

  @Test
  void testBinaryComparisonThrowsForNestedLeftType() {
    ArrayList<Types.Field> fields = new ArrayList<>();
    fields.add(Types.Field.get(0, true, "a", Types.StringType.get()));
    Types.RecordType nestedType = Types.RecordType.get(fields, "nested");
    BoundReference nestedRef = new BoundReference(0, nestedType);

    Predicates.BinaryComparison comparison = Predicates.eq(nestedRef, Literal.from("value"));
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> comparison.eval(null));
    assertTrue(e.getMessage().contains("nested type doesn't support binary comparison"));
  }

  @Test
  void testBinaryComparisonThrowsForUnsupportedOperator() {
    Predicates.BinaryComparison comparison =
        new Predicates.BinaryComparison(Literal.from(1), Expression.Operator.AND, Literal.from(1));
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> comparison.eval(null));
    assertTrue(e.getMessage().contains("doesn't support binary comparison"));
  }

  /**
   * Minimal visitor used to confirm that {@code accept} dispatches to the expected
   * {@link ExpressionVisitor} callback rather than some other overload.
   */
  private static class TaggingVisitor implements ExpressionVisitor<String> {
    @Override
    public String alwaysTrue() {
      return "visitedAlwaysTrue";
    }

    @Override
    public String alwaysFalse() {
      return "visitedAlwaysFalse";
    }

    @Override
    public String visitAnd(Predicates.And and) {
      return "visitedAnd";
    }

    @Override
    public String visitOr(Predicates.Or or) {
      return "visitedOr";
    }

    @Override
    public String visitLiteral(Literal literal) {
      return "visitedLiteral";
    }

    @Override
    public String visitNameReference(NameReference attribute) {
      return "visitedNameReference";
    }

    @Override
    public String visitBoundReference(BoundReference boundReference) {
      return "visitedBoundReference";
    }

    @Override
    public String visitPredicate(Predicate predicate) {
      return "visitedPredicate";
    }
  }
}
