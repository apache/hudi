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

package org.apache.hudi.common.expression;

import org.apache.hudi.common.schema.internal.Types;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

/**
 * Tests {@link BindVisitor}.
 */
public class TestBindVisitor {

  private static Types.RecordType schema;

  @BeforeAll
  public static void init() {
    ArrayList<Types.Field> fields = new ArrayList<>(3);
    fields.add(Types.Field.get(0, true, "a", Types.StringType.get()));
    fields.add(Types.Field.get(1, true, "c", Types.IntType.get()));
    fields.add(Types.Field.get(2, true, "d", Types.LongType.get()));
    schema = Types.RecordType.get(fields, "schema");
  }

  @Test
  public void testVisitNameReferenceBindsExistingField() {
    BindVisitor bindVisitor = new BindVisitor(schema, true);
    Expression bound = new NameReference("a").accept(bindVisitor);

    Assertions.assertTrue(bound instanceof BoundReference);
    Assertions.assertEquals(Types.StringType.get(), bound.getDataType());
  }

  @Test
  public void testVisitNameReferenceThrowsForMissingField() {
    BindVisitor bindVisitor = new BindVisitor(schema, true);
    NameReference unbound = new NameReference("unknown");

    IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
        () -> unbound.accept(bindVisitor));
    Assertions.assertTrue(e.getMessage().contains("cannot be bound from schema"));
  }

  @Test
  public void testVisitNameReferenceCaseSensitiveFailsOnDifferentCase() {
    BindVisitor bindVisitor = new BindVisitor(schema, true);
    NameReference upperCase = new NameReference("A");

    Assertions.assertThrows(IllegalArgumentException.class, () -> upperCase.accept(bindVisitor));
  }

  @Test
  public void testVisitNameReferenceCaseInsensitiveBindsDifferentCase() {
    BindVisitor bindVisitor = new BindVisitor(schema, false);
    Expression bound = new NameReference("A").accept(bindVisitor);

    Assertions.assertTrue(bound instanceof BoundReference);
  }

  @Test
  public void testVisitLiteralReturnsSameInstance() {
    BindVisitor bindVisitor = new BindVisitor(schema, true);
    Literal<String> literal = Literal.from("value");

    Assertions.assertSame(literal, literal.accept(bindVisitor));
  }

  @Test
  public void testVisitBoundReferenceReturnsSameInstance() {
    BindVisitor bindVisitor = new BindVisitor(schema, true);
    BoundReference boundReference = new BoundReference(0, Types.StringType.get());

    Assertions.assertSame(boundReference, boundReference.accept(bindVisitor));
  }

  @Test
  public void testVisitAndBothOperandsBindAndEval() {
    BindVisitor bindVisitor = new BindVisitor(schema, true);
    Predicates.And and = Predicates.and(
        Predicates.eq(new NameReference("a"), Literal.from("Jane")),
        Predicates.gt(new NameReference("c"), Literal.from(10)));
    Expression bound = and.accept(bindVisitor);

    Assertions.assertTrue((Boolean) bound.eval(new ArrayData(Arrays.asList("Jane", 15, 5L))));
    Assertions.assertFalse((Boolean) bound.eval(new ArrayData(Arrays.asList("Jane", 5, 5L))));
  }

  @Test
  public void testVisitAndShortCircuitsWhenLeftIsFalseExpression() {
    BindVisitor bindVisitor = new BindVisitor(schema, true);
    Predicates.And and = Predicates.and(Predicates.alwaysFalse(),
        Predicates.eq(new NameReference("a"), Literal.from("Jane")));

    Assertions.assertTrue(and.accept(bindVisitor) instanceof Predicates.FalseExpression);
  }

  @Test
  public void testVisitAndShortCircuitsWhenRightIsFalseExpression() {
    BindVisitor bindVisitor = new BindVisitor(schema, true);
    Predicates.And and = Predicates.and(
        Predicates.eq(new NameReference("a"), Literal.from("Jane")), Predicates.alwaysFalse());

    Assertions.assertTrue(and.accept(bindVisitor) instanceof Predicates.FalseExpression);
  }

  @Test
  public void testVisitAndSimplifiesWhenBothBoundOperandsAreAlwaysTrue() {
    BindVisitor bindVisitor = new BindVisitor(schema, true);
    Predicates.And and = Predicates.and(Predicates.alwaysTrue(), Predicates.alwaysTrue());

    Assertions.assertTrue(and.accept(bindVisitor) instanceof Predicates.TrueExpression);
  }

  @Test
  public void testVisitAndSimplifiesWhenLeftBoundOperandIsAlwaysTrue() {
    BindVisitor bindVisitor = new BindVisitor(schema, true);
    Predicates.BinaryComparison rightPredicate = Predicates.eq(new NameReference("a"), Literal.from("Jane"));
    Predicates.And and = Predicates.and(Predicates.alwaysTrue(), rightPredicate);

    Expression bound = and.accept(bindVisitor);
    Assertions.assertTrue((Boolean) bound.eval(new ArrayData(Arrays.asList("Jane", 15, 5L))));
    Assertions.assertFalse((Boolean) bound.eval(new ArrayData(Arrays.asList("Lone", 15, 5L))));
  }

  @Test
  public void testVisitAndSimplifiesWhenRightBoundOperandIsAlwaysTrue() {
    BindVisitor bindVisitor = new BindVisitor(schema, true);
    Predicates.BinaryComparison leftPredicate = Predicates.eq(new NameReference("a"), Literal.from("Jane"));
    Predicates.And and = Predicates.and(leftPredicate, Predicates.alwaysTrue());

    Expression bound = and.accept(bindVisitor);
    Assertions.assertTrue((Boolean) bound.eval(new ArrayData(Arrays.asList("Jane", 15, 5L))));
    Assertions.assertFalse((Boolean) bound.eval(new ArrayData(Arrays.asList("Lone", 15, 5L))));
  }

  @Test
  public void testVisitOrBothOperandsBindAndEval() {
    BindVisitor bindVisitor = new BindVisitor(schema, true);
    Predicates.Or or = Predicates.or(
        Predicates.eq(new NameReference("a"), Literal.from("Jane")),
        Predicates.gt(new NameReference("c"), Literal.from(10)));
    Expression bound = or.accept(bindVisitor);

    Assertions.assertTrue((Boolean) bound.eval(new ArrayData(Arrays.asList("Lone", 15, 5L))));
    Assertions.assertFalse((Boolean) bound.eval(new ArrayData(Arrays.asList("Lone", 5, 5L))));
  }

  @Test
  public void testVisitOrShortCircuitsWhenLeftIsTrueExpression() {
    BindVisitor bindVisitor = new BindVisitor(schema, true);
    Predicates.Or or = Predicates.or(Predicates.alwaysTrue(),
        Predicates.eq(new NameReference("a"), Literal.from("Jane")));

    Assertions.assertTrue(or.accept(bindVisitor) instanceof Predicates.TrueExpression);
  }

  @Test
  public void testVisitOrShortCircuitsWhenRightIsTrueExpression() {
    BindVisitor bindVisitor = new BindVisitor(schema, true);
    Predicates.Or or = Predicates.or(
        Predicates.eq(new NameReference("a"), Literal.from("Jane")), Predicates.alwaysTrue());

    Assertions.assertTrue(or.accept(bindVisitor) instanceof Predicates.TrueExpression);
  }

  @Test
  public void testVisitOrSimplifiesWhenBothBoundOperandsAreAlwaysFalse() {
    BindVisitor bindVisitor = new BindVisitor(schema, true);
    Predicates.Or or = Predicates.or(Predicates.alwaysFalse(), Predicates.alwaysFalse());

    Assertions.assertTrue(or.accept(bindVisitor) instanceof Predicates.FalseExpression);
  }

  @Test
  public void testVisitOrSimplifiesWhenLeftBoundOperandIsAlwaysFalse() {
    BindVisitor bindVisitor = new BindVisitor(schema, true);
    Predicates.BinaryComparison rightPredicate = Predicates.eq(new NameReference("a"), Literal.from("Jane"));
    Predicates.Or or = Predicates.or(Predicates.alwaysFalse(), rightPredicate);

    Expression bound = or.accept(bindVisitor);
    Assertions.assertTrue((Boolean) bound.eval(new ArrayData(Arrays.asList("Jane", 15, 5L))));
    Assertions.assertFalse((Boolean) bound.eval(new ArrayData(Arrays.asList("Lone", 15, 5L))));
  }

  @Test
  public void testVisitOrSimplifiesWhenRightBoundOperandIsAlwaysFalse() {
    BindVisitor bindVisitor = new BindVisitor(schema, true);
    Predicates.BinaryComparison leftPredicate = Predicates.eq(new NameReference("a"), Literal.from("Jane"));
    Predicates.Or or = Predicates.or(leftPredicate, Predicates.alwaysFalse());

    Expression bound = or.accept(bindVisitor);
    Assertions.assertTrue((Boolean) bound.eval(new ArrayData(Arrays.asList("Jane", 15, 5L))));
    Assertions.assertFalse((Boolean) bound.eval(new ArrayData(Arrays.asList("Lone", 15, 5L))));
  }

  @Test
  public void testVisitPredicateNot() {
    BindVisitor bindVisitor = new BindVisitor(schema, true);
    Predicates.Not not = Predicates.not(Predicates.eq(new NameReference("a"), Literal.from("Jane")));
    Expression bound = not.accept(bindVisitor);

    Assertions.assertFalse((Boolean) bound.eval(new ArrayData(Arrays.asList("Jane", 15, 5L))));
    Assertions.assertTrue((Boolean) bound.eval(new ArrayData(Arrays.asList("Lone", 15, 5L))));
  }

  @Test
  public void testVisitPredicateNotOfAlwaysTrueChildIsAlwaysFalse() {
    BindVisitor bindVisitor = new BindVisitor(schema, true);
    Predicates.Not not = Predicates.not(Predicates.alwaysTrue());

    Assertions.assertTrue(not.accept(bindVisitor) instanceof Predicates.FalseExpression);
  }

  @Test
  public void testVisitPredicateNotOfAlwaysFalseChildIsAlwaysTrue() {
    BindVisitor bindVisitor = new BindVisitor(schema, true);
    Predicates.Not not = Predicates.not(Predicates.alwaysFalse());

    Assertions.assertTrue(not.accept(bindVisitor) instanceof Predicates.TrueExpression);
  }

  @Test
  public void testVisitPredicateBinaryComparison() {
    BindVisitor bindVisitor = new BindVisitor(schema, true);
    Predicates.BinaryComparison eq = Predicates.eq(new NameReference("c"), Literal.from(15));
    Expression bound = eq.accept(bindVisitor);

    Assertions.assertTrue(bound instanceof Predicates.BinaryComparison);
    Assertions.assertTrue((Boolean) bound.eval(new ArrayData(Arrays.asList("Jane", 15, 5L))));
  }

  @Test
  public void testVisitPredicateIn() {
    BindVisitor bindVisitor = new BindVisitor(schema, true);
    Predicates.In in = Predicates.in(new NameReference("c"),
        Arrays.asList(Literal.from(10), Literal.from(15)));
    Expression bound = in.accept(bindVisitor);

    Assertions.assertTrue((Boolean) bound.eval(new ArrayData(Arrays.asList("Jane", 15, 5L))));
    Assertions.assertFalse((Boolean) bound.eval(new ArrayData(Arrays.asList("Jane", 99, 5L))));
  }

  @Test
  public void testVisitPredicateIsNull() {
    BindVisitor bindVisitor = new BindVisitor(schema, true);
    Predicates.IsNull isNull = Predicates.isNull(new NameReference("a"));
    Expression bound = isNull.accept(bindVisitor);

    Assertions.assertTrue((Boolean) bound.eval(new ArrayData(Arrays.asList(null, 15, 5L))));
    Assertions.assertFalse((Boolean) bound.eval(new ArrayData(Arrays.asList("Jane", 15, 5L))));
  }

  @Test
  public void testVisitPredicateIsNotNull() {
    BindVisitor bindVisitor = new BindVisitor(schema, true);
    Predicates.IsNotNull isNotNull = Predicates.isNotNull(new NameReference("a"));
    Expression bound = isNotNull.accept(bindVisitor);

    Assertions.assertFalse((Boolean) bound.eval(new ArrayData(Arrays.asList(null, 15, 5L))));
    Assertions.assertTrue((Boolean) bound.eval(new ArrayData(Arrays.asList("Jane", 15, 5L))));
  }

  @Test
  public void testVisitPredicateStringStartsWith() {
    BindVisitor bindVisitor = new BindVisitor(schema, true);
    Predicates.StringStartsWith startsWith = Predicates.startsWith(new NameReference("a"), Literal.from("Ja"));
    Expression bound = startsWith.accept(bindVisitor);

    Assertions.assertTrue((Boolean) bound.eval(new ArrayData(Arrays.asList("Jane", 15, 5L))));
    Assertions.assertFalse((Boolean) bound.eval(new ArrayData(Arrays.asList("Lone", 15, 5L))));
  }

  @Test
  public void testVisitPredicateStringContains() {
    BindVisitor bindVisitor = new BindVisitor(schema, true);
    Predicates.StringContains contains = Predicates.contains(new NameReference("a"), Literal.from("an"));
    Expression bound = contains.accept(bindVisitor);

    Assertions.assertTrue((Boolean) bound.eval(new ArrayData(Arrays.asList("Jane", 15, 5L))));
    Assertions.assertFalse((Boolean) bound.eval(new ArrayData(Arrays.asList("Lone", 15, 5L))));
  }

  @Test
  public void testVisitPredicateThrowsForMissingFieldInsideBinaryComparison() {
    BindVisitor bindVisitor = new BindVisitor(schema, true);
    Predicates.BinaryComparison eq = Predicates.eq(new NameReference("unknown"), Literal.from("Jane"));

    Assertions.assertThrows(IllegalArgumentException.class, () -> eq.accept(bindVisitor));
  }

  @Test
  public void testUnsupportedPredicateErrorNamesTheExpression() {
    BindVisitor bindVisitor = new BindVisitor(schema, true);
    Predicates.StringStartsWithAny startsWithAny = Predicates.startsWithAny(new NameReference("a"),
        Collections.singletonList(Literal.from("Ja")));

    IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
        () -> startsWithAny.accept(bindVisitor));

    Assertions.assertTrue(e.getMessage().contains("NameReference(name=a).startsWithAny(Ja)"), e::getMessage);
    Assertions.assertFalse(e.getMessage().contains(BindVisitor.class.getName()), e::getMessage);
    Assertions.assertTrue(e.getMessage().contains(" cannot be visited as predicate"), e::getMessage);
  }
}
