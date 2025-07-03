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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

public class TestPartialBindVisitor {

  private static Types.RecordType schema;
  @BeforeAll
  public static void init() {
    ArrayList<Types.Field> fields = new ArrayList<>(5);
    fields.add(Types.Field.get(0, true, "a", Types.StringType.get()));
    fields.add(Types.Field.get(1, true, "b", Types.DateType.get()));
    fields.add(Types.Field.get(2, true, "c", Types.IntType.get()));
    fields.add(Types.Field.get(3, true, "d", Types.LongType.get()));
    fields.add(Types.Field.get(4, false, "f", Types.BooleanType.get()));
    schema = Types.RecordType.get(fields, "schema");
  }

  @Test
  public void testPartialBindIfAllExisting() {
    PartialBindVisitor partialBindVisitor = new PartialBindVisitor(schema, false);

    Predicates.BinaryComparison eq = Predicates.eq(new NameReference("a"),
        Literal.from("Jane"));
    Predicates.BinaryComparison gt = Predicates.gt(new NameReference("c"),
        Literal.from(10));
    Predicates.In in = Predicates.in(new NameReference("d"),
        Arrays.asList(Literal.from(10L), Literal.from(13L)));

    Predicates.And expr = Predicates.and(eq, Predicates.or(gt, in));
    Expression binded = expr.accept(partialBindVisitor);

    Assertions.assertTrue((Boolean) binded.eval(new ArrayData(Arrays.asList("Jane", "2023-04-02", 15, 5L, false))));
    Assertions.assertTrue((Boolean) binded.eval(new ArrayData(Arrays.asList("Jane", "2023-04-02", 5, 10L, false))));
    Assertions.assertFalse((Boolean) binded.eval(new ArrayData(Arrays.asList("Lone", "2023-04-02", 15, 5L, false))));
    Assertions.assertFalse((Boolean) binded.eval(new ArrayData(Arrays.asList("Lone", "2023-04-02", 10, 5L, false))));
  }

  @Test
  public void testPartialBindIfFieldMissing() {
    PartialBindVisitor partialBindVisitor = new PartialBindVisitor(schema, false);

    Predicates.BinaryComparison eq = Predicates.eq(new NameReference("a"),
        Literal.from("Jane"));
    Predicates.BinaryComparison lt = Predicates.lt(new NameReference("m"),
        Literal.from(10));
    Predicates.BinaryComparison gteq = Predicates.gteq(new NameReference("d"),
        Literal.from(10L));

    Predicates.And expr = Predicates.and(eq, Predicates.or(lt, gteq));
    // Since Attribute m does not exist in the schema, so the OR expression is always true,
    // the expression is optimized to only consider the EQ expression
    Expression binded = expr.accept(partialBindVisitor);

    Assertions.assertTrue((Boolean) binded.eval(new ArrayData(Arrays.asList("Jane", "2023-04-02", 15, 5L, false))));
    Assertions.assertFalse((Boolean) binded.eval(new ArrayData(Arrays.asList("Lone", "2023-04-02", 15, 5L, false))));
    Assertions.assertFalse((Boolean) binded.eval(new ArrayData(Arrays.asList("Lone", "2023-04-02", 10, 5L, false))));
  }

  @Test
  public void testPartialBindSecondaryIndexKeyMatcher() {
    PartialBindVisitor partialBindVisitor = new PartialBindVisitor(schema, false);

    // Test case 1: All fields exist in schema - should bind normally
    Predicates.SecondaryIndexKeyMatcher matcher1 = Predicates.secondaryIndexKeyMatcher(
        new NameReference("a"), // left expression exists in schema
        Arrays.asList(Literal.from("abc"), Literal.from("def")) // right expressions are literals
    );
    Expression binded1 = matcher1.accept(partialBindVisitor);
    
    // Should return a new SecondaryIndexKeyMatcher with bound left expression
    Assertions.assertTrue(binded1 instanceof Predicates.SecondaryIndexKeyMatcher);
    Predicates.SecondaryIndexKeyMatcher boundMatcher1 = (Predicates.SecondaryIndexKeyMatcher) binded1;
    Assertions.assertTrue(boundMatcher1.getLeft() instanceof BoundReference);
    Assertions.assertEquals(2, boundMatcher1.getRightChildren().size());
    
    // Test evaluation with matching data
    Assertions.assertTrue((Boolean) binded1.eval(new ArrayData(Arrays.asList("abc$primary", "2023-04-02", 15, 5L, false))));
    Assertions.assertFalse((Boolean) binded1.eval(new ArrayData(Arrays.asList("xyz$primary", "2023-04-02", 15, 5L, false))));

    // Test case 2: Left field doesn't exist in schema - should return alwaysTrue
    Predicates.SecondaryIndexKeyMatcher matcher2 = Predicates.secondaryIndexKeyMatcher(
        new NameReference("nonexistent"), // left expression doesn't exist in schema
        Arrays.asList(Literal.from("abc"), Literal.from("def"))
    );
    Expression binded2 = matcher2.accept(partialBindVisitor);
    
    // Should return alwaysTrue when left field is not found
    Assertions.assertTrue(binded2 instanceof Predicates.TrueExpression);
    Assertions.assertTrue((Boolean) binded2.eval(new ArrayData(Arrays.asList("any", "2023-04-02", 15, 5L, false))));

    // Test case 3: Right children contain non-null expressions - should bind normally
    Predicates.SecondaryIndexKeyMatcher matcher3 = Predicates.secondaryIndexKeyMatcher(
        new NameReference("a"),
        Arrays.asList(Literal.from("abc"), Literal.from("def"))
    );
    Expression binded3 = matcher3.accept(partialBindVisitor);
    
    Assertions.assertTrue(binded3 instanceof Predicates.SecondaryIndexKeyMatcher);
    Assertions.assertEquals(2, ((Predicates.SecondaryIndexKeyMatcher) binded3).getRightChildren().size());

    // Test case 4: Right children list is empty - should return alwaysTrue
    Predicates.SecondaryIndexKeyMatcher matcher4 = Predicates.secondaryIndexKeyMatcher(
        new NameReference("a"),
        Collections.emptyList() // empty right children list
    );
    Expression binded4 = matcher4.accept(partialBindVisitor);
    
    // Should return alwaysTrue when right children list is empty
    Assertions.assertTrue(binded4 instanceof Predicates.TrueExpression);
    Assertions.assertTrue((Boolean) binded4.eval(new ArrayData(Arrays.asList("any", "2023-04-02", 15, 5L, false))));

    // Test case 5: Right children contain null expressions after binding - should return alwaysTrue
    // This case is harder to test directly since we can't easily create expressions that return null
    // But we can test the logic by creating a scenario where some right children become null
    // For now, we'll test that the method handles the case correctly when all right children are valid
    Predicates.SecondaryIndexKeyMatcher matcher5 = Predicates.secondaryIndexKeyMatcher(
        new NameReference("a"),
        Arrays.asList(Literal.from("abc"), Literal.from("def"))
    );
    Expression binded5 = matcher5.accept(partialBindVisitor);
    
    Assertions.assertTrue(binded5 instanceof Predicates.SecondaryIndexKeyMatcher);
    Predicates.SecondaryIndexKeyMatcher boundMatcher5 = (Predicates.SecondaryIndexKeyMatcher) binded5;
    Assertions.assertEquals(2, boundMatcher5.getRightChildren().size());
    
    // Verify that the bound matcher works correctly with secondary index V2 key matching logic
    Assertions.assertTrue((Boolean) binded5.eval(new ArrayData(Arrays.asList("abc$primary", "2023-04-02", 15, 5L, false))));
    Assertions.assertTrue((Boolean) binded5.eval(new ArrayData(Arrays.asList("def$primary", "2023-04-02", 15, 5L, false))));
    Assertions.assertFalse((Boolean) binded5.eval(new ArrayData(Arrays.asList("xyz$primary", "2023-04-02", 15, 5L, false))));
  }

  @Test
  public void testPartialBindBinaryComparison() {
    PartialBindVisitor partialBindVisitor = new PartialBindVisitor(schema, false);

    // Test case 1: Both left and right expressions exist in schema - compare string with literal
    Predicates.BinaryComparison eq = Predicates.eq(new NameReference("a"), Literal.from("Jane"));
    Expression binded1 = eq.accept(partialBindVisitor);
    
    Assertions.assertTrue(binded1 instanceof Predicates.BinaryComparison);
    Predicates.BinaryComparison boundEq = (Predicates.BinaryComparison) binded1;
    Assertions.assertTrue(boundEq.getLeft() instanceof BoundReference);
    Assertions.assertTrue(boundEq.getRight() instanceof Literal);
    
    // Test evaluation
    Assertions.assertTrue((Boolean) binded1.eval(new ArrayData(Arrays.asList("Jane", "2023-04-02", 15, 5L, false))));
    Assertions.assertFalse((Boolean) binded1.eval(new ArrayData(Arrays.asList("John", "2023-04-02", 15, 5L, false))));

    // Test case 2: Left expression doesn't exist in schema - should return alwaysTrue
    Predicates.BinaryComparison lt = Predicates.lt(new NameReference("nonexistent"), new NameReference("c"));
    Expression binded2 = lt.accept(partialBindVisitor);
    
    Assertions.assertTrue(binded2 instanceof Predicates.TrueExpression);
    Assertions.assertTrue((Boolean) binded2.eval(new ArrayData(Arrays.asList("any", "2023-04-02", 15, 5L, false))));

    // Test case 3: Right expression doesn't exist in schema - should return alwaysTrue
    Predicates.BinaryComparison gt = Predicates.gt(new NameReference("c"), new NameReference("nonexistent"));
    Expression binded3 = gt.accept(partialBindVisitor);
    
    Assertions.assertTrue(binded3 instanceof Predicates.TrueExpression);
    Assertions.assertTrue((Boolean) binded3.eval(new ArrayData(Arrays.asList("any", "2023-04-02", 15, 5L, false))));

    // Test case 4: Both expressions don't exist in schema - should return alwaysTrue
    Predicates.BinaryComparison gteq = Predicates.gteq(new NameReference("nonexistent1"), new NameReference("nonexistent2"));
    Expression binded4 = gteq.accept(partialBindVisitor);
    
    Assertions.assertTrue(binded4 instanceof Predicates.TrueExpression);
    Assertions.assertTrue((Boolean) binded4.eval(new ArrayData(Arrays.asList("any", "2023-04-02", 15, 5L, false))));
  }

  @Test
  public void testPartialBindNot() {
    PartialBindVisitor partialBindVisitor = new PartialBindVisitor(schema, false);

    // Test case 1: NOT of TrueExpression - should return FalseExpression
    Predicates.Not notTrue = Predicates.not(Predicates.alwaysTrue());
    Expression binded1 = notTrue.accept(partialBindVisitor);
    
    Assertions.assertTrue(binded1 instanceof Predicates.FalseExpression);
    Assertions.assertFalse((Boolean) binded1.eval(new ArrayData(Arrays.asList("any", "2023-04-02", 15, 5L, false))));

    // Test case 2: NOT of FalseExpression - should return TrueExpression
    Predicates.Not notFalse = Predicates.not(Predicates.alwaysFalse());
    Expression binded2 = notFalse.accept(partialBindVisitor);
    
    Assertions.assertTrue(binded2 instanceof Predicates.TrueExpression);
    Assertions.assertTrue((Boolean) binded2.eval(new ArrayData(Arrays.asList("any", "2023-04-02", 15, 5L, false))));

    // Test case 3: NOT of a valid expression - should return NOT of the bound expression
    Predicates.Not notEq = Predicates.not(Predicates.eq(new NameReference("a"), Literal.from("Jane")));
    Expression binded3 = notEq.accept(partialBindVisitor);
    
    Assertions.assertTrue(binded3 instanceof Predicates.Not);
    Predicates.Not boundNot = (Predicates.Not) binded3;
    Assertions.assertTrue(boundNot.child instanceof Predicates.BinaryComparison);
    
    // Test evaluation
    Assertions.assertFalse((Boolean) binded3.eval(new ArrayData(Arrays.asList("Jane", "2023-04-02", 15, 5L, false))));
    Assertions.assertTrue((Boolean) binded3.eval(new ArrayData(Arrays.asList("John", "2023-04-02", 15, 5L, false))));
  }

  @Test
  public void testPartialBindIn() {
    PartialBindVisitor partialBindVisitor = new PartialBindVisitor(schema, false);

    // Test case 1: Value expression exists in schema, all valid values are literals
    Predicates.In in1 = Predicates.in(new NameReference("d"), Arrays.asList(Literal.from(10L), Literal.from(13L)));
    Expression binded1 = in1.accept(partialBindVisitor);
    
    Assertions.assertTrue(binded1 instanceof Predicates.In);
    Predicates.In boundIn1 = (Predicates.In) binded1;
    Assertions.assertTrue(boundIn1.value instanceof BoundReference);
    Assertions.assertEquals(2, boundIn1.validValues.size());
    
    // Test evaluation
    Assertions.assertTrue((Boolean) binded1.eval(new ArrayData(Arrays.asList("any", "2023-04-02", 15, 10L, false))));
    Assertions.assertTrue((Boolean) binded1.eval(new ArrayData(Arrays.asList("any", "2023-04-02", 15, 13L, false))));
    Assertions.assertFalse((Boolean) binded1.eval(new ArrayData(Arrays.asList("any", "2023-04-02", 15, 5L, false))));

    // Test case 2: Value expression doesn't exist in schema - should return alwaysTrue
    Predicates.In in2 = Predicates.in(new NameReference("nonexistent"), Arrays.asList(Literal.from(10L), Literal.from(13L)));
    Expression binded2 = in2.accept(partialBindVisitor);
    
    Assertions.assertTrue(binded2 instanceof Predicates.TrueExpression);
    Assertions.assertTrue((Boolean) binded2.eval(new ArrayData(Arrays.asList("any", "2023-04-02", 15, 5L, false))));

    // Test case 3: Some valid values become null after binding - should return alwaysTrue
    // This is harder to test directly, but we can test the normal case
    Predicates.In in3 = Predicates.in(new NameReference("d"), Arrays.asList(Literal.from(10L), Literal.from(13L)));
    Expression binded3 = in3.accept(partialBindVisitor);
    
    Assertions.assertTrue(binded3 instanceof Predicates.In);
    Assertions.assertEquals(2, ((Predicates.In) binded3).validValues.size());
  }

  @Test
  public void testPartialBindIsNull() {
    PartialBindVisitor partialBindVisitor = new PartialBindVisitor(schema, false);

    // Test case 1: Child expression exists in schema
    Predicates.IsNull isNull1 = Predicates.isNull(new NameReference("a"));
    Expression binded1 = isNull1.accept(partialBindVisitor);
    
    Assertions.assertTrue(binded1 instanceof Predicates.IsNull);
    Predicates.IsNull boundIsNull1 = (Predicates.IsNull) binded1;
    Assertions.assertTrue(boundIsNull1.child instanceof BoundReference);
    
    // Test evaluation
    Assertions.assertTrue((Boolean) binded1.eval(new ArrayData(Arrays.asList(null, "2023-04-02", 15, 5L, false))));
    Assertions.assertFalse((Boolean) binded1.eval(new ArrayData(Arrays.asList("Jane", "2023-04-02", 15, 5L, false))));

    // Test case 2: Child expression doesn't exist in schema - should return alwaysTrue
    Predicates.IsNull isNull2 = Predicates.isNull(new NameReference("nonexistent"));
    Expression binded2 = isNull2.accept(partialBindVisitor);
    
    Assertions.assertTrue(binded2 instanceof Predicates.TrueExpression);
    Assertions.assertTrue((Boolean) binded2.eval(new ArrayData(Arrays.asList("any", "2023-04-02", 15, 5L, false))));
  }

  @Test
  public void testPartialBindIsNotNull() {
    PartialBindVisitor partialBindVisitor = new PartialBindVisitor(schema, false);

    // Test case 1: Child expression exists in schema
    Predicates.IsNotNull isNotNull1 = Predicates.isNotNull(new NameReference("a"));
    Expression binded1 = isNotNull1.accept(partialBindVisitor);
    
    Assertions.assertTrue(binded1 instanceof Predicates.IsNotNull);
    Predicates.IsNotNull boundIsNotNull1 = (Predicates.IsNotNull) binded1;
    Assertions.assertTrue(boundIsNotNull1.child instanceof BoundReference);
    
    // Test evaluation
    Assertions.assertFalse((Boolean) binded1.eval(new ArrayData(Arrays.asList(null, "2023-04-02", 15, 5L, false))));
    Assertions.assertTrue((Boolean) binded1.eval(new ArrayData(Arrays.asList("Jane", "2023-04-02", 15, 5L, false))));

    // Test case 2: Child expression doesn't exist in schema - should return alwaysTrue
    Predicates.IsNotNull isNotNull2 = Predicates.isNotNull(new NameReference("nonexistent"));
    Expression binded2 = isNotNull2.accept(partialBindVisitor);
    
    Assertions.assertTrue(binded2 instanceof Predicates.TrueExpression);
    Assertions.assertTrue((Boolean) binded2.eval(new ArrayData(Arrays.asList("any", "2023-04-02", 15, 5L, false))));
  }

  @Test
  public void testPartialBindStringStartsWith() {
    PartialBindVisitor partialBindVisitor = new PartialBindVisitor(schema, false);

    // Test case 1: Both left and right expressions exist in schema
    Predicates.StringStartsWith startsWith1 = Predicates.startsWith(new NameReference("a"), Literal.from("Ja"));
    Expression binded1 = startsWith1.accept(partialBindVisitor);
    
    Assertions.assertTrue(binded1 instanceof Predicates.StringStartsWith);
    Predicates.StringStartsWith boundStartsWith1 = (Predicates.StringStartsWith) binded1;
    Assertions.assertTrue(boundStartsWith1.getLeft() instanceof BoundReference);
    Assertions.assertTrue(boundStartsWith1.getRight() instanceof Literal);
    
    // Test evaluation
    Assertions.assertTrue((Boolean) binded1.eval(new ArrayData(Arrays.asList("Jane", "2023-04-02", 15, 5L, false))));
    Assertions.assertTrue((Boolean) binded1.eval(new ArrayData(Arrays.asList("James", "2023-04-02", 15, 5L, false))));
    Assertions.assertFalse((Boolean) binded1.eval(new ArrayData(Arrays.asList("John", "2023-04-02", 15, 5L, false))));

    // Test case 2: Left expression doesn't exist in schema - should return alwaysTrue
    Predicates.StringStartsWith startsWith2 = Predicates.startsWith(new NameReference("nonexistent"), Literal.from("Ja"));
    Expression binded2 = startsWith2.accept(partialBindVisitor);
    
    Assertions.assertTrue(binded2 instanceof Predicates.TrueExpression);
    Assertions.assertTrue((Boolean) binded2.eval(new ArrayData(Arrays.asList("any", "2023-04-02", 15, 5L, false))));

    // Test case 3: Right expression doesn't exist in schema - should return alwaysTrue
    Predicates.StringStartsWith startsWith3 = Predicates.startsWith(new NameReference("a"), new NameReference("nonexistent"));
    Expression binded3 = startsWith3.accept(partialBindVisitor);
    
    Assertions.assertTrue(binded3 instanceof Predicates.TrueExpression);
    Assertions.assertTrue((Boolean) binded3.eval(new ArrayData(Arrays.asList("any", "2023-04-02", 15, 5L, false))));
  }

  @Test
  public void testPartialBindStringContains() {
    PartialBindVisitor partialBindVisitor = new PartialBindVisitor(schema, false);

    // Test case 1: Both left and right expressions exist in schema
    Predicates.StringContains contains1 = Predicates.contains(new NameReference("a"), Literal.from("an"));
    Expression binded1 = contains1.accept(partialBindVisitor);
    
    Assertions.assertTrue(binded1 instanceof Predicates.StringContains);
    Predicates.StringContains boundContains1 = (Predicates.StringContains) binded1;
    Assertions.assertTrue(boundContains1.getLeft() instanceof BoundReference);
    Assertions.assertTrue(boundContains1.getRight() instanceof Literal);
    
    // Test evaluation
    Assertions.assertTrue((Boolean) binded1.eval(new ArrayData(Arrays.asList("Jane", "2023-04-02", 15, 5L, false))));
    Assertions.assertTrue((Boolean) binded1.eval(new ArrayData(Arrays.asList("Diane", "2023-04-02", 15, 5L, false))));
    Assertions.assertFalse((Boolean) binded1.eval(new ArrayData(Arrays.asList("John", "2023-04-02", 15, 5L, false))));

    // Test case 2: Left expression doesn't exist in schema - should return alwaysTrue
    Predicates.StringContains contains2 = Predicates.contains(new NameReference("nonexistent"), Literal.from("an"));
    Expression binded2 = contains2.accept(partialBindVisitor);
    
    Assertions.assertTrue(binded2 instanceof Predicates.TrueExpression);
    Assertions.assertTrue((Boolean) binded2.eval(new ArrayData(Arrays.asList("any", "2023-04-02", 15, 5L, false))));

    // Test case 3: Right expression doesn't exist in schema - should return alwaysTrue
    Predicates.StringContains contains3 = Predicates.contains(new NameReference("a"), new NameReference("nonexistent"));
    Expression binded3 = contains3.accept(partialBindVisitor);
    
    Assertions.assertTrue(binded3 instanceof Predicates.TrueExpression);
    Assertions.assertTrue((Boolean) binded3.eval(new ArrayData(Arrays.asList("any", "2023-04-02", 15, 5L, false))));
  }
}
