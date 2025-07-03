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

public class TestBindVisitor {

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
  public void testBindVisitorBinaryComparison() {
    BindVisitor bindVisitor = new BindVisitor(schema, false);

    // All fields exist
    Predicates.BinaryComparison eq = Predicates.eq(new NameReference("a"), Literal.from("Jane"));
    Expression bound = eq.accept(bindVisitor);
    Assertions.assertTrue(bound instanceof Predicates.BinaryComparison);
    Assertions.assertTrue((Boolean) bound.eval(new ArrayData(Arrays.asList("Jane", "2023-04-02", 15, 5L, false))));
    Assertions.assertFalse((Boolean) bound.eval(new ArrayData(Arrays.asList("John", "2023-04-02", 15, 5L, false))));

    // Field missing: should throw
    Predicates.BinaryComparison eqMissing = Predicates.eq(new NameReference("missing"), Literal.from("Jane"));
    Assertions.assertThrows(IllegalArgumentException.class, () -> eqMissing.accept(bindVisitor));
  }

  @Test
  public void testBindVisitorNot() {
    BindVisitor bindVisitor = new BindVisitor(schema, false);

    // NOT True -> False
    Predicates.Not notTrue = Predicates.not(Predicates.alwaysTrue());
    Expression bound = notTrue.accept(bindVisitor);
    Assertions.assertTrue(bound instanceof Predicates.FalseExpression);
    Assertions.assertFalse((Boolean) bound.eval(new ArrayData(Arrays.asList("any", "2023-04-02", 15, 5L, false))));

    // NOT False -> True
    Predicates.Not notFalse = Predicates.not(Predicates.alwaysFalse());
    Expression bound2 = notFalse.accept(bindVisitor);
    Assertions.assertTrue(bound2 instanceof Predicates.TrueExpression);
    Assertions.assertTrue((Boolean) bound2.eval(new ArrayData(Arrays.asList("any", "2023-04-02", 15, 5L, false))));

    // NOT of a valid expression
    Predicates.Not notEq = Predicates.not(Predicates.eq(new NameReference("a"), Literal.from("Jane")));
    Expression bound3 = notEq.accept(bindVisitor);
    Assertions.assertTrue(bound3 instanceof Predicates.Not);
    Assertions.assertFalse((Boolean) bound3.eval(new ArrayData(Arrays.asList("Jane", "2023-04-02", 15, 5L, false))));
    Assertions.assertTrue((Boolean) bound3.eval(new ArrayData(Arrays.asList("John", "2023-04-02", 15, 5L, false))));

    // NOT with missing field: should throw
    Predicates.Not notMissing = Predicates.not(Predicates.eq(new NameReference("missing"), Literal.from("Jane")));
    Assertions.assertThrows(IllegalArgumentException.class, () -> notMissing.accept(bindVisitor));
  }

  @Test
  public void testBindVisitorIn() {
    BindVisitor bindVisitor = new BindVisitor(schema, false);

    // All fields exist
    Predicates.In in = Predicates.in(new NameReference("d"), Arrays.asList(Literal.from(10L), Literal.from(13L)));
    Expression bound = in.accept(bindVisitor);
    Assertions.assertTrue(bound instanceof Predicates.In);
    Assertions.assertTrue((Boolean) bound.eval(new ArrayData(Arrays.asList("any", "2023-04-02", 15, 10L, false))));
    Assertions.assertTrue((Boolean) bound.eval(new ArrayData(Arrays.asList("any", "2023-04-02", 15, 13L, false))));
    Assertions.assertFalse((Boolean) bound.eval(new ArrayData(Arrays.asList("any", "2023-04-02", 15, 5L, false))));

    // Field missing: should throw
    Predicates.In inMissing = Predicates.in(new NameReference("missing"), Arrays.asList(Literal.from(10L)));
    Assertions.assertThrows(IllegalArgumentException.class, () -> inMissing.accept(bindVisitor));
  }

  @Test
  public void testBindVisitorIsNull() {
    BindVisitor bindVisitor = new BindVisitor(schema, false);

    // All fields exist
    Predicates.IsNull isNull = Predicates.isNull(new NameReference("a"));
    Expression bound = isNull.accept(bindVisitor);
    Assertions.assertTrue(bound instanceof Predicates.IsNull);
    Assertions.assertTrue((Boolean) bound.eval(new ArrayData(Arrays.asList(null, "2023-04-02", 15, 5L, false))));
    Assertions.assertFalse((Boolean) bound.eval(new ArrayData(Arrays.asList("Jane", "2023-04-02", 15, 5L, false))));

    // Field missing: should throw
    Predicates.IsNull isNullMissing = Predicates.isNull(new NameReference("missing"));
    Assertions.assertThrows(IllegalArgumentException.class, () -> isNullMissing.accept(bindVisitor));
  }

  @Test
  public void testBindVisitorIsNotNull() {
    BindVisitor bindVisitor = new BindVisitor(schema, false);

    // All fields exist
    Predicates.IsNotNull isNotNull = Predicates.isNotNull(new NameReference("a"));
    Expression bound = isNotNull.accept(bindVisitor);
    Assertions.assertTrue(bound instanceof Predicates.IsNotNull);
    Assertions.assertFalse((Boolean) bound.eval(new ArrayData(Arrays.asList(null, "2023-04-02", 15, 5L, false))));
    Assertions.assertTrue((Boolean) bound.eval(new ArrayData(Arrays.asList("Jane", "2023-04-02", 15, 5L, false))));

    // Field missing: should throw
    Predicates.IsNotNull isNotNullMissing = Predicates.isNotNull(new NameReference("missing"));
    Assertions.assertThrows(IllegalArgumentException.class, () -> isNotNullMissing.accept(bindVisitor));
  }

  @Test
  public void testBindVisitorStringStartsWith() {
    BindVisitor bindVisitor = new BindVisitor(schema, false);

    // All fields exist
    Predicates.StringStartsWith startsWith = Predicates.startsWith(new NameReference("a"), Literal.from("Ja"));
    Expression bound = startsWith.accept(bindVisitor);
    Assertions.assertTrue(bound instanceof Predicates.StringStartsWith);
    Assertions.assertTrue((Boolean) bound.eval(new ArrayData(Arrays.asList("Jane", "2023-04-02", 15, 5L, false))));
    Assertions.assertTrue((Boolean) bound.eval(new ArrayData(Arrays.asList("James", "2023-04-02", 15, 5L, false))));
    Assertions.assertFalse((Boolean) bound.eval(new ArrayData(Arrays.asList("John", "2023-04-02", 15, 5L, false))));

    // Field missing: should throw
    Predicates.StringStartsWith startsWithMissing = Predicates.startsWith(new NameReference("missing"), Literal.from("Ja"));
    Assertions.assertThrows(IllegalArgumentException.class, () -> startsWithMissing.accept(bindVisitor));
  }

  @Test
  public void testBindVisitorStringContains() {
    BindVisitor bindVisitor = new BindVisitor(schema, false);

    // All fields exist
    Predicates.StringContains contains = Predicates.contains(new NameReference("a"), Literal.from("an"));
    Expression bound = contains.accept(bindVisitor);
    Assertions.assertTrue(bound instanceof Predicates.StringContains);
    Assertions.assertTrue((Boolean) bound.eval(new ArrayData(Arrays.asList("Jane", "2023-04-02", 15, 5L, false))));
    Assertions.assertTrue((Boolean) bound.eval(new ArrayData(Arrays.asList("Diane", "2023-04-02", 15, 5L, false))));
    Assertions.assertFalse((Boolean) bound.eval(new ArrayData(Arrays.asList("John", "2023-04-02", 15, 5L, false))));

    // Field missing: should throw
    Predicates.StringContains containsMissing = Predicates.contains(new NameReference("missing"), Literal.from("an"));
    Assertions.assertThrows(IllegalArgumentException.class, () -> containsMissing.accept(bindVisitor));
  }

  @Test
  public void testBindVisitorSecondaryIndexKeyMatcher() {
    BindVisitor bindVisitor = new BindVisitor(schema, false);

    // All fields exist
    Predicates.SecondaryIndexKeyMatcher matcher = Predicates.secondaryIndexKeyMatcher(
        new NameReference("a"), Arrays.asList(Literal.from("abc"), Literal.from("def")));
    Expression bound = matcher.accept(bindVisitor);
    Assertions.assertTrue(bound instanceof Predicates.SecondaryIndexKeyMatcher);
    Assertions.assertTrue((Boolean) bound.eval(new ArrayData(Arrays.asList("abc$primary", "2023-04-02", 15, 5L, false))));
    Assertions.assertFalse((Boolean) bound.eval(new ArrayData(Arrays.asList("xyz$primary", "2023-04-02", 15, 5L, false))));

    // Field missing: should throw
    Predicates.SecondaryIndexKeyMatcher matcherMissing = Predicates.secondaryIndexKeyMatcher(
        new NameReference("missing"), Arrays.asList(Literal.from("abc")));
    Assertions.assertThrows(IllegalArgumentException.class, () -> matcherMissing.accept(bindVisitor));
  }
} 