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

import static org.apache.hudi.expression.Predicates.alwaysFalse;
import static org.apache.hudi.expression.Predicates.alwaysTrue;
import static org.apache.hudi.expression.Predicates.and;
import static org.apache.hudi.expression.Predicates.contains;
import static org.apache.hudi.expression.Predicates.eq;
import static org.apache.hudi.expression.Predicates.gt;
import static org.apache.hudi.expression.Predicates.gteq;
import static org.apache.hudi.expression.Predicates.in;
import static org.apache.hudi.expression.Predicates.isNotNull;
import static org.apache.hudi.expression.Predicates.isNull;
import static org.apache.hudi.expression.Predicates.lteq;
import static org.apache.hudi.expression.Predicates.not;
import static org.apache.hudi.expression.Predicates.or;
import static org.apache.hudi.expression.Predicates.startsWith;
import static org.apache.hudi.expression.Predicates.startsWithAny;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.hudi.expression.Predicates.And;
import org.apache.hudi.expression.Predicates.BinaryComparison;
import org.apache.hudi.expression.Predicates.In;
import org.apache.hudi.expression.Predicates.IsNotNull;
import org.apache.hudi.expression.Predicates.IsNull;
import org.apache.hudi.expression.Predicates.Not;
import org.apache.hudi.expression.Predicates.Or;
import org.apache.hudi.expression.Predicates.StringContains;
import org.apache.hudi.expression.Predicates.StringStartsWith;
import org.apache.hudi.expression.Predicates.StringStartsWithAny;
import org.apache.hudi.internal.schema.Types;
import org.junit.jupiter.api.Test;

class PredicatesTest {

  // Define some common literal expressions for use in tests
  private final Expression a = new Literal<>("a", Types.StringType.get());
  private final Expression b = new Literal<>(10, Types.IntType.get());
  private final Expression c = new Literal<>("c", Types.StringType.get());

  @Test
  void testSingletonPredicates() {
    // Test alwaysTrue
    assertEquals(alwaysTrue(), alwaysTrue(), "alwaysTrue() should be equal to itself");
    assertNotEquals(alwaysTrue(), alwaysFalse(), "alwaysTrue() should not be equal to alwaysFalse()");
    assertNotEquals(alwaysTrue(), null, "Predicate should not be equal to null");
    assertNotEquals(alwaysTrue(), new Object(), "Predicate should not be equal to a different type");
    assertEquals(alwaysTrue().hashCode(), alwaysTrue().hashCode(), "Hash code for alwaysTrue() should be consistent");

    // Test alwaysFalse
    assertEquals(alwaysFalse(), alwaysFalse(), "alwaysFalse() should be equal to itself");
    assertEquals(alwaysFalse().hashCode(), alwaysFalse().hashCode(), "Hash code for alwaysFalse() should be consistent");

    // Cross-check hash codes
    assertNotEquals(alwaysTrue().hashCode(), alwaysFalse().hashCode(), "Hash codes of singletons should differ");
  }

  @Test
  void testAnd() {
    And p1 = and(a, b);
    And p2 = and(a, b);
    And p3 = and(b, a);
    And p4 = and(a, c);

    // Standard checks
    assertEquals(p1, p1);
    assertNotEquals(null, p1);
    assertNotEquals(new Object(), p1);

    // Equality
    assertEquals(p1, p2, "And predicates with the same children should be equal");
    assertEquals(p1.hashCode(), p2.hashCode(), "Hash codes for equal And predicates should be the same");

    // Inequality
    assertNotEquals(p1, p3, "And predicates with swapped children should not be equal");
    assertNotEquals(p1, p4, "And predicates with different children should not be equal");
  }

  @Test
  void testOr() {
    Or p1 = or(a, b);
    Or p2 = or(a, b);
    Or p3 = or(b, a);
    Or p4 = or(a, c);

    assertEquals(p1, p2, "Or predicates with the same children should be equal");
    assertEquals(p1.hashCode(), p2.hashCode(), "Hash codes for equal Or predicates should be the same");

    assertNotEquals(p1, p3, "Or predicates with swapped children should not be equal");
    assertNotEquals(p1, p4, "Or predicates with different children should not be equal");
  }

  @Test
  void testNot() {
    Not p1 = not(a);
    Not p2 = not(a);
    Not p3 = not(b);

    assertEquals(p1, p2, "Not predicates with the same child should be equal");
    assertEquals(p1.hashCode(), p2.hashCode(), "Hash codes for equal Not predicates should be the same");
    assertNotEquals(p1, p3, "Not predicates with different children should not be equal");
  }

  @Test
  void testIsNull() {
    IsNull p1 = isNull(a);
    IsNull p2 = isNull(a);
    IsNull p3 = isNull(b);

    assertEquals(p1, p2, "IsNull predicates with the same child should be equal");
    assertEquals(p1.hashCode(), p2.hashCode(), "Hash codes for equal IsNull predicates should be the same");
    assertNotEquals(p1, p3, "IsNull predicates with different children should not be equal");
  }

  @Test
  void testIsNotNull() {
    IsNotNull p1 = isNotNull(a);
    IsNotNull p2 = isNotNull(a);
    IsNotNull p3 = isNotNull(b);

    assertEquals(p1, p2, "IsNotNull predicates with the same child should be equal");
    assertEquals(p1.hashCode(), p2.hashCode(), "Hash codes for equal IsNotNull predicates should be the same");
    assertNotEquals(p1, p3, "IsNotNull predicates with different children should not be equal");
  }

  @Test
  void testBinaryComparison() {
    BinaryComparison p1 = gteq(a, b); // a >= b
    BinaryComparison p2 = gteq(a, b); // a >= b (should be equal)
    BinaryComparison p3 = gt(a, b);   // a > b (not equal, different operator)
    BinaryComparison p4 = gteq(b, a); // b >= a (not equal, different children)
    BinaryComparison p5 = lteq(a, b); // a <= b (not equal, different operator)

    assertEquals(p1, p2, "BinaryComparisons with same children and operator should be equal");
    assertEquals(p1.hashCode(), p2.hashCode(), "Hash codes for equal BinaryComparisons should be the same");

    assertNotEquals(p1, p3, "BinaryComparisons with different operators should not be equal");
    assertNotEquals(p1, p4, "BinaryComparisons with different children should not be equal");
    assertNotEquals(p1, p5, "BinaryComparisons with different operators should not be equal");

    // Test another operator
    BinaryComparison eq1 = eq(a, b);
    BinaryComparison eq2 = eq(a, b);
    assertEquals(eq1, eq2);
    assertEquals(eq1.hashCode(), eq2.hashCode());
    assertNotEquals(p1, eq1);
  }

  @Test
  void testStringStartsWith() {
    StringStartsWith p1 = startsWith(a, b);
    StringStartsWith p2 = startsWith(a, b);
    StringStartsWith p3 = startsWith(b, a);
    StringStartsWith p4 = startsWith(a, c);

    assertEquals(p1, p2);
    assertEquals(p1.hashCode(), p2.hashCode());
    assertNotEquals(p1, p3);
    assertNotEquals(p1, p4);
  }

  @Test
  void testStringContains() {
    StringContains p1 = contains(a, b);
    StringContains p2 = contains(a, b);
    StringContains p3 = contains(b, a);
    StringContains p4 = contains(a, c);

    assertEquals(p1, p2);
    assertEquals(p1.hashCode(), p2.hashCode());
    assertNotEquals(p1, p3);
    assertNotEquals(p1, p4);
  }

  @Test
  void testIn() {
    List<Expression> list1 = Arrays.asList(b, c);
    List<Expression> list2 = Arrays.asList(b, c);
    List<Expression> list3 = Arrays.asList(c, b); // different order
    List<Expression> list4 = Collections.singletonList(b); // different content

    In p1 = in(a, list1);
    In p2 = in(a, list2); // equal
    In p3 = in(b, list1); // different value expression
    In p4 = in(a, list3); // different list order
    In p5 = in(a, list4); // different list content

    assertEquals(p1, p2, "In predicates with same value and list should be equal");
    assertEquals(p1.hashCode(), p2.hashCode(), "Hash codes for equal In predicates should be the same");

    assertNotEquals(p1, p3, "In predicates with different value expressions should not be equal");
    assertNotEquals(p1, p4, "In predicates with different list order should not be equal");
    assertNotEquals(p1, p5, "In predicates with different list content should not be equal");
  }

  @Test
  void testStringStartsWithAny() {
    List<Expression> list1 = Arrays.asList(b, c);
    List<Expression> list2 = Arrays.asList(b, c);
    List<Expression> list3 = Arrays.asList(c, b); // different order
    List<Expression> list4 = Collections.singletonList(b); // different content

    StringStartsWithAny p1 = startsWithAny(a, list1);
    StringStartsWithAny p2 = startsWithAny(a, list2); // equal
    StringStartsWithAny p3 = startsWithAny(b, list1); // different left value
    StringStartsWithAny p4 = startsWithAny(a, list3); // different list order
    StringStartsWithAny p5 = startsWithAny(a, list4); // different list content

    assertEquals(p1, p2, "StringStartsWithAny predicates with same children should be equal");
    assertEquals(p1.hashCode(), p2.hashCode(), "Hash codes for equal StringStartsWithAny predicates should be the same");

    assertNotEquals(p1, p3, "StringStartsWithAny with different left expressions should not be equal");
    assertNotEquals(p1, p4, "StringStartsWithAny with different list order should not be equal");
    assertNotEquals(p1, p5, "StringStartsWithAny with different list content should not be equal");
  }
}