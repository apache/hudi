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

package org.apache.hudi.expression;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
}
