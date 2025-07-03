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

package org.apache.hudi.metadata;

import org.apache.hudi.expression.Expression;
import org.apache.hudi.expression.Literal;
import org.apache.hudi.expression.Predicates;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for SecondaryIndexKeyMatcher functionality.
 */
public class TestSecondaryIndexKeyMatcher {

  @ParameterizedTest
  @MethodSource("keyMatcherCases")
  void testKeyMatchingCases(String left, List<String> right, boolean expected) {
    Expression leftExpr = left != null ? Literal.from(left) : null;
    List<Expression> rightExprs = right.stream().map(Literal::from).collect(Collectors.toList());
    Predicates.SecondaryIndexKeyMatcher predicate = Predicates.secondaryIndexKeyMatcher(leftExpr, rightExprs);
    assertEquals(expected, predicate.eval(null),
        String.format("left='%s', right=%s, expected=%s", left, right, expected));
  }

  private static Stream<Arguments> keyMatcherCases() {
    return Stream.of(
        // Exact match
        Arguments.of("abc", Arrays.asList("abc"), true),
        Arguments.of("abc\\$", Arrays.asList("abc\\$"), true), // lookupKey with escaped $

        // Prefix match with unescaped $ separator in recordKey
        Arguments.of("abc$primary", Arrays.asList("abc"), true), // recordKey has unescaped $
        Arguments.of("abc\\$$primary", Arrays.asList("abc\\$"), true), // recordKey has escaped $ in prefix, unescaped $ as separator

        // Negative: recordKey does not start with lookupKey
        Arguments.of("def$primary", Arrays.asList("abc"), false),

        // Negative: separator is not unescaped $
        Arguments.of("abc\\$primary", Arrays.asList("abc"), false), // $ is escaped

        // Edge: empty string
        Arguments.of("abc$primary", Arrays.asList(""), false),
        Arguments.of("", Arrays.asList(""), true),

        // Edge: recordKey is empty, only matches empty lookupKey
        Arguments.of("", Arrays.asList("abc"), false),
        Arguments.of("", Arrays.asList(""), true),

        // Edge: empty right list should match everything
        Arguments.of("abc$primary", Collections.emptyList(), true),
        Arguments.of("", Collections.emptyList(), true)
    );
  }
} 