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

package org.apache.hudi.common.table.log;

import org.apache.hudi.common.util.Option;
import org.apache.hudi.expression.Expression;
import org.apache.hudi.expression.Literal;
import org.apache.hudi.expression.Predicate;
import org.apache.hudi.expression.Predicates;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestHoodieMergedLogRecordReader {
  @Test
  void testCreateKeySpecWithEmptyOutput() {
    Option<KeySpec> r = HoodieMergedLogRecordReader.createKeySpec(Option.empty());
    assertTrue(r.isEmpty());
  }

  @Test
  void testCreateKeySpecWithKeys() {
    Expression left = Literal.from("key1");
    List<Expression> right = Arrays.asList(
        Literal.from("key1"),
        Literal.from("key2"));
    Predicate predicate = Predicates.in(left, right);
    Option<KeySpec> r = HoodieMergedLogRecordReader.createKeySpec(Option.of(predicate));
    assertTrue(r.isPresent());
    assertTrue(r.get().isFullKey());
    assertEquals(Arrays.asList("key1", "key2"), r.get().getKeys());
  }

  @Test
  void testCreateKeySpecWithKeyPrefixes() {
    Expression left = Literal.from("key1");
    List<Expression> right = Arrays.asList(
        Literal.from("key_prefix1"),
        Literal.from("key_prefix2"));
    Predicate predicate = Predicates.startsWithAny(left, right);
    Option<KeySpec> r = HoodieMergedLogRecordReader.createKeySpec(Option.of(predicate));
    assertTrue(r.isPresent());
    assertFalse(r.get().isFullKey());
    assertEquals(Arrays.asList("key_prefix1", "key_prefix2"), r.get().getKeys());
  }
}
