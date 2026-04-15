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

package org.apache.hudi.util;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestPartitionPathFilterUtil {

  @Test
  public void testRelativePathPrefixPredicateWithEmptyPrefixMatchesAllPartitions() {
    Predicate<String> predicate = PartitionPathFilterUtil.relativePathPrefixPredicate(Collections.singletonList(""));

    assertTrue(predicate.test("2024/01/01"));
    assertTrue(predicate.test("country=us/state=ca"));
  }

  @Test
  public void testRelativePathPrefixPredicateMatchesExactAndNestedPartitions() {
    Predicate<String> predicate = PartitionPathFilterUtil.relativePathPrefixPredicate(Arrays.asList("2024/01", "2024/02/01"));

    assertTrue(predicate.test("2024/01"), "Exact partition path should match");
    assertTrue(predicate.test("2024/01/15"), "Nested partition path should match");
    assertTrue(predicate.test("2024/02/01"), "Exact partition path for second prefix should match");
  }

  @Test
  public void testRelativePathPrefixPredicateRejectsNonMatchingPartitions() {
    Predicate<String> predicate = PartitionPathFilterUtil.relativePathPrefixPredicate(Collections.singletonList("2024/01"));

    assertFalse(predicate.test("2024/02/01"));
    assertFalse(predicate.test("2023/12/31"));
  }
}
