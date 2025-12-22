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

package org.apache.hudi.common.data;

import org.apache.hudi.common.util.collection.Pair;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestHoodieListData {

  private static Stream<Arguments> distinctWithKey() {
    return Stream.of(
        Arguments.of(
            Arrays.asList(Pair.of("k1", 1), Pair.of("k2", 2)),
            Arrays.asList(Pair.of("k1", 1), Pair.of("k1", 10), Pair.of("k1", 100), Pair.of("k2", 2)))
    );
  }

  @ParameterizedTest
  @MethodSource
  void distinctWithKey(List<Pair<String, Integer>> expected, List<Pair<String, Integer>> originalList) {
    List<Pair<String, Integer>> distinctList = HoodieListData.eager(originalList).distinctWithKey(Pair::getLeft, 1).collectAsList();
    assertEquals(expected, distinctList);
  }

  @Test
  void testEagerSemantic() {
    List<String> sourceList = Arrays.asList("quick", "brown", "fox");

    HoodieListData<String> originalListData = HoodieListData.eager(sourceList);
    HoodieData<Integer> lengthsListData = originalListData.map(String::length);

    List<Integer> expectedLengths = sourceList.stream().map(String::length).collect(Collectors.toList());
    assertEquals(expectedLengths, lengthsListData.collectAsList());
    // Here we assert that even though we already de-referenced derivative container,
    // we still can dereference its parent (multiple times)
    assertEquals(3, originalListData.count());
    assertEquals(sourceList, originalListData.collectAsList());
  }

  @Test
  public void testGetNumPartitions() {
    HoodieData<Integer> listData = HoodieListData.eager(
        IntStream.rangeClosed(0, 100).boxed().collect(Collectors.toList()));
    assertEquals(1, listData.getNumPartitions());
  }

  @Test
  public void testIsEmpty() {
    // HoodieListData bearing eager execution semantic
    HoodieData<Integer> listData = HoodieListData.eager(
            IntStream.rangeClosed(0, 100).boxed().collect(Collectors.toList()));
    assertFalse(listData.isEmpty());

    HoodieData<Integer> emptyListData = HoodieListData.eager(Collections.emptyList());
    assertTrue(emptyListData.isEmpty());

    // HoodieListData bearing lazy execution semantic
    listData = HoodieListData.lazy(
            IntStream.rangeClosed(0, 100).boxed().collect(Collectors.toList()));
    assertFalse(listData.isEmpty());

    emptyListData = HoodieListData.lazy(Collections.emptyList());
    assertTrue(emptyListData.isEmpty());
  }
}
