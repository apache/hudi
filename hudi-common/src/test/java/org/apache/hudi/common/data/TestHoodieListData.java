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

import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.Pair;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

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

  @Test
  public void testSumWithEagerSemantic() {
    // Test with positive numbers
    HoodieData<Long> listData = HoodieListData.eager(Arrays.asList(1L, 2L, 3L, 4L, 5L));
    assertEquals(15L, listData.sum());

    // Test with single element
    listData = HoodieListData.eager(Collections.singletonList(42L));
    assertEquals(42L, listData.sum());

    // Test with zero
    listData = HoodieListData.eager(Arrays.asList(0L, 0L, 0L));
    assertEquals(0L, listData.sum());

    // Test with negative numbers
    listData = HoodieListData.eager(Arrays.asList(-5L, -10L, -15L));
    assertEquals(-30L, listData.sum());

    // Test with mixed positive and negative
    listData = HoodieListData.eager(Arrays.asList(10L, -5L, 20L, -10L));
    assertEquals(15L, listData.sum());

    // Test with large numbers
    listData = HoodieListData.eager(Arrays.asList(1000000L, 2000000L, 3000000L));
    assertEquals(6000000L, listData.sum());
  }

  @Test
  public void testSumWithLazySemantic() {
    // Test with positive numbers
    HoodieData<Long> listData = HoodieListData.lazy(Arrays.asList(1L, 2L, 3L, 4L, 5L));
    assertEquals(15L, listData.sum());

    // Test with single element
    listData = HoodieListData.lazy(Collections.singletonList(100L));
    assertEquals(100L, listData.sum());

    // Test with zero
    listData = HoodieListData.lazy(Arrays.asList(0L, 0L, 0L));
    assertEquals(0L, listData.sum());

    // Test with mixed positive and negative
    listData = HoodieListData.lazy(Arrays.asList(50L, -25L, 75L, -50L));
    assertEquals(50L, listData.sum());
  }

  @Test
  public void testSumAfterTransformation() {
    // Test sum after map operation
    HoodieData<Integer> intData = HoodieListData.eager(Arrays.asList(1, 2, 3, 4, 5));
    HoodieData<Long> longData = intData.map(i -> i.longValue() * 2);
    assertEquals(30L, longData.sum());

    // Test sum after filter operation
    HoodieData<Long> filteredData = HoodieListData.lazy(Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L))
        .filter(x -> x % 2 == 0);
    assertEquals(30L, filteredData.sum());
  }

  @Test
  void testCloseableIterator() {
    ClosableIterator<String> iterator = spy(ClosableIterator.wrap(Arrays.asList("value1", "value2").iterator()));
    HoodieData<String> listData = HoodieListData.lazy(iterator);
    List<String> values = listData.collectAsList();
    assertEquals(Arrays.asList("value1", "value2"), values);
    // Ensure the iterator is closed after use
    verify(iterator).close();
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testMapPartitionsWithCloseable(boolean isLazy) {
    String partition1 = "partition1";
    String partition2 = "partition2";
    HoodieData<String> input = new HoodieListData<>(Stream.of(partition1, partition2), isLazy);
    CloseValidationIterator<String> iterator = new CloseValidationIterator<>(Collections.singletonList("value").iterator());
    assertEquals(1, input.mapPartitions(partition -> iterator, true).collectAsList().size());
    assertTrue(iterator.isClosed());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testFlatMapWithCloseable(boolean isLazy) {
    String partition1 = "partition1";
    String partition2 = "partition2";
    CloseValidationIterator<String> iterator1 = new CloseValidationIterator<>(Collections.singletonList("value").iterator());
    CloseValidationIterator<String> iterator2 = new CloseValidationIterator<>(Collections.singletonList("value").iterator());
    HoodieData<String> input = new HoodieListData<>(Stream.of(partition1, partition2), isLazy);
    assertEquals(2, input.flatMap(partition -> partition.equals(partition1) ? iterator1 : iterator2).collectAsList().size());
    assertTrue(iterator1.isClosed());
    assertTrue(iterator2.isClosed());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testFlatMapToPairWithCloseable(boolean isLazy) {
    String partition1 = "partition1";
    String partition2 = "partition2";
    HoodieData<String> input = new HoodieListData<>(Stream.of(partition1, partition2), isLazy);
    CloseValidationIterator<Pair<String, String>> iterator1 = new CloseValidationIterator<>(Collections.singletonList(Pair.of("1", "value")).iterator());
    CloseValidationIterator<Pair<String, String>> iterator2 = new CloseValidationIterator<>(Collections.singletonList(Pair.of("2", "value")).iterator());
    assertEquals(2, input.flatMapToPair(partition -> partition.equals(partition1) ? iterator1 : iterator2).collectAsList().size());
    assertTrue(iterator1.isClosed());
    assertTrue(iterator2.isClosed());
  }
}
