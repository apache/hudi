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

import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ImmutablePair;
import org.apache.hudi.common.util.collection.Pair;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.apache.hudi.common.util.CollectionUtils.createImmutableList;
import static org.apache.hudi.common.util.CollectionUtils.createImmutableMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link HoodieListPairData}.
 */
public class TestHoodieListDataPairData {

  private static final String KEY1 = "key1";
  private static final String KEY2 = "key2";
  private static final String KEY3 = "key3";
  private static final String KEY4 = "key4";
  private static final String KEY5 = "key5";

  private static final String STRING_VALUE1 = "value1";
  private static final String STRING_VALUE2 = "value2";
  private static final String STRING_VALUE3 = "value3";
  private static final String STRING_VALUE4 = "value4";
  private static final String STRING_VALUE5 = "value5";
  private static final String STRING_VALUE6 = "value6";

  private static final int INTEGER_VALUE1 = 1;
  private static final int INTEGER_VALUE2 = 2;
  private static final int INTEGER_VALUE3 = 3;
  private static final int INTEGER_VALUE4 = 4;
  private static final int INTEGER_VALUE5 = 5;

  private List<Pair<String, String>> testPairs;
  private HoodiePairData<String, String> testHoodiePairData;

  @BeforeEach
  public void setup() {
    testPairs = constructPairs();
    testHoodiePairData = HoodieListPairData.lazy(testPairs);
  }

  @Test
  public void testKeys() {
    assertHoodieDataEquals(Arrays.asList(KEY1, KEY1, KEY2, KEY2, KEY3, KEY4), testHoodiePairData.keys());
  }

  @Test
  public void testValues() {
    assertHoodieDataEquals(Arrays.asList(
            STRING_VALUE1, STRING_VALUE2, STRING_VALUE3, STRING_VALUE4, STRING_VALUE5, STRING_VALUE6),
        testHoodiePairData.values());
  }

  @Test
  public void testCount() {
    assertEquals(6, testHoodiePairData.count());
  }

  @Test
  public void testCountByKey() {
    Map<String, Long> expectedResultMap = new HashMap<>();
    expectedResultMap.put(KEY1, 2L);
    expectedResultMap.put(KEY2, 2L);
    expectedResultMap.put(KEY3, 1L);
    expectedResultMap.put(KEY4, 1L);

    assertEquals(expectedResultMap, testHoodiePairData.countByKey());
  }

  @Test
  public void testMap() {
    assertHoodieDataEquals(Arrays.asList(
            "key1,value1", "key1,value2", "key2,value3", "key2,value4", "key3,value5", "key4,value6"),
        testHoodiePairData.map(pair -> pair.getKey() + "," + pair.getValue()));
  }

  @Test
  public void testMapToPair() {
    Map<String, List<Integer>> expectedResultMap = new HashMap<>();
    expectedResultMap.put("key10", Arrays.asList(1, 2));
    expectedResultMap.put("key20", Arrays.asList(3, 4));
    expectedResultMap.put("key30", Arrays.asList(5));
    expectedResultMap.put("key40", Arrays.asList(6));
    assertEquals(expectedResultMap, toMap(
        testHoodiePairData.mapToPair(
            pair -> {
              String value = pair.getValue();
              return new ImmutablePair<>(pair.getKey() + "0",
                  Integer.parseInt(String.valueOf(value.charAt(value.length() - 1))));
            })));
  }

  private static Stream<Arguments> testReduceByKey() {
    return Stream.of(
        Arguments.of(
            createImmutableMap(
                Pair.of(1, createImmutableList(1001)),
                Pair.of(2, createImmutableList(2001)),
                Pair.of(3, createImmutableList(3001))),
            createImmutableMap(
                Pair.of(1, createImmutableList(1001, 1002, 1003)),
                Pair.of(2, createImmutableList(2001, 2002)),
                Pair.of(3, createImmutableList(3001)),
                Pair.of(4, createImmutableList())))
    );
  }

  @ParameterizedTest
  @MethodSource
  public void testReduceByKey(Map<Integer, List<Integer>> expected, Map<Integer, List<Integer>> original) {
    HoodiePairData<Integer, Integer> reduced = HoodieListPairData.lazy(original).reduceByKey((a, b) -> a, 1);
    assertEquals(expected, toMap(reduced));
  }

  @Test
  void testReduceByKeyWithCloseableInput() {
    ConcurrentLinkedQueue<CloseValidationIterator<Pair<Integer, Integer>>> createdIterators = new ConcurrentLinkedQueue<>();
    HoodiePairData<Integer, Integer> data = HoodieListData.lazy(Arrays.asList(1, 1, 1))
        .flatMapToPair(key -> {
          CloseValidationIterator<Pair<Integer, Integer>> iter = new CloseValidationIterator<>(Collections.singletonList(Pair.of(key, 1)).iterator());
          createdIterators.add(iter);
          return iter;
        });
    List<Pair<Integer, Integer>> result = data.reduceByKey(Integer::sum, 1).collectAsList();
    assertEquals(Collections.singletonList(Pair.of(1, 3)), result);
    createdIterators.forEach(iter -> assertTrue(iter.isClosed()));
  }

  @Test
  void testLeftOuterJoinWithCloseableInput() {
    ConcurrentLinkedQueue<CloseValidationIterator<Pair<Integer, Integer>>> createdIterators = new ConcurrentLinkedQueue<>();
    HoodiePairData<Integer, Integer> dataToJoin = HoodieListData.lazy(Arrays.asList(1, 2, 3))
        .flatMapToPair(key -> {
          CloseValidationIterator<Pair<Integer, Integer>> iter = new CloseValidationIterator<>(Collections.singletonList(Pair.of(key, 1)).iterator());
          createdIterators.add(iter);
          return iter;
        });
    HoodiePairData<Integer, Integer> data = HoodieListPairData.lazy(Arrays.asList(Pair.of(1, 1), Pair.of(4, 2)));
    List<Pair<Integer, Pair<Integer, Option<Integer>>>> result = data.leftOuterJoin(dataToJoin).collectAsList();
    assertEquals(2, result.size());
    createdIterators.forEach(iter -> assertTrue(iter.isClosed()));
  }

  @Test
  void testJoinWithCloseableInput() {
    ConcurrentLinkedQueue<CloseValidationIterator<Pair<Integer, Integer>>> createdIterators = new ConcurrentLinkedQueue<>();
    HoodiePairData<Integer, Integer> dataToJoin = HoodieListData.lazy(Arrays.asList(1, 2, 3))
        .flatMapToPair(key -> {
          CloseValidationIterator<Pair<Integer, Integer>> iter = new CloseValidationIterator<>(Collections.singletonList(Pair.of(key, 1)).iterator());
          createdIterators.add(iter);
          return iter;
        });
    HoodiePairData<Integer, Integer> data = HoodieListPairData.lazy(Arrays.asList(Pair.of(1, 1), Pair.of(4, 2)));
    List<Pair<Integer, Pair<Integer, Integer>>> result = data.join(dataToJoin).collectAsList();
    assertEquals(1, result.size());
    createdIterators.forEach(iter -> assertTrue(iter.isClosed()));
  }

  @Test
  public void testLeftOuterJoinSingleValuePerKey() {
    HoodiePairData<String, String> pairData1 = HoodieListPairData.lazy(Arrays.asList(
        ImmutablePair.of(KEY1, STRING_VALUE1),
        ImmutablePair.of(KEY2, STRING_VALUE2),
        ImmutablePair.of(KEY3, STRING_VALUE3),
        ImmutablePair.of(KEY4, STRING_VALUE4)
    ));

    HoodiePairData<String, Integer> pairData2 = HoodieListPairData.lazy(Arrays.asList(
        ImmutablePair.of(KEY1, INTEGER_VALUE1),
        ImmutablePair.of(KEY2, INTEGER_VALUE2),
        ImmutablePair.of(KEY5, INTEGER_VALUE3)
    ));

    Map<String, List<Pair<String, Option<Integer>>>> expectedResultMap = new HashMap<>();
    expectedResultMap.put(KEY1, Arrays.asList(
        ImmutablePair.of(STRING_VALUE1, Option.of(INTEGER_VALUE1))));
    expectedResultMap.put(KEY2, Arrays.asList(
        ImmutablePair.of(STRING_VALUE2, Option.of(INTEGER_VALUE2))));
    expectedResultMap.put(KEY3, Arrays.asList(
        ImmutablePair.of(STRING_VALUE3, Option.empty())));
    expectedResultMap.put(KEY4, Arrays.asList(
        ImmutablePair.of(STRING_VALUE4, Option.empty())));

    assertEquals(expectedResultMap,
        toMap(pairData1.leftOuterJoin(pairData2)));
  }

  @Test
  public void testLeftOuterJoinMultipleValuesPerKey() {
    HoodiePairData<String, Integer> otherPairData = HoodieListPairData.lazy(Arrays.asList(
        ImmutablePair.of(KEY1, INTEGER_VALUE1),
        ImmutablePair.of(KEY2, INTEGER_VALUE2),
        ImmutablePair.of(KEY2, INTEGER_VALUE3),
        ImmutablePair.of(KEY3, INTEGER_VALUE4),
        ImmutablePair.of(KEY5, INTEGER_VALUE5)
    ));

    Map<String, List<Pair<String, Option<Integer>>>> expectedResultMap = new HashMap<>();
    expectedResultMap.put(KEY1, Arrays.asList(
        ImmutablePair.of(STRING_VALUE1, Option.of(INTEGER_VALUE1)),
        ImmutablePair.of(STRING_VALUE2, Option.of(INTEGER_VALUE1))));
    expectedResultMap.put(KEY2, Arrays.asList(
        ImmutablePair.of(STRING_VALUE3, Option.of(INTEGER_VALUE2)),
        ImmutablePair.of(STRING_VALUE3, Option.of(INTEGER_VALUE3)),
        ImmutablePair.of(STRING_VALUE4, Option.of(INTEGER_VALUE2)),
        ImmutablePair.of(STRING_VALUE4, Option.of(INTEGER_VALUE3))));
    expectedResultMap.put(KEY3, Arrays.asList(
        ImmutablePair.of(STRING_VALUE5, Option.of(INTEGER_VALUE4))));
    expectedResultMap.put(KEY4, Arrays.asList(
        ImmutablePair.of(STRING_VALUE6, Option.empty())));

    assertEquals(expectedResultMap,
        toMap(testHoodiePairData.leftOuterJoin(otherPairData)));
  }

  @Test
  void testEagerSemantic() {
    List<Pair<String, Integer>> sourceList =
        Stream.of("quick", "brown", "fox")
            .map(s -> Pair.of(s, s.length()))
            .collect(Collectors.toList());

    HoodieListPairData<String, Integer> originalListData = HoodieListPairData.eager(sourceList);
    HoodieData<Integer> lengthsListData = originalListData.values();

    List<Integer> expectedLengths = sourceList.stream().map(Pair::getValue).collect(Collectors.toList());
    assertEquals(expectedLengths, lengthsListData.collectAsList());
    // Here we assert that even though we already de-referenced derivative container,
    // we still can dereference its parent (multiple times)
    assertEquals(3, originalListData.count());
    assertEquals(sourceList, originalListData.collectAsList());
  }

  @Test
  public void testJoin() {
    // Prepare test data
    List<Pair<String, String>> leftData = Arrays.asList(
        Pair.of("a", "value1"),
        Pair.of("b", "value2"),
        Pair.of("c", "value3")
    );

    List<Pair<String, String>> rightData = Arrays.asList(
        Pair.of("a", "rValue1"),
        Pair.of("a", "rValue2"),
        Pair.of("b", "rValue3"),
        Pair.of("d", "rValue4")
    );

    HoodiePairData<String, String> left = new HoodieListPairData<>(leftData.stream(), true);
    HoodiePairData<String, String> right = new HoodieListPairData<>(rightData.stream(), true);

    // Execute the join
    HoodiePairData<String, Pair<String, String>> joined = left.join(right);

    // Validate the result
    List<Pair<String, Pair<String, String>>> expected = Arrays.asList(
        Pair.of("a", Pair.of("value1", "rValue1")),
        Pair.of("a", Pair.of("value1", "rValue2")),
        Pair.of("b", Pair.of("value2", "rValue3"))
    );

    List<Pair<String, Pair<String, String>>> result = joined.collectAsList();

    assertEquals(expected, result, "Join result does not match expected output");
  }

  @Test
  public void testFilter() {
    // Test filtering by key
    HoodiePairData<String, String> filteredByKey = testHoodiePairData.filter((key, value) -> key.equals(KEY1));
    List<Pair<String, String>> expectedByKey = Arrays.asList(
        ImmutablePair.of(KEY1, STRING_VALUE1),
        ImmutablePair.of(KEY1, STRING_VALUE2)
    );
    assertEquals(expectedByKey, filteredByKey.collectAsList());

    // Test filtering by value
    testHoodiePairData = HoodieListPairData.lazy(testPairs);
    HoodiePairData<String, String> filteredByValue = testHoodiePairData.filter((key, value) -> value.equals(STRING_VALUE3));
    List<Pair<String, String>> expectedByValue = Arrays.asList(
        ImmutablePair.of(KEY2, STRING_VALUE3)
    );
    assertEquals(expectedByValue, filteredByValue.collectAsList());

    // Test filtering by both key and value
    testHoodiePairData = HoodieListPairData.lazy(testPairs);
    HoodiePairData<String, String> filteredByBoth = testHoodiePairData.filter((key, value) ->
        key.equals(KEY2) && value.equals(STRING_VALUE4));
    List<Pair<String, String>> expectedByBoth = Arrays.asList(
        ImmutablePair.of(KEY2, STRING_VALUE4)
    );
    assertEquals(expectedByBoth, filteredByBoth.collectAsList());
  }

  @Test
  public void testFilterWithComplexCondition() {
    // Test filtering with complex condition (key starts with "key" and value length > 5)
    HoodiePairData<String, String> filtered = testHoodiePairData.filter((key, value) -> 
        key.startsWith("key") && value.length() > 5);
    List<Pair<String, String>> expected = Arrays.asList(
        ImmutablePair.of(KEY1, STRING_VALUE1),
        ImmutablePair.of(KEY1, STRING_VALUE2),
        ImmutablePair.of(KEY2, STRING_VALUE3),
        ImmutablePair.of(KEY2, STRING_VALUE4),
        ImmutablePair.of(KEY3, STRING_VALUE5),
        ImmutablePair.of(KEY4, STRING_VALUE6)
    );
    assertEquals(expected, filtered.collectAsList());
  }

  @Test
  public void testFilterWithNoMatches() {
    // Test filtering that returns no matches
    HoodiePairData<String, String> filtered = testHoodiePairData.filter((key, value) -> 
        key.equals("nonexistent") && value.equals("nonexistent"));
    List<Pair<String, String>> expected = Collections.emptyList();
    assertEquals(expected, filtered.collectAsList());
  }

  @Test
  public void testFilterWithAllMatches() {
    // Test filtering that returns all matches
    HoodiePairData<String, String> filtered = testHoodiePairData.filter((key, value) -> true);
    assertEquals(testPairs, filtered.collectAsList());
  }

  @Test
  public void testFilterWithNullValues() {
    // Test filtering with null values in the data
    List<Pair<String, String>> dataWithNulls = Arrays.asList(
        ImmutablePair.of("key1", "value1"),
        ImmutablePair.of("key2", null),
        ImmutablePair.of("key3", "value3")
    );
    HoodiePairData<String, String> pairDataWithNulls = HoodieListPairData.lazy(dataWithNulls);
    
    // Filter out null values
    HoodiePairData<String, String> filtered = pairDataWithNulls.filter((key, value) -> value != null);
    List<Pair<String, String>> expected = Arrays.asList(
        ImmutablePair.of("key1", "value1"),
        ImmutablePair.of("key3", "value3")
    );
    assertEquals(expected, filtered.collectAsList());
  }

  @Test
  public void testFilterWithNumericData() {
    // Test filtering with numeric data
    List<Pair<String, Integer>> numericData = Arrays.asList(
        ImmutablePair.of("a", 1),
        ImmutablePair.of("b", 2),
        ImmutablePair.of("c", 3),
        ImmutablePair.of("d", 4),
        ImmutablePair.of("e", 5)
    );
    HoodiePairData<String, Integer> numericPairData = HoodieListPairData.lazy(numericData);
    
    // Filter even numbers
    HoodiePairData<String, Integer> filtered = numericPairData.filter((key, value) -> value % 2 == 0);
    List<Pair<String, Integer>> expected = Arrays.asList(
        ImmutablePair.of("b", 2),
        ImmutablePair.of("d", 4)
    );
    assertEquals(expected, filtered.collectAsList());
  }

  @Test
  public void testFilterWithExceptionHandling() {
    // Test that exceptions in filter function are properly wrapped
    HoodiePairData<String, String> filtered = testHoodiePairData.filter((key, value) -> {
      if (key.equals(KEY1)) {
        throw new RuntimeException("Test exception");
      }
      return true;
    });
    
    // Should throw RuntimeException
    try {
      filtered.collectAsList();
      // If we reach here, the test should fail
      throw new AssertionError("Expected RuntimeException to be thrown");
    } catch (RuntimeException e) {
      // Expected behavior
      assertTrue(e.getMessage().contains("Test exception") || e.getCause().getMessage().contains("Test exception"));
    }
  }

  @Test
  public void testFilterWithEagerExecution() {
    // Test filtering with eager execution semantic
    HoodiePairData<String, String> eagerPairData = HoodieListPairData.eager(testPairs);
    HoodiePairData<String, String> filtered = eagerPairData.filter((key, value) -> key.equals(KEY1));
    
    List<Pair<String, String>> expected = Arrays.asList(
        ImmutablePair.of(KEY1, STRING_VALUE1),
        ImmutablePair.of(KEY1, STRING_VALUE2)
    );
    assertEquals(expected, filtered.collectAsList());
    
    // Test that we can call collectAsList multiple times with eager execution
    assertEquals(expected, filtered.collectAsList());
    assertEquals(expected, filtered.collectAsList());
  }

  @Test
  public void testFilterChaining() {
    // Test chaining multiple filter operations
    HoodiePairData<String, String> filtered1 = testHoodiePairData.filter((key, value) -> key.equals(KEY1));
    HoodiePairData<String, String> filtered2 = filtered1.filter((key, value) -> value.equals(STRING_VALUE1));
    
    List<Pair<String, String>> expected = Arrays.asList(
        ImmutablePair.of(KEY1, STRING_VALUE1)
    );
    assertEquals(expected, filtered2.collectAsList());
  }

  @Test
  public void testFilterWithEmptyData() {
    // Test filtering on empty data
    HoodiePairData<String, String> emptyPairData = HoodieListPairData.lazy(Collections.emptyList());
    HoodiePairData<String, String> filtered = emptyPairData.filter((key, value) -> true);
    
    assertEquals(Collections.emptyList(), filtered.collectAsList());
  }

  private static List<Pair<String, String>> constructPairs() {
    return Arrays.asList(
        ImmutablePair.of(KEY1, STRING_VALUE1),
        ImmutablePair.of(KEY1, STRING_VALUE2),
        ImmutablePair.of(KEY2, STRING_VALUE3),
        ImmutablePair.of(KEY2, STRING_VALUE4),
        ImmutablePair.of(KEY3, STRING_VALUE5),
        ImmutablePair.of(KEY4, STRING_VALUE6)
    );
  }

  private static <K,V> Map<K, List<V>> toMap(HoodiePairData<K, V> pairData) {
    return ((List<Pair<K, Iterable<V>>>) pairData.groupByKey().get()).stream()
      .collect(
        Collectors.toMap(
            p -> p.getKey(),
            p -> StreamSupport.stream(p.getValue().spliterator(), false).collect(Collectors.toList())
        )
      );
  }

  private static <V> void addPairsToMap(
      Map<String, List<V>> map, final List<Pair<String, V>> pairs) {
    for (Pair<String, V> pair : pairs) {
      String key = pair.getKey();
      V value = pair.getValue();
      List<V> list = map.computeIfAbsent(key, k -> new ArrayList<>());
      list.add(value);
    }
  }

  private <T> void assertHoodieDataEquals(
      List<T> expectedList, HoodieData<T> hoodieData) {
    assertHoodieDataEquals(expectedList, hoodieData, Comparator.naturalOrder());
  }

  private <T> void assertHoodieDataEquals(
      List<T> expectedList, HoodieData<T> hoodieData, Comparator comparator) {
    assertEquals(expectedList,
        hoodieData.collectAsList().stream().sorted(comparator).collect(Collectors.toList())
    );
  }
}
