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

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestHoodieMapPair {

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

  private static List<Pair<String, String>> TEST_PAIRS;
  private static HoodiePairData<String, String> TEST_HOODIE_MAP_PAIR;

  @BeforeAll
  public static void setup() {
    TEST_PAIRS = constructPairs();
    TEST_HOODIE_MAP_PAIR = constructTestMapPairData(TEST_PAIRS);
  }

  @Test
  public void testKeys() {
    assertHoodieDataEquals(Arrays.asList(KEY1, KEY2, KEY3, KEY4), TEST_HOODIE_MAP_PAIR.keys());
  }

  @Test
  public void testValues() {
    assertHoodieDataEquals(Arrays.asList(
        STRING_VALUE1, STRING_VALUE2, STRING_VALUE3, STRING_VALUE4, STRING_VALUE5, STRING_VALUE6),
        TEST_HOODIE_MAP_PAIR.values());
  }

  @Test
  public void testCount() {
    assertEquals(6, TEST_HOODIE_MAP_PAIR.count());
  }

  @Test
  public void testCountByKey() {
    Map<String, Long> expectedResultMap = new HashMap<>();
    expectedResultMap.put(KEY1, 2L);
    expectedResultMap.put(KEY2, 2L);
    expectedResultMap.put(KEY3, 1L);
    expectedResultMap.put(KEY4, 1L);

    assertEquals(expectedResultMap, TEST_HOODIE_MAP_PAIR.countByKey());
  }

  @Test
  public void testMap() {
    assertHoodieDataEquals(Arrays.asList(
        "key1,value1", "key1,value2", "key2,value3", "key2,value4", "key3,value5", "key4,value6"),
        TEST_HOODIE_MAP_PAIR.map(pair -> pair.getKey() + "," + pair.getValue()));
  }

  @Test
  public void testMapToPair() {
    Map<String, List<Integer>> expectedResultMap = new HashMap<>();
    expectedResultMap.put("key10", Arrays.asList(1, 2));
    expectedResultMap.put("key20", Arrays.asList(3, 4));
    expectedResultMap.put("key30", Arrays.asList(5));
    expectedResultMap.put("key40", Arrays.asList(6));
    assertEquals(expectedResultMap, HoodieMapPair.getMapPair(
        TEST_HOODIE_MAP_PAIR.mapToPair(
            pair -> {
              String value = pair.getValue();
              return new ImmutablePair<>(pair.getKey() + "0",
                  Integer.parseInt(String.valueOf(value.charAt(value.length() - 1))));
            })));
  }

  @Test
  public void testLeftOuterJoinSingleValuePerKey() {
    HoodiePairData<String, String> pairData1 = constructTestMapPairData(Arrays.asList(
        ImmutablePair.of(KEY1, STRING_VALUE1),
        ImmutablePair.of(KEY2, STRING_VALUE2),
        ImmutablePair.of(KEY3, STRING_VALUE3),
        ImmutablePair.of(KEY4, STRING_VALUE4)
    ));

    HoodiePairData<String, Integer> pairData2 = constructTestMapPairData(Arrays.asList(
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
        HoodieMapPair.getMapPair(pairData1.leftOuterJoin(pairData2)));
  }

  @Test
  public void testLeftOuterJoinMultipleValuesPerKey() {
    HoodiePairData<String, Integer> otherPairData = constructTestMapPairData(Arrays.asList(
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
        HoodieMapPair.getMapPair(TEST_HOODIE_MAP_PAIR.leftOuterJoin(otherPairData)));
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

  private static <V> HoodiePairData<String, V> constructTestMapPairData(
      final List<Pair<String, V>> pairs) {
    Map<String, List<V>> map = new HashMap<>();
    addPairsToMap(map, pairs);
    return HoodieMapPair.of(map);
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
