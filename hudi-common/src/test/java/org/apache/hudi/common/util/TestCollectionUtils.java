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

package org.apache.hudi.common.util;

import org.apache.hudi.common.util.collection.Pair;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.util.CollectionUtils.append;
import static org.apache.hudi.common.util.CollectionUtils.batches;
import static org.apache.hudi.common.util.CollectionUtils.combine;
import static org.apache.hudi.common.util.CollectionUtils.containsAll;
import static org.apache.hudi.common.util.CollectionUtils.copy;
import static org.apache.hudi.common.util.CollectionUtils.createImmutableList;
import static org.apache.hudi.common.util.CollectionUtils.createImmutableMap;
import static org.apache.hudi.common.util.CollectionUtils.createImmutableSet;
import static org.apache.hudi.common.util.CollectionUtils.createSet;
import static org.apache.hudi.common.util.CollectionUtils.diff;
import static org.apache.hudi.common.util.CollectionUtils.diffSet;
import static org.apache.hudi.common.util.CollectionUtils.elementsEqual;
import static org.apache.hudi.common.util.CollectionUtils.isNullOrEmpty;
import static org.apache.hudi.common.util.CollectionUtils.nonEmpty;
import static org.apache.hudi.common.util.CollectionUtils.reduce;
import static org.apache.hudi.common.util.CollectionUtils.reverseMap;
import static org.apache.hudi.common.util.CollectionUtils.tail;
import static org.apache.hudi.common.util.CollectionUtils.toList;
import static org.apache.hudi.common.util.CollectionUtils.toStream;
import static org.apache.hudi.common.util.CollectionUtils.zipToMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestCollectionUtils {

  private static Stream<Arguments> containsAllArgs() {
    Map<String, String> m0 = new HashMap<>();
    m0.put("k0", "v0");
    m0.put("k1", "v1");
    m0.put("k2", "v2");
    Map<String, String> m1 = new HashMap<>();
    m1.put("k1", "v1");
    Map<String, String> m2 = new HashMap<>();
    m2.put("k2", "v2");
    m2.put("k", "v");
    Map<String, String> m3 = Collections.emptyMap();
    Map<String, String> m4 = new HashMap<>();
    m4.put("k0", null);
    Map<String, Integer> m5 = new HashMap<>();
    m5.put("k0", 0);

    List<Arguments> argsList = new ArrayList<>();

    argsList.add(Arguments.of(m0, m1, true));
    argsList.add(Arguments.of(m0, m3, true));
    argsList.add(Arguments.of(m5, m3, true));
    argsList.add(Arguments.of(m0, m4, false));
    argsList.add(Arguments.of(m0, m2, false));
    argsList.add(Arguments.of(m0, m5, false));

    return argsList.stream();
  }

  @ParameterizedTest
  @MethodSource("containsAllArgs")
  void containsAllOnMaps(Map<?, ?> m1, Map<?, ?> m2, boolean expectedResult) {
    assertEquals(expectedResult, containsAll(m1, m2));
  }

  @Test
  void getBatchesFromList() {
    assertThrows(IllegalArgumentException.class, () -> {
      batches(Collections.emptyList(), -1);
    });

    assertThrows(IllegalArgumentException.class, () -> {
      batches(Collections.emptyList(), 0);
    });

    assertEquals(Collections.emptyList(), batches(Collections.emptyList(), 1));

    List<List<Integer>> intsBatches1 = batches(Arrays.asList(1, 2, 3, 4, 5, 6), 3);
    assertEquals(2, intsBatches1.size());
    assertEquals(Arrays.asList(1, 2, 3), intsBatches1.get(0));
    assertEquals(Arrays.asList(4, 5, 6), intsBatches1.get(1));

    List<List<Integer>> intsBatches2 = batches(Arrays.asList(1, 2, 3, 4, 5, 6), 5);
    assertEquals(2, intsBatches2.size());
    assertEquals(Arrays.asList(1, 2, 3, 4, 5), intsBatches2.get(0));
    assertEquals(Collections.singletonList(6), intsBatches2.get(1));
  }

  @Test
  void isNullOrEmptyAndNonEmptyForCollections() {
    assertTrue(isNullOrEmpty((List<?>) null));
    assertTrue(isNullOrEmpty(Collections.emptyList()));
    assertFalse(isNullOrEmpty(Collections.singletonList("a")));
    assertFalse(nonEmpty((List<?>) null));
    assertFalse(nonEmpty(Collections.emptyList()));
    assertTrue(nonEmpty(Collections.singletonList("a")));
  }

  @Test
  void isNullOrEmptyAndNonEmptyForMaps() {
    assertTrue(isNullOrEmpty((Map<?, ?>) null));
    assertTrue(isNullOrEmpty(Collections.emptyMap()));
    assertFalse(isNullOrEmpty(Collections.singletonMap("k", "v")));
    assertFalse(nonEmpty((Map<?, ?>) null));
    assertTrue(nonEmpty(Collections.singletonMap("k", "v")));
  }

  @Test
  void reduceAppliesReducerSequentially() {
    int sum = reduce(Arrays.asList(1, 2, 3, 4), 0, Integer::sum);
    assertEquals(10, sum);
    assertEquals(100, reduce(Collections.<Integer>emptyList(), 100, Integer::sum));
  }

  @Test
  void copyReturnsIndependentProperties() {
    Properties original = new Properties();
    original.setProperty("k", "v");
    Properties copied = copy(original);
    assertEquals("v", copied.getProperty("k"));
    copied.setProperty("k2", "v2");
    assertFalse(original.containsKey("k2"), "Copy must not share state with the original");
  }

  @Test
  void tailReturnsLastElementAndRejectsEmpty() {
    assertEquals("c", tail(new String[] {"a", "b", "c"}));
    assertThrows(IllegalArgumentException.class, () -> tail(new String[0]));
  }

  @Test
  void toStreamAndToListDrainIterator() {
    List<Integer> source = Arrays.asList(1, 2, 3);
    assertEquals(source, toList(source.iterator()));
    assertEquals(source, toStream(source.iterator()).collect(Collectors.toList()));
  }

  @Test
  void combineArraysAndAppendElement() {
    Integer[] combined = combine(new Integer[] {1, 2}, new Integer[] {3, 4});
    assertEquals(Arrays.asList(1, 2, 3, 4), Arrays.asList(combined));
    Integer[] appended = append(new Integer[] {1, 2}, 3);
    assertEquals(Arrays.asList(1, 2, 3), Arrays.asList(appended));
  }

  @Test
  void combineListsAndMaps() {
    assertEquals(Arrays.asList(1, 2, 3, 4), combine(Arrays.asList(1, 2), Arrays.asList(3, 4)));

    Map<String, Integer> one = new HashMap<>();
    one.put("a", 1);
    one.put("b", 2);
    Map<String, Integer> another = new HashMap<>();
    another.put("b", 20);
    another.put("c", 3);

    Map<String, Integer> overridden = combine(one, another);
    assertEquals(20, overridden.get("b"), "Second map should override on key conflict");
    assertEquals(3, overridden.size());

    Map<String, Integer> merged = combine(one, another, Integer::sum);
    assertEquals(22, merged.get("b"), "Merge function should combine overlapping values");
    assertEquals(1, merged.get("a"));
    assertEquals(3, merged.get("c"));
  }

  @Test
  void zipToMapPairsKeysAndValues() {
    Map<String, Integer> zipped = zipToMap(Arrays.asList("a", "b"), Arrays.asList(1, 2));
    assertEquals(1, zipped.get("a"));
    assertEquals(2, zipped.get("b"));
    assertThrows(IllegalArgumentException.class,
        () -> zipToMap(Arrays.asList("a", "b"), Collections.singletonList(1)));
  }

  @Test
  void diffAndDiffSetRemoveCommonElements() {
    Set<Integer> setDiff = diffSet(Arrays.asList(1, 2, 3), new HashSet<>(Arrays.asList(2, 3)));
    assertEquals(Collections.singleton(1), setDiff);

    List<Integer> listDiff = diff(Arrays.asList(1, 2, 2, 3), Collections.singletonList(3));
    assertEquals(Arrays.asList(1, 2, 2), listDiff);
  }

  @Test
  void elementsEqualComparesInOrder() {
    assertTrue(elementsEqual(Arrays.asList(1, 2, 3).iterator(), Arrays.asList(1, 2, 3).iterator()));
    assertFalse(elementsEqual(Arrays.asList(1, 2).iterator(), Arrays.asList(1, 2, 3).iterator()));
    assertFalse(elementsEqual(Arrays.asList(1, 2, 3).iterator(), Arrays.asList(1, 2).iterator()));
    assertFalse(elementsEqual(Arrays.asList(1, 9).iterator(), Arrays.asList(1, 2).iterator()));
  }

  @Test
  void createImmutableCollectionsRejectMutation() {
    List<String> list = createImmutableList("a", "b");
    assertEquals(Arrays.asList("a", "b"), list);
    assertThrows(UnsupportedOperationException.class, () -> list.add("c"));

    Set<String> set = createImmutableSet("a", "b");
    assertEquals(new HashSet<>(Arrays.asList("a", "b")), set);
    assertThrows(UnsupportedOperationException.class, () -> set.add("c"));

    Map<String, Integer> map = createImmutableMap(Pair.of("a", 1), Pair.of("b", 2));
    assertEquals(1, map.get("a"));
    assertEquals(2, map.get("b"));
    assertThrows(UnsupportedOperationException.class, () -> map.put("c", 3));
  }

  @Test
  void createSetCollectsDistinctElements() {
    assertEquals(new HashSet<>(Arrays.asList("a", "b")), createSet("a", "b", "a"));
  }

  @Test
  void reverseMapSwapsKeysAndValues() {
    Map<String, Integer> source = new HashMap<>();
    source.put("a", 1);
    source.put("b", 2);
    Map<Integer, String> reversed = reverseMap(source);
    assertEquals("a", reversed.get(1));
    assertEquals("b", reversed.get(2));
    assertThrows(UnsupportedOperationException.class, () -> reversed.put(3, "c"));
  }

  @Test
  void emptyPropsReturnsSharedSingleton() {
    // Emptiness also guards against illegal mutation of the shared singleton elsewhere.
    assertTrue(CollectionUtils.emptyProps().isEmpty());
    assertSame(CollectionUtils.emptyProps(), CollectionUtils.emptyProps());
  }
}
