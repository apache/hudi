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

package org.apache.hudi.common.engine;

import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodieListPairData;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.function.SerializableFunction;
import org.apache.hudi.common.util.collection.ImmutablePair;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.storage.HoodieStorage;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.apache.hudi.common.testutils.HoodieTestUtils.getDefaultStorage;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieLocalEngineContext {

  private HoodieLocalEngineContext context;

  @BeforeEach
  void setUp() {
    HoodieStorage storageConf = getDefaultStorage();
    context = new HoodieLocalEngineContext(storageConf.getConf());
  }

  @Test
  void testProcessKeyGroups() {
    // Create test data with unsorted values for the same key
    List<Pair<String, Integer>> unsortedPairs = Arrays.asList(
        ImmutablePair.of("key2", 5),
        ImmutablePair.of("key1", 3),
        ImmutablePair.of("key1", 1),
        ImmutablePair.of("key3", 7),
        ImmutablePair.of("key1", 2),
        ImmutablePair.of("key2", 4)
    );
    
    HoodiePairData<String, Integer> pairData = HoodieListPairData.lazy(unsortedPairs);
    
    // Create a function that collects the values and verifies they are sorted
    SerializableFunction<Iterator<Integer>, Iterator<String>> func = iterator -> {
      List<Integer> values = new ArrayList<>();
      iterator.forEachRemaining(values::add);
      
      // Verify that values are sorted
      for (int i = 1; i < values.size(); i++) {
        assertTrue(values.get(i - 1) <= values.get(i), 
            "Values should be sorted: " + values.get(i - 1) + " <= " + values.get(i));
      }
      
      // Return the values as strings for verification
      return values.stream().map(String::valueOf).iterator();
    };
    
    List<String> shardIndices = Arrays.asList("key1", "key2", "key3");
    HoodieData<String> result = context.mapGroupsByKey(pairData, func, shardIndices, false);
    
    List<String> resultList = result.collectAsList();
    
    // Verify the results
    // key1 should have values: 1, 2, 3
    // key2 should have values: 4, 5
    // key3 should have values: 7
    assertEquals(6, resultList.size());
    
    // Check that we have the expected sorted values
    assertTrue(resultList.contains("1"));
    assertTrue(resultList.contains("2"));
    assertTrue(resultList.contains("3"));
    assertTrue(resultList.contains("4"));
    assertTrue(resultList.contains("5"));
    assertTrue(resultList.contains("7"));
  }

  @Test
  void testProcessKeyGroupsWithStrings() {
    // Create test data with unsorted string values for the same key
    List<Pair<String, String>> unsortedPairs = Arrays.asList(
        ImmutablePair.of("key1", "zebra"),
        ImmutablePair.of("key1", "apple"),
        ImmutablePair.of("key1", "banana"),
        ImmutablePair.of("key2", "cherry"),
        ImmutablePair.of("key2", "date")
    );
    
    HoodiePairData<String, String> pairData = HoodieListPairData.lazy(unsortedPairs);
    
    // Create a function that collects the values and verifies they are sorted
    SerializableFunction<Iterator<String>, Iterator<String>> func = iterator -> {
      List<String> values = new ArrayList<>();
      iterator.forEachRemaining(values::add);
      
      // Verify that values are sorted
      for (int i = 1; i < values.size(); i++) {
        assertTrue(values.get(i - 1).compareTo(values.get(i)) <= 0, 
            "Values should be sorted: " + values.get(i - 1) + " <= " + values.get(i));
      }
      
      // Return the values as-is for verification
      return values.iterator();
    };
    
    List<String> shardIndices = Arrays.asList("key1", "key2");
    HoodieData<String> result = context.mapGroupsByKey(pairData, func, shardIndices, false);
    
    List<String> resultList = result.collectAsList();
    
    // Verify the results
    // key1 should have values: apple, banana, zebra
    // key2 should have values: cherry, date
    assertEquals(5, resultList.size());
    
    // Check that we have the expected sorted values
    assertTrue(resultList.contains("apple"));
    assertTrue(resultList.contains("banana"));
    assertTrue(resultList.contains("zebra"));
    assertTrue(resultList.contains("cherry"));
    assertTrue(resultList.contains("date"));
  }

  @Test
  void testProcessKeyGroupsWithSingleValue() {
    // Create test data with single values per key
    List<Pair<String, Integer>> singleValuePairs = Arrays.asList(
        ImmutablePair.of("key1", 42),
        ImmutablePair.of("key2", 17)
    );
    
    HoodiePairData<String, Integer> pairData = HoodieListPairData.lazy(singleValuePairs);
    
    // Create a function that just returns the values
    SerializableFunction<Iterator<Integer>, Iterator<Integer>> func = iterator -> {
      List<Integer> values = new ArrayList<>();
      iterator.forEachRemaining(values::add);
      return values.iterator();
    };
    
    List<String> shardIndices = Arrays.asList("key1", "key2");
    HoodieData<Integer> result = context.mapGroupsByKey(pairData, func, shardIndices, false);
    
    List<Integer> resultList = result.collectAsList();
    
    // Verify the results
    assertEquals(2, resultList.size());
    assertTrue(resultList.contains(42));
    assertTrue(resultList.contains(17));
  }
} 