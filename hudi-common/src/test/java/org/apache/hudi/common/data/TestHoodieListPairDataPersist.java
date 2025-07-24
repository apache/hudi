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

package org.apache.hudi.common.data;

import org.apache.hudi.common.util.HoodieDataUtils;
import org.apache.hudi.common.util.collection.Pair;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test suite for HoodieListPairData persist functionality to prevent
 * "stream has already been closed or operated upon" errors
 */
public class TestHoodieListPairDataPersist {

  @Test
  public void testLazyStreamWithoutPersistThrowsExceptionOnMultipleOperations() {
    // Given: A lazy HoodieListPairData created from a stream
    List<Pair<String, String>> testData = Arrays.asList(
        Pair.of("key1", "value1"),
        Pair.of("key2", "value2"),
        Pair.of("key3", "value3")
    );
    HoodiePairData<String, String> lazyPairData = HoodieListPairData.lazy(testData);

    // When: We perform the first operation (collectAsList)
    List<Pair<String, String>> firstResult = lazyPairData.collectAsList();
    assertEquals(3, firstResult.size(), "First operation should succeed");

    // Then: The second operation should fail because the stream is already consumed
    IllegalStateException exception = assertThrows(IllegalStateException.class, lazyPairData::collectAsList);
    assertTrue(exception.getMessage().contains("stream has already been operated upon or closed"),
        "Expected stream already operated error, but got: " + exception.getMessage());
  }

  @Test
  public void testLazyStreamWithPersistAllowsMultipleOperations() {
    // Given: A lazy HoodieListPairData created from a stream
    List<Pair<String, String>> testData = Arrays.asList(
        Pair.of("key1", "value1"),
        Pair.of("key2", "value2"),
        Pair.of("key3", "value3")
    );
    HoodiePairData<String, String> lazyPairData = HoodieListPairData.lazy(testData);

    // When: We persist the data before operations
    lazyPairData.persist("MEMORY_ONLY");

    // Then: Multiple operations should succeed without stream errors
    List<Pair<String, String>> firstResult = lazyPairData.collectAsList();
    assertEquals(3, firstResult.size(), "First operation should succeed");

    List<Pair<String, String>> secondResult = lazyPairData.collectAsList();
    assertEquals(3, secondResult.size(), "Second operation should also succeed");

    // Verify the results are the same
    assertEquals(firstResult, secondResult, "Both operations should return the same result");

    // Additional operations should also work
    long count = lazyPairData.count();
    assertEquals(3, count, "Count should work after persist");

    List<String> keys = lazyPairData.keys().collectAsList();
    assertEquals(new HashSet<>(Arrays.asList("key1", "key2", "key3")), new HashSet<>(keys), 
        "Keys should be retrievable");

    List<String> values = lazyPairData.values().collectAsList();
    assertEquals(new HashSet<>(Arrays.asList("value1", "value2", "value3")), new HashSet<>(values), 
        "Values should be retrievable");
  }

  @Test
  public void testEagerDataWorksWithMultipleOperationsWithoutPersist() {
    // Given: An eager HoodieListPairData
    List<Pair<String, String>> testData = Arrays.asList(
        Pair.of("key1", "value1"),
        Pair.of("key2", "value2"),
        Pair.of("key3", "value3")
    );
    HoodiePairData<String, String> eagerPairData = HoodieListPairData.eager(testData);

    // Then: Multiple operations should work without persist
    List<Pair<String, String>> firstResult = eagerPairData.collectAsList();
    assertEquals(3, firstResult.size());

    List<Pair<String, String>> secondResult = eagerPairData.collectAsList();
    assertEquals(3, secondResult.size());

    assertEquals(firstResult, secondResult, "Eager data should allow multiple operations");
  }

  @Test
  public void testUnpersistReleasesPersistedData() {
    // Given: A lazy HoodieListPairData that has been persisted
    List<Pair<String, String>> testData = Arrays.asList(
        Pair.of("key1", "value1"),
        Pair.of("key2", "value2"),
        Pair.of("key3", "value3")
    );
    HoodiePairData<String, String> lazyPairData = HoodieListPairData.lazy(testData);

    // When: We persist and then unpersist
    lazyPairData.persist("MEMORY_ONLY");
    List<Pair<String, String>> afterPersist = lazyPairData.collectAsList();
    assertEquals(3, afterPersist.size(), "Should work after persist");

    lazyPairData.unpersist();

    // Then: After unpersist, the original stream is already consumed,
    // so operations will fail (this verifies unpersist cleared the persisted data)
    IllegalStateException exception = assertThrows(IllegalStateException.class, () -> {
      lazyPairData.collectAsList();
    });
    assertTrue(exception.getMessage().contains("stream has already been operated upon or closed"),
        "After unpersist, should revert to original consumed stream behavior");
  }

  @Test
  public void testPersistIsIdempotent() {
    // Given: A lazy HoodieListPairData
    List<Pair<String, String>> testData = Arrays.asList(
        Pair.of("key1", "value1"),
        Pair.of("key2", "value2")
    );
    HoodiePairData<String, String> lazyPairData = HoodieListPairData.lazy(testData);

    // When: We call persist multiple times
    lazyPairData.persist("MEMORY_ONLY");
    List<Pair<String, String>> firstResult = lazyPairData.collectAsList();

    lazyPairData.persist("MEMORY_ONLY"); // Second persist
    List<Pair<String, String>> secondResult = lazyPairData.collectAsList();

    // Then: Results should be consistent
    assertEquals(firstResult, secondResult, "Multiple persist calls should not affect the result");
    assertEquals(2, firstResult.size());
  }

  @Test
  public void testComplexOperationsWithPersist() {
    // Given: A lazy HoodieListPairData with duplicate keys
    List<Pair<String, String>> testData = Arrays.asList(
        Pair.of("key1", "value1"),
        Pair.of("key1", "value2"),
        Pair.of("key2", "value3"),
        Pair.of("key2", "value4")
    );
    HoodiePairData<String, String> lazyPairData = HoodieListPairData.lazy(testData);

    // When: We persist before multiple complex operations
    lazyPairData.persist("MEMORY_ONLY");

    // Then: Multiple operations should work
    // Operation 1: Filter non-null keys
    HoodiePairData<String, String> nonNullKeys = lazyPairData.filter((k, v) -> k != null);
    List<Pair<String, String>> nonNullList = nonNullKeys.collectAsList();
    assertEquals(4, nonNullList.size(), "Should have 4 non-null entries");

    // Operation 2: Group by key
    HoodiePairData<String, Iterable<String>> grouped = lazyPairData.groupByKey();
    assertEquals(2, grouped.count(), "Should have 2 unique keys (including null)");

    // Operation 3: Count again
    long totalCount = lazyPairData.count();
    assertEquals(4, totalCount, "Should still have all 4 entries");

    // Operation 4: Reduce by key
    HoodiePairData<String, String> reduced = lazyPairData.reduceByKey((v1, v2) -> v1 + "," + v2, 1);
    List<Pair<String, String>> reducedList = reduced.collectAsList();
    assertEquals(2, reducedList.size(), "Should have 2 entries after reduce");

    // Verify reduced values
    Map<String, String> reducedMap = reducedList.stream()
        .collect(java.util.stream.Collectors.toMap(Pair::getKey, Pair::getValue));
    assertNotNull(reducedMap.get("key1"));
    assertTrue(reducedMap.get("key1").contains(","), "key1 should have combined values");
    assertNotNull(reducedMap.get("key2"));
    assertTrue(reducedMap.get("key2").contains(","), "key2 should have combined values");
  }

  @Test
  public void testHoodieDataUtilsDedupeAndCollectAsMapWithPersist() {
    // Given: A lazy HoodieListPairData with duplicate keys and null keys
    List<Pair<String, String>> testData = Arrays.asList(
        Pair.of("key1", "value1"),
        Pair.of("key1", "value2"),  // Duplicate key
        Pair.of("key2", "value3"),
        Pair.of(null, "nullValue1"),
        Pair.of(null, "nullValue2"),  // Duplicate null key
        Pair.of("key3", "value4")
    );
    HoodiePairData<String, String> lazyPairData = HoodieListPairData.lazy(testData);

    // When: We use dedupeAndCollectAsMap which internally uses persist
    Map<String, String> result = HoodieDataUtils.dedupeAndCollectAsMap(lazyPairData);

    // Then: The operation should succeed without stream errors
    assertEquals(4, result.size(), "Should have 4 entries (3 non-null + 1 null)");
    assertTrue(result.containsKey(null), "Should contain null key");
    assertEquals("value2", result.get("key1"), "key1 should have the last value");
    assertEquals("value3", result.get("key2"));
    assertEquals("value4", result.get("key3"));
  }

  @Test
  public void testHoodieDataUtilsDedupeAndCollectAsMapOfSetWithPersist() {
    // Given: A lazy HoodieListPairData with duplicate keys and values
    List<Pair<String, String>> testData = Arrays.asList(
        Pair.of("key1", "value1a"),
        Pair.of("key1", "value1b"),
        Pair.of("key2", "value2"),
        Pair.of(null, "nullValue1"),
        Pair.of(null, "nullValue2"),
        Pair.of(null, "nullValue1")  // Duplicate null value
    );
    HoodiePairData<String, String> lazyPairData = HoodieListPairData.lazy(testData);

    // When: We use dedupeAndCollectAsMapOfSet which internally uses persist
    Map<String, Set<String>> result = HoodieDataUtils.dedupeAndCollectAsMapOfSet(lazyPairData);

    // Then: The operation should succeed without stream errors
    assertEquals(3, result.size(), "Should have 3 entries (2 non-null + 1 null)");
    assertTrue(result.containsKey(null), "Should contain null key");
    
    Set<String> key1Values = result.get("key1");
    assertEquals(2, key1Values.size());
    assertTrue(key1Values.contains("value1a"));
    assertTrue(key1Values.contains("value1b"));
    
    Set<String> nullValues = result.get(null);
    assertEquals(2, nullValues.size(), "Null key should have 2 distinct values");
    assertTrue(nullValues.contains("nullValue1"));
    assertTrue(nullValues.contains("nullValue2"));
  }
}