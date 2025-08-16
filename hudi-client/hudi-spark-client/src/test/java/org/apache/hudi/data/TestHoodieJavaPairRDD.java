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

package org.apache.hudi.data;

import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import scala.Tuple2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SuppressWarnings("unchecked")
public class TestHoodieJavaPairRDD {

  private static JavaSparkContext jsc;

  @BeforeEach
  public void setUp() {
    // Initialize Spark context and JavaPairRDD mock
    SparkConf conf = new SparkConf().setAppName("HoodieJavaPairRDDJoinTest").setMaster("local[2]");
    jsc = new JavaSparkContext(conf);
  }

  @AfterEach
  public void tearDown() {
    if (jsc != null) {
      jsc.stop();
    }
  }

  @Test
  public void testJoinOperation() {
    JavaPairRDD<String, String> partitionRecordKeyPairRDD = jsc.parallelize(Arrays.asList(
        new Tuple2<>("2017/10/22", "003"),
        new Tuple2<>("2017/10/22", "002"),
        new Tuple2<>("2017/10/22", "005"),
        new Tuple2<>("2017/10/22", "004"))).mapToPair(t -> t);

    JavaPairRDD<String, String> otherPairRDD = jsc.parallelize(Arrays.asList(
        new Tuple2<>("2017/10/22", "value1"),
        new Tuple2<>("2017/10/22", "value2"))).mapToPair(t -> t);

    HoodieJavaPairRDD<String, String> hoodiePairData = HoodieJavaPairRDD.of(partitionRecordKeyPairRDD);
    HoodieJavaPairRDD<String, String> otherHoodiePairData = HoodieJavaPairRDD.of(otherPairRDD);

    HoodiePairData<String, Pair<String, String>> result = hoodiePairData.join(otherHoodiePairData);

    List<Pair<String, Pair<String, String>>> resultList = result.collectAsList();
    assertEquals(8, resultList.size());
    resultList.forEach(item -> {
      assertEquals("2017/10/22", item.getLeft());
      assertTrue(Arrays.asList("003", "002", "005", "004").contains(item.getRight().getLeft()));
      assertTrue(Arrays.asList("value1", "value2").contains(item.getRight().getRight()));
    });
  }

  @Test
  public void testLeftOuterJoinOperation() {
    JavaPairRDD<String, String> partitionRecordKeyPairRDD = jsc.parallelize(Arrays.asList(
        new Tuple2<>("2017/10/22", "003"),
        new Tuple2<>("2017/10/22", "002"),
        new Tuple2<>("2017/10/22", "005"),
        new Tuple2<>("2017/10/22", "004"))).mapToPair(t -> t);

    JavaPairRDD<String, String> otherPairRDD = jsc.parallelize(Arrays.asList(
        new Tuple2<>("2017/10/22", "value1"))).mapToPair(t -> t);

    HoodieJavaPairRDD<String, String> hoodiePairData = HoodieJavaPairRDD.of(partitionRecordKeyPairRDD);
    HoodieJavaPairRDD<String, String> otherHoodiePairData = HoodieJavaPairRDD.of(otherPairRDD);

    HoodiePairData<String, Pair<String, Option<String>>> result = hoodiePairData.leftOuterJoin(otherHoodiePairData);

    List<Pair<String, Pair<String, Option<String>>>> resultList = result.collectAsList();
    assertEquals(4, resultList.size());
    resultList.forEach(item -> {
      assertEquals("2017/10/22", item.getLeft());
      assertTrue(Arrays.asList("003", "002", "005", "004").contains(item.getRight().getLeft()));
      assertEquals(Option.of("value1"), item.getRight().getRight());
    });
  }

  @Test
  void testFlatMapValuesWithCloseable() {
    String partition1 = "partition1";
    String partition2 = "partition2";
    HoodiePairData<Integer, String> input = HoodieJavaPairRDD.of(jsc.parallelizePairs(Arrays.asList(Tuple2.apply(1, partition1), Tuple2.apply(2, partition2)), 2));
    input.flatMapValues(partition -> new TrackingCloseableIterator<>(partition, Collections.singletonList(1).iterator()))
        .collectAsList();
    assertTrue(TrackingCloseableIterator.isClosed(partition1));
    assertTrue(TrackingCloseableIterator.isClosed(partition2));
  }

  /**
   * Test unpersistWithDependencies on a simple PairRDD.
   *
   * DAG visualization:
   * <pre>
   *   [javaPairRDD] (PERSISTED)
   *        ↑
   *   unpersistWithDependencies() called here
   * </pre>
   *
   * Given: A simple PairRDD that is persisted in memory
   * When: unpersistWithDependencies() is called on the HoodiePairData wrapper
   * Then: The PairRDD should be unpersisted and removed from Spark's persistent RDD tracking
   */
  @Test
  public void testUnpersistWithDependenciesSimple() {
    // Given: Verify no RDDs are persisted initially
    assertTrue(jsc.sc().getPersistentRDDs().isEmpty());

    // Create a simple PairRDD and persist it
    JavaPairRDD<String, Integer> javaPairRDD = jsc.parallelizePairs(Arrays.asList(
        new Tuple2<>("key1", 1),
        new Tuple2<>("key2", 2),
        new Tuple2<>("key3", 3)
    ), 2);
    HoodiePairData<String, Integer> hoodiePairData = HoodieJavaPairRDD.of(javaPairRDD);

    // Persist the RDD
    hoodiePairData.persist("MEMORY_ONLY");
    assertEquals(StorageLevel.MEMORY_ONLY(), javaPairRDD.getStorageLevel());

    // Verify RDD is in the persistent RDDs list
    assertEquals(1, jsc.sc().getPersistentRDDs().size());
    assertTrue(jsc.sc().getPersistentRDDs().contains(javaPairRDD.rdd().id()));

    // When: Unpersist with dependencies
    hoodiePairData.unpersistWithDependencies();

    // Then: Verify RDD is unpersisted
    assertEquals(StorageLevel.NONE(), javaPairRDD.getStorageLevel());

    // Verify no RDDs remain in the persistent RDDs list
    assertTrue(jsc.sc().getPersistentRDDs().isEmpty());
  }

  /**
   * Test unpersistWithDependencies on a chain of transformations from RDD to PairRDD.
   *
   * DAG visualization:
   * <pre>
   *   [baseRDD] (PERSISTED)
   *       ↓
   *   [pairRDD] (PERSISTED)
   *       ↓
   *   [reducedRDD]
   *       ↑
   *   unpersistWithDependencies() called here
   * </pre>
   *
   * Given: A chain where baseRDD is transformed to pairRDD, both persisted
   * When: unpersistWithDependencies() is called on the final reducedRDD
   * Then: All persisted RDDs in the dependency chain should be unpersisted
   */
  @Test
  public void testUnpersistWithDependenciesWithTransformations() {
    // Given: Verify no RDDs are persisted initially
    assertTrue(jsc.sc().getPersistentRDDs().isEmpty());

    // Create base RDD
    JavaRDD<String> baseRDD = jsc.parallelize(Arrays.asList("a", "b", "c", "d", "e"), 2);
    baseRDD.persist(StorageLevel.MEMORY_ONLY());

    // Transform to PairRDD
    JavaPairRDD<String, Integer> pairRDD = baseRDD.mapToPair(s -> new Tuple2<>(s, s.length()));
    pairRDD.persist(StorageLevel.MEMORY_ONLY());

    // Further transformation
    JavaPairRDD<String, Integer> reducedRDD = pairRDD.reduceByKey(Integer::sum);
    HoodiePairData<String, Integer> hoodiePairData = HoodieJavaPairRDD.of(reducedRDD);

    // Verify RDDs are persisted
    assertEquals(StorageLevel.MEMORY_ONLY(), baseRDD.getStorageLevel());
    assertEquals(StorageLevel.MEMORY_ONLY(), pairRDD.getStorageLevel());

    // Verify we have 2 persistent RDDs in the context
    assertEquals(2, jsc.sc().getPersistentRDDs().size());
    assertTrue(jsc.sc().getPersistentRDDs().contains(baseRDD.rdd().id()));
    assertTrue(jsc.sc().getPersistentRDDs().contains(pairRDD.rdd().id()));

    // When: Unpersist with dependencies
    hoodiePairData.unpersistWithDependencies();

    // Then: Verify all RDDs are unpersisted
    assertEquals(StorageLevel.NONE(), baseRDD.getStorageLevel());
    assertEquals(StorageLevel.NONE(), pairRDD.getStorageLevel());
    assertEquals(StorageLevel.NONE(), reducedRDD.getStorageLevel());

    // Verify no RDDs remain in the persistent RDDs list
    assertTrue(jsc.sc().getPersistentRDDs().isEmpty());
  }

  /**
   * Test unpersistWithDependencies on joined PairRDDs.
   * 
   * DAG visualization:
   * <pre>
   *   [leftRDD]     [rightRDD]
   *  (PERSISTED)   (PERSISTED)
   *       \           /
   *        \         /
   *         [joinedRDD]
   *             ↑
   *   unpersistWithDependencies() called here
   * </pre>
   * 
   * Given: Two persisted PairRDDs that are joined
   * When: unpersistWithDependencies() is called on the joined result
   * Then: Both input PairRDDs should be unpersisted
   */
  @Test
  public void testUnpersistWithDependenciesAfterJoin() {
    // Given: Verify no RDDs are persisted initially
    assertTrue(jsc.sc().getPersistentRDDs().isEmpty());

    // Create first PairRDD
    JavaPairRDD<String, String> leftRDD = jsc.parallelizePairs(Arrays.asList(
        new Tuple2<>("key1", "value1"),
        new Tuple2<>("key2", "value2")
    ), 2);
    leftRDD.persist(StorageLevel.MEMORY_ONLY());

    // Create second PairRDD
    JavaPairRDD<String, String> rightRDD = jsc.parallelizePairs(Arrays.asList(
        new Tuple2<>("key1", "valueA"),
        new Tuple2<>("key2", "valueB")
    ), 2);
    rightRDD.persist(StorageLevel.MEMORY_ONLY());

    // Join them
    JavaPairRDD<String, Tuple2<String, String>> joinedRDD = leftRDD.join(rightRDD);
    HoodiePairData<String, Tuple2<String, String>> hoodiePairData = HoodieJavaPairRDD.of(joinedRDD);

    // Verify RDDs are persisted
    assertEquals(StorageLevel.MEMORY_ONLY(), leftRDD.getStorageLevel());
    assertEquals(StorageLevel.MEMORY_ONLY(), rightRDD.getStorageLevel());

    // Verify we have 2 persistent RDDs in the context
    assertEquals(2, jsc.sc().getPersistentRDDs().size());
    assertTrue(jsc.sc().getPersistentRDDs().contains(leftRDD.rdd().id()));
    assertTrue(jsc.sc().getPersistentRDDs().contains(rightRDD.rdd().id()));

    // When: Unpersist with dependencies
    hoodiePairData.unpersistWithDependencies();

    // Then: Verify all RDDs are unpersisted
    assertEquals(StorageLevel.NONE(), leftRDD.getStorageLevel());
    assertEquals(StorageLevel.NONE(), rightRDD.getStorageLevel());

    // Verify no RDDs remain in the persistent RDDs list
    assertTrue(jsc.sc().getPersistentRDDs().isEmpty());
  }

  /**
   * Test unpersistWithDependencies on PairRDDs with no cached dependencies.
   *
   * DAG visualization:
   * <pre>
   *   [javaPairRDD] (NOT PERSISTED)
   *        ↑
   *   unpersistWithDependencies() called here
   * </pre>
   *
   * Given: A PairRDD that is not persisted
   * When: unpersistWithDependencies() is called
   * Then: No exception should be thrown and the RDD should remain unpersisted
   */
  @Test
  public void testUnpersistWithDependenciesNoCachedRDDs() {
    // Given: Test with no cached RDDs - should not throw any exceptions
    JavaPairRDD<String, Integer> javaPairRDD = jsc.parallelizePairs(Arrays.asList(
        new Tuple2<>("key1", 1),
        new Tuple2<>("key2", 2)
    ), 2);
    HoodiePairData<String, Integer> hoodiePairData = HoodieJavaPairRDD.of(javaPairRDD);

    // Verify RDD is not persisted
    assertEquals(StorageLevel.NONE(), javaPairRDD.getStorageLevel());
    assertTrue(jsc.sc().getPersistentRDDs().isEmpty());

    // When: This should not throw any exception
    hoodiePairData.unpersistWithDependencies();

    // Then: Verify RDD is still not persisted
    assertEquals(StorageLevel.NONE(), javaPairRDD.getStorageLevel());
    assertTrue(jsc.sc().getPersistentRDDs().isEmpty());
  }

  /**
   * Test unpersistWithDependencies on two separate PairRDD DAGs to ensure they don't affect each other.
   *
   * DAG visualization:
   * <pre>
   *   DAG 1:                          DAG 2:
   *   [baseRDD1] (PERSISTED)          [pairRDD2] (PERSISTED)
   *       ↓                                ↓
   *   [pairRDD1] (PERSISTED)          [groupedRDD2] (PERSISTED)
   *       ↓                                ↓
   *   [reducedRDD1]                   [mappedRDD2]
   *       ↑                                ↑
   *   unpersistWithDependencies() called here
   *
   *   After unpersisting DAG 1, DAG 2 should remain persisted
   * </pre>
   *
   * Given: Two separate DAGs with their own persisted RDDs
   * When: unpersistWithDependencies() is called on one DAG
   * Then: Only the RDDs in that DAG should be unpersisted, the other DAG should remain unaffected
   */
  @Test
  public void testUnpersistWithDependenciesSeparatePairRDDDAGs() {
    // Given: Verify no RDDs are persisted initially
    assertTrue(jsc.sc().getPersistentRDDs().isEmpty());
    // Create DAG 1: Regular RDD -> PairRDD transformation
    JavaRDD<String> baseRDD1 = jsc.parallelize(Arrays.asList("apple", "banana", "cherry"), 2);
    baseRDD1.persist(StorageLevel.MEMORY_ONLY());

    JavaPairRDD<String, Integer> pairRDD1 = baseRDD1.mapToPair(s -> new Tuple2<>(s, s.length()));
    pairRDD1.persist(StorageLevel.MEMORY_ONLY());

    JavaPairRDD<String, Integer> reducedRDD1 = pairRDD1.reduceByKey(Integer::sum);
    HoodiePairData<String, Integer> hoodiePairData1 = HoodieJavaPairRDD.of(reducedRDD1);

    // Create DAG 2: Direct PairRDD with transformations
    JavaPairRDD<String, Integer> pairRDD2 = jsc.parallelizePairs(Arrays.asList(
        new Tuple2<>("x", 1),
        new Tuple2<>("y", 2),
        new Tuple2<>("x", 3)
    ), 2);
    pairRDD2.persist(StorageLevel.MEMORY_ONLY());

    JavaPairRDD<String, Iterable<Integer>> groupedRDD2 = pairRDD2.groupByKey();
    groupedRDD2.persist(StorageLevel.MEMORY_ONLY());

    JavaPairRDD<String, Integer> mappedRDD2 = groupedRDD2.mapValues(values -> {
      int sum = 0;
      for (Integer v : values) {
        sum += v;
      }
      return sum;
    });
    HoodiePairData<String, Integer> hoodiePairData2 = HoodieJavaPairRDD.of(mappedRDD2);

    // Verify all 4 RDDs are persisted
    assertEquals(4, jsc.sc().getPersistentRDDs().size());
    assertEquals(StorageLevel.MEMORY_ONLY(), baseRDD1.getStorageLevel());
    assertEquals(StorageLevel.MEMORY_ONLY(), pairRDD1.getStorageLevel());
    assertEquals(StorageLevel.MEMORY_ONLY(), pairRDD2.getStorageLevel());
    assertEquals(StorageLevel.MEMORY_ONLY(), groupedRDD2.getStorageLevel());

    // When: Unpersist only DAG 1
    hoodiePairData1.unpersistWithDependencies();

    // Then: Verify DAG 1 RDDs are unpersisted
    assertEquals(StorageLevel.NONE(), baseRDD1.getStorageLevel());
    assertEquals(StorageLevel.NONE(), pairRDD1.getStorageLevel());
    assertEquals(StorageLevel.NONE(), reducedRDD1.getStorageLevel());

    // Verify DAG 2 RDDs are still persisted
    assertEquals(StorageLevel.MEMORY_ONLY(), pairRDD2.getStorageLevel());
    assertEquals(StorageLevel.MEMORY_ONLY(), groupedRDD2.getStorageLevel());

    // Verify we have 2 persistent RDDs remaining (from DAG 2)
    assertEquals(2, jsc.sc().getPersistentRDDs().size());
    assertTrue(jsc.sc().getPersistentRDDs().contains(pairRDD2.rdd().id()));
    assertTrue(jsc.sc().getPersistentRDDs().contains(groupedRDD2.rdd().id()));

    // Clean up DAG 2
    hoodiePairData2.unpersistWithDependencies();

    // Verify all RDDs are now unpersisted
    assertEquals(StorageLevel.NONE(), pairRDD2.getStorageLevel());
    assertEquals(StorageLevel.NONE(), groupedRDD2.getStorageLevel());
    assertTrue(jsc.sc().getPersistentRDDs().isEmpty());
  }
}
