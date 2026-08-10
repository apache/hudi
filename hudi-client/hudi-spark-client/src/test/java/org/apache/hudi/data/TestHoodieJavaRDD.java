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

import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.testutils.HoodieClientTestBase;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.storage.StorageLevel;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieJavaRDD extends HoodieClientTestBase {
  @Test
  public void testGetNumPartitions() {
    int numPartitions = 6;
    HoodieData<Integer> rddData = HoodieJavaRDD.of(jsc.parallelize(
        IntStream.rangeClosed(0, 100).boxed().collect(Collectors.toList()), numPartitions));
    assertEquals(numPartitions, rddData.getNumPartitions());
  }

  @Test
  public void testDeduceNumPartitions() {
    int numPartitions = 100;
    jsc.sc().conf().remove("spark.default.parallelism");
    SQLConf.get().unsetConf("spark.sql.shuffle.partitions");

    // rdd parallelize
    SQLConf.get().setConfString("spark.sql.shuffle.partitions", "5");
    HoodieData<Integer> rddData = HoodieJavaRDD.of(jsc.parallelize(
        IntStream.rangeClosed(0, 100).boxed().collect(Collectors.toList()), numPartitions));
    assertEquals(5, rddData.deduceNumPartitions());

    // sql parallelize
    SQLConf.get().unsetConf("spark.sql.shuffle.partitions");
    jsc.sc().conf().set("spark.default.parallelism", "6");
    rddData = HoodieJavaRDD.of(jsc.parallelize(
        IntStream.rangeClosed(0, 100).boxed().collect(Collectors.toList()), numPartitions));
    assertEquals(6, rddData.deduceNumPartitions());

    // use partitioner num
    HoodiePairData<Integer, Integer> shuffleRDD = rddData.mapToPair(key -> Pair.of(key, 1))
        .reduceByKey((p1, p2) -> p1, 11);
    assertEquals(11, shuffleRDD.deduceNumPartitions());
  }

  @Test
  public void testRepartitionAndCoalesce() {
    int numPartitions = 100;
    // rdd parallelize
    HoodieData<Integer> rddData = HoodieJavaRDD.of(jsc.parallelize(
        IntStream.rangeClosed(0, 100).boxed().collect(Collectors.toList()), numPartitions));
    assertEquals(100, rddData.getNumPartitions());

    // repartition by 10.
    rddData = rddData.repartition(10);
    assertEquals(10, rddData.getNumPartitions());

    // coalesce to 5
    rddData = rddData.coalesce(5);
    assertEquals(5, rddData.getNumPartitions());

    // repartition to 20
    rddData = rddData.repartition(20);
    assertEquals(20, rddData.getNumPartitions());

    // but colesce may not expand the num partitions
    rddData = rddData.coalesce(40);
    assertEquals(20, rddData.getNumPartitions());
  }

  @Test
  void testMapPartitionsWithCloseable() {
    String partition1 = "partition1";
    String partition2 = "partition2";
    HoodieData<String> input = HoodieJavaRDD.of(Arrays.asList(partition1, partition2), context, 2);
    input.mapPartitions(partition -> new TrackingCloseableIterator<>(partition.next(), Collections.singletonList("a").iterator()), true)
        .collectAsList();
    assertTrue(TrackingCloseableIterator.isClosed(partition1));
    assertTrue(TrackingCloseableIterator.isClosed(partition2));
  }

  @Test
  void testFlatMapWithCloseable() {
    String partition1 = "partition1";
    String partition2 = "partition2";
    HoodieData<String> input = HoodieJavaRDD.of(Arrays.asList(partition1, partition2), context, 2);
    input.flatMap(partition -> new TrackingCloseableIterator<>(partition, Collections.singletonList("a").iterator()))
        .collectAsList();
    assertTrue(TrackingCloseableIterator.isClosed(partition1));
    assertTrue(TrackingCloseableIterator.isClosed(partition2));
  }

  @Test
  void testFlatMapToPairWithCloseable() {
    String partition1 = "partition1";
    String partition2 = "partition2";
    HoodieData<String> input = HoodieJavaRDD.of(Arrays.asList(partition1, partition2), context, 2);
    input.flatMapToPair(partition -> new TrackingCloseableIterator<>(partition, Collections.singletonList(Pair.of(1, "1")).iterator()))
        .collectAsList();
    assertTrue(TrackingCloseableIterator.isClosed(partition1));
    assertTrue(TrackingCloseableIterator.isClosed(partition2));
  }

  /**
   * Test unpersistWithDependencies on a DAG with multiple branches.
   *
   * DAG visualization:
   * <pre>
   *        [baseRDD] (PERSISTED)
   *         /      \
   *        /        \
   *   [branch1]   [branch2]
   *  (PERSISTED)  (PERSISTED)
   *        \        /
   *         \      /
   *        [joined]
   *           ↑
   *   unpersistWithDependencies() called here
   * </pre>
   *
   * Given: A DAG where baseRDD has two branches (branch1 and branch2), all persisted
   * When: unpersistWithDependencies() is called on the joined RDD
   * Then: All persisted RDDs in the entire DAG should be unpersisted
   */
  @Test
  public void testUnpersistWithDependenciesMultipleBranches() {
    // Given: Verify no RDDs are persisted initially
    assertTrue(jsc.sc().getPersistentRDDs().isEmpty());

    // Create a more complex DAG with multiple branches
    JavaRDD<Integer> baseRDD = jsc.parallelize(Arrays.asList(1, 2, 3, 4, 5), 2);
    baseRDD.persist(StorageLevel.MEMORY_ONLY());

    // Branch 1
    JavaRDD<Integer> branch1 = baseRDD.map(x -> x * 2);
    branch1.persist(StorageLevel.MEMORY_ONLY());

    // Branch 2  
    JavaRDD<Integer> branch2 = baseRDD.map(x -> x * 3);
    branch2.persist(StorageLevel.MEMORY_ONLY());

    // Join branches
    JavaRDD<Integer> joined = branch1.union(branch2);
    HoodieData<Integer> hoodieData = HoodieJavaRDD.of(joined);

    // Verify RDDs are persisted
    assertEquals(StorageLevel.MEMORY_ONLY(), baseRDD.getStorageLevel());
    assertEquals(StorageLevel.MEMORY_ONLY(), branch1.getStorageLevel());
    assertEquals(StorageLevel.MEMORY_ONLY(), branch2.getStorageLevel());

    // Verify we have 3 persistent RDDs in the context
    assertEquals(3, jsc.sc().getPersistentRDDs().size());
    assertTrue(jsc.sc().getPersistentRDDs().contains(baseRDD.rdd().id()));
    assertTrue(jsc.sc().getPersistentRDDs().contains(branch1.rdd().id()));
    assertTrue(jsc.sc().getPersistentRDDs().contains(branch2.rdd().id()));

    // When: Unpersist with dependencies
    hoodieData.unpersistWithDependencies();

    // Then: Verify all RDDs in the lineage are unpersisted
    assertEquals(StorageLevel.NONE(), baseRDD.getStorageLevel());
    assertEquals(StorageLevel.NONE(), branch1.getStorageLevel());
    assertEquals(StorageLevel.NONE(), branch2.getStorageLevel());

    // Verify no RDDs remain in the persistent RDDs list
    assertTrue(jsc.sc().getPersistentRDDs().isEmpty());
  }
}
