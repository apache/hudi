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

package org.apache.hudi.data.partitioner;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import scala.Tuple2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestConditionalRangePartitioner {
  private JavaSparkContext jsc;
  private Map<Integer, List<String>> splitPoints;
  private ConditionalRangePartitioner<Integer, String> partitioner;

  @BeforeEach
  public void setUp() {
    jsc = new JavaSparkContext("local[2]", "test");
    splitPoints = new HashMap<>();
    // Setup test data
    splitPoints.put(1, Arrays.asList("b", "d"));
    splitPoints.put(2, Arrays.asList("x", "z"));
    splitPoints.put(6, Collections.emptyList());
    splitPoints.put(7, Collections.singletonList("a"));
    partitioner = new ConditionalRangePartitioner<>(splitPoints);
  }

  @AfterEach
  public void tearDown() {
    if (jsc != null) {
      jsc.stop();
      jsc = null;
    }
  }

  @Test
  public void testConstructor() {
    assertEquals(9, partitioner.numPartitions());
    assertNotNull(partitioner);
  }

  @Test
  public void testComputeSplitPoints() {
    List<String> values = Arrays.asList("a", "b", "c", "d", "e", "f", "g");
    List<String> splits = ConditionalRangePartitioner.computeSplitPoints(values, 2);
    assertEquals(2, splits.size());
    assertEquals("c", splits.get(0));
    assertEquals("e", splits.get(1));
  }

  @Test
  public void testComputeSplitPointsWithEmptyList() {
    List<String> values = new ArrayList<>();
    assertEquals(new ArrayList<>(), ConditionalRangePartitioner.computeSplitPoints(values, 2));
  }

  @Test
  public void testGetPartition() {
    // Test key 1
    assertEquals(0, partitioner.getPartition(new Tuple2<>(1, "a")));
    assertEquals(0, partitioner.getPartition(new Tuple2<>(1, "b")));
    assertEquals(1, partitioner.getPartition(new Tuple2<>(1, "c")));
    assertEquals(1, partitioner.getPartition(new Tuple2<>(1, "d")));
    assertEquals(2, partitioner.getPartition(new Tuple2<>(1, "e")));
    assertEquals(2, partitioner.getPartition(new Tuple2<>(1, "f")));
    assertEquals(2, partitioner.getPartition(new Tuple2<>(1, "ffff")));

    // Test key 2
    assertEquals(3, partitioner.getPartition(new Tuple2<>(2, "w")));
    assertEquals(3, partitioner.getPartition(new Tuple2<>(2, "x")));
    assertEquals(4, partitioner.getPartition(new Tuple2<>(2, "y")));
    assertEquals(4, partitioner.getPartition(new Tuple2<>(2, "z")));
    assertEquals(5, partitioner.getPartition(new Tuple2<>(2, "zz")));

    // Test key 6
    assertEquals(6, partitioner.getPartition(new Tuple2<>(6, "a")));
    assertEquals(6, partitioner.getPartition(new Tuple2<>(6, "b")));
    assertEquals(6, partitioner.getPartition(new Tuple2<>(6, "z")));

    // Test key 7
    assertEquals(7, partitioner.getPartition(new Tuple2<>(7, "a")));
    assertEquals(8, partitioner.getPartition(new Tuple2<>(7, "zz")));

    // Test non-existent key
    assertThrows(IllegalArgumentException.class, () -> partitioner.getPartition(new Tuple2<>(0, "a")));
    assertThrows(IllegalArgumentException.class, () -> partitioner.getPartition(new Tuple2<>(3, "a")));
    assertThrows(IllegalArgumentException.class, () -> partitioner.getPartition(new Tuple2<>(4, "a")));
    assertThrows(IllegalArgumentException.class, () -> partitioner.getPartition(new Tuple2<>(5, "a")));
    assertThrows(IllegalArgumentException.class, () -> partitioner.getPartition(new Tuple2<>(999, "a")));
  }

  @Test
  public void testCompositeKeyComparator() {
    ConditionalRangePartitioner.CompositeKeyComparator<Integer, String> comparator =
        new ConditionalRangePartitioner.CompositeKeyComparator<>();

    // Test different keys
    assertTrue(comparator.compare(
        new Tuple2<>(1, "a"),
        new Tuple2<>(2, "a")) < 0);

    // Test same key, different values
    assertTrue(comparator.compare(
        new Tuple2<>(1, "a"),
        new Tuple2<>(1, "b")) < 0);

    // Test equal keys and values
    assertEquals(0, comparator.compare(
        new Tuple2<>(1, "a"),
        new Tuple2<>(1, "a")));
  }

  @Test
  public void testRepartitionWithSplits() {
    List<Tuple2<Integer, String>> data = Arrays.asList(
        new Tuple2<>(1, "a"),
        new Tuple2<>(1, "b"),
        new Tuple2<>(1, "c"),
        new Tuple2<>(2, "x"),
        new Tuple2<>(2, "y"),
        new Tuple2<>(2, "z")
    );

    JavaPairRDD<Integer, String> baseRdd = jsc.parallelizePairs(data);
    JavaPairRDD<Tuple2<Integer, String>, String> result =
        ConditionalRangePartitioner.repartitionWithSplits(baseRdd, partitioner);

    assertEquals(6, result.count());
    List<Tuple2<Tuple2<Integer, String>, String>> collected = result.collect();
    assertTrue(collected.stream().allMatch(t -> t._1()._2().equals(t._2())));
  }

  @Test
  public void testComputeSplitPointMapDistributed() {
    List<Tuple2<Integer, String>> data = Arrays.asList(
        new Tuple2<>(1, "a"),
        new Tuple2<>(1, "b"),
        new Tuple2<>(1, "c"),
        new Tuple2<>(1, "d"),
        new Tuple2<>(1, "e"),
        new Tuple2<>(2, "x"),
        new Tuple2<>(2, "y"),
        new Tuple2<>(2, "z")
    );

    JavaPairRDD<Integer, String> sampled = jsc.parallelizePairs(data);
    Map<Integer, List<String>> result = ConditionalRangePartitioner.computeSplitPointMapDistributed(
        sampled, 1.0, 2);

    assertEquals(Arrays.asList("b", "d"), result.get(1));
    assertEquals(Arrays.asList("y"), result.get(2));
  }

  @Test
  public void testComputeSplitPointMapDistributedWithEmptyData() {
    List<Tuple2<Integer, String>> data = new ArrayList<>();
    JavaPairRDD<Integer, String> sampled = jsc.parallelizePairs(data);
    Map<Integer, List<String>> result = ConditionalRangePartitioner.computeSplitPointMapDistributed(
        sampled, 1.0, 2);

    assertTrue(result.isEmpty());
  }

  @Test
  public void testComputeSplitPointMapDistributedWithSmallData() {
    List<Tuple2<Integer, String>> data = Arrays.asList(
        new Tuple2<>(1, "a"),
        new Tuple2<>(1, "b")
    );

    JavaPairRDD<Integer, String> sampled = jsc.parallelizePairs(data);
    Map<Integer, List<String>> result = ConditionalRangePartitioner.computeSplitPointMapDistributed(
        sampled, 1.0, 3);

    assertTrue(result.containsKey(1));
    assertTrue(result.get(1).isEmpty());
  }
} 