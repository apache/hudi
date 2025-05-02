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
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.testutils.HoodieClientTestBase;

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
}
