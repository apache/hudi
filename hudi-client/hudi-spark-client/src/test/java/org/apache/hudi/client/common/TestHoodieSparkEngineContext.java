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

package org.apache.hudi.client.common;

import org.apache.hudi.common.data.HoodieBroadcast;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodieData.HoodieDataCacheKey;
import org.apache.hudi.common.function.SerializableBiFunction;
import org.apache.hudi.common.function.SerializableFunction;
import org.apache.hudi.common.function.SerializablePairFlatMapFunction;
import org.apache.hudi.common.util.collection.ImmutablePair;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieSparkEngineContext extends SparkClientFunctionalTestHarness {

  private static final AtomicReference<Object> DRIVER_VALUE = new AtomicReference<>();

  private HoodieSparkEngineContext context;

  @BeforeEach
  void setUp() {
    context = new HoodieSparkEngineContext(jsc());
  }

  @Test
  void testAddRemoveCachedDataIds() {
    String basePath = "/tmp/foo";
    String instantTime = "000";
    context.putCachedDataIds(HoodieDataCacheKey.of(basePath, instantTime), 1, 2, 3);
    assertEquals(Arrays.asList(1, 2, 3), context.getCachedDataIds(HoodieDataCacheKey.of(basePath, instantTime)));
    assertEquals(Arrays.asList(1, 2, 3), context.removeCachedDataIds(HoodieDataCacheKey.of(basePath, instantTime)));
    assertTrue(context.getCachedDataIds(HoodieDataCacheKey.of(basePath, instantTime)).isEmpty());
  }

  @Test
  void testSetJobStatus() {
    // Test data
    String jobGroupId = "jobGroupId";
    String initialJobDescription = "initialJobDescription";
    String activeModule = "TestModule";
    String activityDescription = "Running test operation";
    String expectedJobDescription = String.format("%s:%s", activeModule, activityDescription);
    context.getJavaSparkContext().setJobGroup(jobGroupId, initialJobDescription);
    assertEquals(jobGroupId, context.getJavaSparkContext().getLocalProperty("spark.jobGroup.id"));
    assertEquals(initialJobDescription, context.getJavaSparkContext().getLocalProperty("spark.job.description"));

    // Set the job status
    context.setJobStatus(activeModule, activityDescription);
    assertEquals(expectedJobDescription, context.getJavaSparkContext().getLocalProperty("spark.job.description"));
    // Assert jobGroupId does not change
    assertEquals(jobGroupId, context.getJavaSparkContext().getLocalProperty("spark.jobGroup.id"));
  }

  @Test
  void testMapPartitionsToPairAndReduceByKey() {
    int numPartitions = 6;
    HoodieData<Integer> rddData = context.parallelize(
        IntStream.rangeClosed(0, 99).boxed().collect(Collectors.toList()), numPartitions);

    //
    /* output from map to pair.
    1 = 0, 1
    2 = 1, 5
    3 = 1, 5
    4 = 2, 10
    5 = 2, 10
    6 = 3, 15
    7 = 3, 15
    8 = 4, 20

    And then we do reduce by key, where we sum up the values.
    */
    List<ImmutablePair<Integer, Integer>> result = context.mapPartitionsToPairAndReduceByKey(rddData.collectAsList().stream(),
        new SerializablePairFlatMapTestFunc(), new SerializableReduceTestFunc(), 6).collect(Collectors.toList());
    assertEquals(50, result.size());
    result.forEach(entry -> {
      assertEquals(entry.getKey() * 10, entry.getValue());
    });
  }

  @Test
  void testBroadcastValueRoundTripsThroughTasks() {
    List<String> value = Arrays.asList("a", "bb", "ccc");
    HoodieBroadcast<List<String>> broadcast = context.broadcast(value);

    assertSame(value, broadcast.value());
    assertEquals(Arrays.asList(1, 2, 3), context.map(Arrays.asList(0, 1, 2), i -> broadcast.value().get(i).length(), 3));
  }

  @Test
  void testBroadcastValueIsNotInTaskClosure() throws IOException {
    String marker = "broadcast-value-marker";
    StringBuilder builder = new StringBuilder(marker);
    while (builder.length() < 1024 * 1024) {
      builder.append(marker);
    }
    String value = builder.toString();
    HoodieBroadcast<String> broadcast = context.broadcast(value);
    SerializableFunction<Integer, Integer> func = i -> broadcast.value().length() + i;

    byte[] handleBytes = javaSerialize(broadcast);
    byte[] closureBytes = javaSerialize(func);
    assertTrue(handleBytes.length < 4096, "Serialized broadcast handle is " + handleBytes.length + " bytes");
    assertTrue(closureBytes.length < 4096, "Serialized task closure is " + closureBytes.length + " bytes");
    assertFalse(new String(closureBytes, StandardCharsets.ISO_8859_1).contains(marker));
    assertEquals(Collections.singletonList(value.length() + 1), context.map(Collections.singletonList(1), func, 1));
  }

  /**
   * Tasks deserialize one copy of the value per executor (here the driver JVM in local mode), as they do on
   * a cluster, while the driver keeps its own instance; a destroyed broadcast cannot be used by new tasks.
   */
  @Test
  void testBroadcastTasksReadOneDeserializedCopy() {
    List<String> value = new ArrayList<>(Arrays.asList("a", "bb"));
    DRIVER_VALUE.set(value);
    HoodieBroadcast<List<String>> broadcast = context.broadcast(value);

    List<Integer> copyIdentities = context.map(Arrays.asList(0, 1, 2, 3), i -> {
      List<String> copy = broadcast.value();
      return copy != DRIVER_VALUE.get() && copy.equals(DRIVER_VALUE.get()) ? System.identityHashCode(copy) : -1;
    }, 4);

    assertSame(value, broadcast.value());
    assertFalse(copyIdentities.contains(-1));
    assertEquals(1, new HashSet<>(copyIdentities).size());

    broadcast.destroy();
    assertThrows(Exception.class, () -> context.map(Collections.singletonList(0), i -> broadcast.value().size(), 1));
  }

  private static byte[] javaSerialize(Object object) throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
      out.writeObject(object);
    }
    return bytes.toByteArray();
  }

  static class SerializablePairFlatMapTestFunc implements SerializablePairFlatMapFunction<Iterator<java.lang.Integer>, java.lang.Integer, java.lang.Integer> {
    @Override
    public Stream<Pair<Integer, Integer>> call(Iterator<Integer> t) throws Exception {
      List<Pair<Integer, Integer>> toReturn = new ArrayList<>();
      while (t.hasNext()) {
        Integer next = t.next();
        toReturn.add(Pair.of(next / 2, next / 2 * 5));
      }
      return toReturn.stream();
    }
  }

  static class SerializableReduceTestFunc implements SerializableBiFunction<Integer, Integer, Integer> {
    @Override
    public Integer apply(Integer integer, Integer integer2) {
      return integer + integer2;
    }
  }
}
