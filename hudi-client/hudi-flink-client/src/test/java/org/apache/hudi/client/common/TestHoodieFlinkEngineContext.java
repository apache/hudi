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

package org.apache.hudi.client.common;

import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodieListPairData;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.util.collection.ImmutablePair;
import org.apache.hudi.common.util.collection.Pair;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Unit test against HoodieFlinkEngineContext.
 */
public class TestHoodieFlinkEngineContext {
  private HoodieFlinkEngineContext context;

  @BeforeEach
  public void init() {
    context = HoodieFlinkEngineContext.DEFAULT;
  }

  @Test
  public void testMap() {
    List<Integer> mapList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    List<Integer> result = context.map(mapList, x -> x + 1, 2);
    result.removeAll(mapList);

    Assertions.assertEquals(1, result.size());
    Assertions.assertEquals(11, result.get(0));
  }

  @Test
  public void testFlatMap() {
    List<String> list1 = Arrays.asList("a", "b", "c");
    List<String> list2 = Arrays.asList("d", "e", "f");
    List<String> list3 = Arrays.asList("g", "h", "i");

    List<List<String>> inputList = new ArrayList<>();
    inputList.add(list1);
    inputList.add(list2);
    inputList.add(list3);

    List<String> result = context.flatMap(inputList, Collection::stream, 2);

    Assertions.assertEquals(9, result.size());
  }

  @Test
  public void testForeach() {
    List<Integer> mapList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    List<Integer> result = new ArrayList<>(10);
    context.foreach(mapList, result::add, 2);

    Assertions.assertEquals(result.size(), mapList.size());
    Assertions.assertTrue(result.containsAll(mapList));
  }

  @Test
  public void testMapToPair() {
    List<String> mapList = Arrays.asList("spark_hudi", "flink_hudi");

    Map<String, String> resultMap = context.mapToPair(mapList, x -> {
      String[] splits = x.split("_");
      return new ImmutablePair<>(splits[0], splits[1]);
    }, 2);

    Assertions.assertEquals(resultMap.get("spark"), resultMap.get("flink"));
  }

  @Test
  public void testMapGroupsByKeyUsesDedicatedForkJoinPool() {
    HoodiePairData<Integer, Integer> input = HoodieListPairData.eager(Arrays.asList(
        Pair.of(1, 3), Pair.of(1, 1), Pair.of(2, 4), Pair.of(2, 2), Pair.of(3, 5)));
    Set<ForkJoinPool> executingPools = ConcurrentHashMap.newKeySet();
    AtomicBoolean executedOutsideForkJoinPool = new AtomicBoolean(false);

    HoodieData<Integer> result = context.mapGroupsByKey(input, values -> {
      ForkJoinPool executingPool = ForkJoinTask.getPool();
      if (executingPool == null) {
        executedOutsideForkJoinPool.set(true);
      } else {
        executingPools.add(executingPool);
      }
      return values;
    }, Arrays.asList(1, 2, 3), false);

    Assertions.assertFalse(executedOutsideForkJoinPool.get());
    Assertions.assertEquals(1, executingPools.size());
    Assertions.assertNotSame(ForkJoinPool.commonPool(), executingPools.iterator().next());
    Assertions.assertEquals(3, executingPools.iterator().next().getParallelism());
    List<Integer> actual = result.collectAsList();
    Collections.sort(actual);
    Assertions.assertEquals(Arrays.asList(1, 2, 3, 4, 5), actual);
  }

  @Test
  public void testMapGroupsByKeyWithEmptyInput() {
    HoodiePairData<Integer, Integer> input = HoodieListPairData.eager(Collections.emptyList());

    HoodieData<Integer> result = context.mapGroupsByKey(
        input, values -> values, Collections.emptyList(), false);

    Assertions.assertTrue(result.collectAsList().isEmpty());
  }

}
