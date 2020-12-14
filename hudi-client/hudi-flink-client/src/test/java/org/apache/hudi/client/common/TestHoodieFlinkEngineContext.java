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

import org.apache.hudi.client.FlinkTaskContextSupplier;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import scala.Tuple2;

/**
 * Unit test against HoodieFlinkEngineContext.
 */
public class TestHoodieFlinkEngineContext {
  private HoodieFlinkEngineContext context;

  @BeforeEach
  public void init() {
    context = new HoodieFlinkEngineContext(new FlinkTaskContextSupplier(null));
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
      return Tuple2.apply(splits[0], splits[1]);
    }, 2);

    Assertions.assertEquals(resultMap.get("spark"), resultMap.get("flink"));
  }

}
