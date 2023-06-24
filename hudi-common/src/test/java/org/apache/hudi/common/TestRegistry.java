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

package org.apache.hudi.common;

import org.apache.hudi.common.metrics.Registry;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests metrics {@link Registry}.
 */
public class TestRegistry {

  @Test
  public void testGetRegistry() throws Exception {
    Registry r = Registry.getRegistry("testGetRegistry_1");
    assertEquals(r, Registry.getRegistry("testGetRegistry_1"));
  }

  private void registerMetrics(Map<String, Long> map, Registry registry) {
    map.forEach((k, v) -> registry.add(k, v));
  }

  @Test
  public void testGetAllMetrics() throws Exception {
    String registryName = "testGetAllMetrics";
    Registry r = Registry.getRegistry(registryName);
    Map<String, Long> countsMap = new HashMap<>();

    countsMap.put("one", 1L);
    registerMetrics(countsMap, r);
    Map<String, Long> allMetrics1 = Registry.getAllMetrics(false, true);
    assertTrue(allMetrics1.containsKey(registryName + ".one"));

    countsMap.remove("one");
    countsMap.put("two", 2L);
    registerMetrics(countsMap, r);
    Map<String, Long> allMetrics2 = Registry.getAllMetrics(true, true);
    assertTrue(allMetrics2.containsKey(registryName + ".one"));
    assertTrue(allMetrics2.containsKey(registryName + ".two"));

    Map<String, Long> allMetrics3 = Registry.getAllMetrics(false, true);
    assertTrue(allMetrics3.isEmpty());
  }

  @Test
  public void testCounts() throws Exception {
    Registry r = Registry.getRegistry("testCounts");
    Map<String, Long> countsMap = new HashMap<>();
    countsMap.put("one", 1L);
    countsMap.put("two", 2L);

    registerMetrics(countsMap, r);
    assertEquals(countsMap, r.getAllCounts());
  }

}