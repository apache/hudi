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

import org.apache.hudi.common.data.HoodieData.HoodieDataCacheKey;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestHoodieSparkEngineContext extends SparkClientFunctionalTestHarness {

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
}
