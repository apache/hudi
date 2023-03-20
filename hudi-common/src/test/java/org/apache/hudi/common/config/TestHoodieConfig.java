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

package org.apache.hudi.common.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests {@link HoodieConfig}.
 */
public class TestHoodieConfig {
  @Test
  public void testHoodieConfig() {
    // Case 1: defaults and infer function are used
    HoodieTestFakeConfig config1 = HoodieTestFakeConfig.newBuilder().build();
    assertEquals("1", config1.getFakeString());
    assertEquals(0, config1.getFakeInteger());
    assertEquals("value3", config1.getFakeStringNoDefaultWithInfer());
    assertEquals(null, config1.getFakeStringNoDefaultWithInferEmpty());

    // Case 2: FAKE_STRING_CONFIG is set.  FAKE_INTEGER_CONFIG,
    // FAKE_STRING_CONFIG_NO_DEFAULT_WITH_INFER, and
    // FAKE_STRING_CONFIG_NO_DEFAULT_WITH_INFER_EMPTY are inferred
    HoodieTestFakeConfig config2 = HoodieTestFakeConfig.newBuilder()
        .withFakeString("value1").build();
    assertEquals("value1", config2.getFakeString());
    assertEquals(0, config2.getFakeInteger());
    assertEquals("value2", config2.getFakeStringNoDefaultWithInfer());
    assertEquals("value10", config2.getFakeStringNoDefaultWithInferEmpty());

    // Case 3: FAKE_STRING_CONFIG is set to a different value.  FAKE_INTEGER_CONFIG,
    // FAKE_STRING_CONFIG_NO_DEFAULT_WITH_INFER, and
    // FAKE_STRING_CONFIG_NO_DEFAULT_WITH_INFER_EMPTY are inferred
    HoodieTestFakeConfig config3 = HoodieTestFakeConfig.newBuilder()
        .withFakeString("5").build();
    assertEquals("5", config3.getFakeString());
    assertEquals(100, config3.getFakeInteger());
    assertEquals("value3", config3.getFakeStringNoDefaultWithInfer());
    assertEquals(null, config3.getFakeStringNoDefaultWithInferEmpty());

    // Case 4: all configs are set.  No default or infer function should be used
    HoodieTestFakeConfig config4 = HoodieTestFakeConfig.newBuilder()
        .withFakeString("5")
        .withFakeInteger(200)
        .withFakeStringNoDefaultWithInfer("xyz")
        .withFakeStringNoDefaultWithInferEmpty("uvw").build();
    assertEquals("5", config4.getFakeString());
    assertEquals(200, config4.getFakeInteger());
    assertEquals("xyz", config4.getFakeStringNoDefaultWithInfer());
    assertEquals("uvw", config4.getFakeStringNoDefaultWithInferEmpty());
  }
}
