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

package org.apache.hudi.common.config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link ConfigGroups}.
 */
class TestConfigGroups {

  @ParameterizedTest
  @EnumSource(ConfigGroups.Names.class)
  void testGetDescriptionReturnsNonPlaceholderForEveryName(ConfigGroups.Names name) {
    String description = ConfigGroups.getDescription(name);
    assertNotNull(description);
    assertFalse(description.isEmpty(), "Description should not be empty for " + name);
    assertFalse(description.startsWith("Please fill in the description"),
        "Every enum constant should have a real description branch, missing for " + name);
  }

  @Test
  void testGetDescriptionSpecificValues() {
    assertEquals("Basic Hudi Table configuration parameters.",
        ConfigGroups.getDescription(ConfigGroups.Names.TABLE_CONFIG));
    assertEquals("Configurations specific to Amazon Web Services.",
        ConfigGroups.getDescription(ConfigGroups.Names.AWS));
    assertTrue(ConfigGroups.getDescription(ConfigGroups.Names.ENVIRONMENT_CONFIG)
        .contains("hudi-defaults.conf"));
  }

  @Test
  void testNamesCarryHumanReadableName() {
    assertEquals("Hudi Table Config", ConfigGroups.Names.TABLE_CONFIG.name);
    assertEquals("Metrics Configs", ConfigGroups.Names.METRICS.name);
  }

  @Test
  void testSubGroupNamesCarryNameAndDescription() {
    assertEquals("Index Configs", ConfigGroups.SubGroupNames.INDEX.name);
    assertTrue(ConfigGroups.SubGroupNames.INDEX.getDescription().contains("indexing behavior"));
    assertEquals("None", ConfigGroups.SubGroupNames.NONE.name);
    assertNotNull(ConfigGroups.SubGroupNames.LOCK.getDescription());
  }
}
