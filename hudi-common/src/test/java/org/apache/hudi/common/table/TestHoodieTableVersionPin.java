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

package org.apache.hudi.common.table;

import org.apache.hudi.common.metrics.Registry;
import org.apache.hudi.exception.HoodieTableVersionPinExceededException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.apache.hudi.common.table.HoodieTableConfig.MAX_ALLOWED_TABLE_VERSION;
import static org.apache.hudi.common.table.HoodieTableConfig.NAME;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests the {@code hoodie.table.version.pinned} ceiling guard in {@link HoodieTableConfig}.
 */
class TestHoodieTableVersionPin {

  private static final String TABLE_NAME = "pin-test-table";

  private HoodieTableConfig newTableConfig() {
    HoodieTableConfig tableConfig = new HoodieTableConfig();
    tableConfig.setValue(NAME, TABLE_NAME);
    return tableConfig;
  }

  private String previousPinSystemProperty;

  @BeforeEach
  void setUp() {
    previousPinSystemProperty = System.getProperty(MAX_ALLOWED_TABLE_VERSION.key());
    System.clearProperty(MAX_ALLOWED_TABLE_VERSION.key());
    clearMetrics();
  }

  @AfterEach
  void tearDown() {
    if (previousPinSystemProperty == null) {
      System.clearProperty(MAX_ALLOWED_TABLE_VERSION.key());
    } else {
      System.setProperty(MAX_ALLOWED_TABLE_VERSION.key(), previousPinSystemProperty);
    }
    clearMetrics();
  }

  private void clearMetrics() {
    Registry.getRegistryOfClass(TABLE_NAME, "hoodie.table.version.pin", "org.apache.hudi.common.metrics.LocalRegistry").clear();
  }

  private long metricCount() {
    Long count = Registry.getRegistryOfClass(TABLE_NAME, "hoodie.table.version.pin", "org.apache.hudi.common.metrics.LocalRegistry")
        .getAllCounts().get("exceeded");
    return count == null ? 0 : count;
  }

  @Test
  void testUnpinnedByDefaultAllowsAnyVersion() {
    HoodieTableConfig tableConfig = newTableConfig();
    assertDoesNotThrow(() -> tableConfig.setTableVersion(HoodieTableVersion.current()));
    assertEquals(0, metricCount());
  }

  @Test
  void testExplicitUnPinnedValueAllowsAnyVersion() {
    HoodieTableConfig tableConfig = newTableConfig();
    tableConfig.setValue(MAX_ALLOWED_TABLE_VERSION, "un_pinned");
    assertDoesNotThrow(() -> tableConfig.setTableVersion(HoodieTableVersion.current()));
    assertEquals(0, metricCount());
  }

  @Test
  void testVersionBelowPinPasses() {
    HoodieTableConfig tableConfig = newTableConfig();
    tableConfig.setValue(MAX_ALLOWED_TABLE_VERSION, Integer.toString(HoodieTableVersion.NINE.versionCode()));
    assertDoesNotThrow(() -> tableConfig.setTableVersion(HoodieTableVersion.EIGHT));
    assertEquals(0, metricCount());
  }

  @Test
  void testVersionAtPinPasses() {
    HoodieTableConfig tableConfig = newTableConfig();
    tableConfig.setValue(MAX_ALLOWED_TABLE_VERSION, Integer.toString(HoodieTableVersion.NINE.versionCode()));
    assertDoesNotThrow(() -> tableConfig.setTableVersion(HoodieTableVersion.NINE));
    assertEquals(0, metricCount());
  }

  @Test
  void testVersionAbovePinThrowsAndEmitsMetric() {
    HoodieTableConfig tableConfig = newTableConfig();
    tableConfig.setValue(MAX_ALLOWED_TABLE_VERSION, Integer.toString(HoodieTableVersion.EIGHT.versionCode()));
    HoodieTableVersionPinExceededException e = assertThrows(HoodieTableVersionPinExceededException.class,
        () -> tableConfig.setTableVersion(HoodieTableVersion.NINE));
    assertTrue(e.getMessage().contains(TABLE_NAME));
    assertTrue(e.getMessage().contains(MAX_ALLOWED_TABLE_VERSION.key()));
    assertEquals(1, metricCount());
  }

  @Test
  void testGetTableVersionAlsoEnforcesPin() {
    HoodieTableConfig tableConfig = newTableConfig();
    // Directly set the raw version value, bypassing setTableVersion's own guard, to simulate an
    // out-of-band write (e.g. a legacy table or a manual edit) whose version is only discovered on read.
    tableConfig.setValue(HoodieTableConfig.VERSION, Integer.toString(HoodieTableVersion.NINE.versionCode()));
    tableConfig.setValue(MAX_ALLOWED_TABLE_VERSION, Integer.toString(HoodieTableVersion.EIGHT.versionCode()));
    assertThrows(HoodieTableVersionPinExceededException.class, tableConfig::getTableVersion);
    assertEquals(1, metricCount());
  }

  @Test
  void testSystemPropertyHonoredWhenConfigAbsent() {
    HoodieTableConfig tableConfig = newTableConfig();
    System.setProperty(MAX_ALLOWED_TABLE_VERSION.key(), Integer.toString(HoodieTableVersion.EIGHT.versionCode()));
    assertThrows(HoodieTableVersionPinExceededException.class,
        () -> tableConfig.setTableVersion(HoodieTableVersion.NINE));
  }

  @Test
  void testConfigValueTakesPrecedenceOverSystemProperty() {
    HoodieTableConfig tableConfig = newTableConfig();
    // System property pins low (would reject NINE), but the table's own config pins high (allows NINE).
    System.setProperty(MAX_ALLOWED_TABLE_VERSION.key(), Integer.toString(HoodieTableVersion.EIGHT.versionCode()));
    tableConfig.setValue(MAX_ALLOWED_TABLE_VERSION, Integer.toString(HoodieTableVersion.NINE.versionCode()));
    assertDoesNotThrow(() -> tableConfig.setTableVersion(HoodieTableVersion.NINE));
  }

  @Test
  void testBlankConfigValueFallsBackToSystemPropertyInsteadOfDisablingPin() {
    HoodieTableConfig tableConfig = newTableConfig();
    // A blank table-level value must not be read as an opt-out that shadows the fleet-wide pin.
    System.setProperty(MAX_ALLOWED_TABLE_VERSION.key(), Integer.toString(HoodieTableVersion.EIGHT.versionCode()));
    tableConfig.setValue(MAX_ALLOWED_TABLE_VERSION, "   ");
    assertThrows(HoodieTableVersionPinExceededException.class,
        () -> tableConfig.setTableVersion(HoodieTableVersion.NINE));
  }

  @Test
  void testBlankConfigValueWithNoSystemPropertyLeavesPinDisabled() {
    HoodieTableConfig tableConfig = newTableConfig();
    tableConfig.setValue(MAX_ALLOWED_TABLE_VERSION, "");
    assertDoesNotThrow(() -> tableConfig.setTableVersion(HoodieTableVersion.current()));
    assertEquals(0, metricCount());
  }

  @Test
  void testBlankAtBothLevelsLeavesPinDisabled() {
    HoodieTableConfig tableConfig = newTableConfig();
    System.setProperty(MAX_ALLOWED_TABLE_VERSION.key(), "  ");
    tableConfig.setValue(MAX_ALLOWED_TABLE_VERSION, "  ");
    assertDoesNotThrow(() -> tableConfig.setTableVersion(HoodieTableVersion.current()));
    assertEquals(0, metricCount());
  }

  @Test
  void testMalformedPinValueThrows() {
    HoodieTableConfig tableConfig = newTableConfig();
    tableConfig.setValue(MAX_ALLOWED_TABLE_VERSION, "not-a-number");
    assertThrows(HoodieTableVersionPinExceededException.class,
        () -> tableConfig.setTableVersion(HoodieTableVersion.current()));
  }

  @Test
  void testNegativePinValueThrows() {
    HoodieTableConfig tableConfig = newTableConfig();
    tableConfig.setValue(MAX_ALLOWED_TABLE_VERSION, "-1");
    assertThrows(HoodieTableVersionPinExceededException.class,
        () -> tableConfig.setTableVersion(HoodieTableVersion.current()));
  }

  @Test
  void testUnrecognizedVersionCodePinValueThrows() {
    HoodieTableConfig tableConfig = newTableConfig();
    tableConfig.setValue(MAX_ALLOWED_TABLE_VERSION, "999");
    assertThrows(HoodieTableVersionPinExceededException.class,
        () -> tableConfig.setTableVersion(HoodieTableVersion.current()));
  }

  @Test
  void testMetricNotEmittedOnPass() {
    HoodieTableConfig tableConfig = newTableConfig();
    tableConfig.setValue(MAX_ALLOWED_TABLE_VERSION, Integer.toString(HoodieTableVersion.current().versionCode()));
    assertDoesNotThrow(() -> tableConfig.setTableVersion(HoodieTableVersion.current()));
    assertEquals(0, metricCount());
  }
}
