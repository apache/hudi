/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.testutils.HoodieClientTestBase;
import org.apache.hudi.util.JavaScalaConverters;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Properties;

import static org.apache.hudi.common.testutils.HoodieTestUtils.getMetaClientBuilder;

class TestHoodieWriterUtils extends HoodieClientTestBase {

  @Test
  void validateTableConfig() throws IOException {
    HoodieTableMetaClient tableMetaClient = getMetaClientBuilder(HoodieTableType.COPY_ON_WRITE, new Properties(), "")
        .initTable(storageConf, tempDir.resolve("table1").toString());
    HoodieTableConfig tableConfig = tableMetaClient.getTableConfig();
    TypedProperties properties = TypedProperties.copy(tableConfig.getProps());
    properties.put(HoodieTableConfig.DATABASE_NAME.key(), "databaseFromCatalog");
    Assertions.assertDoesNotThrow(() -> HoodieWriterUtils.validateTableConfig(sparkSession, JavaScalaConverters.convertJavaPropertiesToScalaMap(properties), tableConfig));
  }

  @Test
  void testCheckIfPayloadClassConsistentWithLegacyPayloadClass() {
    // Test case 1: Legacy payload class exists and matches
    HoodieTableConfig tableConfig = createMockTableConfig();
    tableConfig.setValue(HoodieTableConfig.LEGACY_PAYLOAD_CLASS_NAME, "org.apache.hudi.common.model.DefaultHoodieRecordPayload");

    boolean result = HoodieWriterUtils.checkIfPayloadClassConsistent(
        "org.apache.hudi.common.model.DefaultHoodieRecordPayload", tableConfig);
    Assertions.assertTrue(result, "Should return true when payload class matches legacy payload class");
  }

  @Test
  void testCheckIfPayloadClassConsistentWithLegacyPayloadClassMismatch() {
    // Test case 2: Legacy payload class exists but doesn't match
    HoodieTableConfig tableConfig = createMockTableConfig();
    tableConfig.setValue(HoodieTableConfig.LEGACY_PAYLOAD_CLASS_NAME, "org.apache.hudi.common.model.DefaultHoodieRecordPayload");

    boolean result = HoodieWriterUtils.checkIfPayloadClassConsistent(
        "org.apache.hudi.common.model.OverwriteWithLatestAvroPayload", tableConfig);
    Assertions.assertFalse(result, "Should return false when payload class doesn't match legacy payload class");
  }

  @Test
  void testCheckIfPayloadClassConsistentWithoutLegacyPayloadClassEmptyString() {
    // Test case 3: No legacy payload class, payload class should be empty string
    HoodieTableConfig tableConfig = createMockTableConfig();
    // Don't set LEGACY_PAYLOAD_CLASS_NAME

    boolean result = HoodieWriterUtils.checkIfPayloadClassConsistent("", tableConfig);
    Assertions.assertTrue(result, "Should return true when no legacy payload class and payload class is empty string");
  }

  @Test
  void testCheckIfPayloadClassConsistentWithoutLegacyPayloadClassNonEmptyString() {
    // Test case 4: No legacy payload class, but payload class is not empty string
    HoodieTableConfig tableConfig = createMockTableConfig();
    // Don't set LEGACY_PAYLOAD_CLASS_NAME

    boolean result = HoodieWriterUtils.checkIfPayloadClassConsistent(
        "org.apache.hudi.common.model.DefaultHoodieRecordPayload", tableConfig);
    Assertions.assertFalse(result, "Should return false when no legacy payload class but payload class is not empty string");
  }

  @Test
  void testShouldIgnorePayloadValidationTableVersion9EmptyPayloadClass() {
    // Test case: Table version 9, empty payload class should be ignored
    HoodieTableConfig tableConfig = createMockTableConfig();
    tableConfig.setValue(HoodieTableConfig.VERSION, "9");

    boolean result = HoodieWriterUtils.shouldIgnorePayloadValidation(StringUtils.EMPTY_STRING,
        JavaScalaConverters.convertJavaPropertiesToScalaMap(new Properties()), tableConfig);
    Assertions.assertTrue(result, "Should return true when table version is 9 and payload class is empty");
  }

  @Test
  void testShouldIgnorePayloadValidationTableVersion9NonEmptyPayloadClass() {
    // Test case: Table version 9, non-empty payload class should not be ignored
    HoodieTableConfig tableConfig = createMockTableConfig();
    tableConfig.setValue(HoodieTableConfig.VERSION, "9");
    tableConfig.setValue(HoodieTableConfig.LEGACY_PAYLOAD_CLASS_NAME, "org.apache.hudi.common.model.DefaultHoodieRecordPayload");

    boolean result = HoodieWriterUtils.shouldIgnorePayloadValidation("org.apache.hudi.common.model.DefaultHoodieRecordPayload",
        JavaScalaConverters.convertJavaPropertiesToScalaMap(new Properties()), tableConfig);
    Assertions.assertTrue(result, "Should return true when table version is 9 and payload class matches legacy payload class");
  }

  @Test
  void testShouldIgnorePayloadValidationTableVersion9NonEmptyPayloadClassMismatch() {
    // Test case: Table version 9, non-empty payload class that doesn't match legacy payload class
    HoodieTableConfig tableConfig = createMockTableConfig();
    tableConfig.setValue(HoodieTableConfig.VERSION, "9");
    tableConfig.setValue(HoodieTableConfig.LEGACY_PAYLOAD_CLASS_NAME, "org.apache.hudi.common.model.DefaultHoodieRecordPayload");

    boolean result = HoodieWriterUtils.shouldIgnorePayloadValidation("org.apache.hudi.common.model.OverwriteWithLatestAvroPayload",
        JavaScalaConverters.convertJavaPropertiesToScalaMap(new Properties()), tableConfig);
    Assertions.assertFalse(result, "Should return false when table version is 9 and payload class doesn't match legacy payload class");
  }

  @Test
  void testShouldIgnorePayloadValidationTableVersion9NoLegacyPayloadClass() {
    // Test case: Table version 9, no legacy payload class, non-empty payload class
    HoodieTableConfig tableConfig = createMockTableConfig();
    tableConfig.setValue(HoodieTableConfig.VERSION, "9");
    // Don't set LEGACY_PAYLOAD_CLASS_NAME

    boolean result = HoodieWriterUtils.shouldIgnorePayloadValidation("org.apache.hudi.common.model.DefaultHoodieRecordPayload",
        JavaScalaConverters.convertJavaPropertiesToScalaMap(new Properties()), tableConfig);
    Assertions.assertFalse(result, "Should return false when table version is 9, no legacy payload class, and payload class is not empty");
  }

  @Test
  void testCheckIfPayloadClassConsistentWithLegacyPayloadClassEmptyString() {
    // Test case 6: Legacy payload class exists but is empty string, payload class should also be empty
    HoodieTableConfig tableConfig = createMockTableConfig();
    tableConfig.setValue(HoodieTableConfig.LEGACY_PAYLOAD_CLASS_NAME, "");

    boolean result = HoodieWriterUtils.checkIfPayloadClassConsistent("", tableConfig);
    Assertions.assertTrue(result, "Should return true when legacy payload class is empty string and payload class is also empty");
  }

  @Test
  void testCheckIfPayloadClassConsistentWithLegacyPayloadClassNullPayloadClass() {
    // Test case 8: Legacy payload class exists, but payload class is null
    HoodieTableConfig tableConfig = createMockTableConfig();
    tableConfig.setValue(HoodieTableConfig.LEGACY_PAYLOAD_CLASS_NAME, "org.apache.hudi.common.model.DefaultHoodieRecordPayload");

    boolean result = HoodieWriterUtils.checkIfPayloadClassConsistent(null, tableConfig);
    Assertions.assertFalse(result, "Should return false when payload class is null");
  }

  @Test
  void testCheckIfPayloadClassConsistentWithoutLegacyPayloadClassNullPayloadClass() {
    // Test case 9: No legacy payload class, payload class is null
    HoodieTableConfig tableConfig = createMockTableConfig();
    // Don't set LEGACY_PAYLOAD_CLASS_NAME

    boolean result = HoodieWriterUtils.checkIfPayloadClassConsistent(null, tableConfig);
    Assertions.assertFalse(result, "Should return false when no legacy payload class and payload class is null");
  }

  private HoodieTableConfig createMockTableConfig() {
    try {
      HoodieTableMetaClient tableMetaClient = getMetaClientBuilder(HoodieTableType.COPY_ON_WRITE, new Properties(), "")
          .initTable(storageConf, tempDir.resolve("test_table").toString());
      return tableMetaClient.getTableConfig();
    } catch (IOException e) {
      throw new RuntimeException("Failed to create mock table config", e);
    }
  }
}