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

import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.testutils.HoodieClientTestBase;
import org.apache.hudi.util.JavaScalaConverters;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

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
  void testGetKeyInTableConfigTableVersion9PayloadClassKeyWithLegacyPayloadClass() {
    // Mock HoodieTableVersion.current() to return NINE
    try (MockedStatic<HoodieTableVersion> mockedHoodieTableVersion = Mockito.mockStatic(HoodieTableVersion.class)) {
      mockedHoodieTableVersion.when(HoodieTableVersion::current).thenReturn(HoodieTableVersion.NINE);
      // Set up table config with legacy payload class
      HoodieConfig config = new HoodieConfig();
      config.setValue(HoodieTableConfig.LEGACY_PAYLOAD_CLASS_NAME,
          "org.apache.hudi.common.model.DefaultHoodieRecordPayload");
      config.setValue(HoodieTableConfig.VERSION, "9");
      // Test with PAYLOAD_CLASS_NAME key
      String result = HoodieWriterUtils.getKeyInTableConfig(
          HoodieWriteConfig.WRITE_PAYLOAD_CLASS_NAME.key(), config);
      // Should return legacy payload class key
      Assertions.assertEquals(HoodieTableConfig.LEGACY_PAYLOAD_CLASS_NAME.key(), result);
    }
  }

  @Test
  void testGetKeyInTableConfigTableVersion9PayloadClassKeyWithoutLegacyPayloadClass() {
    // Mock HoodieTableVersion.current() to return NINE
    try (MockedStatic<HoodieTableVersion> mockedHoodieTableVersion = Mockito.mockStatic(HoodieTableVersion.class)) {
      mockedHoodieTableVersion.when(HoodieTableVersion::current).thenReturn(HoodieTableVersion.NINE);
      // Set up table config without legacy payload class
      HoodieConfig config = new HoodieConfig();
      // Note: no legacy payload class set
      // Test with PAYLOAD_CLASS_NAME key
      String result = HoodieWriterUtils.getKeyInTableConfig(
          HoodieWriteConfig.WRITE_PAYLOAD_CLASS_NAME.key(), config);
      // Should return original key since legacy payload class is not configured
      Assertions.assertEquals(HoodieWriteConfig.WRITE_PAYLOAD_CLASS_NAME.key(), result);
    }
  }

  @Test
  void testGetKeyInTableConfigTableVersion9NonPayloadClassKey() {
    // Mock HoodieTableVersion.current() to return NINE
    try (MockedStatic<HoodieTableVersion> mockedHoodieTableVersion = Mockito.mockStatic(HoodieTableVersion.class)) {
      mockedHoodieTableVersion.when(HoodieTableVersion::current).thenReturn(HoodieTableVersion.NINE);
      // Set up table config with legacy payload class
      HoodieConfig config = new HoodieConfig();
      config.setValue(HoodieTableConfig.LEGACY_PAYLOAD_CLASS_NAME,
          "org.apache.hudi.common.model.DefaultHoodieRecordPayload");
      // Test with a different key (not PAYLOAD_CLASS_NAME)
      String testKey = "hoodie.datasource.write.recordkey.field";
      String result = HoodieWriterUtils.getKeyInTableConfig(testKey, config);
      // Should return original key since it's not PAYLOAD_CLASS_NAME
      Assertions.assertEquals(testKey, result);
    }
  }

  @Test
  void testGetKeyInTableConfigTableVersionNot9PayloadClassKeyWithLegacyPayloadClass() {
    // Mock HoodieTableVersion.current() to return EIGHT
    try (MockedStatic<HoodieTableVersion> mockedHoodieTableVersion = Mockito.mockStatic(HoodieTableVersion.class)) {
      mockedHoodieTableVersion.when(HoodieTableVersion::current).thenReturn(HoodieTableVersion.EIGHT);
      // Set up table config with legacy payload class
      HoodieConfig config = new HoodieConfig();
      config.setValue(HoodieTableConfig.LEGACY_PAYLOAD_CLASS_NAME, "org.apache.hudi.common.model.DefaultHoodieRecordPayload");
      // Test with PAYLOAD_CLASS_NAME key
      String result = HoodieWriterUtils.getKeyInTableConfig(
          HoodieWriteConfig.WRITE_PAYLOAD_CLASS_NAME.key(), config);
      // Should return original key since table version is not 9
      Assertions.assertEquals(HoodieWriteConfig.WRITE_PAYLOAD_CLASS_NAME.key(), result);
    }
  }

  @Test
  void testGetKeyInTableConfigTableVersion9PayloadClassKeyEmptyLegacyPayloadClass() {
    // Mock HoodieTableVersion.current() to return NINE
    try (MockedStatic<HoodieTableVersion> mockedHoodieTableVersion = Mockito.mockStatic(HoodieTableVersion.class)) {
      mockedHoodieTableVersion.when(HoodieTableVersion::current).thenReturn(HoodieTableVersion.NINE);
      // Set up table config with empty legacy payload class
      HoodieConfig config = new HoodieConfig();
      config.setValue(HoodieTableConfig.LEGACY_PAYLOAD_CLASS_NAME, "");
      // Test with PAYLOAD_CLASS_NAME key
      String result = HoodieWriterUtils.getKeyInTableConfig(
          HoodieWriteConfig.WRITE_PAYLOAD_CLASS_NAME.key(), config);
      // Should return legacy payload class key even if empty
      Assertions.assertEquals(HoodieWriteConfig.WRITE_PAYLOAD_CLASS_NAME.key(), result);
    }
  }

  @Test
  void testGetKeyInTableConfigTableVersion9PayloadClassKeyWhitespaceLegacyPayloadClass() {
    // Mock HoodieTableVersion.current() to return NINE
    try (MockedStatic<HoodieTableVersion> mockedHoodieTableVersion = Mockito.mockStatic(HoodieTableVersion.class)) {
      mockedHoodieTableVersion.when(HoodieTableVersion::current).thenReturn(HoodieTableVersion.NINE);
      // Set up table config with whitespace legacy payload class
      HoodieConfig config = new HoodieConfig();
      config.setValue(HoodieTableConfig.LEGACY_PAYLOAD_CLASS_NAME, "   ");
      // Test with PAYLOAD_CLASS_NAME key
      String result = HoodieWriterUtils.getKeyInTableConfig(
          HoodieWriteConfig.WRITE_PAYLOAD_CLASS_NAME.key(), config);
      // Should return legacy payload class key even if whitespace
      Assertions.assertEquals(HoodieWriteConfig.WRITE_PAYLOAD_CLASS_NAME.key(), result);
    }
  }

  @Test
  void testGetKeyInTableConfigTableVersion9PayloadClassKeyWithCustomLegacyPayloadClass() {
    // Mock HoodieTableVersion.current() to return NINE
    try (MockedStatic<HoodieTableVersion> mockedHoodieTableVersion = Mockito.mockStatic(HoodieTableVersion.class)) {
      mockedHoodieTableVersion.when(HoodieTableVersion::current).thenReturn(HoodieTableVersion.NINE);
      // Set up table config with custom legacy payload class
      HoodieConfig config = new HoodieConfig();
      config.setValue(HoodieTableConfig.LEGACY_PAYLOAD_CLASS_NAME, "com.example.CustomPayload");
      config.setValue(HoodieTableConfig.VERSION, "9");
      // Test with PAYLOAD_CLASS_NAME key
      String result = HoodieWriterUtils.getKeyInTableConfig(
          HoodieWriteConfig.WRITE_PAYLOAD_CLASS_NAME.key(), config);
      // Should return legacy payload class key
      Assertions.assertEquals(HoodieTableConfig.LEGACY_PAYLOAD_CLASS_NAME.key(), result);
    }
  }
}