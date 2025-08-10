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
  void testGetKeyInTableConfigTableVersion9PayloadClassKeyWithoutLegacyPayloadClass() {
    try (MockedStatic<HoodieTableVersion> mockedHoodieTableVersion = Mockito.mockStatic(HoodieTableVersion.class)) {
      mockedHoodieTableVersion.when(HoodieTableVersion::current).thenReturn(HoodieTableVersion.NINE);
      HoodieConfig config = new HoodieConfig();
      String result = HoodieWriterUtils.getKeyInTableConfig(
          HoodieWriteConfig.WRITE_PAYLOAD_CLASS_NAME.key(), config);
      // Should return original key since legacy payload class is not configured
      Assertions.assertEquals(HoodieWriteConfig.WRITE_PAYLOAD_CLASS_NAME.key(), result);
    }
  }

  @Test
  void testGetKeyInTableConfigTableVersion9NonPayloadClassKey() {
    try (MockedStatic<HoodieTableVersion> mockedHoodieTableVersion = Mockito.mockStatic(HoodieTableVersion.class)) {
      mockedHoodieTableVersion.when(HoodieTableVersion::current).thenReturn(HoodieTableVersion.NINE);
      HoodieConfig config = new HoodieConfig();
      config.setValue(HoodieTableConfig.LEGACY_PAYLOAD_CLASS_NAME,
          "org.apache.hudi.common.model.DefaultHoodieRecordPayload");
      // Test with a different key (not PAYLOAD_CLASS_NAME)
      String testKey = "hoodie.datasource.write.recordkey.field";
      String result = HoodieWriterUtils.getKeyInTableConfig(testKey, config);
      Assertions.assertEquals(testKey, result);
    }
  }

  @Test
  void testGetKeyInTableConfigTableVersionNot9PayloadClassKeyWithLegacyPayloadClass() {
    try (MockedStatic<HoodieTableVersion> mockedHoodieTableVersion = Mockito.mockStatic(HoodieTableVersion.class)) {
      mockedHoodieTableVersion.when(HoodieTableVersion::current).thenReturn(HoodieTableVersion.EIGHT);
      HoodieConfig config = new HoodieConfig();
      config.setValue(HoodieTableConfig.LEGACY_PAYLOAD_CLASS_NAME, "org.apache.hudi.common.model.DefaultHoodieRecordPayload");
      // Test with PAYLOAD_CLASS_NAME key
      String result = HoodieWriterUtils.getKeyInTableConfig(
          HoodieWriteConfig.WRITE_PAYLOAD_CLASS_NAME.key(), config);
      Assertions.assertEquals(HoodieWriteConfig.WRITE_PAYLOAD_CLASS_NAME.key(), result);
    }
  }

  @Test
  void testGetKeyInTableConfigTableVersion9PayloadClassKeyEmptyLegacyPayloadClass() {
    try (MockedStatic<HoodieTableVersion> mockedHoodieTableVersion = Mockito.mockStatic(HoodieTableVersion.class)) {
      mockedHoodieTableVersion.when(HoodieTableVersion::current).thenReturn(HoodieTableVersion.NINE);
      HoodieConfig config = new HoodieConfig();
      config.setValue(HoodieTableConfig.LEGACY_PAYLOAD_CLASS_NAME, "");
      String result = HoodieWriterUtils.getKeyInTableConfig(
          HoodieWriteConfig.WRITE_PAYLOAD_CLASS_NAME.key(), config);
      Assertions.assertEquals(HoodieWriteConfig.WRITE_PAYLOAD_CLASS_NAME.key(), result);
    }
  }

  @Test
  void testGetKeyInTableConfigTableVersion9PayloadClassKeyWhitespaceLegacyPayloadClass() {
    try (MockedStatic<HoodieTableVersion> mockedHoodieTableVersion = Mockito.mockStatic(HoodieTableVersion.class)) {
      mockedHoodieTableVersion.when(HoodieTableVersion::current).thenReturn(HoodieTableVersion.NINE);
      HoodieConfig config = new HoodieConfig();
      config.setValue(HoodieTableConfig.LEGACY_PAYLOAD_CLASS_NAME, "   ");
      String result = HoodieWriterUtils.getKeyInTableConfig(
          HoodieWriteConfig.WRITE_PAYLOAD_CLASS_NAME.key(), config);
      Assertions.assertEquals(HoodieWriteConfig.WRITE_PAYLOAD_CLASS_NAME.key(), result);
    }
  }
}