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
import org.apache.hudi.testutils.HoodieClientTestBase;
import org.apache.hudi.util.JavaScalaConverters;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.Properties;
import java.util.stream.Stream;

import static org.apache.hudi.common.testutils.HoodieTestUtils.getMetaClientBuilder;
import static org.junit.jupiter.params.provider.Arguments.arguments;

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
    HoodieConfig config = new HoodieConfig();
    String result = HoodieWriterUtils.getPayloadClassConfigKeyFromTableConfig(
        HoodieTableConfig.PAYLOAD_CLASS_NAME.key(), config);
    Assertions.assertEquals(HoodieTableConfig.PAYLOAD_CLASS_NAME.key(), result);
  }

  private static Stream<Arguments> testGetKeyInTableConfigTableVersion9PayloadClassKeyWithLegacyPayloadClass() {

    Stream<Arguments> arguments = Stream.of(
        arguments(HoodieTableVersion.EIGHT,
            "org.apache.hudi.common.model.DefaultHoodieRecordPayload", "hoodie.compaction.payload.class"),
        arguments(HoodieTableVersion.NINE,
            "", "hoodie.compaction.payload.class"),
        arguments(HoodieTableVersion.NINE,
            "  ", "hoodie.compaction.payload.class"),
        arguments(HoodieTableVersion.NINE,
            "org.apache.hudi.common.model.DefaultHoodieRecordPayload", "hoodie.table.legacy.payload.class")
    );
    return arguments;
  }

  @ParameterizedTest
  @MethodSource()
  void testGetKeyInTableConfigTableVersion9PayloadClassKeyWithLegacyPayloadClass(
      HoodieTableVersion tableVersion, String legacyPayloadToSet, String expectedKey) {
    HoodieConfig config = new HoodieConfig();
    config.setValue(HoodieTableConfig.LEGACY_PAYLOAD_CLASS_NAME, legacyPayloadToSet);
    config.setValue(HoodieTableConfig.VERSION, String.valueOf(tableVersion.versionCode()));
    String result = HoodieWriterUtils.getPayloadClassConfigKeyFromTableConfig(
        HoodieTableConfig.PAYLOAD_CLASS_NAME.key(), config);
    Assertions.assertEquals(expectedKey, result);
  }
}