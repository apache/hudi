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
import org.apache.hudi.common.config.RecordMergeMode;
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

import java.io.IOException;
import java.util.Properties;

import scala.collection.immutable.Map$;

import static org.apache.hudi.common.testutils.HoodieTestUtils.getMetaClientBuilder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
  void testReturnsKeyWhenTableConfigIsNull() {
    assertEquals("randomKey", HoodieWriterUtils.getKeyInTableConfig("randomKey", null));
  }

  @Test
  void testPayloadClassNameNotVersion9() {
    HoodieConfig config = new HoodieConfig();
    config.setValue(HoodieTableConfig.VERSION, "8");
    String result = HoodieWriterUtils.getKeyInTableConfig(HoodieTableConfig.PAYLOAD_CLASS_NAME.key(), config);
    assertEquals(HoodieTableConfig.PAYLOAD_CLASS_NAME.key(), result);
  }

  @Test
  void testPayloadClassNameVersion9WithLegacyPayload() {
    HoodieConfig config = new HoodieConfig();
    config.setValue(HoodieTableConfig.VERSION, String.valueOf(HoodieTableVersion.NINE.versionCode()));
    config.setValue(HoodieTableConfig.LEGACY_PAYLOAD_CLASS_NAME, "com.example.LegacyPayload");
    String result = HoodieWriterUtils.getKeyInTableConfig(HoodieTableConfig.PAYLOAD_CLASS_NAME.key(), config);
    assertEquals(HoodieTableConfig.LEGACY_PAYLOAD_CLASS_NAME.key(), result);
  }

  @Test
  void testPayloadClassNameVersion9WithoutLegacyPayload() {
    HoodieConfig config = new HoodieConfig();
    config.setValue(HoodieTableConfig.VERSION, String.valueOf(HoodieTableVersion.NINE.versionCode()));
    String result = HoodieWriterUtils.getKeyInTableConfig(HoodieTableConfig.PAYLOAD_CLASS_NAME.key(), config);
    assertEquals(HoodieTableConfig.PAYLOAD_CLASS_NAME.key(), result);
  }

  @Test
  void testRecordMergeModeMappingWithVersion9() {
    HoodieConfig config = new HoodieConfig();
    config.setValue(HoodieTableConfig.VERSION.key(), "9");
    String result = HoodieWriterUtils.getKeyInTableConfig(HoodieWriteConfig.RECORD_MERGE_MODE.key(), config);
    assertEquals(HoodieTableConfig.RECORD_MERGE_MODE.key(), result);
  }

  @Test
  void testRecordMergeModeMappingWithVersion8() {
    HoodieConfig config = new HoodieConfig();
    config.setValue(HoodieTableConfig.VERSION.key(), "8");
    String result = HoodieWriterUtils.getKeyInTableConfig(HoodieWriteConfig.RECORD_MERGE_MODE.key(), config);
    assertEquals(HoodieWriteConfig.RECORD_MERGE_MODE.key(), result);
  }

  @Test
  void testRecordMergeStrategyIdMappingWithVersion9() {
    HoodieConfig config = new HoodieConfig();
    config.setValue(HoodieTableConfig.VERSION.key(), "9");
    String result = HoodieWriterUtils.getKeyInTableConfig(HoodieWriteConfig.RECORD_MERGE_STRATEGY_ID.key(), config);
    assertEquals(HoodieTableConfig.RECORD_MERGE_STRATEGY_ID.key(), result);
  }

  @Test
  void testRecordMergeStrategyIdMappingWithVersion8() {
    HoodieConfig config = new HoodieConfig();
    config.setValue(HoodieTableConfig.VERSION.key(), "8");
    String result = HoodieWriterUtils.getKeyInTableConfig(HoodieWriteConfig.RECORD_MERGE_STRATEGY_ID.key(), config);
    assertEquals(HoodieWriteConfig.RECORD_MERGE_STRATEGY_ID.key(), result);
  }

  @Test
  void testFallbackToOriginalKey() {
    HoodieConfig config = new HoodieConfig();
    String result = HoodieWriterUtils.getKeyInTableConfig("my.custom.key", config);
    assertEquals("my.custom.key", result);
  }

  @Test
  void testShouldIgnorePayloadValidationVersion9WithCustomMergeMode() {
    HoodieConfig config = new HoodieConfig();
    config.setValue(HoodieTableConfig.VERSION, String.valueOf(HoodieTableVersion.NINE.versionCode()));
    config.setValue(HoodieTableConfig.RECORD_MERGE_MODE, RecordMergeMode.CUSTOM.name());

    String payloadClass = "com.example.CustomPayload";
    assertFalse(HoodieWriterUtils.shouldIgnorePayloadValidation(payloadClass, Map$.MODULE$.empty(), config));
  }

  @Test
  void testShouldIgnorePayloadValidationVersion9WithEmptyPayload() {
    HoodieConfig config = new HoodieConfig();
    config.setValue(HoodieTableConfig.VERSION, String.valueOf(HoodieTableVersion.NINE.versionCode()));
    config.setValue(HoodieTableConfig.RECORD_MERGE_MODE, RecordMergeMode.COMMIT_TIME_ORDERING.name());

    String payloadClass = "";
    assertTrue(HoodieWriterUtils.shouldIgnorePayloadValidation(payloadClass, Map$.MODULE$.empty(), config));
  }

  @Test
  void testShouldIgnorePayloadValidationVersion9WithCommitTimeOrdering() {
    HoodieConfig config = new HoodieConfig();
    config.setValue(HoodieTableConfig.VERSION, String.valueOf(HoodieTableVersion.NINE.versionCode()));
    config.setValue(HoodieTableConfig.RECORD_MERGE_MODE, RecordMergeMode.COMMIT_TIME_ORDERING.name());

    String payloadClass = "com.example.CustomPayload";
    assertTrue(HoodieWriterUtils.shouldIgnorePayloadValidation(payloadClass, Map$.MODULE$.empty(), config));
  }

  @Test
  void testShouldIgnorePayloadValidationVersion9WithEventTimeOrdering() {
    HoodieConfig config = new HoodieConfig();
    config.setValue(HoodieTableConfig.VERSION, String.valueOf(HoodieTableVersion.NINE.versionCode()));
    config.setValue(HoodieTableConfig.RECORD_MERGE_MODE, RecordMergeMode.EVENT_TIME_ORDERING.name());

    String payloadClass = "com.example.CustomPayload";
    assertTrue(HoodieWriterUtils.shouldIgnorePayloadValidation(payloadClass, Map$.MODULE$.empty(), config));
  }
}