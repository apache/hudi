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

package org.apache.hudi.table.upgrade;

import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.keygen.KeyGenerator;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestTwoToThreeUpgradeHandler {

  HoodieWriteConfig config;

  @BeforeEach
  void setUp() {
    config = HoodieWriteConfig.newBuilder()
        .forTable("foo")
        .withPath("/foo")
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(false).build())
        .build();
  }

  @Disabled
  @ParameterizedTest
  @ValueSource(strings = {"hoodie.table.keygenerator.class", "hoodie.datasource.write.keygenerator.class"})
  void upgradeHandlerShouldRetrieveKeyGeneratorConfig(String keyGenConfigKey) {
    config.setValue(keyGenConfigKey, KeyGenerator.class.getName());
    TwoToThreeUpgradeHandler handler = new TwoToThreeUpgradeHandler();
    Map<ConfigProperty, String> kv = handler.upgrade(config, null, null, null);
    assertEquals(KeyGenerator.class.getName(), kv.get(HoodieTableConfig.KEY_GENERATOR_CLASS_NAME));
  }

  @Disabled
  @ParameterizedTest
  @EnumSource(EngineType.class)
  void upgradeHandlerWhenKeyGeneratorNotSet(EngineType engineType) {
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withEngineType(engineType)
        .forTable("foo")
        .withPath("/foo")
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(false).build())
        .build();
    TwoToThreeUpgradeHandler handler = new TwoToThreeUpgradeHandler();
    if (engineType == EngineType.SPARK) {
      Map<ConfigProperty, String> kv = handler.upgrade(config, null, null, null);
      assertEquals(TwoToThreeUpgradeHandler.SPARK_SIMPLE_KEY_GENERATOR,
          kv.get(HoodieTableConfig.KEY_GENERATOR_CLASS_NAME));
    } else {
      Throwable t = assertThrows(IllegalStateException.class, () -> handler
          .upgrade(writeConfig, null, null, null));
      assertTrue(t.getMessage().startsWith("Missing config:"));
    }
  }
}
