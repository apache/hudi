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

import org.apache.hudi.common.bootstrap.index.hfile.HFileBootstrapIndex;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.model.DefaultHoodieRecordPayload;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.HashMap;
import java.util.Map;

import static org.apache.hudi.common.table.HoodieTableConfig.BOOTSTRAP_INDEX_CLASS_NAME;
import static org.apache.hudi.common.table.HoodieTableConfig.BOOTSTRAP_INDEX_ENABLE;
import static org.apache.hudi.common.table.HoodieTableConfig.BOOTSTRAP_INDEX_TYPE;
import static org.apache.hudi.common.table.HoodieTableConfig.INITIAL_VERSION;
import static org.apache.hudi.common.table.HoodieTableConfig.KEY_GENERATOR_CLASS_NAME;
import static org.apache.hudi.common.table.HoodieTableConfig.KEY_GENERATOR_TYPE;
import static org.apache.hudi.common.table.HoodieTableConfig.PARTITION_FIELDS;
import static org.apache.hudi.common.table.HoodieTableConfig.PAYLOAD_CLASS_NAME;
import static org.apache.hudi.common.table.HoodieTableConfig.RECORD_MERGE_MODE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.isA;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TestSevenToEightUpgradeHandler {
  @Mock
  private HoodieWriteConfig config;
  @Mock
  private HoodieTableConfig tableConfig;
  @Mock
  private SupportsUpgradeDowngrade upgradeDowngradeHelper;

  private SevenToEightUpgradeHandler upgradeHandler;

  @BeforeEach
  void setUp() {
    upgradeHandler = new SevenToEightUpgradeHandler();
  }

  @Test
  void testPropertyUpgrade() {
    Map<ConfigProperty, String> tablePropsToAdd = new HashMap<>();

    // Simulate config values for key generator and partition path
    when(config.getString(anyString())).thenAnswer(i -> {
      Object arg0 = i.getArguments()[0];
      if (arg0.equals(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key())) {
        return "partition_field";
      } else if (arg0.equals(HoodieWriteConfig.RECORD_MERGE_MODE.key())) {
        return RecordMergeMode.EVENT_TIME_ORDERING.name();
      } else {
        return null;
      }
    });
    when(tableConfig.getKeyGeneratorClassName()).thenReturn("org.apache.hudi.keygen.CustomKeyGenerator");

    // Upgrade properties
    SevenToEightUpgradeHandler.upgradePartitionFields(config, tableConfig, tablePropsToAdd);
    assertEquals("partition_field", tablePropsToAdd.get(PARTITION_FIELDS));

    SevenToEightUpgradeHandler.setInitialVersion(tableConfig, tablePropsToAdd);
    assertEquals("6", tablePropsToAdd.get(INITIAL_VERSION));

    // Mock record merge mode configuration for merging behavior
    when(tableConfig.contains(isA(ConfigProperty.class))).thenAnswer(i -> i.getArguments()[0].equals(PAYLOAD_CLASS_NAME));
    when(tableConfig.getPayloadClass()).thenReturn(OverwriteWithLatestAvroPayload.class.getName());
    SevenToEightUpgradeHandler.upgradeMergeMode(tableConfig, tablePropsToAdd);
    assertTrue(tablePropsToAdd.containsKey(RECORD_MERGE_MODE));
    assertNotNull(tablePropsToAdd.get(RECORD_MERGE_MODE));

    // Simulate bootstrap index type upgrade
    when(tableConfig.getBooleanOrDefault(BOOTSTRAP_INDEX_ENABLE)).thenReturn(true);
    when(tableConfig.contains(BOOTSTRAP_INDEX_CLASS_NAME)).thenReturn(true);
    when(tableConfig.getString(BOOTSTRAP_INDEX_CLASS_NAME)).thenReturn(HFileBootstrapIndex.class.getName());
    SevenToEightUpgradeHandler.upgradeBootstrapIndexType(tableConfig, tablePropsToAdd);
    assertTrue(tablePropsToAdd.containsKey(BOOTSTRAP_INDEX_CLASS_NAME));
    assertTrue(tablePropsToAdd.containsKey(BOOTSTRAP_INDEX_TYPE));

    // Simulate key generator type upgrade
    SevenToEightUpgradeHandler.upgradeKeyGeneratorType(tableConfig, tablePropsToAdd);
    assertTrue(tablePropsToAdd.containsKey(KEY_GENERATOR_CLASS_NAME));
    assertTrue(tablePropsToAdd.containsKey(KEY_GENERATOR_TYPE));
  }

  @Test
  void testUpgradeMergeMode() {
    Object[][] scenarios = new Object[][] {
        { OverwriteWithLatestAvroPayload.class.getName(), HoodieTableType.COPY_ON_WRITE,
            createMap(HoodieTableConfig.RECORD_MERGE_MODE, RecordMergeMode.COMMIT_TIME_ORDERING.name()) },
        { OverwriteWithLatestAvroPayload.class.getName(), HoodieTableType.MERGE_ON_READ,
            createMap(
                HoodieTableConfig.PAYLOAD_CLASS_NAME, DefaultHoodieRecordPayload.class.getName(),
                HoodieTableConfig.RECORD_MERGE_MODE, RecordMergeMode.EVENT_TIME_ORDERING.name()
            ) },
        { DefaultHoodieRecordPayload.class.getName(), HoodieTableType.COPY_ON_WRITE,
            createMap(HoodieTableConfig.RECORD_MERGE_MODE, RecordMergeMode.EVENT_TIME_ORDERING.name()) },
        { "custom.payload.Class", HoodieTableType.COPY_ON_WRITE,
            createMap(HoodieTableConfig.RECORD_MERGE_MODE, RecordMergeMode.CUSTOM.name()) }
    };

    for (Object[] scenario : scenarios) {
      String payloadClass = (String) scenario[0];
      Map<ConfigProperty, String> expectedProps = (Map<ConfigProperty, String>) scenario[2];
      HoodieTableType tableType = (HoodieTableType) scenario[1];

      HoodieTableConfig mockTableConfig = Mockito.mock(HoodieTableConfig.class);
      when(mockTableConfig.getPayloadClass()).thenReturn(payloadClass);
      lenient().when(mockTableConfig.getTableType()).thenReturn(tableType);

      Map<ConfigProperty, String> tablePropsToAdd = new HashMap<>();
      SevenToEightUpgradeHandler.upgradeMergeMode(mockTableConfig, tablePropsToAdd);

      assertEquals(expectedProps, tablePropsToAdd);
    }
  }

  private static Map<ConfigProperty, String> createMap(Object... keyValues) {
    Map<ConfigProperty, String> map = new HashMap<>();
    for (int i = 0; i < keyValues.length; i += 2) {
      map.put((ConfigProperty) keyValues[i], (String) keyValues[i + 1]);
    }
    return map;
  }
}
