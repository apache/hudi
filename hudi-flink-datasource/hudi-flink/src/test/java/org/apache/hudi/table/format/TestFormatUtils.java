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

package org.apache.hudi.table.format;

import org.apache.hudi.common.config.HoodieReaderConfig;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.storage.StoragePath;

import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;

import static org.apache.hudi.common.util.TestConfigUtils.TEST_BOOLEAN_CONFIG_PROPERTY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests {@link FormatUtils}
 */
public class TestFormatUtils {
  private static final String INLINE_LOG_PATH = "file:///tmp/.file-id_100.log.1_1-0-1";
  private static final String NATIVE_LOG_PATH = "file:///tmp/file-id_1-0-1_100_1.log.parquet";

  @Test
  public void testLsmReaderSelection() {
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
    when(metaClient.getTableConfig()).thenReturn(tableConfig);

    when(tableConfig.isLSMTreeStorageLayout()).thenReturn(false);
    assertFalse(FormatUtils.shouldUseLsmReader(metaClient, fileSlice(NATIVE_LOG_PATH)));

    when(tableConfig.isLSMTreeStorageLayout()).thenReturn(true);
    assertTrue(FormatUtils.shouldUseLsmReader(metaClient, fileSlice()));
    assertTrue(FormatUtils.shouldUseLsmReader(metaClient, fileSlice(NATIVE_LOG_PATH)));
    assertFalse(FormatUtils.shouldUseLsmReader(metaClient, fileSlice(INLINE_LOG_PATH, NATIVE_LOG_PATH)));
    assertTrue(FormatUtils.shouldUseLsmReader(
        metaClient, fileSlice(NATIVE_LOG_PATH), HoodieReaderConfig.REALTIME_PAYLOAD_COMBINE));
    assertFalse(FormatUtils.shouldUseLsmReader(
        metaClient, fileSlice(NATIVE_LOG_PATH), HoodieReaderConfig.REALTIME_SKIP_MERGE));
  }

  private static FileSlice fileSlice(String... logPaths) {
    FileSlice fileSlice = new FileSlice("partition", "100", "file-id");
    for (String logPath : logPaths) {
      fileSlice.addLogFile(new HoodieLogFile(new StoragePath(logPath)));
    }
    return fileSlice;
  }

  @Test
  public void testGetRawValueWithAltKeys() {
    Configuration flinkConf = new Configuration();
    assertEquals(Option.empty(),
        FormatUtils.getRawValueWithAltKeys(flinkConf, TEST_BOOLEAN_CONFIG_PROPERTY));

    boolean setValue = !Boolean.parseBoolean(TEST_BOOLEAN_CONFIG_PROPERTY.defaultValue());
    flinkConf.setString(TEST_BOOLEAN_CONFIG_PROPERTY.key(), setValue + "");
    assertEquals(Option.of(String.valueOf(setValue)),
        FormatUtils.getRawValueWithAltKeys(flinkConf, TEST_BOOLEAN_CONFIG_PROPERTY));

    flinkConf = new Configuration();
    flinkConf.setString(TEST_BOOLEAN_CONFIG_PROPERTY.getAlternatives().get(0), setValue + "");
    assertEquals(Option.of(String.valueOf(setValue)),
        FormatUtils.getRawValueWithAltKeys(flinkConf, TEST_BOOLEAN_CONFIG_PROPERTY));
  }

  @Test
  public void testGetBooleanWithAltKeys() {
    Configuration flinkConf = new Configuration();
    assertEquals(Boolean.parseBoolean(TEST_BOOLEAN_CONFIG_PROPERTY.defaultValue()),
        FormatUtils.getBooleanWithAltKeys(flinkConf, TEST_BOOLEAN_CONFIG_PROPERTY));

    boolean setValue = !Boolean.parseBoolean(TEST_BOOLEAN_CONFIG_PROPERTY.defaultValue());
    flinkConf.setString(TEST_BOOLEAN_CONFIG_PROPERTY.key(), setValue + "");
    assertEquals(setValue,
        FormatUtils.getBooleanWithAltKeys(flinkConf, TEST_BOOLEAN_CONFIG_PROPERTY));

    flinkConf = new Configuration();
    flinkConf.setString(TEST_BOOLEAN_CONFIG_PROPERTY.getAlternatives().get(0), setValue + "");
    assertEquals(setValue,
        FormatUtils.getBooleanWithAltKeys(flinkConf, TEST_BOOLEAN_CONFIG_PROPERTY));
  }
}
