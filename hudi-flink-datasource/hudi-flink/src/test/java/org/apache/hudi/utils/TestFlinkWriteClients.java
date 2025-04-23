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

package org.apache.hudi.utils;

import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.client.common.HoodieFlinkEngineContext;
import org.apache.hudi.client.transaction.lock.FileSystemBasedLockProvider;
import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.util.FlinkWriteClients;
import org.apache.hudi.util.StreamerUtil;

import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for {@link org.apache.hudi.util.FlinkWriteClients}.
 */
public class TestFlinkWriteClients {
  @TempDir
  File tempFile;

  private Configuration conf;

  @BeforeEach
  public void before() throws Exception {
    this.conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
  }

  @Test
  void testAutoSetupLockProvider() throws Exception {
    conf.setBoolean(FlinkOptions.METADATA_ENABLED, true);
    StreamerUtil.initTableIfNotExists(conf);
    HoodieWriteConfig writeConfig = FlinkWriteClients.getHoodieClientConfig(conf, false, false);
    assertThat(writeConfig.getLockProviderClass(), is(FileSystemBasedLockProvider.class.getName()));
    assertThat(writeConfig.getWriteConcurrencyMode(), is(WriteConcurrencyMode.SINGLE_WRITER));
  }

  @Test
  void testDefaultMetadataConfig() throws Exception {
    HoodieTableMetaClient metaClient = StreamerUtil.initTableIfNotExists(conf);
    HoodieWriteConfig writeConfig = FlinkWriteClients.getHoodieClientConfig(conf, false, false);
    assertTrue(writeConfig.isMetadataTableEnabled(), "MDT is by default enabled");
    assertFalse(writeConfig.isMetadataColumnStatsIndexEnabled(), "column_stats index is by default disabled");
    assertFalse(writeConfig.isSecondaryIndexEnabled(), "secondary index is by default disabled");
    assertFalse(writeConfig.isExpressionIndexEnabled(), "expression index is by default disabled");
    // create write client
    try (HoodieFlinkWriteClient writeClient = new HoodieFlinkWriteClient(HoodieFlinkEngineContext.DEFAULT, writeConfig)) {
      // init metadata table
      writeClient.initMetadataTable();
      // reload the table config been updated by the metadata table
      metaClient.reloadTableConfig();
      assertThat(metaClient.getTableConfig().getMetadataPartitions().toString(), is("[files]"));
    }
  }
}
