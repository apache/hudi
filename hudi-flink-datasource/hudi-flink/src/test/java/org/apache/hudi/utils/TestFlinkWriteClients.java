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

import org.apache.hudi.client.transaction.lock.InProcessLockProvider;
import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.config.HoodieLockConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.util.FlinkWriteClients;
import org.apache.hudi.util.StreamerUtil;

import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

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
    Assertions.assertTrue(writeConfig.isLockRequired());
    assertThat(writeConfig.getLockProviderClass(), is(InProcessLockProvider.class.getName()));
    assertThat(writeConfig.getWriteConcurrencyMode(), is(WriteConcurrencyMode.SINGLE_WRITER));
  }

  @Test
  void testAutoSetupLockProviderWithoutTableService() throws Exception {
    conf.setBoolean(FlinkOptions.METADATA_ENABLED, true);
    conf.setBoolean("hoodie.table.services.enabled", false);
    StreamerUtil.initTableIfNotExists(conf);
    HoodieWriteConfig writeConfig = FlinkWriteClients.getHoodieClientConfig(conf, false, false);
    Assertions.assertFalse(writeConfig.isLockRequired());
    assertThat(writeConfig.getLockProviderClass(), is(HoodieLockConfig.LOCK_PROVIDER_CLASS_NAME.defaultValue()));
    assertThat(writeConfig.getWriteConcurrencyMode(), is(WriteConcurrencyMode.SINGLE_WRITER));
  }
}
