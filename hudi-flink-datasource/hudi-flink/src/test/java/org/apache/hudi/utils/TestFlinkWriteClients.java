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
import org.apache.hudi.common.table.marker.MarkerType;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.marker.DirectWriteMarkers;
import org.apache.hudi.table.marker.TimelineServerBasedWriteMarkers;
import org.apache.hudi.table.marker.WriteMarkersFactory;
import org.apache.hudi.util.FlinkWriteClients;
import org.apache.hudi.util.StreamerUtil;

import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

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

  @ParameterizedTest
  @ValueSource(strings = {"", "DIRECT", "TIMELINE_SERVER_BASED"})
  void testMarkerType(String markerType) throws Exception {
    // create table
    StreamerUtil.initTableIfNotExists(conf);
    // This is expected to be used by the driver, the client can then send requests for files view.
    FlinkWriteClients.createWriteClient(conf);

    // do not set mark type to test the default behavior
    if (!markerType.isEmpty()) {
      conf.setString(HoodieWriteConfig.MARKERS_TYPE.key(), MarkerType.valueOf(markerType).name());
    }
    // This is expected to be used by writer client
    HoodieWriteConfig writeConfig = FlinkWriteClients.getHoodieClientConfig(conf, false, true);
    try (HoodieFlinkWriteClient writeClient = new HoodieFlinkWriteClient(HoodieFlinkEngineContext.DEFAULT, writeConfig)) {
      HoodieTable table = writeClient.getHoodieTable();
      String markerClass = WriteMarkersFactory.get(writeConfig.getMarkersType(), table, "001").getClass().getSimpleName();
      if (markerType.isEmpty() || markerType.equals("DIRECT")) {
        assertEquals(DirectWriteMarkers.class.getSimpleName(), markerClass);
      } else {
        assertEquals(TimelineServerBasedWriteMarkers.class.getSimpleName(), markerClass);
      }
    }
  }
}
