/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.table.format;

import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.read.FileGroupReaderTableState;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.TestConfigurations;
import org.apache.hudi.utils.TestData;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.InstantiationUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link ReaderTableStateProvider}.
 */
class TestReaderTableStateProvider {

  @TempDir
  File tempFile;

  /**
   * On a table before version 8, a bounded read checks log blocks against the instants committed when it was planned,
   * while every split of a streaming read sees the instants committed when the split is read.
   */
  @Test
  void testCommittedInstantsOfBoundedAndStreamingReads() throws Exception {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.set(FlinkOptions.TABLE_TYPE, HoodieTableType.MERGE_ON_READ.name());
    conf.set(FlinkOptions.WRITE_TABLE_VERSION, HoodieTableVersion.SIX.versionCode());
    conf.setString(HoodieTableConfig.TABLE_STORAGE_LAYOUT.key(), HoodieTableConfig.TableStorageLayout.DEFAULT.configValue());
    TestData.writeData(TestData.DATA_SET_INSERT, conf);
    HoodieTableMetaClient metaClient = StreamerUtil.createMetaClient(conf);
    assertSame(HoodieTableVersion.SIX, metaClient.getTableConfig().getTableVersion());
    StorageConfiguration<?> storageConf = metaClient.getStorageConf();

    ReaderTableStateProvider bounded = InstantiationUtil.clone(ReaderTableStateProvider.snapshotOf(metaClient, false));
    ReaderTableStateProvider streaming = InstantiationUtil.clone(ReaderTableStateProvider.perSplit(metaClient));
    assertSame(bounded.forSplit(storageConf), bounded.forSplit(storageConf));
    assertNotSame(streaming.forSplit(storageConf), streaming.forSplit(storageConf));
    assertEquals(metaClient.getTableConfig().getTableName(), streaming.getTableConfig().getTableName());

    TestData.writeData(TestData.DATA_SET_UPDATE_INSERT, conf);
    String laterInstant = metaClient.reloadActiveTimeline().getCommitsTimeline().lastInstant().get().requestedTime();
    FileGroupReaderTableState boundedState = bounded.forSplit(storageConf);
    FileGroupReaderTableState streamingState = streaming.forSplit(storageConf);
    assertFalse(boundedState.isCommitted(laterInstant));
    assertTrue(streamingState.isCommitted(laterInstant));
  }
}
