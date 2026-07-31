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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.common.table.read.lsm;

import org.apache.hudi.common.config.HoodieReaderConfig;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.storage.StoragePath;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestLsmReaderUtils {

  private static final String INLINE_LOG_PATH = ".file-id_100.log.1_1-0-1";
  private static final String NATIVE_LOG_PATH = "file-id_1-0-1_100_1.log.parquet";

  @Test
  void testShouldUseLsmReader() throws IOException {
    HoodieTableConfig tableConfig = new HoodieTableConfig();
    assertFalse(LsmReaderUtils.shouldUseLsmReader(
        tableConfig, Stream.of(logFile(NATIVE_LOG_PATH)), HoodieReaderConfig.REALTIME_PAYLOAD_COMBINE));

    tableConfig.setValue(HoodieTableConfig.TABLE_STORAGE_LAYOUT, HoodieTableConfig.TableStorageLayout.LSM_TREE.configValue());
    assertTrue(LsmReaderUtils.shouldUseLsmReader(
        tableConfig, Stream.empty(), HoodieReaderConfig.REALTIME_PAYLOAD_COMBINE));
    assertTrue(LsmReaderUtils.shouldUseLsmReader(
        tableConfig, Stream.of(logFile(NATIVE_LOG_PATH)), HoodieReaderConfig.REALTIME_PAYLOAD_COMBINE));
    assertFalse(LsmReaderUtils.shouldUseLsmReader(
        tableConfig,
        Stream.of(logFile(INLINE_LOG_PATH), logFile(NATIVE_LOG_PATH)),
        HoodieReaderConfig.REALTIME_PAYLOAD_COMBINE));
    assertFalse(LsmReaderUtils.shouldUseLsmReader(
        tableConfig, Stream.of(logFile(NATIVE_LOG_PATH)), HoodieReaderConfig.REALTIME_SKIP_MERGE));
  }

  private static HoodieLogFile logFile(String fileName) {
    return new HoodieLogFile(new StoragePath("/tmp/" + fileName), 10);
  }
}
