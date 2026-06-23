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

package org.apache.hudi.common.table.read;

import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.storage.StoragePath;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TestInputSplit {

  @Test
  void filtersLegacyAndNativeCdcLogFiles() {
    HoodieLogFile legacyLogFile = new HoodieLogFile(new StoragePath(
        "/tmp/.file1_001.log.1_1-0-1"));
    HoodieLogFile legacyCdcLogFile = new HoodieLogFile(new StoragePath(
        "/tmp/.file1_001.log.2_1-0-1.cdc"));
    HoodieLogFile nativeLogFile = new HoodieLogFile(new StoragePath(
        "/tmp/file1_1-0-1_001_1.log.parquet"));
    HoodieLogFile nativeCdcLogFile = new HoodieLogFile(new StoragePath(
        "/tmp/file1_1-0-1_001_2.cdc.parquet"));
    HoodieLogFile nativeLanceLogFile = new HoodieLogFile(new StoragePath(
        "/tmp/file1_1-0-1_001_3.log.lance"));
    HoodieLogFile nativeCdcLanceLogFile = new HoodieLogFile(new StoragePath(
        "/tmp/file1_1-0-1_001_4.cdc.lance"));

    InputSplit inputSplit = InputSplit.builder()
        .logFileStream(Arrays.asList(legacyLogFile, legacyCdcLogFile, nativeLogFile, nativeCdcLogFile, nativeLanceLogFile, nativeCdcLanceLogFile).stream())
        .build();

    List<HoodieLogFile> logFiles = inputSplit.getLogFiles();
    assertEquals(3, logFiles.size());
    assertEquals(legacyLogFile, logFiles.get(0));
    assertEquals(nativeLogFile, logFiles.get(1));
    assertEquals(nativeLanceLogFile, logFiles.get(2));
  }
}
