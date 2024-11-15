/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.hudi.common.table.log.block;

import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Test class for {@link HoodieLogFormat#newWriterBuilder()}.
 */
public class TestHoodieLogWriterBuilder {

  HoodieLogFormat.WriterBuilder builder;
  HoodieLogFormat.Writer writer;
  HoodieStorage storage;

  @BeforeEach
  public void setup() {
    storage = mock(HoodieStorage.class);
    builder = HoodieLogFormat.newWriterBuilder()
        .withFileId("test-fileid1")
        .withInstantTime("100")
        .onParentPath(new StoragePath("/tmp"))
        .withFileExtension(HoodieLogFile.DELTA_EXTENSION)
        .withStorage(storage);
  }

  @AfterEach
  public void cleanup() throws IOException {
    if (writer != null) {
      writer.close();
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {6, 8})
  public void testLogFileNaming(int tableVersion) throws IOException {
    // given: log file writer with a table version
    writer = builder
        .withTableVersion(HoodieTableVersion.fromVersionCode(tableVersion))
        .build();

    // when: log file name is constructed
    HoodieLogFile logFile = writer.getLogFile();

    // then: log file name is correct
    assertTrue(logFile.getFileName().contains(HoodieLogFile.DELTA_EXTENSION));
    assertEquals(HoodieLogFile.LOGFILE_BASE_VERSION, logFile.getLogVersion());
    assertEquals("100", logFile.getDeltaCommitTime());
    assertEquals("test-fileid1", logFile.getFileId());
    assertEquals("1-0-1", logFile.getLogWriteToken());
  }
}
