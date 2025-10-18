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

package org.apache.hudi.io;

import org.apache.hudi.common.engine.LocalTaskContextSupplier;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.table.HoodieTable;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TestHoodieAppendHandle extends HoodieCommonTestHarness {

  private static final String TEST_INSTANT_TIME = "20231201120000";
  private static final String TEST_PARTITION_PATH = "partition1";
  private static final String TEST_FILE_ID = "file-001";

  @Mock
  private HoodieTable<Object, Object, Object, Object> mockHoodieTable;
  @Mock
  private HoodieStorage mockStorage;

  private HoodieWriteConfig writeConfig;

  private TaskContextSupplier taskContextSupplier;

  @BeforeEach
  public void setUp() throws IOException {
    initPath();
    initMetaClient();

    writeConfig = HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withSchema(TRIP_EXAMPLE_SCHEMA)
        .withMarkersType("DIRECT")
        .withWriteRecordPositionsEnabled(false)
        .withWriteTableVersion(HoodieTableVersion.SIX.versionCode())
        .build();
    taskContextSupplier = new LocalTaskContextSupplier();

    mockMethodsNeededByConstructor();
  }

  @AfterEach
  public void cleanUp() {
    cleanMetaClient();
  }

  private void mockMethodsNeededByConstructor() {
    when(mockHoodieTable.getConfig()).thenReturn(writeConfig);
    when(mockHoodieTable.getMetaClient()).thenReturn(metaClient);
    when(mockHoodieTable.getStorage()).thenReturn(mockStorage);
  }

  @Test
  void testCreateLogFileWriterLogVersion() throws IOException {
    HoodieAppendHandle<Object, Object, Object, Object> appendHandle =
        new HoodieAppendHandle<>(writeConfig, TEST_INSTANT_TIME, mockHoodieTable, TEST_PARTITION_PATH, TEST_FILE_ID, taskContextSupplier);

    FileSlice mockFileSlice = mock(FileSlice.class);
    when(mockFileSlice.getLatestLogFile()).thenReturn(Option.empty());

    // verify writer log version is 1 when there are no log files present
    try (HoodieLogFormat.Writer writer = appendHandle.createLogWriter(TEST_INSTANT_TIME, "", Option.of(mockFileSlice))) {
      assertEquals(1, writer.getLogFile().getLogVersion());
    }

    HoodieLogFile mockLogFile = mock(HoodieLogFile.class);
    when(mockLogFile.getLogVersion()).thenReturn(1);
    when(mockFileSlice.getLatestLogFile()).thenReturn(Option.of(mockLogFile));

    // verify writer log version is incremented when there log files are present.
    try (HoodieLogFormat.Writer writer = appendHandle.createLogWriter(TEST_INSTANT_TIME, "", Option.of(mockFileSlice))) {
      assertEquals(2, writer.getLogFile().getLogVersion());
    }
  }
}
