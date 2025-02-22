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

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.engine.LocalTaskContextSupplier;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.table.view.TableFileSystemView;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.table.HoodieTable;

import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.when;

public class TestHoodieMergeHandle extends HoodieCommonTestHarness {

  private static final String DEFAULT_PARTITION_PATH = "partition";
  private static final String DEFAULT_FILE_ID = "fileId";
  private static final String DEFAULT_INSTANT_TIME = "0000002";
  private static final String NEXT_INSTANT_TIME = "0000005";
  private static final String DEFAULT_FILE_NAME = String.format("%s_0-0-0_%s.parquet", DEFAULT_FILE_ID, DEFAULT_INSTANT_TIME);
  private static final String EXPECTED_NEW_FILE_NAME = String.format("%s_0-0-0_%s.parquet", DEFAULT_FILE_ID, NEXT_INSTANT_TIME);

  @Mock
  private HoodieTable mockTable;

  @Mock
  private Iterator mockRecordItr;

  @Mock
  private TableFileSystemView.BaseFileOnlyView mockFileSystemView;

  @Mock
  private HoodieBaseFile mockBaseFile;

  @Mock
  private BaseKeyGenerator mockKeyGenerator;

  @BeforeEach
  public void setUp() throws IOException {
    MockitoAnnotations.openMocks(this);
    initPath();
    initMetaClient();
    when(mockTable.getMetaClient()).thenReturn(metaClient);
    when(mockTable.getBaseFileOnlyView()).thenReturn(mockFileSystemView);
    when(mockTable.getPartitionMetafileFormat()).thenReturn(Option.empty());
    when(mockTable.getBaseFileExtension()).thenReturn(HoodieFileFormat.PARQUET.getFileExtension());
    when(mockFileSystemView.getLatestBaseFile(DEFAULT_PARTITION_PATH, DEFAULT_FILE_ID)).thenReturn(Option.of(mockBaseFile));
    when(mockBaseFile.getFileName()).thenReturn(DEFAULT_FILE_NAME);
    when(mockBaseFile.getFileId()).thenReturn(DEFAULT_FILE_ID);
    when(mockBaseFile.getCommitTime()).thenReturn(DEFAULT_INSTANT_TIME);
  }

  @AfterEach
  public void clean() {
    cleanMetaClient();
  }

  @Test
  public void validateExceptionThrownWhenInstantTimeLessThanLatestInstant() {
    final String invalidNextInstant = "0000001";
    final String expectedException = "The new instant: 0000001 should be strictly higher than the instant of the latest base file: 0000002 for fileId: fileId";
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA).build();
    when(mockTable.getConfig()).thenReturn(writeConfig);
    // First constructor
    Exception exception = Assertions.assertThrows(HoodieValidationException.class, () -> new HoodieTestMergeHandle(writeConfig,
        invalidNextInstant,
        mockTable,
        mockRecordItr,
        DEFAULT_PARTITION_PATH,
        DEFAULT_FILE_ID,
        new LocalTaskContextSupplier(),
        Option.empty()
    ));
    validateExceptionMessage(exception, expectedException);
    // Second constructor
    exception = Assertions.assertThrows(HoodieValidationException.class, () -> new HoodieTestMergeHandle(writeConfig,
        invalidNextInstant,
        mockTable,
        mockRecordItr,
        DEFAULT_PARTITION_PATH,
        DEFAULT_FILE_ID,
        new LocalTaskContextSupplier(),
        mockBaseFile,
        Option.empty()
    ));
    validateExceptionMessage(exception, expectedException);
    // Third constructor
    exception = Assertions.assertThrows(HoodieValidationException.class, () -> new HoodieTestMergeHandle(writeConfig,
        invalidNextInstant,
        mockTable,
        Collections.emptyMap(),
        DEFAULT_PARTITION_PATH,
        DEFAULT_FILE_ID,
        mockBaseFile,
        new LocalTaskContextSupplier(),
        Option.empty()
    ));
    validateExceptionMessage(exception, expectedException);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void validateExceptionThrownWhenPopulateMetaFieldsAndKeyGeneratorMisConfigured(boolean populateMetaFields) {
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
        .withPopulateMetaFields(populateMetaFields)
        .build();
    when(mockTable.getConfig()).thenReturn(writeConfig);
    Option<BaseKeyGenerator> keyGeneratorOption = populateMetaFields ? Option.of(mockKeyGenerator) : Option.empty();
    // First constructor
    Assertions.assertThrows(IllegalArgumentException.class, () -> new HoodieTestMergeHandle(writeConfig,
        NEXT_INSTANT_TIME,
        mockTable,
        mockRecordItr,
        DEFAULT_PARTITION_PATH,
        DEFAULT_FILE_ID,
        new LocalTaskContextSupplier(),
        keyGeneratorOption
    ));

    // Second constructor
    Assertions.assertThrows(IllegalArgumentException.class, () -> new HoodieTestMergeHandle(writeConfig,
        NEXT_INSTANT_TIME,
        mockTable,
        mockRecordItr,
        DEFAULT_PARTITION_PATH,
        DEFAULT_FILE_ID,
        new LocalTaskContextSupplier(),
        mockBaseFile,
        keyGeneratorOption
    ));

    // Third constructor
    Assertions.assertThrows(IllegalArgumentException.class, () -> new HoodieTestMergeHandle(writeConfig,
        NEXT_INSTANT_TIME,
        mockTable,
        Collections.emptyMap(),
        DEFAULT_PARTITION_PATH,
        DEFAULT_FILE_ID,
        mockBaseFile,
        new LocalTaskContextSupplier(),
        keyGeneratorOption
    ));
  }

  @Test
  public void validateInitializedPaths() {
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA).build();
    when(mockTable.getConfig()).thenReturn(writeConfig);
    // First constructor
    HoodieTestMergeHandle mergeHandle = new HoodieTestMergeHandle(writeConfig,
        NEXT_INSTANT_TIME,
        mockTable,
        mockRecordItr,
        DEFAULT_PARTITION_PATH,
        DEFAULT_FILE_ID,
        new LocalTaskContextSupplier(),
        Option.empty()
    );
    validatePaths(mergeHandle);

    // Second constructor
    mergeHandle = new HoodieTestMergeHandle(writeConfig,
        NEXT_INSTANT_TIME,
        mockTable,
        mockRecordItr,
        DEFAULT_PARTITION_PATH,
        DEFAULT_FILE_ID,
        new LocalTaskContextSupplier(),
        mockBaseFile,
        Option.empty()
    );
    validatePaths(mergeHandle);

    // Third constructor
    mergeHandle = new HoodieTestMergeHandle(writeConfig,
        NEXT_INSTANT_TIME,
        mockTable,
        Collections.emptyMap(),
        DEFAULT_PARTITION_PATH,
        DEFAULT_FILE_ID,
        mockBaseFile,
        new LocalTaskContextSupplier(),
        Option.empty()
    );
    validatePaths(mergeHandle);
  }

  private static void validateExceptionMessage(Exception exception, String expectedException) {
    Assertions.assertEquals(expectedException, exception.getMessage());
  }

  private static void validatePaths(HoodieTestMergeHandle mergeHandle) {
    Assertions.assertTrue(mergeHandle.getOldFilePath().getName().endsWith(DEFAULT_FILE_NAME));
    Assertions.assertTrue(mergeHandle.getTargetFilePath().getName().endsWith(EXPECTED_NEW_FILE_NAME));
  }

  private static class HoodieTestMergeHandle extends HoodieMergeHandle {

    public HoodieTestMergeHandle(HoodieWriteConfig config, String instantTime, HoodieTable hoodieTable, Iterator recordItr, String partitionPath, String fileId,
                                 TaskContextSupplier taskContextSupplier, Option keyGeneratorOpt) {
      super(config, instantTime, hoodieTable, recordItr, partitionPath, fileId, taskContextSupplier, keyGeneratorOpt);
    }

    public HoodieTestMergeHandle(HoodieWriteConfig config, String instantTime, HoodieTable hoodieTable, Iterator recordItr, String partitionPath, String fileId,
                                 TaskContextSupplier taskContextSupplier, HoodieBaseFile baseFile, Option keyGeneratorOpt) {
      super(config, instantTime, hoodieTable, recordItr, partitionPath, fileId, taskContextSupplier, baseFile, keyGeneratorOpt);
    }

    public HoodieTestMergeHandle(HoodieWriteConfig config, String instantTime, HoodieTable hoodieTable, Map keyToNewRecords, String partitionPath, String fileId,
                                 HoodieBaseFile baseFile, TaskContextSupplier taskContextSupplier, Option keyGeneratorOpt) {
      super(config, instantTime, hoodieTable, keyToNewRecords, partitionPath, fileId, baseFile, taskContextSupplier, keyGeneratorOpt);
    }

    @Override
    public void doMerge() throws IOException {
      // no operation
    }

    @Override
    public List<WriteStatus> close() {
      return Collections.emptyList();
    }

    public Path getTargetFilePath() {
      return targetFilePath;
    }
  }
}