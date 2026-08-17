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

import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.engine.LocalTaskContextSupplier;
import org.apache.hudi.common.engine.ReaderContextFactory;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.view.TableFileSystemView;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.core.io.storage.HoodieFileWriter;
import org.apache.hudi.core.io.storage.HoodieFileWriterFactory;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.marker.WriteMarkers;
import org.apache.hudi.table.marker.WriteMarkersFactory;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

/**
 * Tests merge handle instantiation through {@link HoodieMergeHandleFactory}: the reflection-based
 * constructor lookup against the {@link MergeContext} signature, and the fallback behavior for
 * custom merge handle implementations that do not expose that constructor.
 */
class TestHoodieMergeHandleFactoryInstantiation extends HoodieCommonTestHarness {

  private static final String DEFAULT_PARTITION_PATH = "partition";
  private static final String DEFAULT_FILE_ID = "fileId";
  private static final String DEFAULT_INSTANT_TIME = "0000002";
  private static final String NEXT_INSTANT_TIME = "0000005";
  private static final String DEFAULT_FILE_NAME = String.format("%s_0-0-0_%s.parquet", DEFAULT_FILE_ID, DEFAULT_INSTANT_TIME);

  @Mock
  private HoodieTable mockTable;

  @Mock
  private TableFileSystemView.BaseFileOnlyView mockFileSystemView;

  @Mock
  private HoodieBaseFile mockBaseFile;

  private TaskContextSupplier taskContextSupplier;

  @BeforeEach
  void setUp() throws IOException {
    MockitoAnnotations.openMocks(this);
    initPath();
    initMetaClient();
    taskContextSupplier = new LocalTaskContextSupplier();

    when(mockTable.getMetaClient()).thenReturn(metaClient);
    when(mockTable.getBaseFileOnlyView()).thenReturn(mockFileSystemView);
    when(mockTable.getPartitionMetafileFormat()).thenReturn(Option.empty());
    when(mockTable.getBaseFileExtension()).thenReturn(HoodieFileFormat.PARQUET.getFileExtension());
    when(mockTable.shouldTrackSuccessRecords()).thenReturn(false);
    when(mockTable.isMetadataTable()).thenReturn(false);
    when(mockTable.getStorageConf()).thenReturn(metaClient.getStorageConf());
    when(mockTable.getStorage()).thenReturn(metaClient.getStorage());
    ReaderContextFactory mockReaderContextFactory = mock(ReaderContextFactory.class);
    when(mockReaderContextFactory.getContext()).thenReturn(mock(HoodieReaderContext.class));
    when(mockTable.getReaderContextFactoryForWrite()).thenReturn(mockReaderContextFactory);
    when(mockFileSystemView.getLatestBaseFile(DEFAULT_PARTITION_PATH, DEFAULT_FILE_ID)).thenReturn(Option.of(mockBaseFile));
    when(mockBaseFile.getFileName()).thenReturn(DEFAULT_FILE_NAME);
    when(mockBaseFile.getFileId()).thenReturn(DEFAULT_FILE_ID);
    when(mockBaseFile.getCommitTime()).thenReturn(DEFAULT_INSTANT_TIME);
  }

  @AfterEach
  void clean() {
    cleanMetaClient();
  }

  private HoodieWriteConfig buildWriteConfig(Map<String, String> overrides) {
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
        .withProps(overrides)
        .build();
    when(mockTable.getConfig()).thenReturn(writeConfig);
    return writeConfig;
  }

  private HoodieMergeHandle createHandleViaFactory(HoodieWriteConfig writeConfig, long numUpdates) {
    try (MockedStatic<WriteMarkersFactory> mockedMarkers = mockStatic(WriteMarkersFactory.class);
         MockedStatic<HoodieFileWriterFactory> mockedWriterFactory = mockStatic(HoodieFileWriterFactory.class)) {
      WriteMarkers mockWriteMarkers = mock(WriteMarkers.class);
      mockedMarkers.when(() -> WriteMarkersFactory.get(any(), any(), anyString())).thenReturn(mockWriteMarkers);
      HoodieFileWriter mockWriter = mock(HoodieFileWriter.class);
      mockedWriterFactory.when(() -> HoodieFileWriterFactory.getFileWriter(
          anyString(), any(), any(), any(), any(), any(), any())).thenReturn(mockWriter);

      MergeContext<HoodieRecord> mergeContext = MergeContext.create(numUpdates, Collections.emptyIterator());
      return HoodieMergeHandleFactory.create(
          WriteOperationType.UPSERT, writeConfig, NEXT_INSTANT_TIME, mockTable, mergeContext,
          DEFAULT_PARTITION_PATH, DEFAULT_FILE_ID, taskContextSupplier, Option.empty());
    }
  }

  @Test
  void testFactoryCreatesCustomHandleWithMergeContextConstructor() {
    Map<String, String> overrides = new HashMap<>();
    overrides.put(HoodieWriteConfig.MERGE_HANDLE_CLASS_NAME.key(), MergeContextBasedMergeHandle.class.getName());
    HoodieWriteConfig writeConfig = buildWriteConfig(overrides);

    long expectedNumUpdates = 42L;
    HoodieMergeHandle handle = createHandleViaFactory(writeConfig, expectedNumUpdates);

    assertInstanceOf(MergeContextBasedMergeHandle.class, handle);
    assertEquals(expectedNumUpdates, ((HoodieWriteMergeHandle) handle).getNumUpdates(),
        "The factory should thread numUpdates through MergeContext into the handle");
  }

  @Test
  void testFactoryFallsBackToDefaultHandleForLegacyIteratorConstructor() {
    // A custom merge handle that only exposes the pre-1.2.0 Iterator-based constructor is no
    // longer instantiable after the switch to the MergeContext-based constructor lookup. With
    // hoodie.write.merge.handle.fallback enabled (the default), the factory silently substitutes
    // the default implementation. This test documents that behavior.
    Map<String, String> overrides = new HashMap<>();
    overrides.put(HoodieWriteConfig.MERGE_HANDLE_CLASS_NAME.key(), LegacyIteratorBasedMergeHandle.class.getName());
    HoodieWriteConfig writeConfig = buildWriteConfig(overrides);

    HoodieMergeHandle handle = createHandleViaFactory(writeConfig, 42L);

    assertInstanceOf(FileGroupReaderBasedMergeHandle.class, handle,
        "With fallback enabled, a legacy custom handle should be silently replaced by the default");
  }

  @Test
  void testFactoryThrowsForLegacyIteratorConstructorWhenFallbackDisabled() {
    Map<String, String> overrides = new HashMap<>();
    overrides.put(HoodieWriteConfig.MERGE_HANDLE_CLASS_NAME.key(), LegacyIteratorBasedMergeHandle.class.getName());
    overrides.put(HoodieWriteConfig.MERGE_HANDLE_PERFORM_FALLBACK.key(), "false");
    HoodieWriteConfig writeConfig = buildWriteConfig(overrides);

    assertThrows(HoodieException.class, () -> createHandleViaFactory(writeConfig, 42L),
        "With fallback disabled, a legacy custom handle should fail loudly");
  }

  /**
   * A custom merge handle exposing the current MergeContext-based constructor.
   */
  public static class MergeContextBasedMergeHandle<T, I, K, O> extends HoodieWriteMergeHandle<T, I, K, O> {
    public MergeContextBasedMergeHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable,
                                        MergeContext<T> mergeContext, String partitionPath, String fileId,
                                        TaskContextSupplier taskContextSupplier, Option<BaseKeyGenerator> keyGeneratorOpt) {
      super(config, instantTime, hoodieTable, mergeContext, partitionPath, fileId, taskContextSupplier, keyGeneratorOpt);
    }
  }

  /**
   * A custom merge handle exposing only the legacy Iterator-based constructor shape used before
   * the MergeContext refactoring.
   */
  public static class LegacyIteratorBasedMergeHandle<T, I, K, O> extends HoodieWriteMergeHandle<T, I, K, O> {
    public LegacyIteratorBasedMergeHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable,
                                          Iterator<HoodieRecord<T>> recordItr, String partitionPath, String fileId,
                                          TaskContextSupplier taskContextSupplier, Option<BaseKeyGenerator> keyGeneratorOpt) {
      super(config, instantTime, hoodieTable, MergeContext.create(recordItr), partitionPath, fileId, taskContextSupplier, keyGeneratorOpt);
    }
  }
}
