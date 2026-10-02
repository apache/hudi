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
 * distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.metadata;

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.engine.ReaderContextFactory;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.FileSystemViewStorageType;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.MockedStatic;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.testutils.HoodieTestUtils.getDefaultStorageConf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests validation and error propagation before secondary-index record generation.
 */
class TestSecondaryIndexRecordGenerationUtils {

  @Test
  void rejectsLogFileInsertsBeforeReadingFileSlices() {
    // Log-file inserts cannot be reconstructed without a base-file slice.
    HoodieWriteStat writeStat = new HoodieWriteStat();
    writeStat.setPartitionPath("p1");
    writeStat.setPath("p1/.fileid-1_014.log.1_1-0-1");
    writeStat.setNumInserts(1);
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder().withPath("/tmp/table").build();

    assertThrows(HoodieIOException.class,
        () -> SecondaryIndexRecordGenerationUtils.convertWriteStatsToSecondaryIndexRecords(
            Collections.singletonList(writeStat), "001", null, null, null, null, writeConfig, null));
  }

  @Test
  void wrapsTableSchemaResolutionFailure() {
    // Wrap schema lookup failures in the metadata utility's exception type.
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder().withPath("/tmp/table").build();

    try (MockedStatic<HoodieTableMetadataUtil> metadataUtil =
             mockStatic(HoodieTableMetadataUtil.class, CALLS_REAL_METHODS)) {
      metadataUtil.when(() -> HoodieTableMetadataUtil.tryResolveSchemaForTable(metaClient))
          .thenThrow(new IllegalStateException("no schema"));

      assertThrows(HoodieException.class,
          () -> SecondaryIndexRecordGenerationUtils.convertWriteStatsToSecondaryIndexRecords(
              Collections.emptyList(), "001", null, null, metaClient, null, writeConfig, null));
    }
  }

  @Test
  void fallsBackToTheCommitSchemaWhenTheTableHasNoCompletedCommit() {
    // The first commit of a table that registers files written outside Hudi has no completed commit to resolve
    // the table schema from, so the schema of the commit itself is used.
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
    when(metaClient.getTableConfig()).thenReturn(tableConfig);
    when(metaClient.getBasePath()).thenReturn(new StoragePath("/tmp/table"));
    when(tableConfig.getPayloadClass()).thenReturn("org.apache.hudi.common.model.DefaultHoodieRecordPayload");
    HoodieMetadataConfig metadataConfig = mock(HoodieMetadataConfig.class);
    when(metadataConfig.getSecondaryIndexParallelism()).thenReturn(1);
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder().withPath("/tmp/table").build();
    HoodieLocalEngineContext engineContext = new HoodieLocalEngineContext(getDefaultStorageConf());
    HoodieSchema schema = HoodieSchema.createRecord("external", null, null, false, Collections.singletonList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT))));

    try (MockedStatic<HoodieTableMetadataUtil> metadataUtil = mockStatic(HoodieTableMetadataUtil.class, CALLS_REAL_METHODS)) {
      metadataUtil.when(() -> HoodieTableMetadataUtil.tryResolveSchemaForTable(metaClient)).thenReturn(Option.empty());
      metadataUtil.when(() -> HoodieTableMetadataUtil.reduceByKeys(any(), anyInt(), anyBoolean()))
          .thenAnswer(invocation -> invocation.getArgument(0));

      HoodieCommitMetadata commitMetadataWithSchema = new HoodieCommitMetadata();
      commitMetadataWithSchema.addMetadata(HoodieCommitMetadata.SCHEMA_KEY, schema.toString());
      assertTrue(SecondaryIndexRecordGenerationUtils.convertWriteStatsToSecondaryIndexRecords(
          Collections.emptyList(), "001", null, metadataConfig, metaClient, engineContext, writeConfig, commitMetadataWithSchema)
          .collectAsList().isEmpty());

      // Hudi stores an empty schema in the commit metadata when the commit carries none; the failure names the key
      HoodieCommitMetadata commitMetadataWithoutSchema = new HoodieCommitMetadata();
      commitMetadataWithoutSchema.addMetadata(HoodieCommitMetadata.SCHEMA_KEY, "");
      HoodieException missingSchema = assertThrows(HoodieException.class,
          () -> SecondaryIndexRecordGenerationUtils.convertWriteStatsToSecondaryIndexRecords(
              Collections.emptyList(), "001", null, metadataConfig, metaClient, engineContext, writeConfig, commitMetadataWithoutSchema));
      assertEquals("Table /tmp/table has no completed commit to resolve its schema from and the commit metadata carries no schema",
          missingSchema.getMessage());
    }
  }

  @Test
  void rejectsLogFilesOnTableWithoutRecordKeys() {
    // the rows of such a table are keyed by their position in the base file, which the rows of a merged file slice
    // would not line up with
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
    when(metaClient.getTableConfig()).thenReturn(tableConfig);
    when(tableConfig.hasRecordKey()).thenReturn(false);
    when(tableConfig.getRecordKeyFields()).thenReturn(Option.empty());
    HoodieIndexDefinition indexDefinition = mock(HoodieIndexDefinition.class);
    when(indexDefinition.getSourceFieldsKey()).thenReturn("name");
    HoodieSchema schema = HoodieSchema.createRecord("external", null, null, false, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT)),
        HoodieSchemaField.of("name", HoodieSchema.create(HoodieSchemaType.STRING))));
    FileSlice fileSlice = new FileSlice("p1", "001", "file1");
    fileSlice.setBaseFile(new HoodieBaseFile(new StoragePathInfo(
        new StoragePath("/tmp/table/p1/file1_001_hudiext"), 100L, false, (short) 0, 0L, 0L)));
    fileSlice.addLogFile(new HoodieLogFile(new StoragePath("/tmp/table/p1/.file1_001.log.1_0-0-0")));

    IllegalStateException logFiles = assertThrows(IllegalStateException.class,
        () -> SecondaryIndexRecordGenerationUtils.getRecordKeyToSecondaryKey(
            metaClient, mock(HoodieReaderContext.class), fileSlice, schema, indexDefinition, "001", new TypedProperties(), false));
    assertTrue(logFiles.getMessage().contains("needs a base file and no log files"), logFiles.getMessage());
  }

  @ParameterizedTest
  @EnumSource(value = FileSystemViewStorageType.class, names = {"MEMORY", "REMOTE_FIRST"})
  void looksUpPreviousFileSlicesOnAViewThatIsClosed(FileSystemViewStorageType storageType) throws Exception {
    // the previous file slices of the written file groups come from one file system view of the commit, which is
    // closed with the metadata table reader it opened once the file slices are looked up. A local view loads the
    // written partitions at once, the timeline server is asked for each written file group alone
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
    when(metaClient.getTableConfig()).thenReturn(tableConfig);
    when(metaClient.getBasePath()).thenReturn(new StoragePath("/tmp/table"));
    doReturn(getDefaultStorageConf()).when(metaClient).getStorageConf();
    when(tableConfig.hasRecordKey()).thenReturn(true);
    HoodieIndexDefinition indexDefinition = mock(HoodieIndexDefinition.class);
    when(indexDefinition.getIndexName()).thenReturn("secondary_index_idx_name");
    HoodieLocalEngineContext engineContext = spy(new HoodieLocalEngineContext(getDefaultStorageConf()));
    doReturn(mock(ReaderContextFactory.class)).when(engineContext).getReaderContextFactory(metaClient);
    HoodieSchema schema = HoodieSchema.createRecord("rec", null, null, false, Collections.singletonList(
        HoodieSchemaField.of("name", HoodieSchema.create(HoodieSchemaType.STRING))));

    HoodieWriteStat writeStat = new HoodieWriteStat();
    writeStat.setPartitionPath("p1");
    writeStat.setFileId("file1");
    writeStat.setPath("p1/file1_1-0-1_002.parquet");
    FileSlice previousFileSlice = new FileSlice("p1", "001", "file1");
    previousFileSlice.setBaseFile(new HoodieBaseFile(new StoragePathInfo(
        new StoragePath("/tmp/table/p1/file1_1-0-1_001.parquet"), 100L, false, (short) 0, 0L, 0L)));
    FileSystemViewManager viewManager = mock(FileSystemViewManager.class);
    SyncableFileSystemView view = mock(SyncableFileSystemView.class);
    when(viewManager.getFileSystemView(metaClient)).thenReturn(view);
    when(view.getLatestMergedFileSlicesBeforeOrOn("p1", "002")).thenAnswer(invocation -> Stream.of(previousFileSlice));
    when(view.getLatestMergedFileSliceBeforeOrOn("p1", "002", "file1")).thenReturn(Option.of(previousFileSlice));

    try (MockedStatic<FileSystemViewManager> viewManagers = mockStatic(FileSystemViewManager.class);
         MockedStatic<HoodieTableMetadataUtil> metadataUtil = mockStatic(HoodieTableMetadataUtil.class, CALLS_REAL_METHODS);
         MockedStatic<SecondaryIndexRecordGenerationUtils> generationUtils = mockStatic(SecondaryIndexRecordGenerationUtils.class, CALLS_REAL_METHODS)) {
      viewManagers.when(() -> FileSystemViewManager.createViewManager(any(), any(), any(), any(), any())).thenReturn(viewManager);
      metadataUtil.when(() -> HoodieTableMetadataUtil.tryResolveSchemaForTable(metaClient)).thenReturn(Option.of(schema));
      metadataUtil.when(() -> HoodieTableMetadataUtil.reduceByKeys(any(), anyInt(), anyBoolean())).thenAnswer(invocation -> invocation.getArgument(0));
      generationUtils.when(() -> SecondaryIndexRecordGenerationUtils.getRecordKeyToSecondaryKey(any(), any(), any(), any(), any(), any(), any(), anyBoolean()))
          .thenAnswer(invocation -> Collections.singletonMap("key1", ((FileSlice) invocation.getArgument(2)).getBaseInstantTime()));

      List<HoodieRecord> records = SecondaryIndexRecordGenerationUtils.convertWriteStatsToSecondaryIndexRecords(
          Collections.singletonList(writeStat), "002", indexDefinition, HoodieMetadataConfig.newBuilder().build(), metaClient, engineContext,
          HoodieWriteConfig.newBuilder().withPath("/tmp/table")
              .withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder().withStorageType(storageType).build()).build(),
          new HoodieCommitMetadata()).collectAsList();

      // the secondary key changes from the one in the previous file slice to the one in the new base file
      assertEquals(Arrays.asList("001$key1", "002$key1"), records.stream().map(HoodieRecord::getRecordKey).sorted().collect(Collectors.toList()));
      viewManagers.verify(() -> FileSystemViewManager.createViewManager(any(), any(), any(), any(), any()), times(1));
      verify(viewManager).close();
      if (storageType == FileSystemViewStorageType.MEMORY) {
        verify(view).loadPartitions(Collections.singletonList("p1"));
        verify(view, never()).getLatestMergedFileSliceBeforeOrOn(any(), any(), any());
      } else {
        verify(view).getLatestMergedFileSliceBeforeOrOn("p1", "002", "file1");
        verify(view, never()).getLatestMergedFileSlicesBeforeOrOn(any(), any());
      }
    }
  }
}
