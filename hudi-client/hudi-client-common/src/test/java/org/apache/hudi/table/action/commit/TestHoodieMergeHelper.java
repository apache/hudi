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

package org.apache.hudi.table.action.commit;

import org.apache.hudi.common.avro.VariantSchemaUtils;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaCompatibility;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.queue.HoodieExecutor;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.core.io.storage.HoodieFileReader;
import org.apache.hudi.core.io.storage.HoodieFileReaderFactory;
import org.apache.hudi.core.io.storage.HoodieIOFactory;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.execution.ExecutorFactory;
import org.apache.hudi.io.HoodieWriteMergeHandle;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests that {@link HoodieMergeHelper#runMerge} closes the merge handle on a best-effort basis
 * when {@code executor.execute()} fails before the consumer's {@code finish()} (which normally
 * closes the handle) can run, so that the handle's underlying file writer/output stream isn't
 * leaked. A close failure in that path must be logged, not thrown, so it never masks the
 * original failure.
 */
class TestHoodieMergeHelper {

  @SuppressWarnings("unchecked")
  private HoodieSchema stubMergeSetup(HoodieTable table, HoodieWriteMergeHandle mergeHandle, HoodieWriteConfig writeConfig,
                               MockedStatic<HoodieIOFactory> ioFactoryStatic) throws IOException {
    HoodieBaseFile baseFile = mock(HoodieBaseFile.class);
    when(baseFile.getBootstrapBaseFile()).thenReturn(Option.empty());
    when(mergeHandle.baseFileForMerge()).thenReturn(baseFile);

    HoodieRecordMerger recordMerger = mock(HoodieRecordMerger.class);
    when(recordMerger.getRecordType()).thenReturn(HoodieRecord.HoodieRecordType.AVRO);
    when(writeConfig.getRecordMerger()).thenReturn(recordMerger);
    when(writeConfig.getInternalSchema()).thenReturn(null);
    when(table.getConfig()).thenReturn(writeConfig);

    HoodieStorage storage = mock(HoodieStorage.class);
    StorageConfiguration storageConf = mock(StorageConfiguration.class);
    when(table.getStorage()).thenReturn(storage);
    when(table.getStorageConf()).thenReturn(storageConf);
    when(storageConf.newInstance()).thenReturn(storageConf);
    when(storage.newInstance(any(StoragePath.class), any())).thenReturn(storage);
    when(mergeHandle.getOldFilePath()).thenReturn(new StoragePath("/tmp/old-file.parquet"));

    HoodieSchema schema = mock(HoodieSchema.class);
    when(mergeHandle.getWriterSchemaWithMetaFields()).thenReturn(schema);

    HoodieFileReader baseFileReader = mock(HoodieFileReader.class);
    when(baseFileReader.getSchema()).thenReturn(schema);
    when(baseFileReader.getRecordIterator(any())).thenReturn(mock(ClosableIterator.class));

    HoodieFileReaderFactory readerFactory = mock(HoodieFileReaderFactory.class);
    when(readerFactory.getFileReader(any(), any())).thenReturn(baseFileReader);

    HoodieIOFactory ioFactory = mock(HoodieIOFactory.class);
    when(ioFactory.getReaderFactory(any())).thenReturn(readerFactory);
    ioFactoryStatic.when(() -> HoodieIOFactory.getIOFactory(any())).thenReturn(ioFactory);

    when(table.getMetaClient()).thenReturn(mock(HoodieTableMetaClient.class));
    when(table.getPreExecuteRunnable()).thenReturn(() -> { });
    return schema;
  }

  @SuppressWarnings("unchecked")
  private void runMergeWithFailingExecutor(RuntimeException executeFailure, RuntimeException closeFailure) throws IOException {
    HoodieTable table = mock(HoodieTable.class);
    HoodieWriteMergeHandle mergeHandle = mock(HoodieWriteMergeHandle.class);
    HoodieWriteConfig writeConfig = mock(HoodieWriteConfig.class);

    if (closeFailure != null) {
      doThrow(closeFailure).when(mergeHandle).close();
    }

    try (MockedStatic<HoodieIOFactory> ioFactoryStatic = mockStatic(HoodieIOFactory.class);
         MockedStatic<VariantSchemaUtils> variantStatic = mockStatic(VariantSchemaUtils.class);
         MockedStatic<HoodieSchemaCompatibility> compatibilityStatic = mockStatic(HoodieSchemaCompatibility.class);
         MockedStatic<ExecutorFactory> executorFactoryStatic = mockStatic(ExecutorFactory.class)) {
      HoodieSchema schema = stubMergeSetup(table, mergeHandle, writeConfig, ioFactoryStatic);

      variantStatic.when(() -> VariantSchemaUtils.alignShreddedVariants(any(), any())).thenReturn(schema);
      compatibilityStatic.when(() -> HoodieSchemaCompatibility.isStrictProjectionOf(any(), any())).thenReturn(true);

      HoodieExecutor<Void> executor = mock(HoodieExecutor.class);
      doThrow(executeFailure).when(executor).execute();
      executorFactoryStatic.when(() -> ExecutorFactory.create(any(), any(), any(), any(), any())).thenReturn(executor);

      HoodieException thrown = assertThrows(HoodieException.class,
          () -> HoodieMergeHelper.newInstance().runMerge(table, mergeHandle));
      assertEquals(executeFailure, thrown.getCause());

      verify(executor).shutdownNow();
      verify(executor).awaitTermination();
      verify(mergeHandle).close();
    }
  }

  @Test
  void testRunMergeClosesMergeHandleWhenExecutorFails() throws IOException {
    runMergeWithFailingExecutor(new RuntimeException("simulated executor failure"), null);
  }

  @Test
  void testRunMergeSwallowsCloseFailureAfterExecutorFails() throws IOException {
    // The close failure must be logged rather than thrown, so the original executor failure
    // (wrapped below) remains the one reported to the caller.
    RuntimeException executeFailure = new RuntimeException("simulated executor failure");
    RuntimeException closeFailure = new RuntimeException("simulated close failure");
    runMergeWithFailingExecutor(executeFailure, closeFailure);
  }

  @Test
  @SuppressWarnings("unchecked")
  void testRunMergeDoesNotCloseMergeHandleDirectlyOnSuccess() throws IOException {
    // On success, the executor's consumer (UpdateHandler#finish) is responsible for closing the
    // merge handle; runMerge itself must not close it again.
    HoodieTable table = mock(HoodieTable.class);
    HoodieWriteMergeHandle mergeHandle = mock(HoodieWriteMergeHandle.class);
    HoodieWriteConfig writeConfig = mock(HoodieWriteConfig.class);

    try (MockedStatic<HoodieIOFactory> ioFactoryStatic = mockStatic(HoodieIOFactory.class);
         MockedStatic<VariantSchemaUtils> variantStatic = mockStatic(VariantSchemaUtils.class);
         MockedStatic<HoodieSchemaCompatibility> compatibilityStatic = mockStatic(HoodieSchemaCompatibility.class);
         MockedStatic<ExecutorFactory> executorFactoryStatic = mockStatic(ExecutorFactory.class)) {
      HoodieSchema schema = stubMergeSetup(table, mergeHandle, writeConfig, ioFactoryStatic);

      variantStatic.when(() -> VariantSchemaUtils.alignShreddedVariants(any(), any())).thenReturn(schema);
      compatibilityStatic.when(() -> HoodieSchemaCompatibility.isStrictProjectionOf(any(), any())).thenReturn(true);

      HoodieExecutor<Void> executor = mock(HoodieExecutor.class);
      executorFactoryStatic.when(() -> ExecutorFactory.create(any(), any(), any(), any(), any())).thenReturn(executor);

      HoodieMergeHelper.newInstance().runMerge(table, mergeHandle);

      verify(executor).shutdownNow();
      verify(executor).awaitTermination();
      verify(mergeHandle, times(0)).close();
    }
  }
}
