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

package org.apache.hudi.common.table.read.buffer;

import org.apache.hudi.avro.HoodieAvroReaderContext;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.read.BaseFileUpdateCallback;
import org.apache.hudi.common.table.read.DeleteContext;
import org.apache.hudi.common.table.read.FileGroupReaderSchemaHandler;
import org.apache.hudi.common.table.read.HoodieReadStats;
import org.apache.hudi.common.table.read.InputSplit;
import org.apache.hudi.common.table.read.ReaderParameters;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;

import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestFileGroupRecordBufferLoader extends BaseTestFileGroupRecordBuffer {

  @ParameterizedTest
  @CsvSource({"KeyBasedFileGroupRecordBuffer,true", "KeyBasedFileGroupRecordBuffer,false", "SortedKeyBasedFileGroupRecordBuffer,true",
      "SortedKeyBasedFileGroupRecordBuffer,false", "PositionBasedFileGroupRecordBuffer,false"})
  public void testDefaultFileGroupBufferRecordLoader(String fileGroupRecordBufferType, boolean testRecordsBased) {
    FileGroupRecordBufferLoader fileGroupRecordBufferLoader = !testRecordsBased
        ? FileGroupRecordBufferLoader.createDefault()
        : FileGroupRecordBufferLoader.createStreamingRecordsBufferLoader();
    HoodieReadStats readStats = new HoodieReadStats();
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
    when(tableConfig.getRecordMergeMode()).thenReturn(RecordMergeMode.COMMIT_TIME_ORDERING);
    when(tableConfig.getTableVersion()).thenReturn(HoodieTableVersion.NINE);
    when(tableConfig.getPartialUpdateMode()).thenReturn(Option.empty());
    when(tableConfig.getOrderingFieldsStr()).thenReturn(Option.empty());
    when(tableConfig.getRecordKeyFields()).thenReturn(Option.of(new String[] {"record_key"}));
    StorageConfiguration<?> storageConfiguration = mock(StorageConfiguration.class);
    HoodieReaderContext<IndexedRecord> readerContext = new HoodieAvroReaderContext(storageConfiguration, tableConfig, Option.empty(), Option.empty());
    readerContext.initRecordMerger(new TypedProperties());
    FileGroupReaderSchemaHandler<IndexedRecord> fileGroupReaderSchemaHandler = mock(FileGroupReaderSchemaHandler.class);
    when(fileGroupReaderSchemaHandler.getRequiredSchema()).thenReturn(SCHEMA);
    when(fileGroupReaderSchemaHandler.getRequestedSchema()).thenReturn(SCHEMA);
    when(fileGroupReaderSchemaHandler.getInternalSchema()).thenReturn(InternalSchema.getEmptyInternalSchema());
    DeleteContext deleteContext = mock(DeleteContext.class);
    when(deleteContext.getCustomDeleteMarkerKeyValue()).thenReturn(Option.empty());
    when(deleteContext.getHoodieOperationPos()).thenReturn(-1);
    when(fileGroupReaderSchemaHandler.getDeleteContext()).thenReturn(deleteContext);
    readerContext.setSchemaHandler(fileGroupReaderSchemaHandler);
    readerContext.setRecordMerger(Option.ofNullable(null));
    HoodieTableMetaClient mockMetaClient = mock(HoodieTableMetaClient.class, RETURNS_DEEP_STUBS);
    when(mockMetaClient.getTableConfig()).thenReturn(tableConfig);
    HoodieStorage storage = mock(HoodieStorage.class);
    when(mockMetaClient.getStorage()).thenReturn(storage);
    InputSplit inputSplit = mock(InputSplit.class);
    if (testRecordsBased) {
      when(inputSplit.getRecordIterator()).thenReturn(Collections.emptyIterator());
    }
    ReaderParameters readerParameters = mock(ReaderParameters.class);
    if (fileGroupRecordBufferType.contains("Sorted")) {
      when(readerParameters.sortOutputs()).thenReturn(true);
    }
    if (fileGroupRecordBufferType.contains("Position")) {
      HoodieBaseFile baseFile = mock(HoodieBaseFile.class);
      when(inputSplit.getBaseFileOption()).thenReturn(Option.of(baseFile));
      when(readerParameters.useRecordPosition()).thenReturn(true);
    }

    Option<BaseFileUpdateCallback> fileGroupUpdateCallback = Option.empty();

    HoodieFileGroupRecordBuffer fileGroupRecordBuffer = (HoodieFileGroupRecordBuffer) fileGroupRecordBufferLoader
        .getRecordBuffer(readerContext, storage, inputSplit, Collections.singletonList("ts"),
            mockMetaClient, new TypedProperties(), readerParameters, readStats, fileGroupUpdateCallback).getLeft();

    switch (fileGroupRecordBufferType) {
      case "KeyBasedFileGroupRecordBuffer":
        assertTrue(fileGroupRecordBuffer instanceof KeyBasedFileGroupRecordBuffer);
        break;
      case "SortedKeyBasedFileGroupRecordBuffer":
        assertTrue(fileGroupRecordBuffer instanceof SortedKeyBasedFileGroupRecordBuffer);
        break;
      case "PositionBasedFileGroupRecordBuffer":
        assertTrue(fileGroupRecordBuffer instanceof PositionBasedFileGroupRecordBuffer);
        break;
      default:
        throw new HoodieIOException("Undefined type");
    }
  }
}
