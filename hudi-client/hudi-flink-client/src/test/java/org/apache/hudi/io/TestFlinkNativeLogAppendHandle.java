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

import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.engine.LocalTaskContextSupplier;
import org.apache.hudi.common.engine.RecordContext;
import org.apache.hudi.common.model.HoodieDeltaWriteStat;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.io.cdc.HoodieNativeLogFormatWriter;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.commit.BucketType;

import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;

import java.io.IOException;
import java.util.Collections;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestFlinkNativeLogAppendHandle {

  private static final String SCHEMA = "{\"type\":\"record\",\"name\":\"trip\",\"fields\":["
      + "{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"ts\",\"type\":\"long\"}]}";

  @Test
  void testDoesNotRollOverWithinMiniBatch() throws Exception {
    HoodieWriteConfig config = config();
    HoodieTable table = table(config);
    HoodieRecord inputRecord = mock(HoodieRecord.class);
    HoodieRecord populatedRecord = mock(HoodieRecord.class);
    when(inputRecord.prependMetaFields(any(HoodieSchema.class), any(HoodieSchema.class), any(), any()))
        .thenReturn(populatedRecord);

    try (MockedConstruction<HoodieNativeLogFormatWriter> writers = mockConstruction(
        HoodieNativeLogFormatWriter.class, (writer, context) -> {
          when(writer.canWriteDataFile()).thenReturn(false);
          when(writer.canWriteDeleteFile()).thenReturn(false);
          when(writer.hasPendingWrites()).thenReturn(true);
          when(writer.getLastAppendResults()).thenReturn(Collections.emptyList());
        })) {
      TestableFlinkNativeLogAppendHandle handle = new TestableFlinkNativeLogAppendHandle(config, table);
      handle.createWriter();
      HoodieNativeLogFormatWriter writer = writers.constructed().get(0);

      handle.writeData(inputRecord);
      handle.writeData(inputRecord);
      handle.writeDeleteRecord(inputRecord);
      handle.writeDeleteRecord(inputRecord);

      verify(writer, never()).canWriteDataFile();
      verify(writer, never()).canWriteDeleteFile();
      verify(writer, never()).flushAppend(any());
      verify(writer, times(2)).appendRecord(eq(populatedRecord), any(HoodieSchema.class), any());
      verify(writer, times(2)).appendDeleteRecord(eq(inputRecord), any(HoodieSchema.class), any());

      handle.flushWriter();
      verify(writer).flushAppend(any());
    }
  }

  private static HoodieWriteConfig config() {
    return HoodieWriteConfig.newBuilder()
        .withPath("/tmp")
        .withSchema(SCHEMA)
        .withWriteTableVersion(HoodieTableVersion.TEN.versionCode())
        .withWriteRecordPositionsEnabled(false)
        .build();
  }

  private static HoodieTable table(HoodieWriteConfig config) {
    HoodieTable table = mock(HoodieTable.class);
    HoodieStorage storage = mock(HoodieStorage.class);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
    when(table.getStorage()).thenReturn(storage);
    when(table.getConfig()).thenReturn(config);
    when(table.getMetaClient()).thenReturn(metaClient);
    when(table.getBaseFileFormat()).thenReturn(HoodieFileFormat.PARQUET);
    when(table.getRecordContextForWrite()).thenReturn(mock(RecordContext.class));
    when(table.version()).thenReturn(HoodieTableVersion.TEN);
    when(metaClient.getBasePath()).thenReturn(new StoragePath("/tmp"));
    when(metaClient.getTableConfig()).thenReturn(tableConfig);
    when(metaClient.getIndexMetadata()).thenReturn(Option.empty());
    when(tableConfig.getTableVersion()).thenReturn(HoodieTableVersion.TEN);
    when(tableConfig.getRecordMergeMode()).thenReturn(RecordMergeMode.COMMIT_TIME_ORDERING);
    when(tableConfig.getPayloadClassIfPresent()).thenReturn(Option.empty());
    return table;
  }

  private static class TestableFlinkNativeLogAppendHandle extends FlinkNativeLogAppendHandle {

    private TestableFlinkNativeLogAppendHandle(HoodieWriteConfig config, HoodieTable table) {
      super(config, "100", table, "partition", "file-1", BucketType.UPDATE,
          Collections.emptyIterator(), new LocalTaskContextSupplier());
    }

    private void createWriter() {
      HoodieDeltaWriteStat stat = new HoodieDeltaWriteStat();
      stat.setPartitionPath(partitionPath);
      stat.setFileId(fileId);
      writeStatus.setStat(stat);
      writeStatus.setFileId(fileId);
      writeStatus.setPartitionPath(partitionPath);
      createLogWriterForAppend("100", Option.empty());
    }

    private void writeData(HoodieRecord record) throws IOException {
      writeInsertAndUpdate(writeSchema, record, true);
    }

    private void writeDeleteRecord(HoodieRecord record) throws IOException {
      writeDelete(writeSchemaWithMetaFields, record);
    }

    private void flushWriter() {
      flushAppend();
    }
  }
}
