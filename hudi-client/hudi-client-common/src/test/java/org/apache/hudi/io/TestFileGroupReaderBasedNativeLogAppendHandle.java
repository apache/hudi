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
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.engine.LocalTaskContextSupplier;
import org.apache.hudi.common.model.CompactionOperation;
import org.apache.hudi.common.model.HoodieDeltaWriteStat;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieWriteStat.RuntimeStats;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.read.HoodieFileGroupReader;
import org.apache.hudi.common.table.read.HoodieReadStats;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestFileGroupReaderBasedNativeLogAppendHandle {

  private static final String SCHEMA = "{\"type\":\"record\",\"name\":\"trip\",\"fields\":["
      + "{\"name\":\"id\",\"type\":\"string\"}]}";

  @Test
  public void testCompactsLogRecordsAndCopiesReadStatsToFirstOutput() throws Exception {
    HoodieWriteConfig config = config();
    HoodieTable table = table(config);
    CompactionOperation operation = operation();
    HoodieReaderContext readerContext = mock(HoodieReaderContext.class);
    HoodieFileGroupReader reader = mock(HoodieFileGroupReader.class);
    ClosableIterator records = mock(ClosableIterator.class);
    HoodieReadStats readStats = readStats();
    when(records.hasNext()).thenReturn(false);
    when(reader.getLogRecordsOnly()).thenReturn(records);
    when(reader.getValidBlockInstants()).thenReturn(Arrays.asList("001", "002"));
    when(reader.getReadStats()).thenReturn(readStats);

    HoodieFileGroupReader.HoodieFileGroupReaderBuilder builder =
        mock(HoodieFileGroupReader.HoodieFileGroupReaderBuilder.class, RETURNS_SELF);
    when(builder.build()).thenReturn(reader);
    try (MockedStatic<HoodieFileGroupReader> fileGroupReaders = mockStatic(HoodieFileGroupReader.class)) {
      fileGroupReaders.when(HoodieFileGroupReader::builder).thenReturn(builder);
      TestableFileGroupReaderHandle handle = new TestableFileGroupReaderHandle(
          config, table, operation, readerContext);
      handle.doAppend();

      WriteStatus first = status("first.log.parquet");
      WriteStatus second = status("second.deletes.parquet");
      handle.addStatuses(first, second);
      List<WriteStatus> statuses = handle.close();

      assertEquals(2, statuses.size());
      for (WriteStatus status : statuses) {
        assertEquals("partition", status.getStat().getPartitionPath());
        assertEquals("001", status.getStat().getPrevCommit());
      }
      assertEquals(11L, first.getStat().getTotalLogReadTimeMs());
      assertEquals(12L, first.getStat().getTotalUpdatedRecordsCompacted());
      assertEquals(13L, first.getStat().getTotalLogFilesCompacted());
      assertEquals(14L, first.getStat().getTotalLogRecords());
      assertEquals(15L, first.getStat().getTotalLogBlocks());
      assertEquals(16L, first.getStat().getTotalCorruptLogBlock());
      assertEquals(17L, first.getStat().getTotalRollbackBlocks());
      assertEquals(18L, first.getStat().getTotalLogSizeCompacted());
      assertEquals(11L, first.getStat().getRuntimeStats().getTotalScanTime());
      assertEquals(0L, second.getStat().getTotalLogReadTimeMs());
      verify(reader).close();
    }
  }

  @Test
  public void testWrapsReaderInitializationFailure() throws Exception {
    HoodieWriteConfig config = config();
    HoodieTable table = table(config);
    HoodieFileGroupReader reader = mock(HoodieFileGroupReader.class);
    when(reader.getLogRecordsOnly()).thenThrow(new IOException("failure"));
    HoodieFileGroupReader.HoodieFileGroupReaderBuilder builder =
        mock(HoodieFileGroupReader.HoodieFileGroupReaderBuilder.class, RETURNS_SELF);
    when(builder.build()).thenReturn(reader);

    try (MockedStatic<HoodieFileGroupReader> fileGroupReaders = mockStatic(HoodieFileGroupReader.class)) {
      fileGroupReaders.when(HoodieFileGroupReader::builder).thenReturn(builder);
      TestableFileGroupReaderHandle handle = new TestableFileGroupReaderHandle(
          config, table, operation(), mock(HoodieReaderContext.class));
      assertThrows(HoodieIOException.class, handle::doAppend);
      verify(reader).close();
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
    when(table.version()).thenReturn(HoodieTableVersion.TEN);
    when(metaClient.getBasePath()).thenReturn(new StoragePath("/tmp"));
    when(metaClient.getTableConfig()).thenReturn(tableConfig);
    when(metaClient.getIndexMetadata()).thenReturn(Option.empty());
    when(tableConfig.getTableVersion()).thenReturn(HoodieTableVersion.TEN);
    when(tableConfig.getRecordMergeMode()).thenReturn(RecordMergeMode.COMMIT_TIME_ORDERING);
    return table;
  }

  private static CompactionOperation operation() {
    CompactionOperation operation = mock(CompactionOperation.class);
    when(operation.getPartitionPath()).thenReturn("partition");
    when(operation.getFileId()).thenReturn("file-1");
    when(operation.getBaseInstantTime()).thenReturn("001");
    when(operation.getDeltaFileNames()).thenReturn(Collections.singletonList("file-1.log.parquet"));
    return operation;
  }

  private static HoodieReadStats readStats() {
    HoodieReadStats stats = new HoodieReadStats();
    stats.setTotalLogReadTimeMs(11L);
    stats.setTotalUpdatedRecordsCompacted(12L);
    stats.setTotalLogFilesCompacted(13L);
    stats.setTotalLogRecords(14L);
    stats.setTotalLogBlocks(15L);
    stats.setTotalCorruptLogBlock(16L);
    stats.setTotalRollbackBlocks(17L);
    stats.setTotalLogSizeCompacted(18L);
    return stats;
  }

  private static WriteStatus status(String path) {
    WriteStatus status = new WriteStatus(false, 0.0);
    HoodieDeltaWriteStat stat = new HoodieDeltaWriteStat();
    stat.setPath(path);
    stat.setRuntimeStats(new RuntimeStats());
    status.setStat(stat);
    return status;
  }

  private static class TestableFileGroupReaderHandle extends FileGroupReaderBasedNativeLogAppendHandle {

    private TestableFileGroupReaderHandle(
        HoodieWriteConfig config, HoodieTable table, CompactionOperation operation,
        HoodieReaderContext readerContext) {
      super(config, "100", table, operation, new LocalTaskContextSupplier(), readerContext);
    }

    private void addStatuses(WriteStatus... writeStatuses) {
      statuses.addAll(Arrays.asList(writeStatuses));
    }
  }
}
