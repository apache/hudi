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

import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.PartialUpdateMode;
import org.apache.hudi.common.table.log.block.HoodieDataBlock;
import org.apache.hudi.common.table.log.block.HoodieDeleteBlock;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.internal.schema.InternalSchema;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestSortedKeyBasedFileGroupRecordBuffer {
  private final TestRecord testRecord1 = new TestRecord("1", 0);
  private final TestRecord testRecord2 = new TestRecord("2", 0);
  private final TestRecord testRecord2Update = new TestRecord("2", 1);
  private final TestRecord testRecord3 = new TestRecord("3", 0);
  private final TestRecord testRecord4 = new TestRecord("4", 0);
  private final TestRecord testRecord5 = new TestRecord("5", 0);
  private final TestRecord testRecord6 = new TestRecord("6", 0);
  private final TestRecord testRecord6Update = new TestRecord("6", 1);

  @Test
  void readBaseFileAndLogFile() throws IOException {
    HoodieReadStats readStats = new HoodieReadStats();
    HoodieReaderContext<TestRecord> mockReaderContext = mock(HoodieReaderContext.class, RETURNS_DEEP_STUBS);
    SortedKeyBasedFileGroupRecordBuffer<TestRecord> fileGroupRecordBuffer = buildSortedKeyBasedFileGroupRecordBuffer(mockReaderContext, readStats);

    fileGroupRecordBuffer.setBaseFileIterator(ClosableIterator.wrap(Arrays.asList(testRecord2, testRecord3, testRecord5).iterator()));

    HoodieDataBlock dataBlock = mock(HoodieDataBlock.class);
    when(dataBlock.getSchema()).thenReturn(HoodieTestDataGenerator.AVRO_SCHEMA);
    when(dataBlock.getEngineRecordIterator(mockReaderContext)).thenReturn(ClosableIterator.wrap(Arrays.asList(testRecord6, testRecord4, testRecord1, testRecord6Update, testRecord2Update).iterator()));

    HoodieDeleteBlock deleteBlock = mock(HoodieDeleteBlock.class);
    when(deleteBlock.getRecordsToDelete()).thenReturn(new DeleteRecord[]{DeleteRecord.create("3", "")});
    fileGroupRecordBuffer.processDataBlock(dataBlock, Option.empty());
    fileGroupRecordBuffer.processDeleteBlock(deleteBlock);


    List<TestRecord> actualRecords = getActualRecords(fileGroupRecordBuffer);
    assertEquals(Arrays.asList(testRecord1, testRecord2Update, testRecord4, testRecord5, testRecord6Update), actualRecords);
    assertEquals(4, readStats.getNumInserts());
    assertEquals(1, readStats.getNumUpdates());
    assertEquals(1, readStats.getNumDeletes());
  }

  @Test
  void readLogFiles() throws IOException {
    HoodieReadStats readStats = new HoodieReadStats();
    HoodieReaderContext<TestRecord> mockReaderContext = mock(HoodieReaderContext.class, RETURNS_DEEP_STUBS);
    SortedKeyBasedFileGroupRecordBuffer<TestRecord> fileGroupRecordBuffer = buildSortedKeyBasedFileGroupRecordBuffer(mockReaderContext, readStats);

    fileGroupRecordBuffer.setBaseFileIterator(ClosableIterator.wrap(Collections.emptyIterator()));

    HoodieDataBlock dataBlock1 = mock(HoodieDataBlock.class);
    when(dataBlock1.getSchema()).thenReturn(HoodieTestDataGenerator.AVRO_SCHEMA);
    when(dataBlock1.getEngineRecordIterator(mockReaderContext)).thenReturn(ClosableIterator.wrap(Arrays.asList(testRecord6, testRecord4, testRecord6Update, testRecord2).iterator()));

    HoodieDataBlock dataBlock2 = mock(HoodieDataBlock.class);
    when(dataBlock2.getSchema()).thenReturn(HoodieTestDataGenerator.AVRO_SCHEMA);
    when(dataBlock2.getEngineRecordIterator(mockReaderContext)).thenReturn(ClosableIterator.wrap(Arrays.asList(testRecord2Update, testRecord5, testRecord3, testRecord1).iterator()));

    HoodieDeleteBlock deleteBlock = mock(HoodieDeleteBlock.class);
    when(deleteBlock.getRecordsToDelete()).thenReturn(new DeleteRecord[]{DeleteRecord.create("3", "")});
    fileGroupRecordBuffer.processDataBlock(dataBlock1, Option.empty());
    fileGroupRecordBuffer.processDataBlock(dataBlock2, Option.empty());
    fileGroupRecordBuffer.processDeleteBlock(deleteBlock);


    List<TestRecord> actualRecords = getActualRecords(fileGroupRecordBuffer);
    assertEquals(Arrays.asList(testRecord1, testRecord2Update, testRecord4, testRecord5, testRecord6Update), actualRecords);
    assertEquals(5, readStats.getNumInserts());
    assertEquals(0, readStats.getNumUpdates());
    assertEquals(1, readStats.getNumDeletes());
  }

  private SortedKeyBasedFileGroupRecordBuffer<TestRecord> buildSortedKeyBasedFileGroupRecordBuffer(HoodieReaderContext<TestRecord> mockReaderContext, HoodieReadStats readStats) {
    when(mockReaderContext.getSchemaHandler().getRequiredSchema()).thenReturn(HoodieTestDataGenerator.AVRO_SCHEMA);
    when(mockReaderContext.getSchemaHandler().getInternalSchema()).thenReturn(InternalSchema.getEmptyInternalSchema());
    when(mockReaderContext.getDeleteRow(any(), any())).thenAnswer(invocation -> {
      String recordKey = invocation.getArgument(1);
      return new TestRecord(recordKey, 0);
    });
    when(mockReaderContext.getRecordKey(any(), any())).thenAnswer(invocation -> ((TestRecord) invocation.getArgument(0)).recordKey);
    when(mockReaderContext.getOrderingValue(any(), any(), any())).thenReturn(0);
    when(mockReaderContext.toBinaryRow(any(), any())).thenAnswer(invocation -> invocation.getArgument(1));
    when(mockReaderContext.seal(any())).thenAnswer(invocation -> invocation.getArgument(0));
    HoodieTableMetaClient mockMetaClient = mock(HoodieTableMetaClient.class);
    RecordMergeMode recordMergeMode = RecordMergeMode.COMMIT_TIME_ORDERING;
    PartialUpdateMode partialUpdateMode = PartialUpdateMode.NONE;
    TypedProperties props = new TypedProperties();
    UpdateProcessor<TestRecord> updateProcessor = UpdateProcessor.create(readStats, mockReaderContext, false, Option.empty());
    return new SortedKeyBasedFileGroupRecordBuffer<>(
        mockReaderContext, mockMetaClient, recordMergeMode, partialUpdateMode, props, readStats, Option.empty(), updateProcessor);
  }

  private static List<TestRecord> getActualRecords(SortedKeyBasedFileGroupRecordBuffer<TestRecord> fileGroupRecordBuffer) throws IOException {
    List<TestRecord> actualRecords = new ArrayList<>();
    while (fileGroupRecordBuffer.hasNext()) {
      actualRecords.add(fileGroupRecordBuffer.next());
    }
    return actualRecords;
  }

  private static class TestRecord {
    private final String recordKey;
    private final int value;

    @Override
    public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      TestRecord that = (TestRecord) o;
      return value == that.value && Objects.equals(recordKey, that.recordKey);
    }

    @Override
    public int hashCode() {
      return Objects.hash(recordKey, value);
    }

    public TestRecord(String recordKey, int value) {
      this.recordKey = recordKey;
      this.value = value;
    }
  }
}