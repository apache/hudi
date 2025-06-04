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
import org.apache.hudi.common.table.log.block.HoodieDataBlock;
import org.apache.hudi.common.table.log.block.HoodieDeleteBlock;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.internal.schema.InternalSchema;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

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

class TestUnmergedFileGroupRecordBuffer {
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void readWithDeleteBlocks(boolean emitDelete) throws IOException {
    HoodieReaderContext<TestRecord> mockReaderContext = mock(HoodieReaderContext.class, RETURNS_DEEP_STUBS);
    when(mockReaderContext.getSchemaHandler().getRequiredSchema()).thenReturn(HoodieTestDataGenerator.AVRO_SCHEMA);
    when(mockReaderContext.getSchemaHandler().getInternalSchema()).thenReturn(InternalSchema.getEmptyInternalSchema());
    when(mockReaderContext.getDeleteRow(any(), any())).thenAnswer(invocation -> {
      String recordKey = invocation.getArgument(1);
      return new TestRecord(recordKey);
    });
    when(mockReaderContext.seal(any())).thenAnswer(invocation -> invocation.getArgument(0));
    HoodieTableMetaClient mockMetaClient = mock(HoodieTableMetaClient.class);
    RecordMergeMode recordMergeMode = RecordMergeMode.COMMIT_TIME_ORDERING;
    TypedProperties props = new TypedProperties();
    HoodieReadStats readStats = new HoodieReadStats();
    UnmergedFileGroupRecordBuffer<TestRecord> unmergedFileGroupRecordBuffer =
        new UnmergedFileGroupRecordBuffer<>(mockReaderContext, mockMetaClient, recordMergeMode, props, readStats, emitDelete);
    unmergedFileGroupRecordBuffer.setBaseFileIterator(ClosableIterator.wrap(Collections.emptyIterator()));

    HoodieDataBlock dataBlock = mock(HoodieDataBlock.class);
    when(dataBlock.getSchema()).thenReturn(HoodieTestDataGenerator.AVRO_SCHEMA);
    TestRecord testRecord1 = new TestRecord("1");
    TestRecord testRecord2 = new TestRecord("2");
    when(dataBlock.getEngineRecordIterator(mockReaderContext)).thenReturn(ClosableIterator.wrap(Arrays.asList(testRecord1, testRecord2).iterator()));

    HoodieDeleteBlock deleteBlock = mock(HoodieDeleteBlock.class);
    when(deleteBlock.getRecordsToDelete()).thenReturn(new DeleteRecord[]{DeleteRecord.create("3", ""), DeleteRecord.create("4", "")});
    unmergedFileGroupRecordBuffer.processDataBlock(dataBlock, Option.empty());
    unmergedFileGroupRecordBuffer.processDeleteBlock(deleteBlock);

    List<TestRecord> actualRecords = new ArrayList<>();
    while (unmergedFileGroupRecordBuffer.hasNext()) {
      actualRecords.add(unmergedFileGroupRecordBuffer.next());
    }
    if (emitDelete) {
      assertEquals(Arrays.asList(testRecord1, testRecord2, new TestRecord("3"), new TestRecord("4")), actualRecords);
      assertEquals(2, readStats.getNumInserts());
      assertEquals(2, readStats.getNumDeletes());
    } else {
      assertEquals(Arrays.asList(testRecord1, testRecord2), actualRecords);
      assertEquals(2, readStats.getNumInserts());
      assertEquals(0, readStats.getNumDeletes());
    }
  }

  private static class TestRecord {
    private final String recordKey;

    public TestRecord(String recordKey) {
      this.recordKey = recordKey;
    }

    @Override
    public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      TestRecord that = (TestRecord) o;
      return Objects.equals(recordKey, that.recordKey);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(recordKey);
    }
  }
}
