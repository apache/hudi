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

package org.apache.hudi.common.table.read.buffer;

import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.PartialUpdateMode;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.table.read.HoodieReadStats;
import org.apache.hudi.common.table.read.UpdateProcessor;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.expression.Literal;
import org.apache.hudi.expression.Predicate;
import org.apache.hudi.expression.Predicates;
import org.apache.hudi.internal.schema.InternalSchema;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestReusableKeyBasedRecordBuffer {
  private final HoodieReaderContext<TestRecord> mockReaderContext = mock(HoodieReaderContext.class, RETURNS_DEEP_STUBS);
  private final HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);

  @Test
  void testRemainingLogEntryHandling() throws IOException {
    // After all base file records are read, only log entries that match the filter should be returned
    Map<Serializable, BufferedRecord<TestRecord>> preMergedLogRecords = new HashMap<>();
    preMergedLogRecords.put("1", new BufferedRecord<>("1", 10, new TestRecord("1", 1), 0, null));
    preMergedLogRecords.put("2", new BufferedRecord<>("2", 10, new TestRecord("2", 2), 0, null));
    preMergedLogRecords.put("3", new BufferedRecord<>("3", 10, new TestRecord("3", 3), 0, null));
    preMergedLogRecords.put("4", new BufferedRecord<>("4", 10, new TestRecord("4", 4), 0, null));
    HoodieReadStats readStats = new HoodieReadStats();
    UpdateProcessor<TestRecord> updateProcessor = UpdateProcessor.create(readStats, mockReaderContext, false, Option.empty());

    // Filter excludes key "4", so it should not be returned. It also includes key "5" which is not in the base file or log file.
    Predicate keyFilter = Predicates.in(null, Arrays.asList(Literal.from("1"), Literal.from("2"), Literal.from("3"), Literal.from("5")));
    when(mockReaderContext.getKeyFilterOpt()).thenReturn(Option.of(keyFilter));
    when(mockReaderContext.getSchemaHandler().getRequiredSchema()).thenReturn(HoodieTestDataGenerator.AVRO_SCHEMA);
    when(mockReaderContext.getSchemaHandler().getInternalSchema()).thenReturn(InternalSchema.getEmptyInternalSchema());
    when(mockReaderContext.getRecordContext().getDeleteRow(any())).thenAnswer(invocation -> {
      String recordKey = invocation.getArgument(0);
      return new TestRecord(recordKey, 0);
    });
    when(mockReaderContext.getRecordContext().getRecordKey(any(), any())).thenAnswer(invocation -> ((TestRecord) invocation.getArgument(0)).getRecordKey());
    when(mockReaderContext.getRecordContext().getOrderingValue(any(), any(), anyList())).thenAnswer(invocation -> {
      TestRecord record = invocation.getArgument(0);
      if (record.getRecordKey().equals("1")) {
        return 20; // simulate newer record in base file
      }
      // otherwise return an older value
      return 1;
    });
    when(mockReaderContext.getRecordContext().toBinaryRow(any(), any())).thenAnswer(invocation -> invocation.getArgument(1));
    when(mockReaderContext.getRecordContext().seal(any())).thenAnswer(invocation -> invocation.getArgument(0));

    ReusableKeyBasedRecordBuffer<TestRecord> buffer = new ReusableKeyBasedRecordBuffer<>(mockReaderContext, metaClient,
        RecordMergeMode.EVENT_TIME_ORDERING, PartialUpdateMode.NONE, new TypedProperties(), Collections.singletonList("value"), updateProcessor, preMergedLogRecords);

    List<TestRecord> baseFileRecords = Arrays.asList(new TestRecord("1", 10), new TestRecord("3", 30));
    buffer.setBaseFileIterator(ClosableIterator.wrap(baseFileRecords.iterator()));

    List<TestRecord> actualRecords = new ArrayList<>();
    while (buffer.hasNext()) {
      actualRecords.add(buffer.next().getRecord());
    }
    assertEquals(Arrays.asList(new TestRecord("1", 10), new TestRecord("3", 3), new TestRecord("2", 2)), actualRecords);
  }
}
