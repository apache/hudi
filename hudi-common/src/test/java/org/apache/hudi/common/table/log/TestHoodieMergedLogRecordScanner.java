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

package org.apache.hudi.common.table.log;

import org.apache.hudi.common.engine.RecordContext;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodiePreCombineAvroRecordMerger;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.read.BufferedRecord;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestHoodieMergedLogRecordScanner {
  @TempDir
  Path tempDir;

  @Test
  void testSchemaCacheIsOwnedByEachScanner() throws Exception {
    HoodieSchema firstSchema = schema("first");
    HoodieSchema secondSchema = schema("second");
    List<RecordContext<IndexedRecord>> contexts = new ArrayList<>();
    HoodieRecordMerger merger = mock(HoodieRecordMerger.class);
    when(merger.getRecordType()).thenReturn(HoodieRecord.HoodieRecordType.AVRO);
    when(merger.merge(any(), any(), any(), any())).thenAnswer(invocation -> {
      BufferedRecord<IndexedRecord> older = invocation.getArgument(0);
      BufferedRecord<IndexedRecord> newer = invocation.getArgument(1);
      RecordContext<IndexedRecord> context = invocation.getArgument(2);
      contexts.add(context);
      assertEquals(older.getRecord().getSchema(), context.getSchemaFromBufferRecord(older).toAvroSchema());
      assertEquals(newer.getRecord().getSchema(), context.getSchemaFromBufferRecord(newer).toAvroSchema());
      return new HoodiePreCombineAvroRecordMerger().merge(older, newer, context, invocation.getArgument(3));
    });

    try (HoodieMergedLogRecordScanner first = scanner(firstSchema, merger);
         HoodieMergedLogRecordScanner second = scanner(secondSchema, merger)) {
      first.processNextRecord(record(firstSchema, "first-insert"));
      first.processNextRecord(record(firstSchema, "first-update"));
      second.processNextRecord(record(secondSchema, "second-insert"));
      second.processNextRecord(record(secondSchema, "second-update"));
      first.processNextRecord(record(firstSchema, "first-update-again"));

      assertEquals(3, contexts.size());
      RecordContext<IndexedRecord> firstContext = contexts.get(0);
      RecordContext<IndexedRecord> secondContext = contexts.get(1);
      assertNotSame(firstContext, secondContext, "Scanners must not share their schema cache");
      assertSame(firstContext, contexts.get(2), "A scanner must reuse its context across merges");
      assertEquals("first-update-again", ((IndexedRecord) first.getRecords().get("key").getData()).get(0).toString());
      assertEquals("second-update", ((IndexedRecord) second.getRecords().get("key").getData()).get(0).toString());
    }
  }

  private HoodieMergedLogRecordScanner scanner(HoodieSchema schema, HoodieRecordMerger merger) {
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    when(metaClient.getTableConfig()).thenReturn(new HoodieTableConfig());
    return HoodieMergedLogRecordScanner.newBuilder()
        .withTableMetaClient(metaClient)
        .withLogFilePaths(Collections.emptyList())
        .withReaderSchema(schema)
        .withMaxMemorySizeInBytes(1024L * 1024)
        .withSpillableMapBasePath(tempDir.toString())
        .withRecordMerger(merger)
        .withForceFullScan(false)
        .build();
  }

  private static HoodieSchema schema(String name) {
    return HoodieSchema.parse("{\"type\":\"record\",\"name\":\"" + name + "\",\"fields\":[{\"name\":\"value\",\"type\":\"string\"}]}");
  }

  private static HoodieAvroRecord<OverwriteWithLatestAvroPayload> record(HoodieSchema schema, String value) {
    GenericData.Record data = new GenericData.Record(schema.toAvroSchema());
    data.put("value", value);
    return new HoodieAvroRecord<>(new HoodieKey("key", "partition"), new OverwriteWithLatestAvroPayload(data, 0L));
  }
}
