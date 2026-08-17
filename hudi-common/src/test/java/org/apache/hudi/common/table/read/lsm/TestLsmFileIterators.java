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

package org.apache.hudi.common.table.read.lsm;

import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.engine.RecordContext;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.schema.HoodieSchemas;
import org.apache.hudi.common.table.log.InstantRange;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.table.read.DeleteContext;
import org.apache.hudi.common.table.read.FileGroupReaderSchemaHandler;
import org.apache.hudi.common.table.read.IteratorMode;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TestLsmFileIterators {

  @Test
  void testNativeDeleteRecordPreservesOrderingValue() {
    HoodieReaderContext<Map<String, Object>> readerContext = mock(HoodieReaderContext.class);
    RecordContext<Map<String, Object>> recordContext = mock(RecordContext.class);
    Map<String, Object> record = Collections.emptyMap();
    List<String> orderingFields = Collections.singletonList("ts");
    HoodieSchema deleteLogSchema = HoodieSchemas.createDeleteLogSchema(tableSchema(), orderingFields);

    when(readerContext.getRecordContext()).thenReturn(recordContext);
    when(recordContext.getValue(record, deleteLogSchema, HoodieRecord.RECORD_KEY_METADATA_FIELD)).thenReturn("key1");
    when(recordContext.getOrderingValue(eq(record), eq(deleteLogSchema), eq(orderingFields))).thenReturn(42L);

    BufferedRecord<Map<String, Object>> deleteRecord =
        LsmFileIterators.createNativeDeleteRecord(readerContext, record, deleteLogSchema, orderingFields);

    assertEquals("key1", deleteRecord.getRecordKey());
    assertEquals(42L, deleteRecord.getOrderingValue());
    assertTrue(deleteRecord.isDelete());
  }

  @Test
  void testCreateBaseFileIteratorUsesIteratorMode() throws Exception {
    HoodieSchema schema = HoodieSchema.createRecord("test_record", null, null, Collections.singletonList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING))));
    StoragePath baseFilePath = new StoragePath("/tmp/file1_1-0-1_001.parquet");
    HoodieBaseFile baseFile = mock(HoodieBaseFile.class);
    HoodieReaderContext<String> readerContext = mock(HoodieReaderContext.class);
    FileGroupReaderSchemaHandler<String> schemaHandler = mock(FileGroupReaderSchemaHandler.class);
    RecordContext<String> recordContext = mock(RecordContext.class);
    HoodieStorage storage = mock(HoodieStorage.class);
    ClosableIterator<String> fileIterator = ClosableIterator.wrap(
        Collections.singletonList("record1").iterator());

    when(baseFile.getBootstrapBaseFile()).thenReturn(Option.empty());
    when(baseFile.getStoragePath()).thenReturn(baseFilePath);
    when(readerContext.getSchemaHandler()).thenReturn(schemaHandler);
    when(readerContext.getRecordContext()).thenReturn(recordContext);
    when(readerContext.getInstantRange()).thenReturn(Option.of(mock(InstantRange.class)));
    when(readerContext.applyInstantRangeFilter(fileIterator)).thenReturn(fileIterator);
    when(readerContext.getIteratorMode()).thenReturn(IteratorMode.ENGINE_RECORD);
    when(readerContext.getFileRecordIterator(baseFilePath, 0, 10, schema, schema, storage)).thenReturn(fileIterator);
    when(schemaHandler.getRequiredSchema()).thenReturn(schema);
    when(schemaHandler.getTableSchema()).thenReturn(schema);
    when(recordContext.seal(schema, "record1")).thenReturn("record1");

    ClosableIterator<BufferedRecord<String>> iterator = LsmFileIterators.createBaseFileIterator(
        readerContext, storage, baseFile, 0, 10, Collections.emptyList(), true);

    assertTrue(iterator.hasNext());
    BufferedRecord<String> record = iterator.next();
    assertEquals("record1", record.getRecord());
    assertNull(record.getRecordKey());
    verify(readerContext).applyInstantRangeFilter(fileIterator);
    verify(recordContext, never()).getRecordKey("record1", schema);

    iterator.close();
  }

  @Test
  void testCreateBaseFileIterator() throws Exception {
    HoodieSchema schema = HoodieSchema.createRecord("test_record", null, null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING)),
        HoodieSchemaField.of("ts", HoodieSchema.create(HoodieSchemaType.LONG))));
    StoragePath baseFilePath = new StoragePath("/tmp/file1_1-0-1_001.parquet");
    HoodieBaseFile baseFile = mock(HoodieBaseFile.class);
    HoodieReaderContext<String> readerContext = mock(HoodieReaderContext.class);
    FileGroupReaderSchemaHandler<String> schemaHandler = mock(FileGroupReaderSchemaHandler.class);
    RecordContext<String> recordContext = mock(RecordContext.class);
    DeleteContext deleteContext = mock(DeleteContext.class);
    HoodieStorage storage = mock(HoodieStorage.class);
    ClosableIterator<String> fileIterator = spy(
        ClosableIterator.wrap(Collections.singletonList("record1").iterator()));

    when(baseFile.getBootstrapBaseFile()).thenReturn(Option.empty());
    when(baseFile.getStoragePath()).thenReturn(baseFilePath);
    when(readerContext.getSchemaHandler()).thenReturn(schemaHandler);
    when(readerContext.getRecordContext()).thenReturn(recordContext);
    when(readerContext.getInstantRange()).thenReturn(Option.empty());
    when(readerContext.getFileRecordIterator(baseFilePath, 0, 10, schema, schema, storage)).thenReturn(fileIterator);
    when(schemaHandler.getRequiredSchema()).thenReturn(schema);
    when(schemaHandler.getTableSchema()).thenReturn(schema);
    when(schemaHandler.getDeleteContext()).thenReturn(deleteContext);
    when(deleteContext.withReaderSchema(schema)).thenReturn(deleteContext);
    when(recordContext.seal(schema, "record1")).thenReturn("record1");
    when(recordContext.getRecordKey("record1", schema)).thenReturn("key1");

    ClosableIterator<BufferedRecord<String>> iterator = LsmFileIterators.createBaseFileIterator(
        readerContext, storage, baseFile, 0, 10, Collections.emptyList(), false);

    assertTrue(iterator.hasNext());
    assertTrue(iterator.hasNext());
    BufferedRecord<String> record = iterator.next();
    assertEquals("key1", record.getRecordKey());
    assertEquals("record1", record.getRecord());
    assertNull(record.getHoodieOperation());
    assertFalse(iterator.hasNext());
    verify(fileIterator, times(3)).hasNext();

    iterator.close();
    verify(fileIterator).close();
  }

  private static HoodieSchema tableSchema() {
    return HoodieSchema.createRecord("test_record", null, null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING)),
        HoodieSchemaField.of("ts", HoodieSchema.create(HoodieSchemaType.LONG))));
  }
}
