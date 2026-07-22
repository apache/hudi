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

import org.apache.hudi.common.avro.HoodieAvroReaderContext;
import org.apache.hudi.common.config.HoodieMemoryConfig;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.schema.internal.InternalSchema;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.block.HoodieDataBlock;
import org.apache.hudi.common.table.log.block.HoodieDeleteBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.read.BufferedRecords;
import org.apache.hudi.common.table.read.DeleteContext;
import org.apache.hudi.common.table.read.FileGroupReaderSchemaHandler;
import org.apache.hudi.common.table.read.UpdateProcessor;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieKeyException;
import org.apache.hudi.storage.StorageConfiguration;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestPositionBasedFileGroupRecordBuffer {

  private static final HoodieSchema SCHEMA = HoodieSchema.createRecord("record", null, null, Arrays.asList(
      HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING)),
      HoodieSchemaField.of("ts", HoodieSchema.create(HoodieSchemaType.LONG))));

  @TempDir
  Path tempDir;

  @Test
  void testPositionHeaderValidationAndKeyFiltering() throws Exception {
    HoodieLogBlock block = mock(HoodieLogBlock.class);
    when(block.getBaseFileInstantTimeOfPositions()).thenReturn(null, "002", "001", "001");
    when(block.getRecordPositionList()).thenReturn(Collections.emptyList(), Arrays.asList(2L, 4L));

    assertNull(PositionBasedFileGroupRecordBuffer.extractRecordPositions(block, "001"));
    assertNull(PositionBasedFileGroupRecordBuffer.extractRecordPositions(block, "001"));
    assertNull(PositionBasedFileGroupRecordBuffer.extractRecordPositions(block, "001"));
    assertEquals(Arrays.asList(2L, 4L), PositionBasedFileGroupRecordBuffer.extractRecordPositions(block, "001"));

    PositionBasedFileGroupRecordBuffer<IndexedRecord> buffer = createBuffer();
    IndexedRecord a = record("alpha", 1);
    assertFalse(buffer.shouldSkip(a, true, Collections.emptySet(), SCHEMA));
    assertFalse(buffer.shouldSkip(a, true, Collections.singleton("alpha"), SCHEMA));
    assertTrue(buffer.shouldSkip(a, true, Collections.singleton("beta"), SCHEMA));
    assertFalse(buffer.shouldSkip(a, false, Collections.singleton("al"), SCHEMA));
    assertTrue(buffer.shouldSkip(a, false, Collections.singleton("be"), SCHEMA));
    assertThrows(HoodieKeyException.class, () -> buffer.shouldSkip(record("", 1), true,
        new HashSet<>(Collections.singletonList("alpha")), SCHEMA));
    assertEquals(HoodieFileGroupRecordBuffer.BufferType.POSITION_BASED_MERGE, buffer.getBufferType());
    buffer.close();
  }

  @Test
  void testBlockCardinalityGuardsAndContainsLogRecord() throws Exception {
    PositionBasedFileGroupRecordBuffer<IndexedRecord> buffer = createBuffer();
    HoodieDataBlock dataBlock = mock(HoodieDataBlock.class);
    when(dataBlock.getBaseFileInstantTimeOfPositions()).thenReturn("001");
    when(dataBlock.getRecordPositionList()).thenReturn(Collections.singletonList(0L));
    when(dataBlock.containsPartialUpdates()).thenReturn(false);
    when(dataBlock.getSchema()).thenReturn(SCHEMA);
    when(dataBlock.getEngineRecordIterator(buffer.readerContext)).thenReturn(ClosableIterator.wrap(Collections.emptyIterator()));
    assertThrows(HoodieException.class, () -> buffer.processDataBlock(dataBlock, Option.empty()));

    HoodieDeleteBlock deleteBlock = mock(HoodieDeleteBlock.class);
    when(deleteBlock.getBaseFileInstantTimeOfPositions()).thenReturn("001");
    when(deleteBlock.getRecordPositionList()).thenReturn(Collections.singletonList(0L));
    when(deleteBlock.getRecordsToDelete(buffer.readerContext)).thenReturn(Collections.emptyList());
    assertThrows(HoodieException.class, () -> buffer.processDeleteBlock(deleteBlock));

    IndexedRecord alpha = record("alpha", 1);
    buffer.processNextDataRecord(BufferedRecords.fromEngineRecord(
        alpha, SCHEMA, buffer.readerContext.getRecordContext(), Collections.emptyList(), false), 0L);
    assertTrue(buffer.containsLogRecord("alpha"));
    assertFalse(buffer.containsLogRecord("missing"));

    buffer.readerContext.setShouldMergeUseRecordPosition(false);
    HoodieDataBlock emptyBlock = mock(HoodieDataBlock.class);
    when(emptyBlock.getEngineRecordIterator(buffer.readerContext)).thenReturn(ClosableIterator.wrap(Collections.emptyIterator()));
    when(emptyBlock.getSchema()).thenReturn(SCHEMA);
    buffer.processDataBlock(emptyBlock, Option.empty());
    assertEquals(HoodieFileGroupRecordBuffer.BufferType.KEY_BASED_MERGE, buffer.getBufferType());
    buffer.close();
  }

  private PositionBasedFileGroupRecordBuffer<IndexedRecord> createBuffer() {
    HoodieTableConfig tableConfig = new HoodieTableConfig();
    tableConfig.setValue(HoodieTableConfig.RECORDKEY_FIELDS, "id");
    tableConfig.setValue(HoodieTableConfig.RECORD_MERGE_MODE, RecordMergeMode.COMMIT_TIME_ORDERING.name());
    tableConfig.setValue(HoodieTableConfig.BASE_FILE_FORMAT, HoodieFileFormat.PARQUET.name());
    tableConfig.setValue(HoodieTableConfig.POPULATE_META_FIELDS, "false");
    HoodieReaderContext<IndexedRecord> context = new HoodieAvroReaderContext(
        mock(StorageConfiguration.class), tableConfig, Option.empty(), Option.empty());
    context.initRecordMerger(new TypedProperties());
    context.setShouldMergeUseRecordPosition(true);
    FileGroupReaderSchemaHandler<IndexedRecord> schemaHandler = mock(FileGroupReaderSchemaHandler.class);
    when(schemaHandler.getRequiredSchema()).thenReturn(SCHEMA);
    when(schemaHandler.getSchemaForUpdates()).thenReturn(SCHEMA);
    when(schemaHandler.getInternalSchema()).thenReturn(InternalSchema.getEmptyInternalSchema());
    when(schemaHandler.getDeleteContext()).thenReturn(new DeleteContext(new TypedProperties(), SCHEMA));
    context.setSchemaHandler(schemaHandler);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    when(metaClient.getTableConfig()).thenReturn(tableConfig);
    TypedProperties props = new TypedProperties();
    props.setProperty(HoodieMemoryConfig.SPILLABLE_MAP_BASE_PATH.key(), tempDir.toString());
    return new PositionBasedFileGroupRecordBuffer<>(context, metaClient, RecordMergeMode.COMMIT_TIME_ORDERING,
        Option.empty(), "001", props, Collections.emptyList(), mock(UpdateProcessor.class));
  }

  private static IndexedRecord record(String key, long ts) {
    GenericData.Record record = new GenericData.Record(SCHEMA.toAvroSchema());
    record.put("id", key);
    record.put("ts", ts);
    return record;
  }
}
