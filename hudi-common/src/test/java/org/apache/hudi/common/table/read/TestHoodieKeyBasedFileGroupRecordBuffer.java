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

import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.reader.HoodieAvroRecordTestMerger;
import org.apache.hudi.common.testutils.reader.HoodieTestReaderContext;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Map;

import static org.apache.hudi.common.config.RecordMergeMode.CUSTOM;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.AVRO_SCHEMA;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TestHoodieKeyBasedFileGroupRecordBuffer {
  private static final HoodieKey KEY = new HoodieKey("any_key", "any_partition");
  private HoodieBaseFileGroupRecordBuffer<IndexedRecord> buffer;
  @Mock
  private Pair<Option<IndexedRecord>, Map<String, Object>> existingRecordMetaDataPair;

  @BeforeEach
  void setUp() {
    HoodieFileGroupReaderSchemaHandler<IndexedRecord> schemaHandler =
        mock(HoodieFileGroupReaderSchemaHandler.class);
    HoodieReaderContext<IndexedRecord> readerContext =
        new HoodieTestReaderContext(Option.empty(), Option.empty());

    HoodieReaderContext<IndexedRecord> spyReaderContext = spy(readerContext);
    spyReaderContext.setSchemaHandler(schemaHandler);

    // This is the existing record ordering value: integer type 2.
    doReturn(2).when(spyReaderContext)
        .getOrderingValue(any(), any(), any(), any());

    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieRecordMerger recordMerger = new HoodieAvroRecordTestMerger();
    TypedProperties properties = new TypedProperties();
    properties.setProperty(
        HoodieCommonConfig.RECORD_MERGE_MODE.key(), CUSTOM.name());

    IndexedRecord existingRecord = new GenericData.Record(AVRO_SCHEMA);
    when(existingRecordMetaDataPair.getLeft()).thenReturn(Option.of(existingRecord));

    buffer = new HoodieKeyBasedFileGroupRecordBuffer<>(
        spyReaderContext,
        metaClient,
        Option.empty(),
        Option.empty(),
        recordMerger,
        properties);
  }

  @Test
  void testDoProcessNextDataRecord_withNaturalOrderingValue() {
    DeleteRecord record = DeleteRecord.create(KEY, 0);
    Option<DeleteRecord> mergedRecord =
        buffer.doProcessNextDeletedRecord(record, existingRecordMetaDataPair);
    assertTrue(mergedRecord.isPresent());
  }

  @Test
  void testDoProcessNextDataRecord_withLongZero() {
    DeleteRecord record = DeleteRecord.create(KEY, 0L);
    Option<DeleteRecord> mergedRecord =
        buffer.doProcessNextDeletedRecord(record, existingRecordMetaDataPair);
    assertTrue(mergedRecord.isPresent());
  }

  @Test
  void testDoProcessNextDataRecord_withIntegerOne() {
    DeleteRecord record = DeleteRecord.create(KEY, 1);
    Option<DeleteRecord> mergedRecord =
        buffer.doProcessNextDeletedRecord(record, existingRecordMetaDataPair);
    assertFalse(mergedRecord.isPresent());
  }

  @Test
  void testDoProcessNextDataRecord_withLongOne() {
    DeleteRecord record = DeleteRecord.create(KEY, 1L);
    Option<DeleteRecord> mergedRecord =
        buffer.doProcessNextDeletedRecord(record, existingRecordMetaDataPair);
    assertTrue(mergedRecord.isPresent());
  }

  @Test
  void testDoProcessNextDataRecord_withFloat() {
    DeleteRecord record = DeleteRecord.create(KEY, 1.0);
    Option<DeleteRecord> mergedRecord =
        buffer.doProcessNextDeletedRecord(record, existingRecordMetaDataPair);
    assertTrue(mergedRecord.isPresent());
  }

  @Test
  void testDoProcessNextDataRecord_withString() {
    DeleteRecord record = DeleteRecord.create(KEY, "1");
    Option<DeleteRecord> mergedRecord =
        buffer.doProcessNextDeletedRecord(record, existingRecordMetaDataPair);
    assertTrue(mergedRecord.isPresent());
  }
}
