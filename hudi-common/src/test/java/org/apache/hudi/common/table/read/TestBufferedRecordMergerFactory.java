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

import org.apache.hudi.avro.HoodieAvroReaderContext;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.HoodiePayloadProps;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.read.BufferedRecordMergerFactory.EventTimeBufferedRecordMerger;
import org.apache.hudi.common.util.Option;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestBufferedRecordMergerFactory {
  private static final String EVENT_TIME_FIELD = "event_time";
  private static final Schema AVRO_SCHEMA = new Schema.Parser().parse(
      "{\"type\":\"record\",\"name\":\"TestRecord\",\"fields\":["
          + "{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"event_time\",\"type\":\"long\"}]}"
  );

  @Test
  void testShouldKeepEventTimeMetadata() {
    TypedProperties props = new TypedProperties();
    assertFalse(EventTimeBufferedRecordMerger.shouldKeepEventTimeMetadata(props));
    props.setProperty("hoodie.write.track.event.time.watermark", "true");
    assertTrue(EventTimeBufferedRecordMerger.shouldKeepEventTimeMetadata(props));
    props.setProperty("hoodie.write.track.event.time.watermark", "false");
    assertFalse(EventTimeBufferedRecordMerger.shouldKeepEventTimeMetadata(props));
  }

  @Test
  void testShouldKeepConsistentLogicalTimestamp() {
    TypedProperties props = new TypedProperties();
    // Default is false
    assertFalse(EventTimeBufferedRecordMerger.shouldKeepConsistentLogicalTimestamp(props));
    props.setProperty("hoodie.datasource.write.keygenerator.consistent.logical.timestamp.enabled", "true");
    assertTrue(EventTimeBufferedRecordMerger.shouldKeepConsistentLogicalTimestamp(props));
    props.setProperty("hoodie.datasource.write.keygenerator.consistent.logical.timestamp.enabled", "false");
    assertFalse(EventTimeBufferedRecordMerger.shouldKeepConsistentLogicalTimestamp(props));
  }

  @Test
  void testGetEventTimeFieldName() {
    TypedProperties props = new TypedProperties();
    assertFalse(EventTimeBufferedRecordMerger.getEventTimeFieldName(props).isPresent());
    props.setProperty(HoodiePayloadProps.PAYLOAD_EVENT_TIME_FIELD_PROP_KEY, EVENT_TIME_FIELD);
    Option<String> fieldNameOpt = EventTimeBufferedRecordMerger.getEventTimeFieldName(props);
    assertTrue(fieldNameOpt.isPresent());
    assertEquals(EVENT_TIME_FIELD, fieldNameOpt.get());
  }

  @Test
  void testExtractEventTimePresent() {
    HoodieTableConfig tableConfig = new HoodieTableConfig();
    HoodieAvroReaderContext readerContext = new HoodieAvroReaderContext(
        null, tableConfig, Option.empty(), Option.empty());
    TypedProperties props = new TypedProperties();
    props.setProperty(HoodiePayloadProps.PAYLOAD_EVENT_TIME_FIELD_PROP_KEY, EVENT_TIME_FIELD);
    EventTimeBufferedRecordMerger<IndexedRecord> merger =
        new EventTimeBufferedRecordMerger<>(readerContext, props);

    GenericRecord record = new GenericData.Record(AVRO_SCHEMA);
    record.put("id", "abc");
    record.put(EVENT_TIME_FIELD, 123456789L);
    Option<Object> eventTimeOpt = merger.extractEventTime(record, AVRO_SCHEMA);
    assertTrue(eventTimeOpt.isPresent());
    assertEquals(123456789L, eventTimeOpt.get());
  }

  @Test
  void testExtractEventTimeAbsent() {
    HoodieTableConfig tableConfig = new HoodieTableConfig();
    HoodieAvroReaderContext readerContext = new HoodieAvroReaderContext(
        null, tableConfig, Option.empty(), Option.empty());
    TypedProperties props = new TypedProperties();
    props.setProperty(HoodiePayloadProps.PAYLOAD_EVENT_TIME_FIELD_PROP_KEY, EVENT_TIME_FIELD);
    EventTimeBufferedRecordMerger<IndexedRecord> merger =
        new EventTimeBufferedRecordMerger<>(readerContext, props);

    GenericRecord record2 = new GenericData.Record(new Schema.Parser().parse(
        "{\"type\":\"record\",\"name\":\"TestRecord2\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"}]}"
    ));
    record2.put("id", "def");
    Option<Object> eventTimeOpt2 = merger.extractEventTime(record2, record2.getSchema());
    assertFalse(eventTimeOpt2.isPresent());
  }

  @Test
  void testAttachEventTimeMetadataIfNeededTrackingEnabled() {
    HoodieReaderContext readerContext = mock(HoodieReaderContext.class);
    when(readerContext.getRecordKey(any(), any())).thenReturn("abc");
    when(readerContext.encodeAvroSchema(any())).thenReturn(1);
    when(readerContext.getOrderingValue(any(), any(), any())).thenReturn(987654321L);
    when(readerContext.getSchemaFromBufferRecord(any())).thenReturn(AVRO_SCHEMA);
    TypedProperties props = new TypedProperties();
    props.setProperty("hoodie.write.track.event.time.watermark", "true");
    props.setProperty(HoodiePayloadProps.PAYLOAD_EVENT_TIME_FIELD_PROP_KEY, EVENT_TIME_FIELD);
    EventTimeBufferedRecordMerger<IndexedRecord> merger = new EventTimeBufferedRecordMerger<>(readerContext, props);

    GenericRecord record = new GenericData.Record(AVRO_SCHEMA);
    record.put("id", "abc");
    record.put(EVENT_TIME_FIELD, 987654321L);
    when(readerContext.getValue(record, AVRO_SCHEMA, EVENT_TIME_FIELD)).thenReturn(987654321L);
    BufferedRecord<IndexedRecord> bufferedRecord = BufferedRecord.forRecordWithContext(
        record, AVRO_SCHEMA, readerContext, Option.of(EVENT_TIME_FIELD), false);
    assertTrue(bufferedRecord.getEventTime().isEmpty());
    merger.attachEventTimeMetadataIfNeeded(bufferedRecord);
    assertTrue(bufferedRecord.getEventTime().isPresent());
    assertEquals(987654321L, bufferedRecord.getEventTime().get());
  }

  @Test
  void testAttachEventTimeMetadataIfNeededTrackingDisabled() {
    HoodieReaderContext readerContext = mock(HoodieReaderContext.class);
    when(readerContext.getRecordKey(any(), any())).thenReturn("abc");
    when(readerContext.encodeAvroSchema(any())).thenReturn(1);
    when(readerContext.getOrderingValue(any(), any(), any())).thenReturn(987654321L);
    when(readerContext.getSchemaFromBufferRecord(any())).thenReturn(AVRO_SCHEMA);
    TypedProperties props2 = new TypedProperties();
    props2.setProperty("hoodie.write.track.event.time.watermark", "false");
    props2.setProperty(HoodiePayloadProps.PAYLOAD_EVENT_TIME_FIELD_PROP_KEY, EVENT_TIME_FIELD);
    EventTimeBufferedRecordMerger<IndexedRecord> merger2 = new EventTimeBufferedRecordMerger<>(readerContext, props2);

    GenericRecord record = new GenericData.Record(AVRO_SCHEMA);
    record.put("id", "abc");
    record.put(EVENT_TIME_FIELD, 987654321L);
    when(readerContext.getValue(record, AVRO_SCHEMA, EVENT_TIME_FIELD)).thenReturn(987654321L);
    BufferedRecord<IndexedRecord> bufferedRecord2 = BufferedRecord.forRecordWithContext(
        record, AVRO_SCHEMA, readerContext, Option.of(EVENT_TIME_FIELD), false);
    merger2.attachEventTimeMetadataIfNeeded(bufferedRecord2);
    assertTrue(bufferedRecord2.getEventTime().isEmpty());
  }
}
