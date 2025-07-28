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

package org.apache.hudi.io;

import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.LocalTaskContextSupplier;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.hudi.common.model.DefaultHoodieRecordPayload.METADATA_EVENT_TIME_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class TestHoodieWriteHandle {
  @Mock
  private HoodieTable mockHoodieTable;
  @Mock
  private HoodieTableMetaClient mockMetaClient;
  @Mock
  private HoodieTableConfig mockTableConfig;
  @Mock
  private HoodieRecordMerger mockRecordMerger;
  @Mock
  private HoodieWriteConfig mockWriteConfig;

  private DummyHoodieWriteHandle testWriteHandle;

  @BeforeEach
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    when(mockHoodieTable.getMetaClient()).thenReturn(mockMetaClient);
    when(mockMetaClient.getTableConfig()).thenReturn(mockTableConfig);
    when(mockWriteConfig.getRecordMerger()).thenReturn(mockRecordMerger);

    // Set up a basic schema for the write config
    String basicSchema = "{\"type\":\"record\",\"name\":\"test\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"}]}";
    when(mockWriteConfig.getWriteSchema()).thenReturn(basicSchema);
    when(mockWriteConfig.getProps()).thenReturn(new TypedProperties());
    when(mockWriteConfig.allowOperationMetadataField()).thenReturn(false);
    when(mockWriteConfig.getWriteStatusClassName()).thenReturn("org.apache.hudi.client.WriteStatus");
    when(mockWriteConfig.getWriteStatusFailureFraction()).thenReturn(0.0);
    when(mockHoodieTable.shouldTrackSuccessRecords()).thenReturn(true);
    when(mockHoodieTable.isMetadataTable()).thenReturn(false);
    when(mockHoodieTable.getConfig()).thenReturn(mockWriteConfig);
    when(mockTableConfig.getTableVersion()).thenReturn(org.apache.hudi.common.table.HoodieTableVersion.EIGHT);

    when(mockHoodieTable.getBaseFileExtension()).thenReturn(".parquet");
    
    TaskContextSupplier taskContextSupplier = new LocalTaskContextSupplier();
    testWriteHandle = new DummyHoodieWriteHandle(
        mockWriteConfig,
        "test_instant",
        "test_partition",
        "test_file_id",
        mockHoodieTable,
        taskContextSupplier,
        false);
  }

  @Test
  void testShouldTrackEventTimeWaterMarkerAvroRecordTypeWithEventTimeOrderingAndConfigEnabled() {
    // Setup: AVRO record type with event time ordering and config enabled
    when(mockRecordMerger.getRecordType()).thenReturn(HoodieRecord.HoodieRecordType.AVRO);
    when(mockTableConfig.getRecordMergeMode()).thenReturn(RecordMergeMode.EVENT_TIME_ORDERING);
    
    TypedProperties props = new TypedProperties();
    props.put("hoodie.write.track.event.time.watermark", "true");
    when(mockWriteConfig.getProps()).thenReturn(props);

    boolean result = testWriteHandle.testShouldTrackEventTimeWaterMarker(mockMetaClient, mockWriteConfig);

    assertTrue(result, "Should track event time watermark for AVRO records with event time ordering and config enabled");
  }

  @Test
  void testShouldTrackEventTimeWaterMarkerAvroRecordTypeWithEventTimeOrderingAndConfigDisabled() {
    // Setup: AVRO record type with event time ordering but config disabled
    when(mockRecordMerger.getRecordType()).thenReturn(HoodieRecord.HoodieRecordType.AVRO);
    when(mockTableConfig.getRecordMergeMode()).thenReturn(RecordMergeMode.EVENT_TIME_ORDERING);
    
    TypedProperties props = new TypedProperties();
    props.put("hoodie.write.track.event.time.watermark", "false");
    when(mockWriteConfig.getProps()).thenReturn(props);

    boolean result = testWriteHandle.testShouldTrackEventTimeWaterMarker(mockMetaClient, mockWriteConfig);

    assertFalse(result, "Should not track event time watermark when config is disabled");
  }

  @Test
  void testShouldTrackEventTimeWaterMarkerNonAvroRecordType() {
    // Setup: Non-AVRO record type
    when(mockRecordMerger.getRecordType()).thenReturn(HoodieRecord.HoodieRecordType.SPARK);

    boolean result = testWriteHandle.testShouldTrackEventTimeWaterMarker(mockMetaClient, mockWriteConfig);

    assertFalse(result, "Should not track event time watermark for non-AVRO record types");
  }

  @Test
  void testShouldTrackEventTimeWaterMarkerAvroRecordTypeWithCommitTimeOrdering() {
    // Setup: AVRO record type but with commit time ordering
    when(mockRecordMerger.getRecordType()).thenReturn(HoodieRecord.HoodieRecordType.AVRO);
    when(mockTableConfig.getRecordMergeMode()).thenReturn(RecordMergeMode.COMMIT_TIME_ORDERING);
    TypedProperties props = new TypedProperties();
    props.put("hoodie.write.track.event.time.watermark", "true");
    when(mockWriteConfig.getProps()).thenReturn(props);

    boolean result = testWriteHandle.testShouldTrackEventTimeWaterMarker(mockMetaClient, mockWriteConfig);

    assertFalse(result, "Should not track event time watermark when using commit time ordering");
  }

  @Test
  void testAppendEventTimeMetadataWithEventTimeField() {
    // Setup: Create a test record with event time field
    Schema schema = Schema.createRecord("test", null, null, false);
    schema.setFields(java.util.Arrays.asList(
        new Schema.Field("id", Schema.create(Schema.Type.STRING), null, null),
        new Schema.Field("event_time", Schema.create(Schema.Type.LONG), null, null)
    ));

    GenericRecord record = new GenericData.Record(schema);
    record.put("id", "test_id");
    record.put("event_time", 1234567890L);

    HoodieRecord hoodieRecord = new HoodieAvroIndexedRecord(null, record);
    
    // Setup event time field name
    testWriteHandle.setEventTimeFieldNameOpt(Option.of("event_time"));
    testWriteHandle.setKeepConsistentLogicalTimestamp(false);

    // Test with empty metadata
    Option<Map<String, String>> result =
        testWriteHandle.testAppendEventTimeMetadata(hoodieRecord, Option.empty(), schema, new Properties());

    assertTrue(result.isPresent(), "Should return metadata when event time is present");
    Map<String, String> metadata = result.get();
    assertEquals(
        "1234567890",
        metadata.get(METADATA_EVENT_TIME_KEY),
        "Event time should be correctly extracted");
  }

  @Test
  void testAppendEventTimeMetadataWithExistingMetadata() {
    // Setup: Create a test record with event time field
    Schema schema = Schema.createRecord("test", null, null, false);
    schema.setFields(java.util.Arrays.asList(
        new Schema.Field("id", Schema.create(Schema.Type.STRING), null, null),
        new Schema.Field("event_time", Schema.create(Schema.Type.LONG), null, null)
    ));

    GenericRecord record = new GenericData.Record(schema);
    record.put("id", "test_id");
    record.put("event_time", 1234567890L);

    HoodieRecord hoodieRecord = new HoodieAvroIndexedRecord(null, record);
    
    // Setup event time field name
    testWriteHandle.setEventTimeFieldNameOpt(Option.of("event_time"));
    testWriteHandle.setKeepConsistentLogicalTimestamp(false);

    // Test with existing metadata
    Map<String, String> existingMetadata = new HashMap<>();
    existingMetadata.put("existing_key", "existing_value");
    Option<Map<String, String>> existingMetadataOpt = Option.of(existingMetadata);

    Option<Map<String, String>> result =
        testWriteHandle.testAppendEventTimeMetadata(hoodieRecord, existingMetadataOpt, schema, new Properties());

    assertTrue(result.isPresent(), "Should return metadata when event time is present");
    Map<String, String> metadata = result.get();
    assertEquals(
        "1234567890",
        metadata.get(METADATA_EVENT_TIME_KEY),
        "Event time should be correctly extracted");
    assertEquals(
        "existing_value",
        metadata.get("existing_key"),
        "Existing metadata should be preserved");
  }

  @Test
  void testAppendEventTimeMetadataWithoutEventTimeField() {
    // Setup: Create a test record without event time field
    Schema schema = Schema.createRecord("test", null, null, false);
    schema.setFields(java.util.Arrays.asList(
        new Schema.Field("id", Schema.create(Schema.Type.STRING), null, null)
    ));

    GenericRecord record = new GenericData.Record(schema);
    record.put("id", "test_id");

    HoodieRecord hoodieRecord = new HoodieAvroIndexedRecord(null, record);
    
    // Setup event time field name
    testWriteHandle.setEventTimeFieldNameOpt(Option.of("event_time"));
    testWriteHandle.setKeepConsistentLogicalTimestamp(false);

    Option<Map<String, String>> result =
        testWriteHandle.testAppendEventTimeMetadata(hoodieRecord, Option.empty(), schema, new Properties());

    assertFalse(result.isPresent(), "Should return empty when event time field is not present");
  }

  @Test
  void testAppendEventTimeMetadataWithNullEventTimeValue() {
    // Setup: Create a test record with null event time value
    Schema schema = Schema.createRecord("test", null, null, false);
    schema.setFields(java.util.Arrays.asList(
        new Schema.Field("id", Schema.create(Schema.Type.STRING), null, null),
        new Schema.Field("event_time", Schema.create(Schema.Type.LONG), null, null)
    ));

    GenericRecord record = new GenericData.Record(schema);
    record.put("id", "test_id");
    record.put("event_time", null);

    HoodieRecord hoodieRecord = new HoodieAvroIndexedRecord(null, record);
    
    // Setup event time field name
    testWriteHandle.setEventTimeFieldNameOpt(Option.of("event_time"));
    testWriteHandle.setKeepConsistentLogicalTimestamp(false);

    Option<Map<String, String>> result =
        testWriteHandle.testAppendEventTimeMetadata(hoodieRecord, Option.empty(), schema, new Properties());

    assertFalse(result.isPresent(), "Should return empty when event time value is null");
  }

  @Test
  void testAppendEventTimeMetadataWithStringEventTime() {
    // Setup: Create a test record with string event time
    Schema schema = Schema.createRecord("test", null, null, false);
    schema.setFields(java.util.Arrays.asList(
        new Schema.Field("id", Schema.create(Schema.Type.STRING), null, null),
        new Schema.Field("event_time", Schema.create(Schema.Type.STRING), null, null)
    ));

    GenericRecord record = new GenericData.Record(schema);
    record.put("id", "test_id");
    record.put("event_time", "2023-01-01T00:00:00Z");

    HoodieRecord hoodieRecord = new HoodieAvroIndexedRecord(null, record);
    
    // Setup event time field name
    testWriteHandle.setEventTimeFieldNameOpt(Option.of("event_time"));
    testWriteHandle.setKeepConsistentLogicalTimestamp(false);

    Option<Map<String, String>> result =
        testWriteHandle.testAppendEventTimeMetadata(hoodieRecord, Option.empty(), schema, new Properties());

    assertTrue(result.isPresent(), "Should return metadata when event time is present");
    Map<String, String> metadata = result.get();
    assertEquals(
        "2023-01-01T00:00:00Z",
        metadata.get(METADATA_EVENT_TIME_KEY),
        "String event time should be correctly extracted");
  }

  @Test
  void testAppendEventTimeMetadataWithNestedEventTimeField() {
    // Setup: Create a test record with nested event time field
    Schema nestedSchema = Schema.createRecord("nested", null, null, false);
    nestedSchema.setFields(java.util.Arrays.asList(
        new Schema.Field("event_time", Schema.create(Schema.Type.LONG), null, null)
    ));

    Schema schema = Schema.createRecord("test", null, null, false);
    schema.setFields(java.util.Arrays.asList(
        new Schema.Field("id", Schema.create(Schema.Type.STRING), null, null),
        new Schema.Field("nested", nestedSchema, null, null)
    ));

    GenericRecord nestedRecord = new GenericData.Record(nestedSchema);
    nestedRecord.put("event_time", 1234567890L);

    GenericRecord record = new GenericData.Record(schema);
    record.put("id", "test_id");
    record.put("nested", nestedRecord);

    HoodieRecord hoodieRecord = new HoodieAvroIndexedRecord(null, record);
    // Setup event time field name for nested field
    testWriteHandle.setEventTimeFieldNameOpt(Option.of("nested.event_time"));
    testWriteHandle.setKeepConsistentLogicalTimestamp(false);

    Option<Map<String, String>> result =
        testWriteHandle.testAppendEventTimeMetadata(hoodieRecord, Option.empty(), schema, new Properties());

    assertTrue(result.isPresent(), "Should return metadata when nested event time is present");
    Map<String, String> metadata = result.get();
    assertEquals(
        "1234567890",
        metadata.get(METADATA_EVENT_TIME_KEY),
        "Nested event time should be correctly extracted");
  }

  // Test implementation class to access private methods
  private static class DummyHoodieWriteHandle extends HoodieWriteHandle<Object, Object, Object, Object> {
    public DummyHoodieWriteHandle(HoodieWriteConfig config,
                                  String instantTime,
                                  String partitionPath,
                                  String fileId,
                                  HoodieTable<Object, Object, Object, Object> hoodieTable,
                                  TaskContextSupplier taskContextSupplier,
                                  boolean preserveMetadata) {
      super(config, instantTime, partitionPath, fileId, hoodieTable, taskContextSupplier, preserveMetadata);
    }

    public boolean testShouldTrackEventTimeWaterMarker(HoodieTableMetaClient metaClient,
                                                       HoodieWriteConfig config) {
      return shouldTrackEventTimeWaterMarker(metaClient, config);
    }

    public Option<Map<String, String>> testAppendEventTimeMetadata(
        HoodieRecord record, Option<Map<String, String>> metadataOpt, Schema schema, Properties props) {
      return appendEventTimeMetadata(record, metadataOpt, schema, props);
    }

    public void setEventTimeFieldNameOpt(Option<String> eventTimeFieldNameOpt) {
      try {
        java.lang.reflect.Field field = HoodieWriteHandle.class.getDeclaredField("eventTimeFieldNameOpt");
        field.setAccessible(true);
        field.set(this, eventTimeFieldNameOpt);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    public void setKeepConsistentLogicalTimestamp(boolean keepConsistentLogicalTimestamp) {
      try {
        java.lang.reflect.Field field = HoodieWriteHandle.class.getDeclaredField("keepConsistentLogicalTimestamp");
        field.setAccessible(true);
        field.set(this, keepConsistentLogicalTimestamp);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public java.util.List<org.apache.hudi.client.WriteStatus> close() {
      return java.util.Collections.emptyList();
    }

    @Override
    public org.apache.hudi.common.model.IOType getIOType() {
      return org.apache.hudi.common.model.IOType.MERGE;
    }
  }
}
