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

package org.apache.hudi.client.model;

import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.OrderingValues;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link HoodieFlinkRecord}.
 */
public class TestHoodieFlinkRecord {

  @Test
  public void testGetOrderingValueWithExistingField() {
    HoodieSchema schema = HoodieSchema.createRecord("test", null, null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING), null, null),
        HoodieSchemaField.of("timestamp", HoodieSchema.create(HoodieSchemaType.LONG), null, null)
    ));

    GenericRowData rowData = GenericRowData.of(
        StringData.fromString("id-001"),
        1000L
    );

    HoodieFlinkRecord record = new HoodieFlinkRecord(
        new HoodieKey("id-001", "partition-1"),
        HoodieOperation.INSERT,
        rowData
    );

    Properties props = new Properties();
    String[] orderingFields = new String[]{"timestamp"};

    Comparable<?> orderingValue = record.getOrderingValue(schema, props, orderingFields);
    assertEquals(1000L, orderingValue);
  }

  @Test
  public void testGetOrderingValueWithNonExistentField() {
    HoodieSchema schema = HoodieSchema.createRecord("test", null, null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING), null, null)
    ));

    GenericRowData rowData = GenericRowData.of(StringData.fromString("id-001"));

    HoodieFlinkRecord record = new HoodieFlinkRecord(
        new HoodieKey("id-001", "partition-1"),
        HoodieOperation.INSERT,
        rowData
    );

    Properties props = new Properties();
    String[] orderingFields = new String[]{"non_existent_field"};

    Comparable<?> orderingValue = record.getOrderingValue(schema, props, orderingFields);
    assertEquals(OrderingValues.getDefault(), orderingValue);
  }

  @Test
  public void testGetOrderingValueAsJavaWithExistingField() {
    HoodieSchema schema = HoodieSchema.createRecord("test", null, null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING), null, null),
        HoodieSchemaField.of("timestamp", HoodieSchema.create(HoodieSchemaType.LONG), null, null)
    ));

    GenericRowData rowData = GenericRowData.of(
        StringData.fromString("id-001"),
        2000L
    );

    HoodieFlinkRecord record = new HoodieFlinkRecord(
        new HoodieKey("id-001", "partition-1"),
        HoodieOperation.INSERT,
        rowData
    );

    Properties props = new Properties();
    String[] orderingFields = new String[]{"timestamp"};

    Comparable<?> orderingValue = record.getOrderingValueAsJava(schema, props, orderingFields);
    assertEquals(2000L, orderingValue);
  }

  @Test
  public void testGetOrderingValueAsJavaWithNonExistentField() {
    HoodieSchema schema = HoodieSchema.createRecord("test", null, null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING), null, null)
    ));

    GenericRowData rowData = GenericRowData.of(StringData.fromString("id-001"));

    HoodieFlinkRecord record = new HoodieFlinkRecord(
        new HoodieKey("id-001", "partition-1"),
        HoodieOperation.INSERT,
        rowData
    );

    Properties props = new Properties();
    String[] orderingFields = new String[]{"non_existent_field"};

    Comparable<?> orderingValue = record.getOrderingValueAsJava(schema, props, orderingFields);
    assertEquals(OrderingValues.getDefault(), orderingValue);
  }

  @Test
  public void testGetOrderingValueWithNullOrderingFields() {
    HoodieSchema schema = HoodieSchema.createRecord("test", null, null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING), null, null)
    ));

    GenericRowData rowData = GenericRowData.of(StringData.fromString("id-001"));

    HoodieFlinkRecord record = new HoodieFlinkRecord(
        new HoodieKey("id-001", "partition-1"),
        HoodieOperation.INSERT,
        rowData
    );

    Properties props = new Properties();

    Comparable<?> orderingValue = record.getOrderingValue(schema, props, null);
    assertEquals(OrderingValues.getDefault(), orderingValue);
  }

  @Test
  public void testGetOrderingValueWithMultipleFields() {
    HoodieSchema schema = HoodieSchema.createRecord("test", null, null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING), null, null),
        HoodieSchemaField.of("timestamp", HoodieSchema.create(HoodieSchemaType.LONG), null, null),
        HoodieSchemaField.of("sequence", HoodieSchema.create(HoodieSchemaType.INT), null, null)
    ));

    GenericRowData rowData = GenericRowData.of(
        StringData.fromString("id-001"),
        3000L,
        123
    );

    HoodieFlinkRecord record = new HoodieFlinkRecord(
        new HoodieKey("id-001", "partition-1"),
        HoodieOperation.INSERT,
        rowData
    );

    Properties props = new Properties();
    String[] orderingFields = new String[]{"timestamp", "sequence"};

    Comparable<?> orderingValue = record.getOrderingValue(schema, props, orderingFields);
    assertNotEquals(OrderingValues.getDefault(), orderingValue);
  }

  @Test
  public void testGetRecordKeyWithMetadataField() {
    HoodieSchema schema = HoodieSchema.createRecord("test", null, null, Arrays.asList(
        HoodieSchemaField.of("_hoodie_commit_time", HoodieSchema.create(HoodieSchemaType.STRING), null, null),
        HoodieSchemaField.of("_hoodie_commit_seqno", HoodieSchema.create(HoodieSchemaType.STRING), null, null),
        HoodieSchemaField.of("_hoodie_record_key", HoodieSchema.create(HoodieSchemaType.STRING), null, null),
        HoodieSchemaField.of("_hoodie_partition_path", HoodieSchema.create(HoodieSchemaType.STRING), null, null),
        HoodieSchemaField.of("_hoodie_file_name", HoodieSchema.create(HoodieSchemaType.STRING), null, null),
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING), null, null)
    ));

    GenericRowData rowData = GenericRowData.of(
        StringData.fromString("20240101000000"),  // commit_time
        StringData.fromString("seq-1"),             // commit_seqno
        StringData.fromString("record-key-001"),    // record_key at position 2
        StringData.fromString("partition-1"),       // partition_path
        StringData.fromString("file-1"),            // file_name
        StringData.fromString("id-001")             // id
    );

    HoodieFlinkRecord record = new HoodieFlinkRecord(rowData);

    String recordKey = record.getRecordKey(schema, org.apache.hudi.common.util.Option.empty());
    assertEquals("record-key-001", recordKey);
  }

  @Test
  public void testUpdateMetaFieldWithOperationField() {
    HoodieSchema schema = HoodieSchema.createRecord("test", null, null, Arrays.asList(
        HoodieSchemaField.of("_hoodie_commit_time", HoodieSchema.create(HoodieSchemaType.STRING), null, null),
        HoodieSchemaField.of("_hoodie_commit_seqno", HoodieSchema.create(HoodieSchemaType.STRING), null, null),
        HoodieSchemaField.of("_hoodie_record_key", HoodieSchema.create(HoodieSchemaType.STRING), null, null),
        HoodieSchemaField.of("_hoodie_partition_path", HoodieSchema.create(HoodieSchemaType.STRING), null, null),
        HoodieSchemaField.of("_hoodie_file_name", HoodieSchema.create(HoodieSchemaType.STRING), null, null),
        HoodieSchemaField.of("_hoodie_operation", HoodieSchema.create(HoodieSchemaType.STRING), null, null),
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING), null, null)
    ));

    GenericRowData rowData = GenericRowData.of(
        StringData.fromString("20240101000000"),
        StringData.fromString("seq-1"),
        StringData.fromString("id-001"),
        StringData.fromString("partition-1"),
        StringData.fromString("file-1"),
        StringData.fromString("I"),
        StringData.fromString("id-001")
    );

    HoodieFlinkRecord record = new HoodieFlinkRecord(
        new HoodieKey("id-001", "partition-1"),
        HoodieOperation.INSERT,
        rowData
    );

    HoodieFlinkRecord updatedRecord = (HoodieFlinkRecord) record.updateMetaField(schema, 0, "20240101000001");
    assertEquals(HoodieOperation.INSERT, updatedRecord.getOperation());
  }

  @Test
  public void testUpdateMetaFieldWithoutOperationField() {
    HoodieSchema schema = HoodieSchema.createRecord("test", null, null, Arrays.asList(
        HoodieSchemaField.of("_hoodie_commit_time", HoodieSchema.create(HoodieSchemaType.STRING), null, null),
        HoodieSchemaField.of("_hoodie_commit_seqno", HoodieSchema.create(HoodieSchemaType.STRING), null, null),
        HoodieSchemaField.of("_hoodie_record_key", HoodieSchema.create(HoodieSchemaType.STRING), null, null),
        HoodieSchemaField.of("_hoodie_partition_path", HoodieSchema.create(HoodieSchemaType.STRING), null, null),
        HoodieSchemaField.of("_hoodie_file_name", HoodieSchema.create(HoodieSchemaType.STRING), null, null),
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING), null, null)
    ));

    GenericRowData rowData = GenericRowData.of(
        StringData.fromString("20240101000000"),
        StringData.fromString("seq-1"),
        StringData.fromString("id-001"),
        StringData.fromString("partition-1"),
        StringData.fromString("file-1"),
        StringData.fromString("id-001")
    );

    HoodieFlinkRecord record = new HoodieFlinkRecord(
        new HoodieKey("id-001", "partition-1"),
        HoodieOperation.INSERT,
        rowData
    );

    HoodieFlinkRecord updatedRecord = (HoodieFlinkRecord) record.updateMetaField(schema, 0, "20240101000001");
    assertEquals(HoodieOperation.INSERT, updatedRecord.getOperation());
  }

  @Test
  public void testRecordContract() {
    HoodieKey key = new HoodieKey("id-001", "partition-1");
    GenericRowData row = GenericRowData.of(StringData.fromString("id-001"));
    HoodieFlinkRecord record = new HoodieFlinkRecord(
        key, HoodieOperation.INSERT, 100L, row, true);

    assertEquals(HoodieRecord.HoodieRecordType.FLINK, record.getRecordType());
    assertEquals(key, record.newInstance().getKey());
    assertEquals("new-key", record.newInstance(
        new HoodieKey("new-key", "p"), HoodieOperation.UPDATE_AFTER).getRecordKey());
    assertEquals("another-key", record.newInstance(
        new HoodieKey("another-key", "p")).getRecordKey());
    assertFalse(record.shouldIgnore(null, new Properties()));
    assertSame(record, record.copy());
    assertTrue(record.getMetadata().isEmpty());

    HoodieFlinkRecord empty = new HoodieFlinkRecord(
        key, HoodieOperation.INSERT, (RowData) null);
    assertTrue(empty.checkIsDelete(null, new Properties()));
  }

  @Test
  public void testConvertColumnValueForLogicalType() {
    TimestampData timestamp = TimestampData.fromInstant(
        Instant.parse("2025-02-03T04:05:06.123456Z"));
    HoodieFlinkRecord record = new HoodieFlinkRecord(GenericRowData.of(timestamp));

    assertNull(record.convertColumnValueForLogicalType(
        HoodieSchema.create(HoodieSchemaType.STRING), null, true));
    assertEquals(LocalDate.ofEpochDay(2), record.convertColumnValueForLogicalType(
        HoodieSchema.createDate(), 2, true));

    HoodieSchema millisSchema = HoodieSchema.createTimestampMillis();
    HoodieSchema millisRecordSchema = HoodieSchema.createRecord(
        "millis_record", null, null,
        Arrays.asList(HoodieSchemaField.of("event_time", millisSchema)));
    Object millisValue = record.getColumnValueAsJava(
        millisRecordSchema, "event_time", new Properties());
    assertEquals(timestamp.getMillisecond(), millisValue);
    assertEquals(timestamp.getMillisecond(), record.convertColumnValueForLogicalType(
        millisSchema, millisValue, true));

    HoodieSchema microsSchema = HoodieSchema.createTimestampMicros();
    HoodieSchema microsRecordSchema = HoodieSchema.createRecord(
        "micros_record", null, null,
        Arrays.asList(HoodieSchemaField.of("event_time", microsSchema)));
    Object microsValue = record.getColumnValueAsJava(
        microsRecordSchema, "event_time", new Properties());
    long expectedMicros = timestamp.toInstant().getEpochSecond() * 1_000_000
        + timestamp.toInstant().getNano() / 1_000;
    assertEquals(expectedMicros, microsValue);
    assertEquals(timestamp.getMillisecond(), record.convertColumnValueForLogicalType(
        microsSchema, microsValue, true));

    assertEquals(new BigDecimal("12.34"), record.convertColumnValueForLogicalType(
        HoodieSchema.createDecimal("decimal", null, null, 10, 2, 5),
        DecimalData.fromBigDecimal(new BigDecimal("12.34"), 10, 2), true));
    assertSame(millisValue, record.convertColumnValueForLogicalType(
        millisSchema, millisValue, false));
  }

  @Test
  public void testUnsupportedOperations() {
    HoodieKey key = new HoodieKey("id-001", "partition-1");
    GenericRowData row = GenericRowData.of(StringData.fromString("id-001"));
    HoodieFlinkRecord record = new HoodieFlinkRecord(
        key, HoodieOperation.INSERT, 100L, row, true);
    assertThrows(UnsupportedOperationException.class,
        () -> record.writeRecordPayload(row, null, null));
    assertThrows(UnsupportedOperationException.class,
        () -> record.readRecordPayload(null, null));
    assertThrows(UnsupportedOperationException.class,
        () -> record.getColumnValues(null, null, false));
    assertThrows(UnsupportedOperationException.class,
        () -> record.joinWith(record, null));
    assertThrows(UnsupportedOperationException.class,
        () -> record.wrapIntoHoodieRecordPayloadWithParams(
            null, null, Option.empty(), false, Option.empty(), false, Option.empty()));
    assertThrows(UnsupportedOperationException.class,
        () -> record.wrapIntoHoodieRecordPayloadWithKeyGen(null, null, Option.empty()));
    assertThrows(UnsupportedOperationException.class,
        () -> record.truncateRecordKey(null, null, null));
  }

  @Test
  public void testKeyLookupAndAvroMaterialization() {
    HoodieSchema schema = HoodieSchema.createRecord("test", null, null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING)),
        HoodieSchemaField.of("value", HoodieSchema.create(HoodieSchemaType.INT))));
    HoodieFlinkRecord record = new HoodieFlinkRecord(GenericRowData.of(
        StringData.fromString("id-001"), 42));

    assertEquals("id-001", record.getRecordKey(schema, "id"));
    // The second lookup exercises the cached record-key path.
    assertEquals("id-001", record.getRecordKey(schema, "id"));
    assertEquals("id-001", record.toIndexedRecord(schema, new Properties())
        .get().getData().get(0).toString());
    assertTrue(record.getAvroBytes(schema, new Properties()).size() > 0);
    assertEquals(OrderingValues.getDefault(),
        record.getOrderingValueAsJava(schema, new Properties(), null));
  }
}
