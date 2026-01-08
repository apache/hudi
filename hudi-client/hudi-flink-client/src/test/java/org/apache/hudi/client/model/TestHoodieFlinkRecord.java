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
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.util.OrderingValues;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
    assertEquals(false, orderingValue.equals(OrderingValues.getDefault()));
  }

  @Test
  public void testGetRecordKeyWithMetadataField() {
    HoodieSchema schema = HoodieSchema.createRecord("test", null, null, Arrays.asList(
        HoodieSchemaField.of("_hoodie_record_key", HoodieSchema.create(HoodieSchemaType.STRING), null, null),
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING), null, null)
    ));

    GenericRowData rowData = GenericRowData.of(
        StringData.fromString("record-key-001"),
        StringData.fromString("id-001")
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
}
