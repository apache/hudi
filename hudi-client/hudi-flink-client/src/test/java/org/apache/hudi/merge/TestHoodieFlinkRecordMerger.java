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

package org.apache.hudi.merge;

import org.apache.hudi.client.model.EventTimeFlinkRecordMerger;
import org.apache.hudi.client.model.HoodieFlinkRecord;
import org.apache.hudi.client.model.CommitTimeFlinkRecordMerger;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieEmptyRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.util.AvroSchemaConverter;

import org.apache.avro.Schema;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@code HoodieFlinkRecordMerger}.
 */
public class TestHoodieFlinkRecordMerger {

  private static final RowType RECORD_ROWTYPE = new RowType(
      false,
      Arrays.asList(
          new RowType.RowField(HoodieRecord.COMMIT_TIME_METADATA_FIELD, DataTypes.STRING().getLogicalType()),
          new RowType.RowField(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD, DataTypes.STRING().getLogicalType()),
          new RowType.RowField(HoodieRecord.RECORD_KEY_METADATA_FIELD, DataTypes.STRING().getLogicalType()),
          new RowType.RowField(HoodieRecord.PARTITION_PATH_METADATA_FIELD, DataTypes.STRING().getLogicalType()),
          new RowType.RowField(HoodieRecord.FILENAME_METADATA_FIELD, DataTypes.STRING().getLogicalType()),
          new RowType.RowField("uuid", DataTypes.STRING().getLogicalType()),
          new RowType.RowField("partition", DataTypes.STRING().getLogicalType()),
          new RowType.RowField("f1_int", DataTypes.INT().getLogicalType()),
          new RowType.RowField("f2_string", DataTypes.STRING().getLogicalType()),
          new RowType.RowField("ts", DataTypes.BIGINT().getLogicalType())));

  private static final String RECORD_KEY = "key";
  private static final String PARTITION = "partition";

  /**
   * If the input records are not Flink HoodieRecord, it throws.
   */
  @Test
  void testMergerWithAvroRecord() {
    try (HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator(0L)) {
      List<HoodieRecord> records = dataGenerator.generateInserts("001", 2);
      EventTimeFlinkRecordMerger merger = new EventTimeFlinkRecordMerger();
      TypedProperties props = new TypedProperties();
      Schema recordSchema = new Schema.Parser().parse(TRIP_EXAMPLE_SCHEMA);
      assertThrows(
          IllegalArgumentException.class,
          () -> merger.merge(records.get(0), recordSchema, records.get(1), recordSchema, props));
    }
  }

  @Test
  void testMergingWithNewRecordAsDelete() throws IOException {
    Schema schema = AvroSchemaConverter.convertToSchema(RECORD_ROWTYPE);
    HoodieKey key = new HoodieKey(RECORD_KEY, PARTITION);
    RowData oldRow = createRow(key, "001", "001_01", "file1", 1, "str_val1", 1L);
    HoodieFlinkRecord oldRecord = new HoodieFlinkRecord(key, HoodieOperation.INSERT, 1L, oldRow);

    HoodieEmptyRecord deleteRecord = new HoodieEmptyRecord<>(key, HoodieOperation.DELETE, 2L, HoodieRecord.HoodieRecordType.FLINK);

    EventTimeFlinkRecordMerger merger = new EventTimeFlinkRecordMerger();
    Option<Pair<HoodieRecord, Schema>> mergingResult = merger.merge(oldRecord, schema, deleteRecord, schema, new TypedProperties());
    assertTrue(mergingResult.isPresent());
    assertTrue(mergingResult.get().getLeft().isDelete(schema, CollectionUtils.emptyProps()));
  }

  @Test
  void testMergingWithOldRecordAsDelete() throws IOException {
    Schema schema = AvroSchemaConverter.convertToSchema(RECORD_ROWTYPE);
    HoodieKey key = new HoodieKey(RECORD_KEY, PARTITION);
    RowData newRow = createRow(key, "001", "001_01", "file1", 1, "str_val1", 1L);
    HoodieFlinkRecord newRecord = new HoodieFlinkRecord(key, HoodieOperation.INSERT, 1L, newRow);

    HoodieEmptyRecord deleteRecord = new HoodieEmptyRecord<>(key, HoodieOperation.DELETE, 2L, HoodieRecord.HoodieRecordType.FLINK);

    EventTimeFlinkRecordMerger merger = new EventTimeFlinkRecordMerger();
    Option<Pair<HoodieRecord, Schema>> mergingResult = merger.merge(deleteRecord, schema, newRecord, schema, new TypedProperties());
    assertTrue(mergingResult.isPresent());
    assertTrue(mergingResult.get().getLeft().isDelete(schema, CollectionUtils.emptyProps()));
  }

  @Test
  void testMergingWithOldRecordAccepted() throws IOException {
    Schema schema = AvroSchemaConverter.convertToSchema(RECORD_ROWTYPE);
    HoodieKey key = new HoodieKey(RECORD_KEY, PARTITION);
    RowData oldRow = createRow(key, "001", "001_01", "file1", 1, "str_val1", 3L);
    HoodieFlinkRecord oldRecord = new HoodieFlinkRecord(key, HoodieOperation.INSERT, 3L, oldRow);

    RowData newRow = createRow(key, "001", "001_02", "file1", 2, "str_val2", 2L);
    HoodieFlinkRecord newRecord = new HoodieFlinkRecord(key, HoodieOperation.INSERT, 2L, newRow);

    EventTimeFlinkRecordMerger merger = new EventTimeFlinkRecordMerger();
    Option<Pair<HoodieRecord, Schema>> mergingResult = merger.merge(oldRecord, schema, newRecord, schema, new TypedProperties());
    assertTrue(mergingResult.isPresent());
    assertEquals(oldRecord.getData(), mergingResult.get().getLeft().getData());
  }

  @Test
  void testMergingWithNewRecordAccepted() throws IOException {
    Schema schema = AvroSchemaConverter.convertToSchema(RECORD_ROWTYPE);
    HoodieKey key = new HoodieKey(RECORD_KEY, PARTITION);
    RowData oldRow = createRow(key, "001", "001_01", "file1", 1, "str_val1", 1L);
    HoodieFlinkRecord oldRecord = new HoodieFlinkRecord(key, HoodieOperation.INSERT, 1L, oldRow);

    RowData newRow = createRow(key, "001", "001_02", "file1", 2, "str_val2", 2L);
    HoodieFlinkRecord newRecord = new HoodieFlinkRecord(key, HoodieOperation.INSERT, 2L, newRow);

    EventTimeFlinkRecordMerger merger = new EventTimeFlinkRecordMerger();
    Option<Pair<HoodieRecord, Schema>> mergingResult = merger.merge(oldRecord, schema, newRecord, schema, new TypedProperties());
    assertTrue(mergingResult.isPresent());
    assertEquals(newRecord.getData(), mergingResult.get().getLeft().getData());
  }

  @Test
  void testMergingWithCommitTimeRecordMerger() throws IOException {
    Schema schema = AvroSchemaConverter.convertToSchema(RECORD_ROWTYPE);
    HoodieKey key = new HoodieKey(RECORD_KEY, PARTITION);
    RowData oldRow = createRow(key, "001", "001_01", "file1", 1, "str_val1", 2L);
    HoodieFlinkRecord oldRecord = new HoodieFlinkRecord(key, HoodieOperation.INSERT, 2L, oldRow);

    RowData newRow = createRow(key, "001", "001_02", "file1", 2, "str_val2", 1L);
    HoodieFlinkRecord newRecord = new HoodieFlinkRecord(key, HoodieOperation.INSERT, 1L, newRow);

    CommitTimeFlinkRecordMerger merger = new CommitTimeFlinkRecordMerger();
    Option<Pair<HoodieRecord, Schema>> mergingResult = merger.merge(oldRecord, schema, newRecord, schema, new TypedProperties());
    assertTrue(mergingResult.isPresent());
    assertEquals(newRecord.getData(), mergingResult.get().getLeft().getData());
  }

  private RowData createRow(HoodieKey key, String commitTime, String seqNo, String filePath,
                            int intValue, String stringValue, long longValue) {
    GenericRowData rowData = new GenericRowData(RECORD_ROWTYPE.getFieldCount());
    rowData.setField(0, commitTime);
    rowData.setField(1, seqNo);
    rowData.setField(2, key.getRecordKey());
    rowData.setField(3, key.getPartitionPath());
    rowData.setField(4, filePath);
    rowData.setField(5, key.getRecordKey());
    rowData.setField(6, key.getPartitionPath());
    rowData.setField(7, intValue);
    rowData.setField(8, stringValue);
    rowData.setField(9, longValue);
    return rowData;
  }
}
