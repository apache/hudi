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

import org.apache.hudi.client.model.CommitTimeFlinkRecordMerger;
import org.apache.hudi.client.model.EventTimeFlinkRecordMerger;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.RecordContext;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.table.read.BufferedRecords;
import org.apache.hudi.util.AvroSchemaConverter;

import org.apache.avro.Schema;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

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
  private final RecordContext<RowData> recordContext = mock(RecordContext.class);

  @Test
  void testMergingWithNewRecordAsDelete() throws IOException {
    Schema schema = AvroSchemaConverter.convertToSchema(RECORD_ROWTYPE);
    HoodieKey key = new HoodieKey(RECORD_KEY, PARTITION);
    RowData oldRow = createRow(key, "001", "001_01", "file1", 1, "str_val1", 1L);
    BufferedRecord<RowData> oldRecord = BufferedRecords.fromEngineRecord(oldRow, schema, recordContext, 1L, RECORD_KEY, false);

    BufferedRecord<RowData> deleteRecord = BufferedRecords.fromEngineRecord(null, schema, recordContext, 2L, RECORD_KEY, true);

    EventTimeFlinkRecordMerger merger = new EventTimeFlinkRecordMerger();
    BufferedRecord<RowData> mergingResult = merger.merge(oldRecord, deleteRecord, recordContext, new TypedProperties());
    assertTrue(mergingResult.isDelete());
  }

  @Test
  void testMergingWithOldRecordAsDelete() throws IOException {
    Schema schema = AvroSchemaConverter.convertToSchema(RECORD_ROWTYPE);
    HoodieKey key = new HoodieKey(RECORD_KEY, PARTITION);
    RowData newRow = createRow(key, "001", "001_01", "file1", 1, "str_val1", 1L);
    BufferedRecord<RowData> newRecord = BufferedRecords.fromEngineRecord(newRow, schema, recordContext, 1L, RECORD_KEY, false);

    BufferedRecord<RowData> deleteRecord = BufferedRecords.fromEngineRecord(null, schema, recordContext, 2L, RECORD_KEY, true);

    EventTimeFlinkRecordMerger merger = new EventTimeFlinkRecordMerger();
    BufferedRecord<RowData> mergingResult = merger.merge(deleteRecord, newRecord, recordContext, new TypedProperties());
    assertTrue(mergingResult.isDelete());
  }

  @Test
  void testMergingWithOldRecordAccepted() throws IOException {
    Schema schema = AvroSchemaConverter.convertToSchema(RECORD_ROWTYPE);
    HoodieKey key = new HoodieKey(RECORD_KEY, PARTITION);
    RowData oldRow = createRow(key, "001", "001_01", "file1", 1, "str_val1", 3L);
    BufferedRecord<RowData> oldRecord = BufferedRecords.fromEngineRecord(oldRow, schema, recordContext, 3L, RECORD_KEY, false);

    RowData newRow = createRow(key, "001", "001_02", "file1", 2, "str_val2", 2L);
    BufferedRecord<RowData> newRecord = BufferedRecords.fromEngineRecord(newRow, schema, recordContext, 2L, RECORD_KEY, false);

    EventTimeFlinkRecordMerger merger = new EventTimeFlinkRecordMerger();
    BufferedRecord<RowData> mergingResult = merger.merge(oldRecord, newRecord, recordContext, new TypedProperties());
    assertEquals(oldRecord.getRecord(), mergingResult.getRecord());
  }

  @Test
  void testMergingWithNewRecordAccepted() throws IOException {
    Schema schema = AvroSchemaConverter.convertToSchema(RECORD_ROWTYPE);
    HoodieKey key = new HoodieKey(RECORD_KEY, PARTITION);
    RowData oldRow = createRow(key, "001", "001_01", "file1", 1, "str_val1", 1L);
    BufferedRecord<RowData> oldRecord = BufferedRecords.fromEngineRecord(oldRow, schema, recordContext, 1L, RECORD_KEY, false);

    RowData newRow = createRow(key, "001", "001_02", "file1", 2, "str_val2", 2L);
    BufferedRecord<RowData> newRecord = BufferedRecords.fromEngineRecord(newRow, schema, recordContext, 1L, RECORD_KEY, false);

    EventTimeFlinkRecordMerger merger = new EventTimeFlinkRecordMerger();
    BufferedRecord<RowData> mergingResult = merger.merge(oldRecord, newRecord, recordContext, new TypedProperties());
    assertEquals(newRecord.getRecord(), mergingResult.getRecord());
  }

  @Test
  void testMergingWithCommitTimeRecordMerger() throws IOException {
    Schema schema = AvroSchemaConverter.convertToSchema(RECORD_ROWTYPE);
    HoodieKey key = new HoodieKey(RECORD_KEY, PARTITION);
    RowData oldRow = createRow(key, "001", "001_01", "file1", 1, "str_val1", 2L);
    BufferedRecord<RowData> oldRecord = BufferedRecords.fromEngineRecord(oldRow, schema, recordContext, 2L, RECORD_KEY, false);

    RowData newRow = createRow(key, "001", "001_02", "file1", 2, "str_val2", 1L);
    BufferedRecord<RowData> newRecord = BufferedRecords.fromEngineRecord(newRow, schema, recordContext, 1L, RECORD_KEY, false);

    CommitTimeFlinkRecordMerger merger = new CommitTimeFlinkRecordMerger();
    BufferedRecord<RowData> mergingResult = merger.merge(oldRecord, newRecord, recordContext, new TypedProperties());
    assertEquals(newRecord.getRecord(), mergingResult.getRecord());
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
