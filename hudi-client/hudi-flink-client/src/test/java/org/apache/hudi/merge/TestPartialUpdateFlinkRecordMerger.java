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

import org.apache.hudi.client.model.PartialUpdateFlinkRecordMerger;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.RecordContext;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.table.read.BufferedRecords;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@code PartialUpdateFlinkRecordMerger}.
 */
public class TestPartialUpdateFlinkRecordMerger {
  private final RecordContext<RowData> recordContext = mock(RecordContext.class);
  private HoodieSchema schema;
  private HoodieSchema schemaWithoutMetaField;

  String jsonSchema = "{\n"
      + "  \"type\": \"record\",\n"
      + "  \"name\": \"partialRecord\", \"namespace\":\"org.apache.hudi\",\n"
      + "  \"fields\": [\n"
      + "    {\"name\": \"_hoodie_commit_time\", \"type\": [\"null\", \"string\"]},\n"
      + "    {\"name\": \"_hoodie_commit_seqno\", \"type\": [\"null\", \"string\"]},\n"
      + "    {\"name\": \"_hoodie_record_key\", \"type\": [\"null\", \"string\"]},\n"
      + "    {\"name\": \"_hoodie_partition_path\", \"type\": [\"null\", \"string\"]},\n"
      + "    {\"name\": \"_hoodie_file_name\", \"type\": [\"null\", \"string\"]},\n"
      + "    {\"name\": \"id\", \"type\": [\"null\", \"string\"]},\n"
      + "    {\"name\": \"partition\", \"type\": [\"null\", \"string\"]},\n"
      + "    {\"name\": \"ts\", \"type\": [\"null\", \"long\"]},\n"
      + "    {\"name\": \"city\", \"type\": [\"null\", \"string\"]},\n"
      + "    {\"name\": \"child\", \"type\": [\"null\", \"string\"]}\n"
      + "  ]\n"
      + "}";

  String jsonSchemaWithoutMetaField = "{\n"
      + "  \"type\": \"record\",\n"
      + "  \"name\": \"partialRecordNoMeta\", \"namespace\":\"org.apache.hudi\",\n"
      + "  \"fields\": [\n"
      + "    {\"name\": \"id\", \"type\": [\"null\", \"string\"]},\n"
      + "    {\"name\": \"partition\", \"type\": [\"null\", \"string\"]},\n"
      + "    {\"name\": \"ts\", \"type\": [\"null\", \"long\"]},\n"
      + "    {\"name\": \"city\", \"type\": [\"null\", \"string\"]},\n"
      + "    {\"name\": \"child\", \"type\": [\"null\", \"string\"]}\n"
      + "  ]\n"
      + "}";

  @BeforeEach
  public void setUp() throws Exception {
    schema = HoodieSchema.parse(jsonSchema);
    schemaWithoutMetaField = HoodieSchema.parse(jsonSchemaWithoutMetaField);
  }

  @Test
  public void testActiveRecords() throws IOException {
    when(recordContext.getSchemaFromBufferRecord(any())).thenReturn(schema);
    PartialUpdateFlinkRecordMerger recordMerger = new PartialUpdateFlinkRecordMerger();

    BufferedRecord<RowData> record1 = createRecord(new HoodieKey("1", "par1"), "001", "001_1", "file", 0L, "NY", "A");
    BufferedRecord<RowData> record2 = createRecord(new HoodieKey("1", "par1"), "001", "001_2", "file", 1L, null, "B");
    BufferedRecord<RowData> expected = createRecord(new HoodieKey("1", "par1"), "001", "001_2", "file", 1L, "NY", "B");
    BufferedRecord<RowData> mergingResult = recordMerger.merge(record1, record2, recordContext, new TypedProperties());
    assertEquals(mergingResult.getRecord(), expected.getRecord());
    mergingResult = recordMerger.merge(record2, record1, recordContext, new TypedProperties());
    assertEquals(expected.getRecord(), mergingResult.getRecord());

    // let record11's ordering val larger than record2, then record1 will overwrite record2 with its non-default field's value
    record1 = createRecord(new HoodieKey("1", "par1"), "001", "001_1", "file", 2L, "NY", "A");
    mergingResult = recordMerger.merge(record1, record2, recordContext, new TypedProperties());
    assertEquals(record1.getRecord(), mergingResult.getRecord());

    // let record1's ordering val equal to record2, then record2 will be considered to newer record
    record1 = createRecord(new HoodieKey("1", "par1"), "001", "001_1", "file", 1L, "NY", "A");
    expected = createRecord(new HoodieKey("1", "par1"), "001", "001_2", "file", 1L, "NY", "B");
    mergingResult = recordMerger.merge(record1, record2, recordContext, new TypedProperties());
    assertEquals(expected.getRecord(), mergingResult.getRecord());
  }

  @Test
  public void testDeleteRecord() throws IOException {
    PartialUpdateFlinkRecordMerger recordMerger = new PartialUpdateFlinkRecordMerger();

    BufferedRecord<RowData> record1 = createRecord(new HoodieKey("1", "par1"), "001", "001_1", "file", 0L, "NY", "A");
    BufferedRecord<RowData> deleteRecord = BufferedRecords.fromEngineRecord(null, schema, recordContext, 1L, "1", true);

    BufferedRecord<RowData> mergingResult = recordMerger.merge(record1, deleteRecord, recordContext, new TypedProperties());
    assertTrue(mergingResult.isDelete());

    mergingResult = recordMerger.merge(deleteRecord, record1, recordContext, new TypedProperties());
    assertTrue(mergingResult.isDelete());
  }

  /**
   * This test is to highlight the gotcha, where there are differences in result of the two queries on the same input data below:
   * <pre>
   *   Query A (No precombine):
   *
   *   INSERT INTO t1 VALUES (1, 'partition1', 1, false, NY0, ['A']);
   *   INSERT INTO t1 VALUES (1, 'partition1', 0, false, NY1, ['A']);
   *   INSERT INTO t1 VALUES (1, 'partition1', 2, false, NULL, ['A']);
   *
   *   Final output of Query A:
   *   (1, 'partition1', 2, false, NY0, ['A'])
   *
   *   Query B (preCombine invoked)
   *   INSERT INTO t1 VALUES (1, 'partition1', 1, false, NULL, ['A']);
   *   INSERT INTO t1 VALUES (1, 'partition1', 0, false, NY1, ['A']), (1, 'partition1', 2, false, NULL, ['A']);
   *
   *   Final output of Query B:
   *   (1, 'partition1', 2, false, NY1, ['A'])
   * </pre>
   */
  @Test
  public void testPartialUpdateGotchas() throws IOException {
    when(recordContext.getSchemaFromBufferRecord(any())).thenReturn(schema);
    PartialUpdateFlinkRecordMerger recordMerger = new PartialUpdateFlinkRecordMerger();
    BufferedRecord<RowData> record1 = createRecord(new HoodieKey("1", "par1"), "001", "001_1", "file", 1L, "NY0", "A");
    BufferedRecord<RowData> record2 = createRecord(new HoodieKey("1", "par1"), "001", "001_2", "file", 0L, "NY1", "B");
    BufferedRecord<RowData> record3 = createRecord(new HoodieKey("1", "par1"), "001", "001_3", "file", 2L, null, "A");

    // Merge(Merge(record1, record2), record3)
    BufferedRecord<RowData> expected = createRecord(new HoodieKey("1", "par1"), "001", "001_3", "file", 2L, "NY0", "A");
    BufferedRecord<RowData> mergingResult = recordMerger.merge(record1, record2, recordContext, new TypedProperties());
    mergingResult = recordMerger.merge(mergingResult, record3, recordContext, new TypedProperties());
    assertEquals(expected.getRecord(), mergingResult.getRecord());

    // Merge(record1, Merge(record2, record3))
    expected = createRecord(new HoodieKey("1", "par1"), "001", "001_3", "file", 2L, "NY1", "A");
    mergingResult = recordMerger.merge(record2, record3, recordContext, new TypedProperties());
    mergingResult = recordMerger.merge(mergingResult, record3, recordContext, new TypedProperties());
    assertEquals(expected.getRecord(), mergingResult.getRecord());
  }

  @Test
  public void testPartialUpdateWithSchemaDiscrepancy() throws IOException {
    PartialUpdateFlinkRecordMerger recordMerger = new PartialUpdateFlinkRecordMerger();
    BufferedRecord<RowData> record1 = createRecord(new HoodieKey("1", "par1"), "001", "001_1", "file", 1L, "NY0", "A");
    BufferedRecord<RowData> record2 = createRecordWithoutMetaField(new HoodieKey("1", "par1"),  2L, "NY1", "A");
    when(recordContext.getSchemaFromBufferRecord(record1)).thenReturn(schema);
    when(recordContext.getSchemaFromBufferRecord(record2)).thenReturn(schemaWithoutMetaField);
    BufferedRecord<RowData> mergingResult = recordMerger.merge(record1, record2, recordContext, new TypedProperties());
    assertEquals(record2.getRecord(), mergingResult.getRecord());

    record1 = createRecord(new HoodieKey("1", "par1"), "001", "001_1", "file", 1L, "NY0", "A");
    record2 = createRecordWithoutMetaField(new HoodieKey("1", "par1"),  2L, "NY1", null);
    when(recordContext.getSchemaFromBufferRecord(record1)).thenReturn(schema);
    when(recordContext.getSchemaFromBufferRecord(record2)).thenReturn(schemaWithoutMetaField);
    BufferedRecord<RowData> expected = createRecordWithoutMetaField(new HoodieKey("1", "par1"),  2L, "NY1", "A");
    mergingResult = recordMerger.merge(record1, record2, recordContext, new TypedProperties());
    assertEquals(expected.getRecord(), mergingResult.getRecord());
  }

  private BufferedRecord<RowData> createRecord(
      HoodieKey key, String commitTime, String seqNo, String filePath, long ts, String city, String child) {
    GenericRowData rowData = new GenericRowData(10);
    rowData.setField(0, StringData.fromString(commitTime));
    rowData.setField(1, StringData.fromString(seqNo));
    rowData.setField(2, StringData.fromString(key.getRecordKey()));
    rowData.setField(3, StringData.fromString(key.getPartitionPath()));
    rowData.setField(4, StringData.fromString(filePath));
    rowData.setField(5, StringData.fromString(key.getRecordKey()));
    rowData.setField(6, StringData.fromString(key.getPartitionPath()));
    rowData.setField(7, ts);
    rowData.setField(8, StringData.fromString(city));
    rowData.setField(9, StringData.fromString(child));
    return BufferedRecords.fromEngineRecord(rowData, schema, recordContext, ts, key.getRecordKey(), false);
  }

  private BufferedRecord<RowData> createRecordWithoutMetaField(
      HoodieKey key, long ts, String city, String child) {
    GenericRowData rowData = new GenericRowData(5);
    rowData.setField(0, StringData.fromString(key.getRecordKey()));
    rowData.setField(1, StringData.fromString(key.getPartitionPath()));
    rowData.setField(2, ts);
    rowData.setField(3, StringData.fromString(city));
    rowData.setField(4, StringData.fromString(child));
    return BufferedRecords.fromEngineRecord(rowData, schemaWithoutMetaField, recordContext, ts, key.getRecordKey(), false);
  }
}
