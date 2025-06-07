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

import org.apache.hudi.client.model.HoodieFlinkRecord;
import org.apache.hudi.client.model.PartialUpdateFlinkRecordMerger;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieEmptyRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.avro.Schema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@code PartialUpdateFlinkRecordMerger}.
 */
public class TestPartialUpdateFlinkRecordMerger {
  private Schema schema;
  private Schema schemaWithoutMetaField;

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
    schema = new Schema.Parser().parse(jsonSchema);
    schemaWithoutMetaField = new Schema.Parser().parse(jsonSchemaWithoutMetaField);
  }

  @Test
  public void testActiveRecords() throws IOException {
    PartialUpdateFlinkRecordMerger recordMerger = new PartialUpdateFlinkRecordMerger();

    HoodieFlinkRecord record1 = createRecord(new HoodieKey("1", "par1"), "001", "001_1", "file", 0L, "NY", "A");
    HoodieFlinkRecord record2 = createRecord(new HoodieKey("1", "par1"), "001", "001_2", "file", 1L, null, "B");
    HoodieFlinkRecord expected = createRecord(new HoodieKey("1", "par1"), "001", "001_2", "file", 1L, "NY", "B");
    Option<Pair<HoodieRecord, Schema>> mergingResult = recordMerger.merge(record1, schema, record2, schema, new TypedProperties());
    assertTrue(mergingResult.isPresent());
    assertEquals(mergingResult.get().getLeft().getData(), expected.getData());
    mergingResult = recordMerger.merge(record2, schema, record1, schema, new TypedProperties());
    assertTrue(mergingResult.isPresent());
    assertEquals(expected.getData(), mergingResult.get().getLeft().getData());

    // let record11's ordering val larger than record2, then record1 will overwrite record2 with its non-default field's value
    record1 = createRecord(new HoodieKey("1", "par1"), "001", "001_1", "file", 2L, "NY", "A");
    mergingResult = recordMerger.merge(record1, schema, record2, schema, new TypedProperties());
    assertTrue(mergingResult.isPresent());
    assertEquals(record1.getData(), mergingResult.get().getLeft().getData());

    // let record1's ordering val equal to record2, then record2 will be considered to newer record
    record1 = createRecord(new HoodieKey("1", "par1"), "001", "001_1", "file", 1L, "NY", "A");
    expected = createRecord(new HoodieKey("1", "par1"), "001", "001_2", "file", 1L, "NY", "B");
    mergingResult = recordMerger.merge(record1, schema, record2, schema, new TypedProperties());
    assertTrue(mergingResult.isPresent());
    assertEquals(expected.getData(), mergingResult.get().getLeft().getData());
  }

  @Test
  public void testDeleteRecord() throws IOException {
    PartialUpdateFlinkRecordMerger recordMerger = new PartialUpdateFlinkRecordMerger();

    HoodieFlinkRecord record1 = createRecord(new HoodieKey("1", "par1"), "001", "001_1", "file", 0L, "NY", "A");
    HoodieEmptyRecord deleteRecord = new HoodieEmptyRecord(new HoodieKey("1", "par1"), HoodieOperation.DELETE, 1L, HoodieRecord.HoodieRecordType.FLINK);

    Option<Pair<HoodieRecord, Schema>> mergingResult = recordMerger.merge(record1, schema, deleteRecord, schema, new TypedProperties());
    assertTrue(mergingResult.isPresent());
    assertTrue(mergingResult.get().getLeft().isDelete(schema, CollectionUtils.emptyProps()));

    mergingResult = recordMerger.merge(deleteRecord, schema, record1, schema, new TypedProperties());
    assertTrue(mergingResult.isPresent());
    assertTrue(mergingResult.get().getLeft().isDelete(schema, CollectionUtils.emptyProps()));
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
    PartialUpdateFlinkRecordMerger recordMerger = new PartialUpdateFlinkRecordMerger();
    HoodieFlinkRecord record1 = createRecord(new HoodieKey("1", "par1"), "001", "001_1", "file", 1L, "NY0", "A");
    HoodieFlinkRecord record2 = createRecord(new HoodieKey("1", "par1"), "001", "001_2", "file", 0L, "NY1", "B");
    HoodieFlinkRecord record3 = createRecord(new HoodieKey("1", "par1"), "001", "001_3", "file", 2L, null, "A");

    // Merge(Merge(record1, record2), record3)
    HoodieFlinkRecord expected = createRecord(new HoodieKey("1", "par1"), "001", "001_3", "file", 2L, "NY0", "A");
    Option<Pair<HoodieRecord, Schema>> mergingResult = recordMerger.merge(record1, schema, record2, schema, new TypedProperties());
    mergingResult = recordMerger.merge(mergingResult.get().getLeft(), schema, record3, schema, new TypedProperties());
    assertTrue(mergingResult.isPresent());
    assertEquals(expected.getData(), mergingResult.get().getLeft().getData());

    // Merge(record1, Merge(record2, record3))
    expected = createRecord(new HoodieKey("1", "par1"), "001", "001_3", "file", 2L, "NY1", "A");
    mergingResult = recordMerger.merge(record2, schema, record3, schema, new TypedProperties());
    mergingResult = recordMerger.merge(mergingResult.get().getLeft(), schema, record3, schema, new TypedProperties());
    assertTrue(mergingResult.isPresent());
    assertEquals(expected.getData(), mergingResult.get().getLeft().getData());
  }

  @Test
  public void testPartialUpdateWithSchemaDiscrepancy() throws IOException {
    PartialUpdateFlinkRecordMerger recordMerger = new PartialUpdateFlinkRecordMerger();
    HoodieFlinkRecord record1 = createRecord(new HoodieKey("1", "par1"), "001", "001_1", "file", 1L, "NY0", "A");
    HoodieFlinkRecord record2 = createRecordWithoutMetaField(new HoodieKey("1", "par1"),  2L, "NY1", "A");
    Option<Pair<HoodieRecord, Schema>> mergingResult = recordMerger.merge(record1, schema, record2, schemaWithoutMetaField, new TypedProperties());
    assertTrue(mergingResult.isPresent());
    assertEquals(record2.getData(), mergingResult.get().getLeft().getData());

    record1 = createRecord(new HoodieKey("1", "par1"), "001", "001_1", "file", 1L, "NY0", "A");
    record2 = createRecordWithoutMetaField(new HoodieKey("1", "par1"),  2L, "NY1", null);
    HoodieFlinkRecord expected = createRecordWithoutMetaField(new HoodieKey("1", "par1"),  2L, "NY1", "A");
    mergingResult = recordMerger.merge(record1, schema, record2, schemaWithoutMetaField, new TypedProperties());
    assertTrue(mergingResult.isPresent());
    assertEquals(expected.getData(), mergingResult.get().getLeft().getData());
  }

  private HoodieFlinkRecord createRecord(
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
    return new HoodieFlinkRecord(key, HoodieOperation.INSERT, ts, rowData);
  }

  private HoodieFlinkRecord createRecordWithoutMetaField(
      HoodieKey key, long ts, String city, String child) {
    GenericRowData rowData = new GenericRowData(5);
    rowData.setField(0, StringData.fromString(key.getRecordKey()));
    rowData.setField(1, StringData.fromString(key.getPartitionPath()));
    rowData.setField(2, ts);
    rowData.setField(3, StringData.fromString(city));
    rowData.setField(4, StringData.fromString(child));
    return new HoodieFlinkRecord(key, HoodieOperation.INSERT, ts, rowData);
  }
}
