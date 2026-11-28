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

package org.apache.hudi.common.model;

import org.apache.hudi.common.avro.HoodieAvroUtils;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.util.Option;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests {@link TestPartialUpdateAvroPayload}.
 */
public class TestPartialUpdateAvroPayload {
  private Schema schema;

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
      + "    {\"name\": \"_hoodie_is_deleted\", \"type\": \"boolean\", \"default\": false},\n"
      + "    {\"name\": \"city\", \"type\": [\"null\", \"string\"]},\n"
      + "    {\"name\": \"child\", \"type\": [\"null\", {\"type\": \"array\", \"items\": \"string\"}]}\n"
      + "  ]\n"
      + "}";

  @BeforeEach
  public void setUp() throws Exception {
    schema = new Schema.Parser().parse(jsonSchema);
  }

  @Test
  public void testActiveRecords() throws IOException {
    Properties properties = new Properties();
    properties.put(HoodiePayloadProps.PAYLOAD_ORDERING_FIELD_PROP_KEY, "ts");

    GenericRecord record1 = new GenericData.Record(schema);
    record1.put("id", "1");
    record1.put("partition", "partition1");
    record1.put("ts", 0L);
    record1.put("_hoodie_is_deleted", false);
    record1.put("city", "NY0");
    record1.put("child", Arrays.asList("A"));

    GenericRecord record2 = new GenericData.Record(schema);
    record2.put("id", "1");
    record2.put("partition", "partition1");
    record2.put("ts", 1L);
    record2.put("_hoodie_is_deleted", false);
    record2.put("city", null);
    record2.put("child", Arrays.asList("B"));

    GenericRecord record3 = new GenericData.Record(schema);
    record3.put("id", "1");
    record3.put("partition", "partition1");
    record3.put("ts", 2L);
    record3.put("_hoodie_is_deleted", false);
    record3.put("city", "NY0");
    record3.put("child", Arrays.asList("A"));

    GenericRecord record4 = new GenericData.Record(schema);
    record4.put("id", "1");
    record4.put("partition", "partition1");
    record4.put("ts", 1L);
    record4.put("_hoodie_is_deleted", false);
    record4.put("city", "NY0");
    record4.put("child", Arrays.asList("B"));

    // Test preCombine: since payload2's ordering val is larger, so payload2 will overwrite payload1 with its non-default field's value
    PartialUpdateAvroPayload payload1 = new PartialUpdateAvroPayload(record1, 0L);
    PartialUpdateAvroPayload payload2 = new PartialUpdateAvroPayload(record2, 1L);
    assertArrayEquals(payload1.preCombine(payload2, schema, properties).getRecordBytes(), new PartialUpdateAvroPayload(record4, 1L).getRecordBytes());
    assertArrayEquals(payload2.preCombine(payload1, schema, properties).getRecordBytes(), new PartialUpdateAvroPayload(record4, 1L).getRecordBytes());

    assertEquals(record1, payload1.getInsertValue(schema).get());
    assertEquals(record2, payload2.getInsertValue(schema).get());

    // Test combineAndGetUpdateValue: let payload1's ordering val larger than payload2, then payload1 will overwrite payload2 with its non-default field's value
    record1.put("ts", 2L);
    payload1 = new PartialUpdateAvroPayload(record1, 2L);
    assertEquals(payload1.combineAndGetUpdateValue(record2, schema, properties).get(), record3);
    // Test combineAndGetUpdateValue: let payload1's ordering val equal to  payload2, then payload2 will be considered to newer record
    record1.put("ts", 1L);
    assertEquals(payload2.combineAndGetUpdateValue(record1, schema, properties).get(), record4);

    // Test preCombine again: let payload1's ordering val larger than payload2
    record1.put("ts", 2L);
    payload1 = new PartialUpdateAvroPayload(record1, 2L);
    payload2 = new PartialUpdateAvroPayload(record2, 1L);
    assertArrayEquals(payload1.preCombine(payload2, schema, properties).getRecordBytes(), new PartialUpdateAvroPayload(record3, 2L).getRecordBytes());
    assertArrayEquals(payload2.preCombine(payload1, schema, properties).getRecordBytes(), new PartialUpdateAvroPayload(record3, 2L).getRecordBytes());
  }

  @Test
  public void testDeletedRecord() throws IOException {
    GenericRecord record1 = new GenericData.Record(schema);
    record1.put("id", "1");
    record1.put("partition", "partition0");
    record1.put("ts", 0L);
    record1.put("_hoodie_is_deleted", false);
    record1.put("city", "NY0");
    record1.put("child", Collections.emptyList());

    GenericRecord delRecord1 = new GenericData.Record(schema);
    delRecord1.put("id", "2");
    delRecord1.put("partition", "partition1");
    delRecord1.put("ts", 1L);
    delRecord1.put("_hoodie_is_deleted", true);
    delRecord1.put("city", "NY0");
    delRecord1.put("child", Collections.emptyList());

    GenericRecord record2 = new GenericData.Record(schema);
    record2.put("id", "1");
    record2.put("partition", "partition0");
    record2.put("ts", 2L);
    record2.put("_hoodie_is_deleted", false);
    record2.put("city", "NY0");
    record2.put("child", Collections.emptyList());

    PartialUpdateAvroPayload payload1 = new PartialUpdateAvroPayload(record1, 0L);
    PartialUpdateAvroPayload delPayload = new PartialUpdateAvroPayload(delRecord1, 1L);
    PartialUpdateAvroPayload payload2 = new PartialUpdateAvroPayload(record2, 2L);

    PartialUpdateAvroPayload mergedPayload = payload1.preCombine(delPayload, schema, new Properties());
    assertTrue(HoodieAvroUtils.bytesToAvro(mergedPayload.getRecordBytes(), schema).get("_hoodie_is_deleted").equals(true));
    assertArrayEquals(mergedPayload.getRecordBytes(), delPayload.getRecordBytes());

    mergedPayload = delPayload.preCombine(payload1, schema, new Properties());
    assertTrue(HoodieAvroUtils.bytesToAvro(mergedPayload.getRecordBytes(), schema).get("_hoodie_is_deleted").equals(true));
    assertArrayEquals(mergedPayload.getRecordBytes(), delPayload.getRecordBytes());

    mergedPayload = payload2.preCombine(delPayload, schema, new Properties());
    assertTrue(HoodieAvroUtils.bytesToAvro(mergedPayload.getRecordBytes(), schema).get("_hoodie_is_deleted").equals(false));
    assertArrayEquals(mergedPayload.getRecordBytes(), payload2.getRecordBytes());

    mergedPayload = delPayload.preCombine(payload2, schema, new Properties());
    assertTrue(HoodieAvroUtils.bytesToAvro(mergedPayload.getRecordBytes(), schema).get("_hoodie_is_deleted").equals(false));
    assertArrayEquals(mergedPayload.getRecordBytes(), payload2.getRecordBytes());

    assertEquals(record1, payload1.getInsertValue(schema).get());
    assertFalse(delPayload.getInsertValue(schema).isPresent());

    Properties properties = new Properties();
    properties.put(HoodiePayloadProps.PAYLOAD_ORDERING_FIELD_PROP_KEY, "ts");
    assertEquals(payload1.combineAndGetUpdateValue(delRecord1, schema, properties), Option.empty());
    assertFalse(delPayload.combineAndGetUpdateValue(record1, schema, properties).isPresent());
  }

  @Test
  public void testUseLatestRecordMetaValue() throws IOException {
    Properties properties = new Properties();
    properties.put(HoodiePayloadProps.PAYLOAD_ORDERING_FIELD_PROP_KEY, "ts");

    GenericRecord record1 = new GenericData.Record(schema);
    record1.put("_hoodie_commit_time", "20220915000000000");
    record1.put("_hoodie_commit_seqno", "20220915000000000_1_000");
    record1.put("id", "1");
    record1.put("partition", "partition1");
    record1.put("ts", 0L);
    record1.put("_hoodie_is_deleted", false);
    record1.put("city", "NY0");
    record1.put("child", Arrays.asList("A"));

    GenericRecord record2 = new GenericData.Record(schema);
    record2.put("_hoodie_commit_time", "20220915000000001");
    record2.put("_hoodie_commit_seqno", "20220915000000001_2_000");
    record2.put("id", "1");
    record2.put("partition", "partition1");
    record2.put("ts", 1L);
    record2.put("_hoodie_is_deleted", false);
    record2.put("city", null);
    record2.put("child", Arrays.asList("B"));

    PartialUpdateAvroPayload payload1 = new PartialUpdateAvroPayload(record1, 0L);
    PartialUpdateAvroPayload payload2 = new PartialUpdateAvroPayload(record2, 1L);

    // let payload1 as the latest one, then should use payload1's meta field's value as the result even its ordering val is smaller
    GenericRecord mergedRecord1 = (GenericRecord) payload1.preCombine(payload2, schema, properties).getInsertValue(schema, properties).get();
    assertEquals(mergedRecord1.get("_hoodie_commit_time").toString(), record1.get("_hoodie_commit_time").toString());
    assertEquals(mergedRecord1.get("_hoodie_commit_seqno").toString(), record1.get("_hoodie_commit_seqno").toString());

    // let payload2 as the latest one, then should use payload2's meta field's value as the result
    GenericRecord mergedRecord2 = (GenericRecord) payload2.preCombine(payload1, schema, properties).getInsertValue(schema, properties).get();
    assertEquals(mergedRecord2.get("_hoodie_commit_time").toString(), record2.get("_hoodie_commit_time").toString());
    assertEquals(mergedRecord2.get("_hoodie_commit_seqno").toString(), record2.get("_hoodie_commit_seqno").toString());
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
   *
   *
   * @throws IOException
   */
  @Test
  public void testPartialUpdateGotchas() throws IOException {
    Properties properties = new Properties();
    properties.put(HoodiePayloadProps.PAYLOAD_ORDERING_FIELD_PROP_KEY, "ts");

    GenericRecord record1 = new GenericData.Record(schema);
    record1.put("id", "1");
    record1.put("partition", "partition1");
    record1.put("ts", 1L);
    record1.put("_hoodie_is_deleted", false);
    record1.put("city", "NY0");
    record1.put("child", Arrays.asList("A"));

    GenericRecord record2 = new GenericData.Record(schema);
    record2.put("id", "1");
    record2.put("partition", "partition1");
    record2.put("ts", 0L);
    record2.put("_hoodie_is_deleted", false);
    record2.put("city", "NY1");
    record2.put("child", Arrays.asList("B"));

    GenericRecord record3 = new GenericData.Record(schema);
    record3.put("id", "1");
    record3.put("partition", "partition1");
    record3.put("ts", 2L);
    record3.put("_hoodie_is_deleted", false);
    record3.put("city", null);
    record3.put("child", Arrays.asList("A"));

    // define expected outputs
    GenericRecord pureCombineOutput = new GenericData.Record(schema);
    pureCombineOutput.put("id", "1");
    pureCombineOutput.put("partition", "partition1");
    pureCombineOutput.put("ts", 2L);
    pureCombineOutput.put("_hoodie_is_deleted", false);
    pureCombineOutput.put("city", "NY0");
    pureCombineOutput.put("child", Arrays.asList("A"));

    GenericRecord outputWithPreCombineUsed = new GenericData.Record(schema);
    outputWithPreCombineUsed.put("id", "1");
    outputWithPreCombineUsed.put("partition", "partition1");
    outputWithPreCombineUsed.put("ts", 2L);
    outputWithPreCombineUsed.put("_hoodie_is_deleted", false);
    outputWithPreCombineUsed.put("city", "NY1");
    outputWithPreCombineUsed.put("child", Arrays.asList("A"));

    PartialUpdateAvroPayload payload2 = new PartialUpdateAvroPayload(record2, 0L);
    PartialUpdateAvroPayload payload3 = new PartialUpdateAvroPayload(record3, 2L);

    // query A (no preCombine)
    IndexedRecord firstCombineOutput = payload2.combineAndGetUpdateValue(record1, schema, properties).get();
    IndexedRecord secondCombineOutput = payload3.combineAndGetUpdateValue(firstCombineOutput, schema, properties).get();
    assertEquals(pureCombineOutput, secondCombineOutput);

    // query B (preCombine invoked)
    PartialUpdateAvroPayload payloadAfterPreCombine = payload3.preCombine(payload2, schema, properties);
    IndexedRecord finalOutputWithPreCombine = payloadAfterPreCombine.combineAndGetUpdateValue(record1, schema, properties).get();
    assertEquals(outputWithPreCombineUsed, finalOutputWithPreCombine);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testNewerLiveRecordDoesNotInheritFromOlderDelete(boolean withMetaFields) throws IOException {
    Schema markerSchema = nullableDeleteMarkerSchema(withMetaFields);
    Properties properties = new Properties();
    properties.put(HoodiePayloadProps.PAYLOAD_ORDERING_FIELD_PROP_KEY, "ts");

    GenericRecord olderDelete = nullableDeleteMarkerRecord(markerSchema, 1L, true, "NY0");
    GenericRecord newerLive = nullableDeleteMarkerRecord(markerSchema, 2L, null, null);
    if (withMetaFields) {
      // the delete arrived later; the live row must still keep its own meta fields
      olderDelete.put(HoodieRecord.COMMIT_TIME_METADATA_FIELD, "002");
      newerLive.put(HoodieRecord.COMMIT_TIME_METADATA_FIELD, "001");
    }
    // nothing is filled from the delete: same as the delete and the live row landing in separate commits
    GenericRecord expected = new GenericData.Record((GenericData.Record) newerLive, true);
    PartialUpdateAvroPayload olderPayload = new PartialUpdateAvroPayload(olderDelete, 1L);
    PartialUpdateAvroPayload newerPayload = new PartialUpdateAvroPayload(newerLive, 2L);

    assertEquals(expected, newerPayload.combineAndGetUpdateValue(olderDelete, markerSchema, properties).get());
    for (PartialUpdateAvroPayload merged : Arrays.asList(
        newerPayload.preCombine(olderPayload, markerSchema, properties),
        olderPayload.preCombine(newerPayload, markerSchema, properties))) {
      assertFalse(merged.isDeleted(markerSchema, properties));
      assertEquals(expected, merged.getInsertValue(markerSchema).get());
      assertEquals(2L, merged.getOrderingVal());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testNewerDeleteWinsOverOlderLiveRecord(boolean withMetaFields) throws IOException {
    Schema markerSchema = nullableDeleteMarkerSchema(withMetaFields);
    Properties properties = new Properties();
    properties.put(HoodiePayloadProps.PAYLOAD_ORDERING_FIELD_PROP_KEY, "ts");

    GenericRecord olderLive = nullableDeleteMarkerRecord(markerSchema, 1L, null, "NY0");
    GenericRecord newerDelete = nullableDeleteMarkerRecord(markerSchema, 2L, true, null);
    PartialUpdateAvroPayload olderPayload = new PartialUpdateAvroPayload(olderLive, 1L);
    PartialUpdateAvroPayload newerPayload = new PartialUpdateAvroPayload(newerDelete, 2L);

    assertFalse(newerPayload.combineAndGetUpdateValue(olderLive, markerSchema, properties).isPresent());
    assertFalse(olderPayload.combineAndGetUpdateValue(newerDelete, markerSchema, properties).isPresent());
    for (PartialUpdateAvroPayload merged : Arrays.asList(
        newerPayload.preCombine(olderPayload, markerSchema, properties),
        olderPayload.preCombine(newerPayload, markerSchema, properties))) {
      assertTrue(merged.isDeleted(markerSchema, properties));
      assertFalse(merged.getInsertValue(markerSchema).isPresent());
      assertEquals(2L, merged.getOrderingVal());
    }
  }

  private static Schema nullableDeleteMarkerSchema(boolean withMetaFields) {
    Schema schema = new Schema.Parser().parse("{\"type\": \"record\", \"name\": \"markerRecord\", \"fields\": ["
        + "{\"name\": \"id\", \"type\": [\"null\", \"string\"], \"default\": null},"
        + "{\"name\": \"ts\", \"type\": [\"null\", \"long\"], \"default\": null},"
        + "{\"name\": \"_hoodie_is_deleted\", \"type\": [\"null\", \"boolean\"], \"default\": null},"
        + "{\"name\": \"city\", \"type\": [\"null\", \"string\"], \"default\": null}]}");
    return withMetaFields ? HoodieSchemaUtils.addMetadataFields(HoodieSchema.fromAvroSchema(schema)).toAvroSchema() : schema;
  }

  private static GenericRecord nullableDeleteMarkerRecord(Schema schema, long ts, Boolean isDeleted, String city) {
    GenericRecord record = new GenericData.Record(schema);
    record.put("id", "1");
    record.put("ts", ts);
    record.put(HoodieRecord.HOODIE_IS_DELETED_FIELD, isDeleted);
    record.put("city", city);
    return record;
  }
}
