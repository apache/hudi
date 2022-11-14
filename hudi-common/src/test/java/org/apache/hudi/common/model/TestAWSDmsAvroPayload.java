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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class TestAWSDmsAvroPayload {

  private static final String AVRO_SCHEMA_STRING = "{\"type\": \"record\","
      + "\"name\": \"events\"," + "\"fields\": [ "
      + "{\"name\": \"field1\", \"type\" : \"int\"},"
      + "{\"name\": \"Op\", \"type\": \"string\"}"
      + "]}";

  @Test
  public void testInsert() {

    Schema avroSchema = new Schema.Parser().parse(AVRO_SCHEMA_STRING);
    GenericRecord record = new GenericData.Record(avroSchema);
    Properties properties = new Properties();
    record.put("field1", 0);
    record.put("Op", "I");

    AWSDmsAvroPayload payload = new AWSDmsAvroPayload(Option.of(record));

    try {
      Option<IndexedRecord> outputPayload = payload.getInsertValue(avroSchema, properties);
      assertTrue((int) outputPayload.get().get(0) == 0);
      assertTrue(outputPayload.get().get(1).toString().equals("I"));
    } catch (Exception e) {
      fail("Unexpected exception");
    }

  }

  @Test
  public void testGetInsertValueWithoutPartitionFields() throws IOException {
    Schema schema = Schema.createRecord(Arrays.asList(
        new Schema.Field("id", Schema.create(Schema.Type.STRING), "", null),
        new Schema.Field("partition", Schema.create(Schema.Type.STRING), "", null),
        new Schema.Field("ts", Schema.create(Schema.Type.LONG), "", null),
        new Schema.Field("Op", Schema.create(Schema.Type.STRING), "", null)
    ));
    Properties props = new Properties();
    props.setProperty(HoodiePayloadProps.PAYLOAD_ORDERING_FIELD_PROP_KEY, "ts");
    props.setProperty(HoodiePayloadProps.PAYLOAD_EVENT_TIME_FIELD_PROP_KEY, "ts");
    props.setProperty(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "id");
    props.setProperty(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "partition");
    props.setProperty(HoodieTableConfig.DROP_PARTITION_COLUMNS.key(), String.valueOf(true));
    Schema recordSchema = Schema.createRecord(Arrays.asList(
        new Schema.Field("id", Schema.create(Schema.Type.STRING), "", null),
        new Schema.Field("ts", Schema.create(Schema.Type.LONG), "", null),
        new Schema.Field("Op", Schema.create(Schema.Type.STRING), "", null)));
    GenericRecord record = new GenericData.Record(recordSchema);
    record.put("id", "1");
    record.put("ts", 0L);
    record.put("Op", "I");
    Option<GenericRecord> recordOption = Option.of(record);
    AWSDmsAvroPayload awsDmsAvroPayload = new AWSDmsAvroPayload(recordOption);
    assertEquals(awsDmsAvroPayload.getInsertValue(schema, props), recordOption);
  }

  @Test
  public void testUpdate() {
    Schema avroSchema = new Schema.Parser().parse(AVRO_SCHEMA_STRING);
    GenericRecord newRecord = new GenericData.Record(avroSchema);
    Properties properties = new Properties();
    newRecord.put("field1", 1);
    newRecord.put("Op", "U");

    GenericRecord oldRecord = new GenericData.Record(avroSchema);
    oldRecord.put("field1", 0);
    oldRecord.put("Op", "I");

    AWSDmsAvroPayload payload = new AWSDmsAvroPayload(Option.of(newRecord));

    try {
      Option<IndexedRecord> outputPayload = payload.combineAndGetUpdateValue(oldRecord, avroSchema, properties);
      assertTrue((int) outputPayload.get().get(0) == 1);
      assertTrue(outputPayload.get().get(1).toString().equals("U"));
    } catch (Exception e) {
      fail("Unexpected exception");
    }

  }

  @Test
  public void testDelete() {
    Schema avroSchema = new Schema.Parser().parse(AVRO_SCHEMA_STRING);
    GenericRecord deleteRecord = new GenericData.Record(avroSchema);
    Properties properties = new Properties();
    deleteRecord.put("field1", 2);
    deleteRecord.put("Op", "D");

    GenericRecord oldRecord = new GenericData.Record(avroSchema);
    oldRecord.put("field1", 2);
    oldRecord.put("Op", "U");

    AWSDmsAvroPayload payload = new AWSDmsAvroPayload(Option.of(deleteRecord));

    try {
      Option<IndexedRecord> outputPayload = payload.combineAndGetUpdateValue(oldRecord, avroSchema, properties);
      // expect nothing to be committed to table
      assertFalse(outputPayload.isPresent());
    } catch (Exception e) {
      fail("Unexpected exception");
    }

  }

  @Test
  public void testDeleteWithEmptyPayLoad() {
    Schema avroSchema = new Schema.Parser().parse(AVRO_SCHEMA_STRING);
    Properties properties = new Properties();

    GenericRecord oldRecord = new GenericData.Record(avroSchema);
    oldRecord.put("field1", 2);
    oldRecord.put("Op", "U");

    AWSDmsAvroPayload payload = new AWSDmsAvroPayload(Option.empty());

    try {
      Option<IndexedRecord> outputPayload = payload.combineAndGetUpdateValue(oldRecord, avroSchema, properties);
      // expect nothing to be committed to table
      assertFalse(outputPayload.isPresent());
    } catch (Exception e) {
      e.printStackTrace();
      fail("Unexpected exception");
    }
  }

  @Test
  public void testPreCombineWithDelete() {
    Schema avroSchema = new Schema.Parser().parse(AVRO_SCHEMA_STRING);
    GenericRecord deleteRecord = new GenericData.Record(avroSchema);
    Properties properties = new Properties();
    deleteRecord.put("field1", 4);
    deleteRecord.put("Op", "D");

    GenericRecord oldRecord = new GenericData.Record(avroSchema);
    oldRecord.put("field1", 4);
    oldRecord.put("Op", "I");

    AWSDmsAvroPayload payload = new AWSDmsAvroPayload(Option.of(deleteRecord));
    AWSDmsAvroPayload insertPayload = new AWSDmsAvroPayload(Option.of(oldRecord));

    try {
      OverwriteWithLatestAvroPayload output = payload.preCombine(insertPayload);
      Option<IndexedRecord> outputPayload = output.getInsertValue(avroSchema, properties);
      // expect nothing to be committed to table
      assertFalse(outputPayload.isPresent());
    } catch (Exception e) {
      fail("Unexpected exception");
    }
  }
}
