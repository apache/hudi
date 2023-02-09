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

package org.apache.hudi.utilities.functional;

import org.apache.hudi.common.model.AWSDmsAvroPayload;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;
import org.apache.hudi.utilities.transform.AWSDmsTransformer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("functional")
public class TestAWSDatabaseMigrationServiceSource extends SparkClientFunctionalTestHarness {

  @Test
  public void testPayload() throws IOException {
    final Schema schema = Schema.createRecord(Arrays.asList(
        new Schema.Field("id", Schema.create(Schema.Type.STRING), "", null),
        new Schema.Field("ts", Schema.create(Schema.Type.LONG), "", null),
        new Schema.Field(AWSDmsAvroPayload.OP_FIELD, Schema.create(Schema.Type.STRING), "", null)
    ));
    final GenericRecord record = new GenericData.Record(schema);

    record.put("id", "1");
    record.put("Op", "");
    record.put("ts", 0L);
    AWSDmsAvroPayload payload = new AWSDmsAvroPayload(record, (Comparable) record.get("ts"));
    assertTrue(payload.combineAndGetUpdateValue(null, schema).isPresent());

    record.put("Op", "I");
    payload = new AWSDmsAvroPayload(record, (Comparable) record.get("ts"));
    assertTrue(payload.combineAndGetUpdateValue(null, schema).isPresent());

    record.put("Op", "D");
    payload = new AWSDmsAvroPayload(record, (Comparable) record.get("ts"));
    assertFalse(payload.combineAndGetUpdateValue(null, schema).isPresent());
  }

  static class Record implements Serializable {

    String id;
    long ts;

    Record(String id, long ts) {
      this.id = id;
      this.ts = ts;
    }
  }

  @Test
  public void testTransformer() {
    AWSDmsTransformer transformer = new AWSDmsTransformer();
    Dataset<Row> inputFrame = spark().createDataFrame(Arrays.asList(
        new Record("1", 3433L),
        new Record("2", 3433L)), Record.class);

    Dataset<Row> outputFrame = transformer.apply(jsc(), spark(), inputFrame, null);
    assertTrue(Arrays.stream(outputFrame.schema().fields())
        .map(f -> f.name()).anyMatch(n -> n.equals(AWSDmsAvroPayload.OP_FIELD)));
    assertTrue(outputFrame.select(AWSDmsAvroPayload.OP_FIELD).collectAsList().stream()
        .allMatch(r -> r.getString(0).equals("")));
  }
}
