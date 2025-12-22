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
import org.apache.hudi.common.testutils.PreCombineTestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestFirstValueAvroPayload {

  private Schema schema;
  private Properties props;

  @BeforeEach
  public void setUp() throws Exception {
    schema = Schema.createRecord(Arrays.asList(
            new Schema.Field("id", Schema.create(Schema.Type.STRING), "", null),
            new Schema.Field("partition", Schema.create(Schema.Type.STRING), "", null),
            new Schema.Field("ts", Schema.create(Schema.Type.LONG), "", null),
            new Schema.Field("_hoodie_is_deleted", Schema.create(Schema.Type.BOOLEAN), "", false)
    ));
    props = new Properties();
    props.setProperty(HoodiePayloadProps.PAYLOAD_ORDERING_FIELD_PROP_KEY, "ts");
    props.setProperty(HoodiePayloadProps.PAYLOAD_EVENT_TIME_FIELD_PROP_KEY, "ts");
  }

  @ParameterizedTest
  @MethodSource("org.apache.hudi.common.testutils.PreCombineTestUtils#configurePreCombine")
  public void testActiveRecordsForFirstValueAvroPayload(String key) throws IOException {
    PreCombineTestUtils.setPreCombineConfig(props, key, "ts");
    GenericRecord record1 = new GenericData.Record(schema);
    record1.put("id", "0");
    record1.put("partition", "partition0");
    record1.put("ts", 0L);
    record1.put("_hoodie_is_deleted", false);

    GenericRecord record2 = new GenericData.Record(schema);
    record2.put("id", "0");
    record2.put("partition", "partition0");
    record2.put("ts", 0L);
    record2.put("_hoodie_is_deleted", false);

    DefaultHoodieRecordPayload payload1 = new FirstValueAvroPayload(record1, 1);
    DefaultHoodieRecordPayload payload2 = new FirstValueAvroPayload(record2, 1);
    assertEquals(payload1.preCombine(payload2, props), payload2);
    assertEquals(payload2.preCombine(payload1, props), payload1);

    assertEquals(record1, payload1.getInsertValue(schema, props).get());
    assertEquals(record2, payload2.getInsertValue(schema, props).get());

    assertEquals(payload1.combineAndGetUpdateValue(record2, schema, props).get(), record2);
    assertEquals(payload2.combineAndGetUpdateValue(record1, schema, props).get(), record1);
  }
}
