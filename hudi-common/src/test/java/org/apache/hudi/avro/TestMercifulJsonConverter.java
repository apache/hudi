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

package org.apache.hudi.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestMercifulJsonConverter {

  @Test
  void testSimpleJsonMessage_convertsToGenericRecord() {
    MercifulJsonConverter mc = new MercifulJsonConverter();
    String avroString = "{\"name\":\"abc\",\"type\":\"record\",\"namespace\":\"com.marqeta.jcard\",\"fields\":[{\"name\":\"firstName\",\"type\":\"string\"},{\"name\":\"lastName\","
        + "\"type\":\"string\"}]}";
    Schema avroSchema = new Schema.Parser().parse(avroString);
    String jsonMessage = "{\"firstName\":\"Sachin\",\"lastName\":\"Tendulkar\"}";
    GenericRecord g = mc.convert(jsonMessage, avroSchema);

    Assertions.assertEquals("Sachin", g.get("firstName"));
    Assertions.assertEquals("Tendulkar", g.get("lastName"));
  }

  @Test
  void testSimpleJsonSchemaMessage_convertsToGenericRecord() {
    MercifulJsonConverter mc = new MercifulJsonConverter();
    String avroString = "{\"name\":\"abc\",\"type\":\"record\",\"namespace\":\"com.marqeta.jcard\",\"fields\":[{\"name\":\"firstName\",\"type\":\"string\"},{\"name\":\"lastName\","
        + "\"type\":\"string\"}]}";
    Schema avroSchema = new Schema.Parser().parse(avroString);
    String jsonMessage = "?????{\"firstName\":\"Sachin\",\"lastName\":\"Tendulkar\"}";
    GenericRecord g = mc.convert(jsonMessage, avroSchema);

    Assertions.assertEquals("Sachin", g.get("firstName"));
    Assertions.assertEquals("Tendulkar", g.get("lastName"));
  }

  @Test
  void testJsonSchemaMessage_convertsNumberToDouble() {
    MercifulJsonConverter mc = new MercifulJsonConverter();
    String avroString = "{\"name\":\"abc\",\"type\":\"record\",\"namespace\":\"com.marqeta.jcard\",\"fields\":[{\"name\":\"firstName\",\"type\":\"string\"},{\"name\":\"lastName\","
        + "\"type\":\"string\"},{\"name\":\"score\",\"type\":\"double\"}]}";
    Schema avroSchema = new Schema.Parser().parse(avroString);
    String jsonMessage = "?????{\"firstName\":\"Sachin\",\"lastName\":\"Tendulkar\",\"score\":46.89}";
    GenericRecord g = mc.convert(jsonMessage, avroSchema);

    Assertions.assertEquals(46.89, g.get("score"));

  }
}
