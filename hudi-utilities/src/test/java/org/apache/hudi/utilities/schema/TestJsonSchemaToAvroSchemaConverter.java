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

package org.apache.hudi.utilities.schema;

import org.apache.avro.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;

class TestJsonSchemaToAvroSchemaConverter {

  @Test
  void testSimpleJson_providesValidAvroSchema() throws IOException {
    String jsonString = "{\"type\":\"object\",\"title\":\"cdc_marqeta_jcard_abc.Envelope\",\"properties\":{\"firstName\":{\"type\":\"string\"},\"lastName\":{\"type\":\"string\"}}}";
    String avroString = JsonSchemaToAvroSchemaConverter.convertJsonSchemaToAvroSchema(jsonString);
    Schema avroSchema = new Schema.Parser().parse(avroString);

    Assertions.assertFalse(avroSchema.isError());
  }

  @Test
  void testNestedJson_providesValidAvroSchema() throws IOException {
    String jsonString = "{\"type\":\"object\",\"title\":\"cdc_marqeta_jcard_abc.Envelope\",\"properties\":{\"city\":{\"type\":\"string\"},\"street\":{\"type\":\"object\",\"title\":\"street\","
        + "\"properties\":{\"line1\":{\"type\":\"string\"},\"line2\":{\"type\":\"string\"}}}}}";
    String avroString = JsonSchemaToAvroSchemaConverter.convertJsonSchemaToAvroSchema(jsonString);
    Schema avroSchema = new Schema.Parser().parse(avroString);

    Assertions.assertFalse(avroSchema.isError());
  }
}
