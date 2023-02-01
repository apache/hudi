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

package org.apache.hudi.utilities.schema.converter;

import org.apache.avro.Schema;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;

import static org.apache.hudi.common.util.FileIOUtils.readAsUTFString;
import static org.junit.jupiter.api.Assertions.assertEquals;

class TestJsonToAvroSchemaConverter {

  @ParameterizedTest
  @ValueSource(strings = {
      "enum-properties",
      "example-address",
      "example-calendar",
      "example-card",
      "example-geographical-location",
      "multiple-properties",
      "nested-properties",
      "single-properties"
  })
  void testConvertJsonSchemaToAvroSchema(String inputCase) throws IOException {
    String jsonSchema = loadJsonSchema(inputCase);
    String avroSchema = new JsonToAvroSchemaConverter().convert(jsonSchema);
    Schema schema = new Schema.Parser().parse(avroSchema);
    Schema expected = new Schema.Parser().parse(loadAvroSchema(inputCase));
    assertEquals(expected, schema);
  }

  private String loadJsonSchema(String inputCase) throws IOException {
    return readAsUTFString(getClass()
        .getClassLoader()
        .getResourceAsStream(String.format("schema-provider/json/%s/input.json", inputCase)));
  }

  private String loadAvroSchema(String inputCase) throws IOException {
    return readAsUTFString(getClass()
        .getClassLoader()
        .getResourceAsStream(String.format("schema-provider/json/%s/expected.json", inputCase)));
  }
}
