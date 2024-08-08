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

import org.apache.hudi.common.config.TypedProperties;

import io.confluent.kafka.schemaregistry.json.JsonSchema;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;

import static org.apache.hudi.common.util.FileIOUtils.readAsUTFString;
import static org.apache.hudi.utilities.schema.converter.JsonToAvroSchemaConverter.stripQuotesFromStringValue;
import static org.apache.hudi.utilities.schema.converter.JsonToAvroSchemaConverterConfig.STRIP_DEFAULT_VALUE_QUOTES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class TestJsonToAvroSchemaConverter {

  @ParameterizedTest
  @CsvSource({
      "enum-properties,",
      "example-address,",
      "example-calendar,",
      "example-card,",
      "example-geographical-location,",
      "multiple-properties,",
      "nested-properties,",
      "single-properties,",
      "schema-repeating-names,",
      "complex-json-union-types,",
      "not-null-default-value-schema,_no_stripping_quotes",
      "not-null-default-value-schema,_stripping_quotes",
      "array-with-item-type-union,",
      "kafka-decimal-simple,"
  })
  void testConvertJsonSchemaToAvroSchema(String inputCase, String avroSchemaFileSuffix) throws IOException {
    String jsonSchema = loadJsonSchema(inputCase);
    TypedProperties config = new TypedProperties();
    if ("_no_stripping_quotes".equals(avroSchemaFileSuffix)) {
      config.put(STRIP_DEFAULT_VALUE_QUOTES.key(), "false");
    }
    String avroSchema = new JsonToAvroSchemaConverter(config).convert(new JsonSchema(jsonSchema));
    Schema schema = new Schema.Parser().parse(avroSchema);
    Schema expected = new Schema.Parser().parse(loadAvroSchema(inputCase, avroSchemaFileSuffix));
    assertEquals(expected, schema);
  }

  @Test
  void testStripQuotesFromStringValue() {
    assertNull(stripQuotesFromStringValue(null));
    assertEquals("", stripQuotesFromStringValue(""));
    assertEquals("\"", stripQuotesFromStringValue("\""));
    assertEquals("", stripQuotesFromStringValue("\"\""));
    assertEquals("\"", stripQuotesFromStringValue("\"\"\""));
    assertEquals("123", stripQuotesFromStringValue("123"));
    assertEquals("123", stripQuotesFromStringValue("\"123\""));
    assertEquals("x", stripQuotesFromStringValue("x"));
  }

  private String loadJsonSchema(String inputCase) throws IOException {
    return readAsUTFString(getClass()
        .getClassLoader()
        .getResourceAsStream(String.format("schema-provider/json/%s/input.json", inputCase)));
  }

  private String loadAvroSchema(String inputCase, String avroSchemaFileSuffix) throws IOException {
    return readAsUTFString(getClass()
        .getClassLoader()
        .getResourceAsStream(String.format("schema-provider/json/%s/expected%s.json", inputCase,
            avroSchemaFileSuffix == null ? "" : avroSchemaFileSuffix)));
  }
}
