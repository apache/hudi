/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.avro;

import org.apache.hudi.common.testutils.SchemaTestUtil;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Conversions;
import org.apache.avro.Conversions.DecimalConversion;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Fixed;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestMercifulJsonConverter {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final MercifulJsonConverter CONVERTER = new MercifulJsonConverter(true,"__");

  @Test
  public void basicConversion() throws IOException {
    Schema simpleSchema = SchemaTestUtil.getSimpleSchema();
    String name = "John Smith";
    int number = 1337;
    String color = "Blue. No yellow!";
    Map<String, Object> data = new HashMap<>();
    data.put("name", name);
    data.put("favorite_number", number);
    data.put("favorite_color", color);
    String json = MAPPER.writeValueAsString(data);

    GenericRecord rec = new GenericData.Record(simpleSchema);
    rec.put("name", name);
    rec.put("favorite_number", number);
    rec.put("favorite_color", color);

    Assertions.assertEquals(rec, CONVERTER.convert(json, simpleSchema));
  }

  @Test
  public void conversionWithFieldNameSanitization() throws IOException {
    String sanitizedSchemaString = "{\"namespace\": \"example.avro\", \"type\": \"record\", \"name\": \"User\", \"fields\": [{\"name\": \"__name\", \"type\": \"string\"}, "
        + "{\"name\": \"favorite__number\", \"type\": \"int\"}, {\"name\": \"favorite__color__\", \"type\": \"string\"}]}";
    Schema sanitizedSchema = Schema.parse(sanitizedSchemaString);
    String name = "John Smith";
    int number = 1337;
    String color = "Blue. No yellow!";
    Map<String, Object> data = new HashMap<>();
    data.put("$name", name);
    data.put("favorite-number", number);
    data.put("favorite.color!", color);
    String json = MAPPER.writeValueAsString(data);

    GenericRecord rec = new GenericData.Record(sanitizedSchema);
    rec.put("__name", name);
    rec.put("favorite__number", number);
    rec.put("favorite__color__", color);

    Assertions.assertEquals(rec, CONVERTER.convert(json, sanitizedSchema));
  }

  @Test
  public void conversionWithFieldNameAliases() throws IOException {
    String schemaStringWithAliases = "{\"namespace\": \"example.avro\", \"type\": \"record\", \"name\": \"User\", \"fields\": [{\"name\": \"name\", \"type\": \"string\", \"aliases\": [\"$name\"]}, "
        + "{\"name\": \"favorite_number\",  \"type\": \"int\", \"aliases\": [\"unused\", \"favorite-number\"]}, {\"name\": \"favorite_color\", \"type\": \"string\", \"aliases\": "
        + "[\"favorite.color!\"]}, {\"name\": \"unmatched\", \"type\": \"string\", \"default\": \"default_value\"}]}";
    Schema sanitizedSchema = Schema.parse(schemaStringWithAliases);
    String name = "John Smith";
    int number = 1337;
    String color = "Blue. No yellow!";
    Map<String, Object> data = new HashMap<>();
    data.put("$name", name);
    data.put("favorite-number", number);
    data.put("favorite.color!", color);
    String json = MAPPER.writeValueAsString(data);

    GenericRecord rec = new GenericData.Record(sanitizedSchema);
    rec.put("name", name);
    rec.put("favorite_number", number);
    rec.put("favorite_color", color);

    Assertions.assertEquals(rec, CONVERTER.convert(json, sanitizedSchema));
  }

  @Test
  public void testConvertNumberToFixed() throws IOException {
    String testSchemaStr = "{\"type\": \"record\",\"name\": \"test_record\",\"namespace\": \"test_namespace\",\"fields\": "
        + "[{\"name\": \"decimal_field\",\"type\": [\"null\",{\"type\": \"fixed\",\"name\": \"fixed\",\"namespace\": \"test_namespace.decimal_field\",\"size\": 9,"
        + "    \"logicalType\": \"decimal\",\"precision\": 20,\"scale\": 0}],\"default\": null},"
        + "{\"name\": \"decimal2_field\",\"type\": [\"null\",{\"type\": \"fixed\",\"name\": \"fixed\",\"namespace\": \"test_namespace.decimal2_field\",\"size\": 9,"
        + "    \"logicalType\": \"decimal\",\"precision\": 20,\"scale\": 2}],\"default\": null},"
        + "{\"name\": \"decimal3_field\",\"type\": [\"null\",{\"type\": \"fixed\",\"name\": \"fixed\",\"namespace\": \"test_namespace.decimal3_field\",\"size\": 9,"
        + "    \"logicalType\": \"decimal\",\"precision\": 20,\"scale\": 2}],\"default\": null},"
        + "{\"name\": \"int_field\",\"type\": [\"null\",\"int\"],\"default\": null},"
        + "{\"name\": \"long_field\",\"type\": [\"null\",\"long\"],\"default\": null},"
        + "{\"name\": \"string_field\",\"type\": [\"null\",\"string\"],\"default\": null}]}";
    Schema schema = Schema.parse(testSchemaStr);

    String testValueStr = "{\n"
        + "    \"decimal_field\": 1720623716,\n"
        + "    \"decimal2_field\": 1720623716.23,\n"
        + "    \"decimal3_field\": [0, 0, 0, 0, 40, 15, -73, 111, 39],\n"
        + "    \"int_field\": 1720623716,\n"
        + "    \"long_field\": 1720623716,\n"
        + "    \"string_field\": \"STRING040467046577\"\n"
        + "}";

    GenericRecord record = CONVERTER.convert(testValueStr, schema);
    ObjectMapper objectMapper = new ObjectMapper();
    JsonNode root = objectMapper.readTree(testValueStr);

    assertEquals(root.get("decimal_field").asLong(), convertFixedToDecimal((Fixed) record.get("decimal_field")).longValue());
    assertEquals(root.get("decimal2_field").asDouble(), convertFixedToDecimal((Fixed) record.get("decimal2_field")).doubleValue());

    Fixed testFixedValue = new Fixed(((Fixed) record.get("decimal3_field")).getSchema(), new byte[] {0, 0, 0, 0, 40, 15, -73, 111, 39});
    assertEquals(1720623716.23, convertFixedToDecimal(testFixedValue).doubleValue());
    assertEquals(testFixedValue, record.get("decimal3_field"));
  }

  private BigDecimal convertFixedToDecimal(GenericData.Fixed fixedRecord) {
    DecimalConversion decimalConversion = new Conversions.DecimalConversion();
    return decimalConversion.fromFixed(fixedRecord, fixedRecord.getSchema(), fixedRecord.getSchema().getLogicalType());
  }
}
