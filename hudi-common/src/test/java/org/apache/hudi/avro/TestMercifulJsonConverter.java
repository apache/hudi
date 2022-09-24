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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for {@link MercifulJsonConverter}.
 */
public class TestMercifulJsonConverter {

  private static String SCHEMA_WITH_DECIMAL_FIELD = "{\"name\":\"record\",\"type\":\"record\",\"fields\":["
      + "{\"name\":\"id\",\"type\":[\"null\",\"string\"]},"
      + "{\"name\":\"decimal_col\",\"type\":[\"null\",{\"logicalType\":\"decimal\",\"size\":5,"
      + "\"precision\":10,\"name\":\"fixed\",\"scale\":2,\"type\":\"fixed\"}]}]}\n";

  private static String DATA_WITH_DECIMAL_FIELD = "{\"decimal_col\":35.51,\"id\":\"1\"}";

  private final ObjectMapper mapper = new ObjectMapper();

  @Test
  public void testDecimalInFixedType() throws IOException {
    MercifulJsonConverter converter = new MercifulJsonConverter();
    GenericRecord record = converter.convert(DATA_WITH_DECIMAL_FIELD, new Schema.Parser().parse(SCHEMA_WITH_DECIMAL_FIELD));

    double decimalValue = (double) mapper
        .readValue(DATA_WITH_DECIMAL_FIELD, Map.class)
        .get("decimal_col");

    byte[] bytesFromFixedType = ((GenericData.Fixed) record.get("decimal_col")).bytes();

    assertEquals("1", record.get("id").toString());
    assertArrayEquals(Double.toString(decimalValue).getBytes(), bytesFromFixedType);

  }
}
