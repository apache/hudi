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

package org.apache.hudi.common.table;

import org.apache.hudi.common.util.Option;

import org.apache.avro.Schema;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TestPartitionPathParser {

  private static Stream<Arguments> partitionPathCases() {
    return Stream.of(
        Arguments.of("2025/01/03/22", new String[]{"timestamp_field"}, new Object[]{new Timestamp(1735941600000L)}),
        Arguments.of("2025-01-03-22", new String[]{"timestamp_field"}, new Object[]{new Timestamp(1735941600000L)}),
        Arguments.of("timestamp_field=2025-01-03-22", new String[]{"timestamp_field"}, new Object[]{new Timestamp(1735941600000L)}),
        Arguments.of("2025/01/03", new String[]{"date_field"}, new Object[]{Date.valueOf("2025-01-03")}),
        Arguments.of("2025/01", new String[]{"date_field"}, new Object[]{Date.valueOf("2025-01-01")}),
        Arguments.of("2025", new String[]{"date_field"}, new Object[]{Date.valueOf("2025-01-01")}),
        Arguments.of("value1/2025/01/03", new String[]{"string_field","date_field"}, new Object[]{"value1", Date.valueOf("2025-01-03")}),
        Arguments.of("2025/01/03/value1", new String[]{"date_field", "string_field"}, new Object[]{Date.valueOf("2025-01-03"), "value1"}),
        Arguments.of("string_field=value1/year=2020/month=08/day=28/hour=06", new String[]{"string_field", "timestamp_field"}, new Object[]{"value1", new Timestamp(1598594400000L)}),
        Arguments.of("year=2020/month=08/day=28/hour=06/string_field=value1", new String[]{"timestamp_field", "string_field"}, new Object[]{new Timestamp(1598594400000L), "value1"}),
        Arguments.of("", null, new Object[]{})
    );
  }

  @ParameterizedTest
  @MethodSource("partitionPathCases")
  void testGetPartitionFieldVals(String partitionPath, String[] partitionFields, Object[] expectedValues) {
    PartitionPathParser parser = new PartitionPathParser();
    Schema schema = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"TestRecord\",\"fields\":[{\"name\":\"string_field\",\"type\":[\"null\", \"string\"]},"
        + "{\"name\":\"date_field\",\"type\": {\"type\":\"int\",\"logicalType\": \"date\"}},{\"name\":\"timestamp_field\",\"type\": {\"type\":\"long\",\"logicalType\": \"timestamp-millis\"}}]}");

    Object[] result = parser.getPartitionFieldVals(Option.ofNullable(partitionFields), partitionPath, schema);
    assertEquals(expectedValues.length, result.length);
    for (int i = 0; i < expectedValues.length; i++) {
      assertEquals(expectedValues[i], result[i]);
    }
  }

  private static Stream<Arguments> fieldCases() {
    return Stream.of(
        Arguments.of("123", Schema.create(Schema.Type.LONG), 123L),
        Arguments.of("123", Schema.create(Schema.Type.INT), 123),
        Arguments.of("123.45", Schema.create(Schema.Type.DOUBLE), 123.45),
        Arguments.of("123.45", Schema.create(Schema.Type.FLOAT), 123.45f),
        Arguments.of("false", Schema.create(Schema.Type.BOOLEAN), false),
        Arguments.of("__HIVE_DEFAULT_PARTITION__", Schema.create(Schema.Type.INT), null),
        Arguments.of("default", Schema.create(Schema.Type.INT), null),
        Arguments.of("2025-01-03", Schema.create(Schema.Type.STRING), "2025-01-03"),
        Arguments.of("value1", Schema.create(Schema.Type.BYTES), "value1".getBytes(StandardCharsets.UTF_8)),
        Arguments.of("value1", Schema.createFixed("fixed", "docs", null, 50), "value1".getBytes(StandardCharsets.UTF_8))
    );
  }

  @ParameterizedTest
  @MethodSource("fieldCases")
  void testValueParsing(String value, Schema fieldSchema, Object expected) {
    if (expected instanceof byte[]) {
      String expectedString = new String((byte[]) expected, StandardCharsets.UTF_8);
      String actualString = new String((byte[]) PartitionPathParser.parseValue(value, fieldSchema));
      assertEquals(expectedString, actualString);
    } else {
      assertEquals(expected, PartitionPathParser.parseValue(value, fieldSchema));
    }
  }
}
