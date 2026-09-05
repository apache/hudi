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

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.util.Option;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

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

    Object[] result = parser.getPartitionFieldVals(Option.ofNullable(partitionFields), partitionPath, testSchema());
    assertEquals(expectedValues.length, result.length);
    for (int i = 0; i < expectedValues.length; i++) {
      assertEquals(expectedValues[i], result[i]);
    }
  }

  private static HoodieSchema testSchema() {
    return HoodieSchema.parse("{\"type\":\"record\",\"name\":\"TestRecord\",\"fields\":[{\"name\":\"string_field\",\"type\":[\"null\", \"string\"]},"
        + "{\"name\":\"date_field\",\"type\": {\"type\":\"int\",\"logicalType\": \"date\"}},{\"name\":\"timestamp_field\",\"type\": {\"type\":\"long\",\"logicalType\": \"timestamp-millis\"}}]}");
  }

  private static Stream<Arguments> slashSeparatedPartitionPathCases() {
    return Stream.of(
        // the writer turned every dash into a directory separator, so the segments are rejoined
        // back into the single value they were written from
        Arguments.of("2026/01/05", new String[] {"string_field"}, new Object[] {"2026-01-05"}),
        Arguments.of("2026/01", new String[] {"string_field"}, new Object[] {"2026-01"}),
        // a value that held no dash occupies one segment, as it does without the config
        Arguments.of("2026", new String[] {"string_field"}, new Object[] {"2026"}),
        Arguments.of("__HIVE_DEFAULT_PARTITION__", new String[] {"string_field"}, new Object[] {null}),
        // a time-based column already handled the multi-directory layout and is left alone
        Arguments.of("2026/01/05", new String[] {"date_field"}, new Object[] {Date.valueOf("2026-01-05")}),
        // the writer only slash-separates a single-field table, so a multi-field path is read the
        // same way whether or not the config is set (HUDI issue #19666)
        Arguments.of("value1/2025/01/03", new String[] {"string_field", "date_field"},
            new Object[] {"value1", Date.valueOf("2025-01-03")})
    );
  }

  @ParameterizedTest
  @MethodSource("slashSeparatedPartitionPathCases")
  void testGetPartitionFieldValsWithSlashSeparatedDatePartitioning(String partitionPath,
                                                                   String[] partitionFields,
                                                                   Object[] expectedValues) {
    PartitionPathParser parser = new PartitionPathParser();

    Object[] result = parser.getPartitionFieldVals(Option.ofNullable(partitionFields), partitionPath, testSchema(), true);
    assertEquals(expectedValues.length, result.length);
    for (int i = 0; i < expectedValues.length; i++) {
      assertEquals(expectedValues[i], result[i]);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"2026/01/05", "2026/01"})
  void testSlashSeparatedValueIsOnlyRejoinedWhenTheTableConfigSaysSo(String partitionPath) {
    PartitionPathParser parser = new PartitionPathParser();

    // without the config the segments are assumed to belong to different partition fields, so a
    // single-field table only ever sees the first one
    Object[] result = parser.getPartitionFieldVals(
        Option.of(new String[] {"string_field"}), partitionPath, testSchema());
    assertEquals(1, result.length);
    assertEquals("2026", result[0]);
  }

  private static Stream<Arguments> fieldCases() {
    return Stream.of(
        Arguments.of("123", HoodieSchema.create(HoodieSchemaType.LONG), 123L),
        Arguments.of("123", HoodieSchema.create(HoodieSchemaType.INT), 123),
        Arguments.of("123.45", HoodieSchema.create(HoodieSchemaType.DOUBLE), 123.45),
        Arguments.of("123.45", HoodieSchema.create(HoodieSchemaType.FLOAT), 123.45f),
        Arguments.of("false", HoodieSchema.create(HoodieSchemaType.BOOLEAN), false),
        Arguments.of("__HIVE_DEFAULT_PARTITION__", HoodieSchema.create(HoodieSchemaType.INT), null),
        Arguments.of("default", HoodieSchema.create(HoodieSchemaType.INT), null),
        Arguments.of("2025-01-03", HoodieSchema.create(HoodieSchemaType.STRING), "2025-01-03"),
        Arguments.of("value1", HoodieSchema.create(HoodieSchemaType.BYTES), "value1".getBytes(StandardCharsets.UTF_8)),
        Arguments.of("value1", HoodieSchema.createFixed("fixed", null, "docs",50), "value1".getBytes(StandardCharsets.UTF_8))
    );
  }

  @ParameterizedTest
  @MethodSource("fieldCases")
  void testValueParsing(String value, HoodieSchema fieldSchema, Object expected) {
    if (expected instanceof byte[]) {
      String expectedString = new String((byte[]) expected, StandardCharsets.UTF_8);
      String actualString = new String((byte[]) PartitionPathParser.parseValue(value, fieldSchema));
      assertEquals(expectedString, actualString);
    } else {
      assertEquals(expected, PartitionPathParser.parseValue(value, fieldSchema));
    }
  }
}
