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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
    HoodieSchema schema = HoodieSchema.parse("{\"type\":\"record\",\"name\":\"TestRecord\",\"fields\":[{\"name\":\"string_field\",\"type\":[\"null\", \"string\"]},"
        + "{\"name\":\"date_field\",\"type\": {\"type\":\"int\",\"logicalType\": \"date\"}},{\"name\":\"timestamp_field\",\"type\": {\"type\":\"long\",\"logicalType\": \"timestamp-millis\"}}]}");

    Object[] result = parser.getPartitionFieldVals(Option.ofNullable(partitionFields), partitionPath, schema);
    assertEquals(expectedValues.length, result.length);
    for (int i = 0; i < expectedValues.length; i++) {
      assertEquals(expectedValues[i], result[i]);
    }
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

  private static Stream<Arguments> lookupPartitionPathsCases() {
    List<String> singleKey  = Collections.singletonList("pt");
    List<String> multiKeys  = Arrays.asList("year", "month");

    return Stream.of(
        // single partition key, hive-style
        Arguments.of("pt=p1",           singleKey, true,  Arrays.asList("pt=p1")),
        // single partition key, non-hive-style
        Arguments.of("pt=p1",           singleKey, false, Arrays.asList("p1")),
        // multiple partitions separated by ';', hive-style
        Arguments.of("pt=p1;pt=p2",     singleKey, true,  Arrays.asList("pt=p1", "pt=p2")),
        // multiple partitions separated by ';', non-hive-style
        Arguments.of("pt=p1;pt=p2",     singleKey, false, Arrays.asList("p1", "p2")),
        // multi-key partition, hive-style — keys in spec order, path in partitionKeys order
        Arguments.of("year=2024,month=01", multiKeys, true,  Arrays.asList("year=2024/month=01")),
        // multi-key partition, non-hive-style — values in partitionKeys order
        Arguments.of("year=2024,month=01", multiKeys, false, Arrays.asList("2024/01")),
        // keys specified in reverse order — output must still follow partitionKeys order
        Arguments.of("month=01,year=2024", multiKeys, true,  Arrays.asList("year=2024/month=01")),
        Arguments.of("month=01,year=2024", multiKeys, false, Arrays.asList("2024/01")),
        // multiple multi-key partitions, hive-style
        Arguments.of("year=2024,month=01;year=2024,month=02", multiKeys, true,
            Arrays.asList("year=2024/month=01", "year=2024/month=02")),
        // multiple multi-key partitions, non-hive-style
        Arguments.of("year=2024,month=01;year=2024,month=02", multiKeys, false,
            Arrays.asList("2024/01", "2024/02")),
        // extra whitespace around semicolons and key=value pairs
        Arguments.of(" pt=p1 ; pt=p2 ", singleKey, true,  Arrays.asList("pt=p1", "pt=p2")),
        Arguments.of(" pt=p1 ; pt=p2 ", singleKey, false, Arrays.asList("p1", "p2")),
        // consecutive semicolons produce empty entries that are silently skipped
        Arguments.of("pt=p1;;pt=p2",    singleKey, true,  Arrays.asList("pt=p1", "pt=p2")),
        // trailing semicolon is silently ignored
        Arguments.of("pt=p1;",          singleKey, false, Arrays.asList("p1"))
    );
  }

  @ParameterizedTest
  @MethodSource("lookupPartitionPathsCases")
  void testParseLookupPartitionPaths(String spec, List<String> partitionKeys, boolean hiveStyle,
                                     List<String> expected) {
    List<String> result = PartitionPathParser.parseLookupPartitionPaths(spec, partitionKeys, hiveStyle);
    assertEquals(expected, result);
  }

  @Test
  void testParseLookupPartitionPaths_unknownKey_throwsIllegalArgument() {
    List<String> partitionKeys = Arrays.asList("dt", "region");
    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
        () -> PartitionPathParser.parseLookupPartitionPaths(
            "dt=2024-01-01,unknown=foo", partitionKeys, false));
    assertTrue(ex.getMessage().contains("unknown"));
    assertTrue(ex.getMessage().contains("[dt, region]"));
  }

  @Test
  void testParseLookupPartitionPaths_missingEqualsSign_throwsIllegalArgument() {
    List<String> partitionKeys = Collections.singletonList("pt");
    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
        () -> PartitionPathParser.parseLookupPartitionPaths("p1", partitionKeys, false));
    assertTrue(ex.getMessage().contains("key=value"));
  }

  @Test
  void testParseLookupPartitionPaths_unknownKeyInSecondPartition_throwsIllegalArgument() {
    List<String> partitionKeys = Collections.singletonList("pt");
    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
        () -> PartitionPathParser.parseLookupPartitionPaths(
            "pt=p1;badkey=p2", partitionKeys, true));
    assertTrue(ex.getMessage().contains("badkey"));
  }

  @Test
  void testParseLookupPartitionPaths_emptySpec_returnsEmptyList() {
    List<String> result = PartitionPathParser.parseLookupPartitionPaths(
        "", Collections.singletonList("pt"), false);
    assertTrue(result.isEmpty());
  }
}
