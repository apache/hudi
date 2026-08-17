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

package org.apache.hudi.keygen;

import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.hudi.common.util.PartitionPathEncodeUtils.DEFAULT_PARTITION_PATH;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests the partition-path formatters backing the key generators, making sure that both the
 * {@link String} (Avro/{@link org.apache.spark.sql.Row} write path) and the {@link UTF8String}
 * (row-writer/{@link org.apache.spark.sql.catalyst.InternalRow} write path) flavors produce
 * identical partition paths.
 */
class TestPartitionPathFormatter {

  private static final List<String> SINGLE_FIELD = Collections.singletonList("date_col");
  private static final List<String> TWO_FIELDS = Arrays.asList("date_col", "city");

  private String combine(boolean unsafe,
                         boolean hiveStylePartitioning,
                         boolean encode,
                         boolean slashSeparatedDatePartitioning,
                         List<String> fields,
                         Object... parts) {
    if (unsafe) {
      return new UTF8StringPartitionPathFormatter(
          UTF8StringPartitionPathFormatter.UTF8StringBuilder::new, hiveStylePartitioning, encode,
          slashSeparatedDatePartitioning).combine(fields, parts).toString();
    }
    return new StringPartitionPathFormatter(
        StringPartitionPathFormatter.JavaStringBuilder::new, hiveStylePartitioning, encode,
        slashSeparatedDatePartitioning).combine(fields, parts);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testSlashSeparatedDatePartitioningSingleField(boolean unsafe) {
    assertEquals("2026/01/05",
        combine(unsafe, false, false, true, SINGLE_FIELD, "2026-01-05"));
    // Input that is already slash-separated is left untouched
    assertEquals("2026/01/05",
        combine(unsafe, false, false, true, SINGLE_FIELD, "2026/01/05"));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testSlashSeparatedDatePartitioningOnlyAppliesToSingleFieldPartitioning(boolean unsafe) {
    // NOTE: This mirrors [[KeyGenUtils#getRecordPartitionPath]] driving the Avro write-path
    assertEquals("2026-01-05/san-francisco",
        combine(unsafe, false, false, true, TWO_FIELDS, "2026-01-05", "san-francisco"));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testSlashSeparatedDatePartitioningHandlesNullAndEmptyValues(boolean unsafe) {
    assertEquals(DEFAULT_PARTITION_PATH,
        combine(unsafe, false, false, true, SINGLE_FIELD, new Object[] {null}));
    assertEquals(DEFAULT_PARTITION_PATH,
        combine(unsafe, false, false, true, SINGLE_FIELD, ""));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testSlashSeparatedDatePartitioningEncodesValues(boolean unsafe) {
    // '?' has to be escaped, while the date separators are turned into directory separators
    assertEquals("2026/01/05", combine(unsafe, false, true, true, SINGLE_FIELD, "2026-01-05"));
    assertEquals("a%3Fb", combine(unsafe, false, true, true, SINGLE_FIELD, "a?b"));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testHiveStylePartitioningTakesPrecedence(boolean unsafe) {
    assertEquals("date_col=2026-01-05",
        combine(unsafe, true, false, true, SINGLE_FIELD, "2026-01-05"));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testPlainPartitioningIsUnaffected(boolean unsafe) {
    assertEquals("2026-01-05",
        combine(unsafe, false, false, false, SINGLE_FIELD, "2026-01-05"));
    assertEquals("2026-01-05/san-francisco",
        combine(unsafe, false, false, false, TWO_FIELDS, "2026-01-05", "san-francisco"));
  }
}
