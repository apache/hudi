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

import static org.apache.hudi.keygen.KeyGenUtils.HUDI_DEFAULT_PARTITION_PATH;
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

  private String combine(boolean useRowWriterPath,
                         boolean hiveStylePartitioning,
                         boolean encode,
                         boolean slashSeparatedDatePartitioning,
                         List<String> fields,
                         Object... parts) {
    if (useRowWriterPath) {
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
  void testSlashSeparatedDatePartitioningOnlyAppliesToSingleFieldPartitioning(boolean useRowWriterPath) {
    // NOTE: This mirrors [[KeyGenUtils#getRecordPartitionPath]] driving the Avro write-path
    assertEquals("2026-01-05/san-francisco",
        combine(useRowWriterPath, false, false, true, TWO_FIELDS, "2026-01-05", "san-francisco"));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testSlashSeparatedDatePartitioningHandlesNullAndEmptyValues(boolean useRowWriterPath) {
    assertEquals(HUDI_DEFAULT_PARTITION_PATH,
        combine(useRowWriterPath, false, false, true, SINGLE_FIELD, new Object[] {null}));
    assertEquals(HUDI_DEFAULT_PARTITION_PATH,
        combine(useRowWriterPath, false, false, true, SINGLE_FIELD, ""));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testSlashSeparatedDatePartitioningLeavesLeadingDashesAlone(boolean useRowWriterPath) {
    // NOTE: Substituting here would yield a partition path starting with "/", which
    //       [[FSUtils#constructAbsolutePath(String, String)]] and the [[StoragePath]] overload used
    //       by [[AbstractTableFileSystemView]] resolve differently -- the former chops the leading
    //       "/", the latter URI-resolves the table base path away -- so the writer and the
    //       file-system view would disagree on where the partition lives
    assertEquals("-5", combine(useRowWriterPath, false, false, true, SINGLE_FIELD, "-5"));
    assertEquals("-", combine(useRowWriterPath, false, false, true, SINGLE_FIELD, "-"));
    assertEquals("--5", combine(useRowWriterPath, false, false, true, SINGLE_FIELD, "--5"));
    // A dash anywhere else is still a separator: only a leading one produces an absolute path
    assertEquals("5/", combine(useRowWriterPath, false, false, true, SINGLE_FIELD, "5-"));
    assertEquals("2026/01/05", combine(useRowWriterPath, false, false, true, SINGLE_FIELD, "2026-01-05"));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testSlashSeparatedDatePartitioningEncodesValues(boolean useRowWriterPath) {
    // '?' has to be escaped, while the date separators are turned into directory separators
    assertEquals("2026/01/05", combine(useRowWriterPath, false, true, true, SINGLE_FIELD, "2026-01-05"));
    assertEquals("a%3Fb", combine(useRowWriterPath, false, true, true, SINGLE_FIELD, "a?b"));
    // Encoding runs before the substitution (parity with KeyGenUtils), so an already slash-separated
    // value is escaped rather than turned into directories
    assertEquals("2026%2F01%2F05", combine(useRowWriterPath, false, true, true, SINGLE_FIELD, "2026/01/05"));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testHiveStylePartitioningTakesPrecedence(boolean useRowWriterPath) {
    // NOTE: Hive-style partitioning and slash-separated date partitioning are documented as mutually
    //       exclusive ([[KeyGeneratorOptions#SLASH_SEPARATED_DATE_PARTITIONING]]), but only
    //       [[HoodieCatalogTable#extraTableConfig]] enforces it, and it inspects the SQL options
    //       alone -- df.write and HoodieStreamer still accept the combination. The formatter
    //       deliberately leaves the value alone here rather than mirroring the Avro path, which
    //       produces a layout [[HoodieSparkUtils#doParsePartitionColumnValues]] cannot read back.
    //       This asserts the pre-existing behavior stays put, it is not a statement about what the
    //       combination *should* produce
    assertEquals("date_col=2026-01-05",
        combine(useRowWriterPath, true, false, true, SINGLE_FIELD, "2026-01-05"));
  }
}
