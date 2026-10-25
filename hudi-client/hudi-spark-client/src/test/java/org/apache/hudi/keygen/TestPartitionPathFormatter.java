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
  void testSlashSeparatedDatePartitioningAppliesToEveryField(boolean useRowWriterPath) {
    // NOTE: Every part is substituted, which is what legacy [[CustomKeyGenerator]] tables hold on
    //       disk (one single-field sub-keygen per field) and what
    //       [[SparkHoodieTableFileIndex#composeRelativePartitionPath]] has to reproduce when it
    //       composes a listing prefix over all N columns in one call. New writes cannot reach this
    //       combination -- [[HoodieWriterUtils#validateTableConfig]] rejects slash partitioning
    //       with more than one partition field. See HUDI issue #19666
    assertEquals("2026/01/05/san/francisco",
        combine(useRowWriterPath, false, false, true, TWO_FIELDS, "2026-01-05", "san-francisco"));
    // The guard applies per part, not just to the first one
    assertEquals("2026/01/05/-5",
        combine(useRowWriterPath, false, false, true, TWO_FIELDS, "2026-01-05", "-5"));
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
  void testSlashSeparatedDatePartitioningLeavesPathBreakingDashesAlone(boolean useRowWriterPath) {
    // NOTE: Substituting in any of these would yield a partition path that does not survive the
    //       round trip back from storage. A leading "/" is resolved differently by
    //       [[FSUtils#constructAbsolutePath(String, String)]] and the [[StoragePath]] overload used
    //       by [[AbstractTableFileSystemView]] -- the former chops it, the latter URI-resolves the
    //       table base path away. A trailing "/" is normalized off by [[StoragePath#normalize]] and
    //       a doubled "//" is collapsed by [[java.net.URI#normalize]], leaving the writer recording
    //       a partition string longer than the directory it actually resolves to
    assertEquals("-5", combine(useRowWriterPath, false, false, true, SINGLE_FIELD, "-5"));
    assertEquals("-", combine(useRowWriterPath, false, false, true, SINGLE_FIELD, "-"));
    assertEquals("--5", combine(useRowWriterPath, false, false, true, SINGLE_FIELD, "--5"));
    assertEquals("5-", combine(useRowWriterPath, false, false, true, SINGLE_FIELD, "5-"));
    assertEquals("a--b", combine(useRowWriterPath, false, false, true, SINGLE_FIELD, "a--b"));
    // A dash-delimited "." or ".." would become a URI dot segment: "2026/./05" normalizes to
    // "2026/05", and "../a" resolves outside the table base path entirely
    assertEquals("2026-.-05", combine(useRowWriterPath, false, false, true, SINGLE_FIELD, "2026-.-05"));
    assertEquals("a-..-b", combine(useRowWriterPath, false, false, true, SINGLE_FIELD, "a-..-b"));
    assertEquals("..-a", combine(useRowWriterPath, false, false, true, SINGLE_FIELD, "..-a"));
    // A single interior dash is still a separator, and dots inside a token are untouched
    assertEquals("2026/01/05", combine(useRowWriterPath, false, false, true, SINGLE_FIELD, "2026-01-05"));
    assertEquals("v1.2/v3.4", combine(useRowWriterPath, false, false, true, SINGLE_FIELD, "v1.2-v3.4"));
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
