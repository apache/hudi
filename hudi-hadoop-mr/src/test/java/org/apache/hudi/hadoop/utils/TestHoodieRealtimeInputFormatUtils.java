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

package org.apache.hudi.hadoop.utils;

import org.apache.hudi.common.testutils.HoodieTestUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class TestHoodieRealtimeInputFormatUtils {

  private Configuration hadoopConf;

  @TempDir
  public java.nio.file.Path basePath;

  @BeforeEach
  public void setUp() {
    hadoopConf = HoodieTestUtils.getDefaultStorageConf().unwrap();
    hadoopConf.set("fs.defaultFS", "file:///");
    hadoopConf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
  }

  @Test
  public void testAddProjectionField() {
    hadoopConf.set(hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS, "");
    HoodieRealtimeInputFormatUtils.addProjectionField(hadoopConf, hadoopConf.get(hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS, "").split("/"));
  }

  private String clean(String columnIds) {
    hadoopConf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, columnIds);
    HoodieRealtimeInputFormatUtils.cleanProjectionColumnIds(hadoopConf);
    return hadoopConf.get(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR);
  }

  private static Stream<Arguments> projectionIdCases() {
    return Stream.of(
        Arguments.of(",2,0", "2,0", "a leading blank id should be dropped"),
        Arguments.of(",,2,0", "2,0",
            "Hive appending empty ids repeatedly yields more than one leading blank"),
        Arguments.of("3,,2,0", "3,2,0",
            "an interior blank, which leading-comma stripping never reached; the resulting pairing is still "
                + "unsound per #19506, this only stops the bare NumberFormatException"),
        Arguments.of(" 2 , 0 ", "2,0",
            "ids are trimmed, so a padded id parses and de-duplicates as the same entry"),
        Arguments.of("2,0", "2,0", "a list with nothing to drop keeps its value"),
        Arguments.of("", "", "an empty list keeps its value"));
  }

  /**
   * HIVE-22438: for {@code SELECT COUNT(*)} on Hive before 3.0.0 the read-column ids arrive empty and Hive
   * combines them into e.g. {@code ",2,0,3"}. Every consumer of this conf value parses the ids with
   * {@code Integer#parseInt}, so any blank entry left behind fails with a bare {@code NumberFormatException}.
   *
   * <p>One case per shape, so a regression in the first does not hide the rest: only three of the six differ
   * from what the previous single-leading-comma sanitiser produced.
   */
  @ParameterizedTest(name = "[{index}] \"{0}\" -> \"{1}\"")
  @MethodSource("projectionIdCases")
  public void testCleanProjectionColumnIdsDropsBlankEntries(String columnIds, String expected, String why) {
    assertEquals(expected, clean(columnIds), why);
  }

  /**
   * The key is unset until something projects a column, and {@code conf.get} had no default here, so this
   * threw {@code NullPointerException} before. Same shape as the fix applied to {@code addProjectionField}.
   *
   * <p>This also pins the write-back guard: without it the empty default would be written back and the key
   * would no longer read as unset.
   */
  @Test
  public void testCleanProjectionColumnIdsWithUnsetKey() {
    hadoopConf.unset(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR);
    assertDoesNotThrow(() -> HoodieRealtimeInputFormatUtils.cleanProjectionColumnIds(hadoopConf));
    assertNull(hadoopConf.get(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR),
        "an unset key should stay unset rather than being written back as empty");
  }
}
