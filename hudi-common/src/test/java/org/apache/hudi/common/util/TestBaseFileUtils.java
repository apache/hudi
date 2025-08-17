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

package org.apache.hudi.common.util;

import org.apache.hudi.common.model.HoodieColumnRangeMetadata;
import org.apache.hudi.metadata.HoodieIndexVersion;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestBaseFileUtils {
  private static final String PARTITION_PATH = "partition";
  private static final String COLUMN_NAME = "columnName";

  @Test
  public void testGetColumnRangeInPartition() {
    HoodieIndexVersion indexVersion = HoodieIndexVersion.V1;
    // Step 1: Set Up Test Data
    HoodieColumnRangeMetadata<Comparable> fileColumnRange1 = HoodieColumnRangeMetadata.<Comparable>create(
        "path/to/file1", COLUMN_NAME, 1, 5, 0, 10, 100, 200, HoodieColumnRangeMetadata.NoneMetadata.INSTANCE, indexVersion);
    HoodieColumnRangeMetadata<Comparable> fileColumnRange2 = HoodieColumnRangeMetadata.<Comparable>create(
        "path/to/file2", COLUMN_NAME, 3, 8, 1, 15, 120, 250, HoodieColumnRangeMetadata.NoneMetadata.INSTANCE, indexVersion);
    List<HoodieColumnRangeMetadata<Comparable>> fileColumnRanges = Arrays.asList(fileColumnRange1, fileColumnRange2);
    // Step 2: Call the Method
    HoodieColumnRangeMetadata<Comparable> result = FileFormatUtils.getColumnRangeInPartition(PARTITION_PATH, COLUMN_NAME, fileColumnRanges, Collections.emptyMap(), indexVersion);
    // Step 3: Assertions
    assertEquals(PARTITION_PATH, result.getFilePath());
    assertEquals(COLUMN_NAME, result.getColumnName());
    assertEquals(Integer.valueOf(1), new Integer(result.getMinValue().toString()));
    assertEquals(Integer.valueOf(8), new Integer(result.getMaxValue().toString()));
    assertEquals(1, result.getNullCount());
    assertEquals(25, result.getValueCount());
    assertEquals(220, result.getTotalSize());
    assertEquals(450, result.getTotalUncompressedSize());
  }

  @Test
  public void testGetColumnRangeInPartitionWithNullMinMax() {
    HoodieIndexVersion indexVersion = HoodieIndexVersion.V1;
    // Step 1: Set Up Test Data
    HoodieColumnRangeMetadata<Comparable> fileColumnRange1 = HoodieColumnRangeMetadata.<Comparable>create(
        "path/to/file1", COLUMN_NAME, 1, null, 0, 10, 100, 200, HoodieColumnRangeMetadata.NoneMetadata.INSTANCE, indexVersion);
    HoodieColumnRangeMetadata<Comparable> fileColumnRange2 = HoodieColumnRangeMetadata.<Comparable>create(
        "path/to/file2", COLUMN_NAME, null, 8, 1, 15, 120, 250, HoodieColumnRangeMetadata.NoneMetadata.INSTANCE, indexVersion);

    List<HoodieColumnRangeMetadata<Comparable>> fileColumnRanges = Arrays.asList(fileColumnRange1, fileColumnRange2);
    // Step 2: Call the Method
    HoodieColumnRangeMetadata<Comparable> result = FileFormatUtils.getColumnRangeInPartition(PARTITION_PATH, COLUMN_NAME, fileColumnRanges, Collections.emptyMap(), indexVersion);
    // Step 3: Assertions
    assertEquals(PARTITION_PATH, result.getFilePath());
    assertEquals(COLUMN_NAME, result.getColumnName());
    assertEquals(Integer.valueOf(1), new Integer(result.getMinValue().toString()));
    assertEquals(Integer.valueOf(8), new Integer(result.getMaxValue().toString()));
    assertEquals(1, result.getNullCount());
    assertEquals(25, result.getValueCount());
    assertEquals(220, result.getTotalSize());
    assertEquals(450, result.getTotalUncompressedSize());
  }

  @Test
  public void testGetColumnRangeInPartitionWithDifferentColumnNameThrowsException() {
    HoodieIndexVersion indexVersion = HoodieIndexVersion.V1;
    // Step 1: Set Up Test Data
    HoodieColumnRangeMetadata<Comparable> fileColumnRange1 = HoodieColumnRangeMetadata.<Comparable>create(
        "path/to/file1", "columnName1", 1, null, 0, 10, 100, 200, HoodieColumnRangeMetadata.NoneMetadata.INSTANCE, indexVersion);
    HoodieColumnRangeMetadata<Comparable> fileColumnRange2 = HoodieColumnRangeMetadata.<Comparable>create(
        "path/to/file2", "columnName2", null, 8, 1, 15, 120, 250, HoodieColumnRangeMetadata.NoneMetadata.INSTANCE, indexVersion);
    List<HoodieColumnRangeMetadata<Comparable>> fileColumnRanges = Arrays.asList(fileColumnRange1, fileColumnRange2);
    // Step 2: Call the Method
    assertThrows(IllegalArgumentException.class, () -> FileFormatUtils.getColumnRangeInPartition(PARTITION_PATH, COLUMN_NAME, fileColumnRanges,
        Collections.emptyMap(), indexVersion));
  }
}
