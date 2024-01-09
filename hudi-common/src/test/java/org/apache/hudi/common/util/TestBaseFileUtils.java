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

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestBaseFileUtils {

  @Test
  public void testGetColumnRangeInPartition() {
    // Step 1: Set Up Test Data
    HoodieColumnRangeMetadata<Comparable> fileRange1 = HoodieColumnRangeMetadata.<Comparable>create(
        "path/to/file1", "columnName", 1, 5, 0, 10, 100, 200);
    HoodieColumnRangeMetadata<Comparable> fileRange2 = HoodieColumnRangeMetadata.<Comparable>create(
        "path/to/file2", "columnName", 3, 8, 1, 15, 120, 250);

    List<HoodieColumnRangeMetadata<Comparable>> fileRanges = Arrays.asList(fileRange1, fileRange2);

    // Step 2: Call the Method
    HoodieColumnRangeMetadata<Comparable> result = BaseFileUtils.getColumnRangeInPartition(fileRanges);

    // Step 3: Assertions
    assertEquals(Integer.valueOf(1), new Integer(result.getMinValue().toString()));
    assertEquals(Integer.valueOf(8), new Integer(result.getMaxValue().toString()));
    assertEquals(1, result.getNullCount());
    assertEquals(25, result.getValueCount());
    assertEquals(220, result.getTotalSize());
    assertEquals(450, result.getTotalUncompressedSize());
  }
}
