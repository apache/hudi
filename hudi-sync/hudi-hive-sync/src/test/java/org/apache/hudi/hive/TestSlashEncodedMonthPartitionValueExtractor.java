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

package org.apache.hudi.hive;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestSlashEncodedMonthPartitionValueExtractor {

  @Test
  public void testExtractPartitionValuesInPath_ValidPath() {
    // Given
    String validPartitionPath = "2021/07";
    SlashEncodedYearMonthPartitionValueExtractor extractor = new SlashEncodedYearMonthPartitionValueExtractor();

    // When
    List<String> result = extractor.extractPartitionValuesInPath(validPartitionPath);

    // Then
    assertEquals(Collections.singletonList("2021-07"), result, "The extractor should correctly format the partition path.");
  }

  @Test
  public void testExtractPartitionValuesInPath_InvalidPath() {
    // Given
    String invalidPartitionPath = "2021";
    SlashEncodedYearMonthPartitionValueExtractor extractor = new SlashEncodedYearMonthPartitionValueExtractor();

    // Then
    assertThrows(IllegalArgumentException.class, () -> {
      // When
      extractor.extractPartitionValuesInPath(invalidPartitionPath);
    }, "The extractor should throw an IllegalArgumentException for an invalid partition path.");
  }
}
