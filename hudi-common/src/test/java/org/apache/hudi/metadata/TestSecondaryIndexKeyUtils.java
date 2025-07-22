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

package org.apache.hudi.metadata;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestSecondaryIndexKeyUtils {

  // Test round-trip consistency with various inputs
  @ParameterizedTest(name = "Round-trip test: secondaryKey='{0}', recordKey='{1}', expectedKey='{2}'")
  @MethodSource("roundTripTestCases")
  public void testRoundTripConsistency(String secondaryKey, String recordKey, String expectedConstructedKey) {
    String constructedKey = SecondaryIndexKeyUtils.constructSecondaryIndexKey(secondaryKey, recordKey);
    assertEquals(expectedConstructedKey, constructedKey);
    
    String extractedSecondaryKey = SecondaryIndexKeyUtils.getSecondaryKeyFromSecondaryIndexKey(constructedKey);
    String extractedRecordKey = SecondaryIndexKeyUtils.getRecordKeyFromSecondaryIndexKey(constructedKey);
    
    assertEquals(secondaryKey, extractedSecondaryKey);
    assertEquals(recordKey, extractedRecordKey);
  }

  @SuppressWarnings("checkstyle:IllegalTokenText")
  private static Stream<Arguments> roundTripTestCases() {
    return Stream.of(
        // Normal cases
        Arguments.of("user_id", "record_123", "user_id$record_123"),
        Arguments.of("", "", "$"),
        Arguments.of("simple", "simple", "simple$simple"),

        // Empty string cases
        Arguments.of("", "record_123", "$record_123"),
        Arguments.of("user_id", "", "user_id$"),

        // Special characters
        Arguments.of("user\\id", "record\\123", "user\\\\id$record\\\\123"),
        Arguments.of("user$id", "record$123", "user\\$id$record\\$123"),

        // Complex cases
        Arguments.of("user\\\\id", "record\\\\123", "user\\\\\\\\id$record\\\\\\\\123"),
        Arguments.of("user\\$id\\0", "record\\$123\\0", "user\\\\\\$id\\\\0$record\\\\\\$123\\\\0"),
        Arguments.of("\\$\\0\\", "\\$\\0\\", "\\\\\\$\\\\0\\\\$\\\\\\$\\\\0\\\\"),

        // Unicode cases
        Arguments.of("用户ID", "记录123", "用户ID$记录123"),

        // Edge cases
        Arguments.of("\\", "\\", "\\\\$\\\\"),
        Arguments.of("\0123", "\0123", "\0123$\0123"), // \012 is read as a single char \n, not \0 + 12
        Arguments.of("user\\$id", "rec\\$123", "user\\\\\\$id$rec\\\\\\$123"),
        Arguments.of("$", "$", "\\$$\\$"),
        Arguments.of("\\0", "\\0", "\\\\0$\\\\0"),
        Arguments.of("\\$\\0", "\\$\\0", "\\\\\\$\\\\0$\\\\\\$\\\\0"),
        Arguments.of("\\\\\\$\\0", "\\\\\\$\\0", "\\\\\\\\\\\\\\$\\\\0$\\\\\\\\\\\\\\$\\\\0")
    );
  }

  // Test error cases
  @ParameterizedTest(name = "Invalid key format: '{0}'")
  @ValueSource(strings = {
      "",           // Empty string
      "no_delimiter", // No delimiter
      "\0", // No delimiter
      "\\\0", // No delimiter
      "\\\\", // No delimiter
      "\\", // No delimiter
      "key\\$key",  // Escaped delimiter
      "key\\\\\\$key" // Multiple escaped characters
  })
  public void testInvalidKeyFormats(String invalidKey) {
    assertThrows(IllegalStateException.class, () -> {
      SecondaryIndexKeyUtils.getSecondaryKeyFromSecondaryIndexKey(invalidKey);
    });
    
    assertThrows(IllegalStateException.class, () -> {
      SecondaryIndexKeyUtils.getRecordKeyFromSecondaryIndexKey(invalidKey);
    });
  }

  // Test boundary conditions
  @Test
  public void testBoundaryConditions() {
    // Test with very long strings
    StringBuilder longSecondaryKeyBuilder = new StringBuilder();
    StringBuilder longRecordKeyBuilder = new StringBuilder();
    for (int i = 0; i < 1000; i++) {
      longSecondaryKeyBuilder.append('a');
      longRecordKeyBuilder.append('b');
    }
    String longSecondaryKey = longSecondaryKeyBuilder.toString();
    String longRecordKey = longRecordKeyBuilder.toString();
    
    String constructedKey = SecondaryIndexKeyUtils.constructSecondaryIndexKey(longSecondaryKey, longRecordKey);
    assertEquals(longSecondaryKey + "$" + longRecordKey, constructedKey);
    
    String extractedSecondaryKey = SecondaryIndexKeyUtils.getSecondaryKeyFromSecondaryIndexKey(constructedKey);
    assertEquals(longSecondaryKey, extractedSecondaryKey);
    
    String extractedRecordKey = SecondaryIndexKeyUtils.getRecordKeyFromSecondaryIndexKey(constructedKey);
    assertEquals(longRecordKey, extractedRecordKey);
    
    // Test with strings containing only special characters
    String onlySpecialChars = "\\$\\0";
    constructedKey = SecondaryIndexKeyUtils.constructSecondaryIndexKey(onlySpecialChars, onlySpecialChars);
    assertEquals("\\\\\\$\\\\0$\\\\\\$\\\\0", constructedKey);
    
    extractedSecondaryKey = SecondaryIndexKeyUtils.getSecondaryKeyFromSecondaryIndexKey(constructedKey);
    assertEquals(onlySpecialChars, extractedSecondaryKey);
    
    extractedRecordKey = SecondaryIndexKeyUtils.getRecordKeyFromSecondaryIndexKey(constructedKey);
    assertEquals(onlySpecialChars, extractedRecordKey);
  }
} 