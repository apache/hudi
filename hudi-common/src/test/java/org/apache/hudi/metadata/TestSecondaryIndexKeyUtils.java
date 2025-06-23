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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestSecondaryIndexKeyUtils {

  // Test basic functionality with normal strings
  @Test
  public void testBasicFunctionality() {
    String secondaryKey = "user_id";
    String recordKey = "record_123";
    
    String constructedKey = SecondaryIndexKeyUtils.constructSecondaryIndexKey(secondaryKey, recordKey);
    assertEquals("user_id$record_123", constructedKey);
    
    String extractedSecondaryKey = SecondaryIndexKeyUtils.getSecondaryKeyFromSecondaryIndexKey(constructedKey);
    assertEquals(secondaryKey, extractedSecondaryKey);
    
    String extractedRecordKey = SecondaryIndexKeyUtils.getRecordKeyFromSecondaryIndexKey(constructedKey);
    assertEquals(recordKey, extractedRecordKey);
  }

  // Test null string handling
  @Test
  public void testNullStringHandling() {
    // Test with null secondary key
    String constructedKey1 = SecondaryIndexKeyUtils.constructSecondaryIndexKey(null, "record_123");
    assertEquals("\0$record_123", constructedKey1);
    
    String extractedSecondaryKey1 = SecondaryIndexKeyUtils.getSecondaryKeyFromSecondaryIndexKey(constructedKey1);
    assertNull(extractedSecondaryKey1);
    
    String extractedRecordKey1 = SecondaryIndexKeyUtils.getRecordKeyFromSecondaryIndexKey(constructedKey1);
    assertEquals("record_123", extractedRecordKey1);
    
    // Test with null record key
    String constructedKey2 = SecondaryIndexKeyUtils.constructSecondaryIndexKey("user_id", null);
    assertEquals("user_id$\0", constructedKey2);
    
    String extractedSecondaryKey2 = SecondaryIndexKeyUtils.getSecondaryKeyFromSecondaryIndexKey(constructedKey2);
    assertEquals("user_id", extractedSecondaryKey2);
    
    String extractedRecordKey2 = SecondaryIndexKeyUtils.getRecordKeyFromSecondaryIndexKey(constructedKey2);
    assertNull(extractedRecordKey2);
    
    // Test with both null
    String constructedKey3 = SecondaryIndexKeyUtils.constructSecondaryIndexKey(null, null);
    assertEquals("\0$\0", constructedKey3);
    
    String extractedSecondaryKey3 = SecondaryIndexKeyUtils.getSecondaryKeyFromSecondaryIndexKey(constructedKey3);
    assertNull(extractedSecondaryKey3);
    
    String extractedRecordKey3 = SecondaryIndexKeyUtils.getRecordKeyFromSecondaryIndexKey(constructedKey3);
    assertNull(extractedRecordKey3);
  }

  // Test strings containing null characters
  @Test
  public void testStringsWithNullCharacters() {
    String secondaryKey = "user\0id";
    String recordKey = "record\0123";
    
    String constructedKey = SecondaryIndexKeyUtils.constructSecondaryIndexKey(secondaryKey, recordKey);
    assertEquals("user\\0id$record\\0123", constructedKey);
    
    String extractedSecondaryKey = SecondaryIndexKeyUtils.getSecondaryKeyFromSecondaryIndexKey(constructedKey);
    assertEquals(secondaryKey, extractedSecondaryKey);
    
    String extractedRecordKey = SecondaryIndexKeyUtils.getRecordKeyFromSecondaryIndexKey(constructedKey);
    assertEquals(recordKey, extractedRecordKey);
  }

  // Test strings containing escape characters
  @Test
  public void testStringsWithEscapeCharacters() {
    String secondaryKey = "user\\id";
    String recordKey = "record\\123";
    
    String constructedKey = SecondaryIndexKeyUtils.constructSecondaryIndexKey(secondaryKey, recordKey);
    assertEquals("user\\\\id$record\\\\123", constructedKey);
    
    String extractedSecondaryKey = SecondaryIndexKeyUtils.getSecondaryKeyFromSecondaryIndexKey(constructedKey);
    assertEquals(secondaryKey, extractedSecondaryKey);
    
    String extractedRecordKey = SecondaryIndexKeyUtils.getRecordKeyFromSecondaryIndexKey(constructedKey);
    assertEquals(recordKey, extractedRecordKey);
  }

  // Test strings containing dollar signs
  @Test
  public void testStringsWithDollarSigns() {
    String secondaryKey = "user$id";
    String recordKey = "record$123";
    
    String constructedKey = SecondaryIndexKeyUtils.constructSecondaryIndexKey(secondaryKey, recordKey);
    assertEquals("user\\$id$record\\$123", constructedKey);
    
    String extractedSecondaryKey = SecondaryIndexKeyUtils.getSecondaryKeyFromSecondaryIndexKey(constructedKey);
    assertEquals(secondaryKey, extractedSecondaryKey);
    
    String extractedRecordKey = SecondaryIndexKeyUtils.getRecordKeyFromSecondaryIndexKey(constructedKey);
    assertEquals(recordKey, extractedRecordKey);
  }

  // Test strings with all special characters
  @Test
  public void testStringsWithAllSpecialCharacters() {
    String secondaryKey = "user\\$id\0";
    String recordKey = "record\\$123\0";
    
    String constructedKey = SecondaryIndexKeyUtils.constructSecondaryIndexKey(secondaryKey, recordKey);
    assertEquals("user\\\\\\$id\\0$record\\\\\\$123\\0", constructedKey);
    
    String extractedSecondaryKey = SecondaryIndexKeyUtils.getSecondaryKeyFromSecondaryIndexKey(constructedKey);
    assertEquals(secondaryKey, extractedSecondaryKey);
    
    String extractedRecordKey = SecondaryIndexKeyUtils.getRecordKeyFromSecondaryIndexKey(constructedKey);
    assertEquals(recordKey, extractedRecordKey);
  }

  // Test empty strings
  @Test
  public void testEmptyStrings() {
    String secondaryKey = "";
    String recordKey = "";
    
    String constructedKey = SecondaryIndexKeyUtils.constructSecondaryIndexKey(secondaryKey, recordKey);
    assertEquals("$", constructedKey);
    
    String extractedSecondaryKey = SecondaryIndexKeyUtils.getSecondaryKeyFromSecondaryIndexKey(constructedKey);
    assertEquals(secondaryKey, extractedSecondaryKey);
    
    String extractedRecordKey = SecondaryIndexKeyUtils.getRecordKeyFromSecondaryIndexKey(constructedKey);
    assertEquals(recordKey, extractedRecordKey);
  }

  // Test edge cases with complex escaping
  @Test
  public void testComplexEscaping() {
    // Test with multiple consecutive escape characters
    String secondaryKey = "user\\\\id";
    String recordKey = "record\\\\123";
    
    String constructedKey = SecondaryIndexKeyUtils.constructSecondaryIndexKey(secondaryKey, recordKey);
    assertEquals("user\\\\\\\\id$record\\\\\\\\123", constructedKey);
    
    String extractedSecondaryKey = SecondaryIndexKeyUtils.getSecondaryKeyFromSecondaryIndexKey(constructedKey);
    assertEquals(secondaryKey, extractedSecondaryKey);
    
    String extractedRecordKey = SecondaryIndexKeyUtils.getRecordKeyFromSecondaryIndexKey(constructedKey);
    assertEquals(recordKey, extractedRecordKey);
  }

  // Test with Unicode characters
  @Test
  public void testUnicodeCharacters() {
    String secondaryKey = "用户ID";
    String recordKey = "记录123";
    
    String constructedKey = SecondaryIndexKeyUtils.constructSecondaryIndexKey(secondaryKey, recordKey);
    assertEquals("用户ID$记录123", constructedKey);
    
    String extractedSecondaryKey = SecondaryIndexKeyUtils.getSecondaryKeyFromSecondaryIndexKey(constructedKey);
    assertEquals(secondaryKey, extractedSecondaryKey);
    
    String extractedRecordKey = SecondaryIndexKeyUtils.getRecordKeyFromSecondaryIndexKey(constructedKey);
    assertEquals(recordKey, extractedRecordKey);
  }

  // Test with Unicode characters and special characters
  @Test
  public void testUnicodeWithSpecialCharacters() {
    String secondaryKey = "用户\\$ID\0";
    String recordKey = "记录\\$123\0";
    
    String constructedKey = SecondaryIndexKeyUtils.constructSecondaryIndexKey(secondaryKey, recordKey);
    assertEquals("用户\\\\\\$ID\\0$记录\\\\\\$123\\0", constructedKey);
    
    String extractedSecondaryKey = SecondaryIndexKeyUtils.getSecondaryKeyFromSecondaryIndexKey(constructedKey);
    assertEquals(secondaryKey, extractedSecondaryKey);
    
    String extractedRecordKey = SecondaryIndexKeyUtils.getRecordKeyFromSecondaryIndexKey(constructedKey);
    assertEquals(recordKey, extractedRecordKey);
  }

  // Test error cases
  @ParameterizedTest
  @ValueSource(strings = {
      "",           // Empty string
      "no_delimiter", // No delimiter
      "$",          // Only delimiter
      "key$",       // Delimiter at end
      "$key",       // Delimiter at start
      "key\\$key",  // Escaped delimiter
      "key\\\\$key" // Multiple escaped characters
  })
  public void testInvalidKeyFormats(String invalidKey) {
    assertThrows(IllegalStateException.class, () -> {
      SecondaryIndexKeyUtils.getSecondaryKeyFromSecondaryIndexKey(invalidKey);
    });
    
    assertThrows(IllegalStateException.class, () -> {
      SecondaryIndexKeyUtils.getRecordKeyFromSecondaryIndexKey(invalidKey);
    });
  }

  // Test round-trip consistency with various inputs
  @ParameterizedTest(name = "Round-trip test: secondaryKey='{0}', recordKey='{1}'")
  @MethodSource("roundTripTestCases")
  public void testRoundTripConsistency(String secondaryKey, String recordKey) {
    String constructedKey = SecondaryIndexKeyUtils.constructSecondaryIndexKey(secondaryKey, recordKey);
    
    String extractedSecondaryKey = SecondaryIndexKeyUtils.getSecondaryKeyFromSecondaryIndexKey(constructedKey);
    String extractedRecordKey = SecondaryIndexKeyUtils.getRecordKeyFromSecondaryIndexKey(constructedKey);
    
    assertEquals(secondaryKey, extractedSecondaryKey);
    assertEquals(recordKey, extractedRecordKey);
  }

  private static Stream<Arguments> roundTripTestCases() {
    return Stream.of(
        // Normal cases
        Arguments.of("user_id", "record_123"),
        Arguments.of("", ""),
        Arguments.of("simple", "simple"),
        
        // Null cases
        Arguments.of(null, "record_123"),
        Arguments.of("user_id", null),
        Arguments.of(null, null),
        
        // Special characters
        Arguments.of("user\\id", "record\\123"),
        Arguments.of("user$id", "record$123"),
        Arguments.of("user\0id", "record\0123"),
        Arguments.of("user\\$id\0", "record\\$123\0"),
        
        // Complex cases
        Arguments.of("user\\\\id", "record\\\\123"),
        Arguments.of("user\\$id\\0", "record\\$123\\0"),
        Arguments.of("\\$\\0\\", "\\$\\0\\"),
        
        // Unicode cases
        Arguments.of("用户ID", "记录123"),
        Arguments.of("用户\\$ID\0", "记录\\$123\0"),
        
        // Edge cases
        Arguments.of("\\", "\\"),
        Arguments.of("$", "$"),
        Arguments.of("\0", "\0"),
        Arguments.of("\\$\\0", "\\$\\0"),
        Arguments.of("\\\\\\$\\0", "\\\\\\$\\0")
    );
  }

  // Test specific null character handling scenarios
  @Test
  public void testNullCharacterHandling() {
    // Test that a single null character represents null
    String singleNullChar = "\0";
    String extracted = SecondaryIndexKeyUtils.getSecondaryKeyFromSecondaryIndexKey(singleNullChar + "$record");
    assertNull(extracted);
    
    extracted = SecondaryIndexKeyUtils.getRecordKeyFromSecondaryIndexKey("secondary$" + singleNullChar);
    assertNull(extracted);
    
    // Test that escaped null character is preserved
    String escapedNull = "\\0";
    String constructedKey = SecondaryIndexKeyUtils.constructSecondaryIndexKey(escapedNull, "record");
    assertEquals("\\0$record", constructedKey);
    
    extracted = SecondaryIndexKeyUtils.getSecondaryKeyFromSecondaryIndexKey(constructedKey);
    assertEquals("\0", extracted); // Should be the actual null character, not null
    
    // Test multiple null characters
    String multipleNulls = "\0\0\0";
    constructedKey = SecondaryIndexKeyUtils.constructSecondaryIndexKey(multipleNulls, "record");
    assertEquals("\\0\\0\\0$record", constructedKey);
    
    extracted = SecondaryIndexKeyUtils.getSecondaryKeyFromSecondaryIndexKey(constructedKey);
    assertEquals(multipleNulls, extracted);
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
    assertEquals("\\\\\\$\\0$\\\\\\$\\0", constructedKey);
    
    extractedSecondaryKey = SecondaryIndexKeyUtils.getSecondaryKeyFromSecondaryIndexKey(constructedKey);
    assertEquals(onlySpecialChars, extractedSecondaryKey);
    
    extractedRecordKey = SecondaryIndexKeyUtils.getRecordKeyFromSecondaryIndexKey(constructedKey);
    assertEquals(onlySpecialChars, extractedRecordKey);
  }

  // Test that the separator constant is used correctly
  @Test
  public void testSeparatorConstantUsage() {
    // Verify that the separator used in the key matches the constant
    String secondaryKey = "test";
    String recordKey = "test";
    
    String constructedKey = SecondaryIndexKeyUtils.constructSecondaryIndexKey(secondaryKey, recordKey);
    assertTrue(constructedKey.contains(HoodieMetadataPayload.SECONDARY_INDEX_RECORD_KEY_SEPARATOR));
    
    // Verify that the separator is exactly one character
    assertEquals(1, HoodieMetadataPayload.SECONDARY_INDEX_RECORD_KEY_SEPARATOR.length());
  }
} 