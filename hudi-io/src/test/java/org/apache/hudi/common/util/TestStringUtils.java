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

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.apache.hudi.common.util.StringUtils.concatenateWithThreshold;
import static org.apache.hudi.common.util.StringUtils.getUTF8Bytes;
import static org.apache.hudi.common.util.StringUtils.toStringWithThreshold;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link StringUtils}.
 */
public class TestStringUtils {

  private static final String[] STRINGS = {"This", "is", "a", "test"};

  private static final String CHARACTERS_FOR_RANDOM_GEN = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_/:";
  private static final Random RANDOM = new SecureRandom();

  private static String toHexString(byte[] bytes) {
    StringBuilder sb = new StringBuilder(bytes.length * 2);
    for (byte b : bytes) {
      sb.append(String.format("%02x", b));
    }
    return sb.toString();
  }

  public static String generateRandomString(int length) {
    if (length < 1) {
      throw new IllegalArgumentException("Length must be greater than 0");
    }
    StringBuilder builder = new StringBuilder(length);
    for (int i = 0; i < length; i++) {
      int randomIndex = RANDOM.nextInt(CHARACTERS_FOR_RANDOM_GEN.length());
      builder.append(CHARACTERS_FOR_RANDOM_GEN.charAt(randomIndex));
    }
    return new String(getUTF8Bytes(builder.toString()), StandardCharsets.UTF_8);
  }

  @Test
  public void testStringJoinWithDelim() {
    String joinedString = StringUtils.joinUsingDelim("-", STRINGS);
    assertEquals(STRINGS.length, joinedString.split("-").length);
  }

  @Test
  public void testStringJoin() {
    assertNotEquals(null, StringUtils.join(""));
    assertNotEquals(null, StringUtils.join(STRINGS));
  }

  @Test
  public void testStringJoinWithMap() {
    Map<String, Object> map = new HashMap<>();
    map.put("a", 1);
    map.put("b", true);
    assertNotNull(StringUtils.join(map));
    assertEquals("{a=1, b=true}", StringUtils.join(map));
  }

  @Test
  public void testStringJoinWithJavaImpl() {
    assertNull(StringUtils.join(",", null));
    assertEquals("", String.join(",", Collections.singletonList("")));
    assertEquals(",", String.join(",", Arrays.asList("", "")));
    assertEquals("a,", String.join(",", Arrays.asList("a", "")));
  }

  @Test
  public void testStringNullToEmpty() {
    String str = "This is a test";
    assertEquals(str, StringUtils.nullToEmpty(str));
    assertEquals("", StringUtils.nullToEmpty(null));
  }

  @Test
  public void testStringObjToString() {
    assertNull(StringUtils.objToString(null));
    assertEquals("Test String", StringUtils.objToString("Test String"));

    // assert byte buffer
    ByteBuffer byteBuffer1 = ByteBuffer.wrap(getUTF8Bytes("1234"));
    ByteBuffer byteBuffer2 = ByteBuffer.wrap(getUTF8Bytes("5678"));
    // assert equal because ByteBuffer has overwritten the toString to return a summary string
    assertEquals(byteBuffer1.toString(), byteBuffer2.toString());
    // assert not equal
    assertNotEquals(StringUtils.objToString(byteBuffer1), StringUtils.objToString(byteBuffer2));
  }

  @Test
  public void testStringEmptyToNull() {
    assertNull(StringUtils.emptyToNull(""));
    assertEquals("Test String", StringUtils.emptyToNull("Test String"));
  }

  @Test
  public void testStringNullOrEmpty() {
    assertTrue(StringUtils.isNullOrEmpty(null));
    assertTrue(StringUtils.isNullOrEmpty(""));
    assertNotEquals(null, StringUtils.isNullOrEmpty("this is not empty"));
    assertTrue(StringUtils.isNullOrEmpty(""));
  }

  @Test
  public void testSplit() {
    assertEquals(new ArrayList<>(), StringUtils.split(null, ","));
    assertEquals(new ArrayList<>(), StringUtils.split("", ","));
    assertEquals(Arrays.asList("a", "b", "c"), StringUtils.split("a,b, c", ","));
    assertEquals(Arrays.asList("a", "b", "c"), StringUtils.split("a,b,, c ", ","));
  }

  @Test
  public void testHexString() {
    String str = "abcd";
    assertEquals(StringUtils.toHexString(getUTF8Bytes(str)), toHexString(getUTF8Bytes(str)));
  }

  @Test
  public void testTruncate() {
    assertNull(StringUtils.truncate(null, 10, 10));
    assertEquals("http://use...ons/latest", StringUtils.truncate("http://username:password@myregistry.com:5000/versions/latest", 10, 10));
    assertEquals("http://abc.com", StringUtils.truncate("http://abc.com", 10, 10));
  }

  @Test
  public void testCompareVersions() {
    assertTrue(StringUtils.compareVersions("1.10", "1.9") > 0);
    assertTrue(StringUtils.compareVersions("1.9", "1.10") < 0);
    assertTrue(StringUtils.compareVersions("1.100.1", "1.10") > 0);
    assertTrue(StringUtils.compareVersions("1.10.1", "1.10") > 0);
    assertEquals(0, StringUtils.compareVersions("1.10", "1.10"));
  }

  @Test
  void testConcatenateWithinThreshold() {
    String a = generateRandomString(1000); // 1000 bytes in UTF-8
    String b = generateRandomString(1048); // 1048 bytes in UTF-8
    int threshold = 2048;

    // The total length of bytes of `a` + `b` exceeds the threshold
    String result = StringUtils.concatenateWithThreshold(a, b, threshold);

    // The resulting string should be exactly `threshold` bytes long
    assertEquals(threshold, getUTF8Bytes(result).length);
    assertEquals(a + b, result);

    // Test case when a + b is within the threshold
    String a2 = generateRandomString(900);
    String b2 = generateRandomString(1000);
    String result2 = concatenateWithThreshold(a2, b2, threshold);

    // The resulting string should be `a2 + b2`
    assertEquals(a2 + b2, result2);
  }

  @Test
  void testConcatenateInvalidInput() {
    // Test case when b alone exceeds the threshold
    String a = generateRandomString(900);
    String b = generateRandomString(3000); // 3000 bytes in UTF-8
    Exception exception = assertThrows(IllegalArgumentException.class, () -> {
      concatenateWithThreshold(a, b, 2048);
    });

    String expectedMessage = "Length of the Second string to concatenate exceeds the threshold (3000 > 2048)";
    String actualMessage = exception.getMessage();

    assertTrue(actualMessage.contains(expectedMessage));
  }

  @Test
  void testConcatenateTruncateCase() {
    // 'é' is 2 bytes
    assertEquals("ad", concatenateWithThreshold("aé", "d", 3));
    // Chinese chars are 3 bytes
    assertEquals("世d", concatenateWithThreshold("世界", "d", 4));
    assertEquals("ad", concatenateWithThreshold("ab", "d", 2));
  }

  @Test
  void testGenerateInvalidRandomString() {
    assertThrows(
        IllegalArgumentException.class,
        () -> generateRandomString(-1)
    );
  }

  @Test
  void testToStringWithThreshold() {
    String str1 = "string_value1";
    String str2 = "string_value2";
    String str3 = "string_value3";
    assertEquals("",
        toStringWithThreshold(null, 10));
    assertEquals("",
        toStringWithThreshold(Collections.emptyList(), 10));
    assertEquals("..",
        toStringWithThreshold(Collections.singletonList(str1), 2));
    assertEquals("string_...",
        toStringWithThreshold(Collections.singletonList(str1), str1.length() - 3));
    assertEquals("[string_value1]",
        toStringWithThreshold(Collections.singletonList(str1), 0));
    assertEquals(str1,
        toStringWithThreshold(Collections.singletonList(str1), str1.length()));
    assertEquals(str1,
        toStringWithThreshold(Collections.singletonList(str1), str1.length() + 10));
    List<String> stringList = new ArrayList<>();
    stringList.add(str1);
    stringList.add(str2);
    stringList.add(str3);
    assertEquals("string_val...",
        toStringWithThreshold(stringList, str1.length()));
    assertEquals("string_valu...",
        toStringWithThreshold(stringList, str1.length() + 1));
    assertEquals("string_value1,string...",
        toStringWithThreshold(stringList, str1.length() + str2.length() - 3));
    assertEquals("string_value1,string_v...",
        toStringWithThreshold(stringList, str1.length() + str2.length() - 1));
    assertEquals("string_value1,string_value2,strin...",
        toStringWithThreshold(stringList, str1.length() + str2.length() + str3.length() - 3));
    assertEquals("string_value1,string_value2,string_value3",
        toStringWithThreshold(stringList, str1.length() + str2.length() + str3.length() + 2));
    assertEquals("[string_value1, string_value2, string_value3]",
        toStringWithThreshold(stringList, - 1));
  }

  @Test
  public void testStripEnd() {
    assertNull(StringUtils.stripEnd(null, "ab"));
    assertEquals("", StringUtils.stripEnd("", "ab"));
    assertEquals("abc", StringUtils.stripEnd("abc", null));
    assertEquals("abc", StringUtils.stripEnd("abc  ", null));
    assertEquals("abc", StringUtils.stripEnd("abc", ""));
    assertEquals("abc", StringUtils.stripEnd("abcabab", "ab"));
  }
}
