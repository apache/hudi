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

package org.apache.hudi.common.util;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * Tests numeric utils.
 */
public class TestNumericUtils {

  @Test
  public void testHumanReadableByteCount() {
    assertEquals("0.0 B", NumericUtils.humanReadableByteCount(0));
    assertEquals("27.0 B", NumericUtils.humanReadableByteCount(27));
    assertEquals("1023.0 B", NumericUtils.humanReadableByteCount(1023));
    assertEquals("1.0 KB", NumericUtils.humanReadableByteCount(1024));
    assertEquals("108.0 KB", NumericUtils.humanReadableByteCount(110592));
    assertEquals("27.0 GB", NumericUtils.humanReadableByteCount(28991029248L));
    assertEquals("1.7 TB", NumericUtils.humanReadableByteCount(1855425871872L));
    assertEquals("8.0 EB", NumericUtils.humanReadableByteCount(9223372036854775807L));
  }

  @Test
  public void testGetMessageDigestHash() {
    assertEquals(6808551913422584641L, NumericUtils.getMessageDigestHash("MD5", "This is a string"));
    assertEquals(2549749777095932358L, NumericUtils.getMessageDigestHash("MD5", "This is a test string"));
    assertNotEquals(1L, NumericUtils.getMessageDigestHash("MD5", "This"));
    assertNotEquals(6808551913422584641L, NumericUtils.getMessageDigestHash("SHA-256", "This is a string"));
  }

  private static byte[] byteArrayWithNum(int size, int num) {
    byte[] bytez = new byte[size];
    Arrays.fill(bytez, (byte) num);
    return bytez;
  }

  @Test
  public void testPadToLong() {
    assertEquals(0x0000000099999999L, NumericUtils.padToLong(byteArrayWithNum(4, 0x99)));
    assertEquals(0x0000999999999999L, NumericUtils.padToLong(byteArrayWithNum(6, 0x99)));
    assertEquals(0x9999999999999999L, NumericUtils.padToLong(byteArrayWithNum(8, 0x99)));
    assertEquals(0x1111111111111111L, NumericUtils.padToLong(byteArrayWithNum(8, 0x11)));
    assertEquals(0x0000000011111111L, NumericUtils.padToLong(byteArrayWithNum(4, 0x11)));
    assertEquals(0x0000181818181818L, NumericUtils.padToLong(byteArrayWithNum(6, 0x18)));
  }
}
