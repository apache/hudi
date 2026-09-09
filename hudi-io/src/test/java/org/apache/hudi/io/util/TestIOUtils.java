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

package org.apache.hudi.io.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests {@link IOUtils}.
 */
public class TestIOUtils {
  private static final byte[] BYTE_ARRAY = new byte[] {
      (byte) 0xc8, 0x36, 0x09, (byte) 0xf2, (byte) 0xa5, 0x7d, 0x01, (byte) 0x48,
      (byte) 0x89, 0x66};

  @Test
  public void testReadInt() {
    assertEquals(-935982606, IOUtils.readInt(BYTE_ARRAY, 0));
    assertEquals(906621605, IOUtils.readInt(BYTE_ARRAY, 1));
    assertEquals(166897021, IOUtils.readInt(BYTE_ARRAY, 2));
  }

  @Test
  public void testReadLong() {
    assertEquals(-4020014679618420408L, IOUtils.readLong(BYTE_ARRAY, 0));
    assertEquals(3893910145419266185L, IOUtils.readLong(BYTE_ARRAY, 1));
    assertEquals(716817247016356198L, IOUtils.readLong(BYTE_ARRAY, 2));
  }

  @Test
  public void testReadShort() {
    assertEquals(-14282, IOUtils.readShort(BYTE_ARRAY, 0));
    assertEquals(13833, IOUtils.readShort(BYTE_ARRAY, 1));
    assertEquals(2546, IOUtils.readShort(BYTE_ARRAY, 2));
  }

  private static Stream<Arguments> decodeVariableLengthNumberParams() {
    // preserveMetaField, partitioned
    Object[][] data = new Object[][] {
        {new byte[] {0}, 0},
        {new byte[] {-108}, -108},
        {new byte[] {98}, 98},
        {new byte[] {-113, -48}, 208},
        {new byte[] {-114, 125, 80}, 32080},
        {new byte[] {-115, 31, 13, 14}, 2034958},
        {new byte[] {-121, -54}, -203},
        {new byte[] {-116, 37, -77, 17, 62}, 632492350},
        {new byte[] {-124, 1, -10, 100, -127}, -32924802},
        {new byte[] {-116, 127, -1, -1, -1}, Integer.MAX_VALUE},
        {new byte[] {-124, 127, -1, -1, -1}, Integer.MIN_VALUE},
        {new byte[] {-118, 20, -17, -92, -41, 107, -78}, 23019495320498L},
        {new byte[] {-127, 2, -7, -102, -100, -69, -93, -109}, -837392403243924L},
        {new byte[] {-120, 127, -1, -1, -1, -1, -1, -1, -1}, Long.MAX_VALUE},
        {new byte[] {-128, 127, -1, -1, -1, -1, -1, -1, -1}, Long.MIN_VALUE},
    };
    return Stream.of(data).map(Arguments::of);
  }

  @ParameterizedTest
  @MethodSource("decodeVariableLengthNumberParams")
  public void testDecodeVariableLengthNumber(byte[] bytes, long expectedNumber) throws IOException {
    int size = IOUtils.decodeVarLongSizeOnDisk(bytes, 0);
    assertEquals(bytes.length, size);
    assertEquals(bytes.length, IOUtils.decodeVarLongSize(bytes[0]));
    assertEquals(expectedNumber, IOUtils.readVarLong(bytes, 0));
    assertEquals(expectedNumber, IOUtils.readVarLong(bytes, 0, size));
    assertEquals(expectedNumber < 0, IOUtils.isNegativeVarLong(bytes[0]));
  }

  @Test
  public void testByteArrayCompareTo() {
    byte[] bytes1 = new byte[] {(byte) 0x9b, 0, 0x18, 0x65, 0x2e, (byte) 0xf3};
    byte[] bytes2 = new byte[] {(byte) 0x9b, 0, 0x18, 0x65, 0x1c, 0x38, (byte) 0x53};

    assertEquals(0, IOUtils.compareTo(bytes1, 0, 4, bytes1, 0, 4));
    assertEquals(-2, IOUtils.compareTo(bytes1, 0, 4, bytes1, 0, 6));
    assertEquals(1, IOUtils.compareTo(bytes1, 0, 5, bytes1, 0, 4));
    assertEquals(0, IOUtils.compareTo(bytes1, 0, 4, bytes2, 0, 4));
    assertEquals(-2, IOUtils.compareTo(bytes1, 0, 4, bytes2, 0, 6));
    assertEquals(2, IOUtils.compareTo(bytes1, 0, 6, bytes1, 0, 4));
    assertEquals(18, IOUtils.compareTo(bytes1, 0, 5, bytes2, 0, 5));
    assertEquals(18, IOUtils.compareTo(bytes1, 0, 6, bytes2, 0, 6));
    assertEquals(-155, IOUtils.compareTo(bytes1, 1, 4, bytes2, 0, 5));
    assertEquals(22, IOUtils.compareTo(bytes1, 4, 2, bytes2, 2, 4));
  }

  @Test
  public void testIndexOf() {
    byte[] array = new byte[] {(byte) 0x9b, 0, 0x18, 0x65, 0x2e, (byte) 0xf3};
    assertEquals(0, IOUtils.indexOf(array, new byte[] {}));
    assertEquals(0, IOUtils.indexOf(array, new byte[] {(byte) 0x9b, 0}));
    assertEquals(2, IOUtils.indexOf(array, new byte[] {0x18, 0x65, 0x2e}));
    assertEquals(4, IOUtils.indexOf(array, new byte[] {0x2e, (byte) 0xf3}));
    assertEquals(-1, IOUtils.indexOf(array, new byte[] {0x2e, (byte) 0xf3, 0x31}));
    assertEquals(-1, IOUtils.indexOf(array, new byte[] {0x31}));
  }

  @Test
  public void testToBytes() {
    assertArrayEquals(new byte[] {0, 0, 0, 20}, IOUtils.toBytes(20));
    assertArrayEquals(new byte[] {0x02, (byte) 0x93, (byte) 0xed, (byte) 0x88}, IOUtils.toBytes(43249032));
    assertArrayEquals(new byte[] {0x19, (byte) 0x99, (byte) 0x9a, 0x61}, IOUtils.toBytes(Integer.MAX_VALUE / 5 + 200));
    assertArrayEquals(new byte[] {(byte) 0x7f, (byte) 0xff, (byte) 0xff, (byte) 0xff}, IOUtils.toBytes(Integer.MAX_VALUE));
    assertArrayEquals(new byte[] {0, 0, 0, 0, 0, 0, 0, 20}, IOUtils.toBytes(20L));
    assertArrayEquals(new byte[] {0, 0, 0, 0, 0x49, 0x52, 0x45, 0x32}, IOUtils.toBytes(1230128434L));
    assertArrayEquals(
        new byte[] {0x19, (byte) 0x99, (byte) 0x99, (byte) 0x99, (byte) 0x99, (byte) 0x99, (byte) 0x9a, 0x61},
        IOUtils.toBytes(Long.MAX_VALUE / 5 + 200));
    assertArrayEquals(
        new byte[] {(byte) 0x7f, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff},
        IOUtils.toBytes(Long.MAX_VALUE));
  }
}
