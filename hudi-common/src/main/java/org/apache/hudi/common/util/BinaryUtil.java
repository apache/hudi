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

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

import static org.apache.hudi.common.util.StringUtils.getUTF8Bytes;

/**
 * Utils for Java byte array.
 */
public class BinaryUtil {

  /**
   * Lexicographically compare two arrays.
   * copy from hbase
   * @param buffer1 left operand
   * @param buffer2 right operand
   * @param offset1 Where to start comparing in the left buffer
   * @param offset2 Where to start comparing in the right buffer
   * @param length1 How much to compare from the left buffer
   * @param length2 How much to compare from the right buffer
   * @return 0 if equal, < 0 if left is less than right, etc.
   */
  public static int compareTo(byte[] buffer1, int offset1, int length1,
                              byte[] buffer2, int offset2, int length2) {
    // Short circuit equal case
    if (buffer1 == buffer2
        && offset1 == offset2
        && length1 == length2) {
      return 0;
    }
    // Bring WritableComparator code local
    int end1 = offset1 + length1;
    int end2 = offset2 + length2;
    for (int i = offset1, j = offset2; i < end1 && j < end2; i++, j++) {
      int a = (buffer1[i] & 0xff);
      int b = (buffer2[j] & 0xff);
      if (a != b) {
        return a - b;
      }
    }
    return length1 - length2;
  }

  public static byte[] paddingTo8Byte(byte[] a) {
    if (a.length == 8) {
      return a;
    }
    if (a.length > 8) {
      byte[] result = new byte[8];
      System.arraycopy(a, 0, result, 0, 8);
      return result;
    }
    int paddingSize = 8 - a.length;
    byte[] result = new byte[8];
    for (int i = 0; i < paddingSize; i++) {
      result[i] = 0;
    }
    System.arraycopy(a, 0, result, paddingSize, a.length);

    return result;
  }

  /**
   * Interleaving array bytes.
   * Interleaving means take one bit from the first matrix element, one bit
   * from the next, etc, then take the second bit from the first matrix
   * element, second bit from the second, all the way to the last bit of the
   * last element. Combine those bits in that order into a single BigInteger,
   * @param buffer candidate element to do interleaving
   * @return byte size of candidate element
   */
  public static byte[] interleaving(byte[][] buffer, int size) {
    int candidateSize = buffer.length;
    byte[] result = new byte[size * candidateSize];
    int resBitPos = 0;
    int totalBits = size * 8;
    for (int bitStep = 0; bitStep < totalBits; bitStep++) {
      int currentBytePos = (int) Math.floor(bitStep / 8);
      int currentBitPos = bitStep % 8;

      for (int i = 0; i < candidateSize; i++) {
        int tempResBytePos = (int) Math.floor(resBitPos / 8);
        int tempResBitPos = resBitPos % 8;
        result[tempResBytePos] = updatePos(result[tempResBytePos], tempResBitPos, buffer[i][currentBytePos], currentBitPos);
        resBitPos++;
      }
    }
    return result;
  }

  public static byte updatePos(byte a, int apos, byte b, int bpos) {
    byte temp = (byte) (b & (1 << (7 - bpos)));
    if (apos < bpos) {
      temp = (byte) (temp << (bpos - apos));
    }
    if (apos > bpos) {
      temp = (byte) (temp >> (apos - bpos));
    }
    byte atemp = (byte) (a & (1 << (7 - apos)));
    if ((byte) (atemp ^ temp) == 0) {
      return a;
    }
    return (byte) (a ^ (1 << (7 - apos)));
  }

  /**
   * Copies {@link ByteBuffer} into allocated {@code byte[]} array
   */
  public static byte[] toBytes(ByteBuffer buffer) {
    byte[] bytes = new byte[buffer.remaining()];
    buffer.get(bytes);
    return bytes;
  }

  public static byte[] toBytes(int val) {
    byte[] b = new byte[4];
    for (int i = 3; i > 0; i--) {
      b[i] = (byte) val;
      val >>>= 8;
    }
    b[0] = (byte) val;
    return b;
  }

  public static byte[] toBytes(long val) {
    long temp = val;
    byte[] b = new byte[8];
    for (int i = 7; i > 0; i--) {
      b[i] = (byte) temp;
      temp >>>= 8;
    }
    b[0] = (byte) temp;
    return b;
  }

  public static byte[] toBytes(final double d) {
    return toBytes(Double.doubleToRawLongBits(d));
  }

  public static byte[] intTo8Byte(int a) {
    int temp = a;
    temp = temp ^ (1 << 31);
    return paddingTo8Byte(toBytes(temp));
  }

  public static byte[] byteTo8Byte(byte a) {
    return paddingTo8Byte(new byte[] { a });
  }

  public static byte[] longTo8Byte(long a) {
    long temp = a;
    temp = temp ^ (1L << 63);
    return toBytes(temp);
  }

  public static byte[] doubleTo8Byte(double a) {
    byte[] temp = toBytes(a);
    if (a > 0) {
      temp[0] = (byte) (temp[0] ^ (1 << 7));
    }
    if (a < 0) {
      for (int i = 0; i < temp.length; i++) {
        temp[i] = (byte) ~temp[i];
      }
    }
    return temp;
  }

  public static byte[] utf8To8Byte(String a) {
    return paddingTo8Byte(getUTF8Bytes(a));
  }

  public static Long convertStringToLong(String a) {
    byte[] bytes = utf8To8Byte(a);
    return convertBytesToLong(bytes);
  }

  public static long convertBytesToLong(byte[] bytes) {
    byte[] paddedBytes = paddingTo8Byte(bytes);
    long temp = 0L;
    for (int i = 7; i >= 0; i--) {
      temp = temp | (((long) paddedBytes[i] & 0xff) << (7 - i) * 8);
    }
    return temp;
  }

  /**
   * Generate a checksum for a given set of bytes.
   */
  public static long generateChecksum(byte[] data) {
    CRC32 crc = new CRC32();
    crc.update(data);
    return crc.getValue();
  }
}

