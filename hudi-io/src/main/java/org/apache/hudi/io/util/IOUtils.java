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

/**
 * Util methods on I/O.
 */
public class IOUtils {
  /**
   * Reads four bytes starting from the offset in the input and returns {@code int} value.
   *
   * @param bytes  input byte array.
   * @param offset offset to start reading.
   * @return the {@code int} value.
   */
  public static int readInt(byte[] bytes, int offset) {
    return (((bytes[offset] & 0xff) << 24)
        | ((bytes[offset + 1] & 0xff) << 16)
        | ((bytes[offset + 2] & 0xff) << 8)
        | (bytes[offset + 3] & 0xff));
  }

  /**
   * Reads eight bytes starting from the offset in the input and returns {@code long} value.
   *
   * @param bytes  input byte array.
   * @param offset offset to start reading.
   * @return the {@code long} value.
   */
  public static long readLong(byte[] bytes, int offset) {
    return (((long) (bytes[offset] & 0xff) << 56)
        | ((long) (bytes[offset + 1] & 0xff) << 48)
        | ((long) (bytes[offset + 2] & 0xff) << 40)
        | ((long) (bytes[offset + 3] & 0xff) << 32)
        | ((long) (bytes[offset + 4] & 0xff) << 24)
        | ((long) (bytes[offset + 5] & 0xff) << 16)
        | ((long) (bytes[offset + 6] & 0xff) << 8)
        | (long) (bytes[offset + 7] & 0xff));
  }

  /**
   * Reads two bytes starting from the offset in the input and returns {@code short} value.
   *
   * @param bytes  input byte array.
   * @param offset offset to start reading.
   * @return the {@code short} value.
   */
  public static short readShort(byte[] bytes, int offset) {
    short n = 0;
    n = (short) ((n ^ bytes[offset]) & 0xFF);
    n = (short) (n << 8);
    n ^= (short) (bytes[offset + 1] & 0xFF);
    return n;
  }

  /**
   * Parses the first byte of a variable-length encoded number (integer or long value) to determine
   * total number of bytes representing the number on disk.
   *
   * @param bytes  input byte array of the encoded number.
   * @param offset offset to start reading.
   * @return the total number of bytes (1 to 9) on disk.
   */
  public static int decodeVarLongSizeOnDisk(byte[] bytes, int offset) {
    byte firstByte = bytes[offset];
    return decodeVarLongSize(firstByte);
  }

  /**
   * Parses the first byte of a variable-length encoded number (integer or long value) to determine
   * total number of bytes representing the number on disk.
   *
   * @param value the first byte of the encoded number.
   * @return the total number of bytes (1 to 9) on disk.
   */
  public static int decodeVarLongSize(byte value) {
    if (value >= -112) {
      return 1;
    } else if (value < -120) {
      return -119 - value;
    }
    return -111 - value;
  }

  /**
   * Reads a variable-length encoded number from input bytes and returns it.
   *
   * @param bytes  input byte array.
   * @param offset offset to start reading.
   * @return decoded {@code long} from the input.
   */
  public static long readVarLong(byte[] bytes, int offset) {
    return readVarLong(bytes, offset, decodeVarLongSizeOnDisk(bytes, offset));
  }

  /**
   * Reads a variable-length encoded number from input bytes and the decoded size on disk,
   * and returns it.
   *
   * @param bytes             input byte array.
   * @param offset            offset to start reading.
   * @param varLongSizeOnDisk the total number of bytes (1 to 9) on disk.
   * @return decoded {@code long} from the input.
   */
  public static long readVarLong(byte[] bytes, int offset, int varLongSizeOnDisk) {
    byte firstByte = bytes[offset];
    if (varLongSizeOnDisk == 1) {
      return firstByte;
    }
    long value = 0;
    for (int i = 0; i < varLongSizeOnDisk - 1; i++) {
      value = value << 8;
      value = value | (bytes[offset + 1 + i] & 0xFF);
    }
    return (isNegativeVarLong(firstByte) ? (~value) : value);
  }

  /**
   * Given the first byte of a variable-length encoded number, determines the sign.
   *
   * @param value the first byte.
   * @return is the value negative.
   */
  public static boolean isNegativeVarLong(byte value) {
    return value < -120 || (value >= -112 && value < 0);
  }

  /**
   * @param bytes  input byte array.
   * @param offset offset to start reading.
   * @param length length of bytes to copy.
   * @return a new copy of the byte array.
   */
  public static byte[] copy(byte[] bytes, int offset, int length) {
    byte[] copy = new byte[length];
    System.arraycopy(bytes, offset, copy, 0, length);
    return copy;
  }

  /**
   * Lexicographically compares two byte arrays.
   *
   * @param bytes1 left operand.
   * @param bytes2 right operand.
   * @return 0 if equal, < 0 if left is less than right, etc.
   */
  public static int compareTo(byte[] bytes1, byte[] bytes2) {
    return compareTo(bytes1, 0, bytes1.length, bytes2, 0, bytes2.length);
  }

  /**
   * Lexicographically compares two byte arrays.
   *
   * @param bytes1  left operand.
   * @param bytes2  right operand.
   * @param offset1 where to start comparing in the left buffer.
   * @param offset2 where to start comparing in the right buffer.
   * @param length1 how much to compare from the left buffer.
   * @param length2 how much to compare from the right buffer.
   * @return 0 if equal, < 0 if left is less than right, etc.
   */
  public static int compareTo(byte[] bytes1, int offset1, int length1,
                              byte[] bytes2, int offset2, int length2) {
    if (bytes1 == bytes2 && offset1 == offset2 && length1 == length2) {
      return 0;
    }
    int end1 = offset1 + length1;
    int end2 = offset2 + length2;
    for (int i = offset1, j = offset2; i < end1 && j < end2; i++, j++) {
      int a = (bytes1[i] & 0xff);
      int b = (bytes2[j] & 0xff);
      if (a != b) {
        return a - b;
      }
    }
    return length1 - length2;
  }

  /**
   * @param bytes  input byte array.
   * @param offset offset to start reading.
   * @param length length of bytes to read.
   * @return {@link String} value based on the byte array.
   */
  public static String bytesToString(byte[] bytes, int offset, int length) {
    StringBuilder sb = new StringBuilder();
    for (int i = offset; i < offset + length; i++) {
      sb.append((char) bytes[i]);
    }
    return sb.toString();
  }
}
