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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

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
   * @return 0 if equal, < 0 if left is less than right, > 0 otherwise.
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
   * Returns the start position of the first occurrence of the specified {@code
   * target} within {@code array}, or {@code -1} if there is no such occurrence.
   *
   * <p>More formally, returns the lowest index {@code i} such that the range
   * [i, i + target.length) in {@code array} contains exactly the same elements
   * as {@code target}.
   *
   * @param array  the array to search for the sequence {@code target}.
   * @param target the array to search for as a sub-sequence of {@code array}.
   * @return the start position if found; {@code -1} if there is no such occurrence.
   */
  public static int indexOf(byte[] array, byte[] target) {
    if (target.length == 0) {
      return 0;
    }

    outer:
    for (int i = 0; i < array.length - target.length + 1; i++) {
      for (int j = 0; j < target.length; j++) {
        if (array[i + j] != target[j]) {
          continue outer;
        }
      }
      return i;
    }
    return -1;
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

  /**
   * Converts an int value to a byte array using big-endian.
   *
   * @param val value to convert.
   * @return the byte array.
   */
  public static byte[] toBytes(int val) {
    byte[] b = new byte[4];
    for (int i = 3; i > 0; i--) {
      b[i] = (byte) val;
      val >>>= 8;
    }
    b[0] = (byte) val;
    return b;
  }

  /**
   * Converts a long value to a byte array using big-endian.
   *
   * @param val value to convert.
   * @return the byte array.
   */
  public static byte[] toBytes(long val) {
    byte[] b = new byte[8];
    for (int i = 7; i > 0; i--) {
      b[i] = (byte) val;
      val >>>= 8;
    }
    b[0] = (byte) val;
    return b;
  }

  /**
   * @param bytes  byte array to hash.
   * @param offset offset to start hashing.
   * @param length length of bytes to hash.
   * @return the generated hash code.
   */
  public static int hashCode(byte[] bytes, int offset, int length) {
    int hash = 1;
    for (int i = offset; i < offset + length; i++) {
      hash = (31 * hash) + bytes[i];
    }
    return hash;
  }

  /**
   * Reads the data fully from the {@link InputStream} to the byte array.
   *
   * @param inputStream     {@link InputStream} containing the data.
   * @param targetByteArray target byte array.
   * @param offset          offset in the target byte array to start to write data.
   * @param length          maximum amount of data to write.
   * @return size of bytes read.
   * @throws IOException upon error.
   */
  public static int readFully(InputStream inputStream,
                              byte[] targetByteArray,
                              int offset,
                              int length) throws IOException {
    int totalBytesRead = 0;
    int bytesRead;
    while (totalBytesRead < length) {
      bytesRead = inputStream.read(targetByteArray, offset + totalBytesRead, length - totalBytesRead);
      if (bytesRead < 0) {
        break;
      }
      totalBytesRead += bytesRead;
    }
    return totalBytesRead;
  }

  public static byte[] readAsByteArray(InputStream input, int outputSize) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream(outputSize);
    copy(input, bos);
    return bos.toByteArray();
  }

  public static void copy(InputStream inputStream, OutputStream outputStream) throws IOException {
    byte[] buffer = new byte[1024];
    int len;
    while ((len = inputStream.read(buffer)) != -1) {
      outputStream.write(buffer, 0, len);
    }
  }

  /**
   * @param byteBuffer {@link ByteBuffer} containing the bytes.
   * @return {@link DataInputStream} based on the byte buffer.
   */
  public static DataInputStream getDataInputStream(ByteBuffer byteBuffer) {
    return new DataInputStream(new ByteArrayInputStream(
        byteBuffer.array(), byteBuffer.arrayOffset(), byteBuffer.limit() - byteBuffer.arrayOffset()));
  }

  /**
   * Returns a new byte array, copied from the given {@code buf}, from the index 0 (inclusive)
   * to the limit (exclusive), regardless of the current position.
   * The position and the other index parameters are not changed.
   *
   * @param buf a byte buffer.
   * @return the byte array.
   */
  public static byte[] toBytes(ByteBuffer buf) {
    ByteBuffer dup = buf.duplicate();
    dup.position(0);
    return readBytes(dup);
  }

  private static byte[] readBytes(ByteBuffer buf) {
    byte[] result = new byte[buf.remaining()];
    buf.get(result);
    return result;
  }
}
