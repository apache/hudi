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

package org.apache.hudi.io.hfile;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class ByteUtils {
  /**
   * Reads four bytes starting from the offset in the input and returns {@code int} value.
   *
   * @param bytes  Input byte array.
   * @param offset Offset to start reading.
   * @return The {@code int} value.
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
   * @param bytes  Input byte array.
   * @param offset Offset to start reading.
   * @return The {@code long} value.
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
   * @param bytes  Input byte array.
   * @param offset Offset to start reading.
   * @return The {@code short} value.
   */
  public static short readShort(byte[] bytes, int offset) {
    short n = 0;
    n = (short) ((n ^ bytes[offset]) & 0xFF);
    n = (short) (n << 8);
    n ^= (short) (bytes[offset + 1] & 0xFF);
    return n;
  }

  /**
   * Parses the first byte of a vint/vlong to determine the number of bytes on disk.
   *
   * @param bytes  Input byte array.
   * @param offset Offset to start reading.
   * @return The total number of bytes (1 to 9) on disk.
   */
  public static int decodeVLongSizeOnDisk(byte[] bytes, int offset) {
    byte firstByte = bytes[offset];
    return decodeVIntSize(firstByte);
  }

  /**
   * Parses the first byte of a vint/vlong to determine the number of bytes
   *
   * @param value the first byte of the vint/vlong
   * @return the total number of bytes (1 to 9)
   */
  public static int decodeVIntSize(byte value) {
    if (value >= -112) {
      return 1;
    } else if (value < -120) {
      return -119 - value;
    }
    return -111 - value;
  }

  /**
   * Reads a zero-compressed encoded long (VLong) from input stream and returns it.
   *
   * @param bytes  Input byte array.
   * @param offset Offset to start reading.
   * @return Deserialized long from the input.
   */
  public static long readVLong(byte[] bytes, int offset) {
    return readVLong(bytes, offset, decodeVLongSizeOnDisk(bytes, offset));
  }

  /**
   * Reads a zero-compressed encoded long (VLong) from input stream and the decoded size on disk,
   * and returns it.
   *
   * @param bytes  Input byte array.
   * @param offset Offset to start reading.
   * @return Deserialized long from the input.
   */
  public static long readVLong(byte[] bytes, int offset, int vLongSizeOnDisk) {
    byte firstByte = bytes[offset];
    if (vLongSizeOnDisk == 1) {
      return firstByte;
    }
    long value = 0;
    for (int i = 0; i < vLongSizeOnDisk - 1; i++) {
      value = value << 8;
      value = value | (bytes[offset + 1 + i] & 0xFF);
    }
    return (isNegativeVInt(firstByte) ? (~value) : value);
  }

  /**
   * Reads a zero-compressed encoded int (VInt) from input stream and returns it.
   *
   * @param bytes  Input byte array.
   * @param offset Offset to start reading.
   * @return Deserialized int from the input.
   */
  public static int readVInt(byte[] bytes, int offset) throws IOException {
    long n = readVLong(bytes, offset);
    if ((n > Integer.MAX_VALUE) || (n < Integer.MIN_VALUE)) {
      throw new IOException("value too long to fit in integer");
    }
    return (int) n;
  }

  /**
   * Given the first byte of a vint/vlong, determines the sign
   *
   * @param value the first byte
   * @return is the value negative
   */
  public static boolean isNegativeVInt(byte value) {
    return value < -120 || (value >= -112 && value < 0);
  }

  /**
   * Decodes the size of the byte array based on the encoded integer at the beginning, and copies
   * out the bytes.
   *
   * @param bytes  Input byte array.
   * @param offset Offset to start reading.
   * @return The actual bytes read.
   * @throws IOException
   */
  public static byte[] readByteArray(byte[] bytes, int offset) throws IOException {
    int length = readVInt(bytes, offset);
    if (length < 0) {
      throw new NegativeArraySizeException(Integer.toString(length));
    }
    return copy(bytes, offset, length);
  }

  /**
   * @param bytes  Input byte array.
   * @param offset Offset to start reading.
   * @param length Length of bytes to copy.
   * @return A new copy of the byte array.
   */
  public static byte[] copy(byte[] bytes, int offset, int length) {
    byte[] copy = new byte[length];
    System.arraycopy(bytes, offset, copy, 0, length);
    return copy;
  }

  /**
   * Lexicographically compares two byte arrays.
   *
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
    if (buffer1 == buffer2 &&
        offset1 == offset2 &&
        length1 == length2) {
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

  /**
   * Copys bytes from InputStream to {@link ByteBuffer} by using an temporary heap byte[] (default
   * size is 1024 now).
   *
   * @param in     the InputStream to read
   * @param out    the destination {@link ByteBuffer}
   * @param length to read
   * @throws IOException if any io error encountered.
   */
  public static void readFullyWithHeapBuffer(InputStream in, ByteBuffer out, int length)
      throws IOException {
    byte[] buffer = new byte[1024];
    if (length < 0) {
      throw new IllegalArgumentException("Length must not be negative: " + length);
    }
    int remain = length, count;
    while (remain > 0) {
      count = in.read(buffer, 0, Math.min(remain, buffer.length));
      if (count < 0) {
        throw new IOException(
            "Premature EOF from inputStream, but still need " + remain + " bytes");
      }
      out.put(buffer, 0, count);
      remain -= count;
    }
  }
}
