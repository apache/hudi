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

package org.apache.hudi.optimize;

import sun.misc.Unsafe;

import java.nio.charset.Charset;

public class ZOrderingUtil {

  static final Unsafe THEUNSAFE;
  public static final int SIZEOF_LONG = Long.SIZE / Byte.SIZE;

  static {
    THEUNSAFE = UnsafeAccess.THEUNSAFE;

    // sanity check - this should never fail
    if (THEUNSAFE.arrayIndexScale(byte[].class) != 1) {
      throw new AssertionError();
    }
  }

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
    if (buffer1 == buffer2 && offset1 == offset2 && length1 == length2) {
      return 0;
    }
    final int stride = 8;
    final int minLength = Math.min(length1, length2);
    int strideLimit = minLength & ~(stride - 1);
    final long offset1Adj = offset1 + UnsafeAccess.BYTE_ARRAY_BASE_OFFSET;
    final long offset2Adj = offset2 + UnsafeAccess.BYTE_ARRAY_BASE_OFFSET;
    int i;

    /*
     * Compare 8 bytes at a time. Benchmarking on x86 shows a stride of 8 bytes is no slower
     * than 4 bytes even on 32-bit. On the other hand, it is substantially faster on 64-bit.
     */
    for (i = 0; i < strideLimit; i += stride) {
      long lw = THEUNSAFE.getLong(buffer1, offset1Adj + i);
      long rw = THEUNSAFE.getLong(buffer2, offset2Adj + i);
      if (lw != rw) {
        if (!UnsafeAccess.LITTLE_ENDIAN) {
          return ((lw + Long.MIN_VALUE) < (rw + Long.MIN_VALUE)) ? -1 : 1;
        }

        /*
         * We want to compare only the first index where left[index] != right[index]. This
         * corresponds to the least significant nonzero byte in lw ^ rw, since lw and rw are
         * little-endian. Long.numberOfTrailingZeros(diff) tells us the least significant
         * nonzero bit, and zeroing out the first three bits of L.nTZ gives us the shift to get
         * that least significant nonzero byte. This comparison logic is based on UnsignedBytes
         * comparator from guava v21
         */
        int n = Long.numberOfTrailingZeros(lw ^ rw) & ~0x7;
        return ((int) ((lw >>> n) & 0xFF)) - ((int) ((rw >>> n) & 0xFF));
      }
    }

    // The epilogue to cover the last (minLength % stride) elements.
    for (; i < minLength; i++) {
      int a = (buffer1[offset1 + i] & 0xFF);
      int b = (buffer2[offset2 + i] & 0xFF);
      if (a != b) {
        return a - b;
      }
    }
    return length1 - length2;
  }

  private static byte[] paddingTo8Byte(byte[] a) {
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
    return paddingTo8Byte(a.getBytes(Charset.forName("utf-8")));
  }

  public static Long convertStringToLong(String a) {
    byte[] bytes = utf8To8Byte(a);
    long temp = 0L;
    for (int i = 7; i >= 0; i--) {
      temp = temp | (((long)bytes[i] & 0xff) << (7 - i) * 8);
    }
    return temp;
  }
}

