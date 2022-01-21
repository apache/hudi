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

package org.apache.hudi.hbase.util;

import static org.apache.hbase.thirdparty.com.google.common.base.Preconditions.checkArgument;
import static org.apache.hbase.thirdparty.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.hbase.thirdparty.com.google.common.base.Preconditions.checkPositionIndex;

import com.google.protobuf.ByteString;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import org.apache.hudi.hbase.Cell;
import org.apache.hudi.hbase.CellComparator;
import org.apache.hudi.hbase.KeyValue;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Unsafe;

import org.apache.hbase.thirdparty.org.apache.commons.collections4.CollectionUtils;

/**
 * Utility class that handles byte arrays, conversions to/from other types,
 * comparisons, hash code generation, manufacturing keys for HashMaps or
 * HashSets, and can be used as key in maps or trees.
 */
@SuppressWarnings("restriction")
@InterfaceAudience.Public
public class Bytes implements Comparable<Bytes> {

  // Using the charset canonical name for String/byte[] conversions is much
  // more efficient due to use of cached encoders/decoders.
  private static final String UTF8_CSN = StandardCharsets.UTF_8.name();

  //HConstants.EMPTY_BYTE_ARRAY should be updated if this changed
  private static final byte [] EMPTY_BYTE_ARRAY = new byte [0];

  private static final Logger LOG = LoggerFactory.getLogger(Bytes.class);

  /**
   * Size of boolean in bytes
   */
  public static final int SIZEOF_BOOLEAN = Byte.SIZE / Byte.SIZE;

  /**
   * Size of byte in bytes
   */
  public static final int SIZEOF_BYTE = SIZEOF_BOOLEAN;

  /**
   * Size of char in bytes
   */
  public static final int SIZEOF_CHAR = Character.SIZE / Byte.SIZE;

  /**
   * Size of double in bytes
   */
  public static final int SIZEOF_DOUBLE = Double.SIZE / Byte.SIZE;

  /**
   * Size of float in bytes
   */
  public static final int SIZEOF_FLOAT = Float.SIZE / Byte.SIZE;

  /**
   * Size of int in bytes
   */
  public static final int SIZEOF_INT = Integer.SIZE / Byte.SIZE;

  /**
   * Size of long in bytes
   */
  public static final int SIZEOF_LONG = Long.SIZE / Byte.SIZE;

  /**
   * Size of short in bytes
   */
  public static final int SIZEOF_SHORT = Short.SIZE / Byte.SIZE;

  /**
   * Mask to apply to a long to reveal the lower int only. Use like this:
   * int i = (int)(0xFFFFFFFF00000000L ^ some_long_value);
   */
  public static final long MASK_FOR_LOWER_INT_IN_LONG = 0xFFFFFFFF00000000L;

  /**
   * Estimate of size cost to pay beyond payload in jvm for instance of byte [].
   * Estimate based on study of jhat and jprofiler numbers.
   */
  // JHat says BU is 56 bytes.
  // SizeOf which uses java.lang.instrument says 24 bytes. (3 longs?)
  public static final int ESTIMATED_HEAP_TAX = 16;

  @InterfaceAudience.Private
  static final boolean UNSAFE_UNALIGNED = UnsafeAvailChecker.unaligned();

  /**
   * Returns length of the byte array, returning 0 if the array is null.
   * Useful for calculating sizes.
   * @param b byte array, which can be null
   * @return 0 if b is null, otherwise returns length
   */
  final public static int len(byte[] b) {
    return b == null ? 0 : b.length;
  }

  private byte[] bytes;
  private int offset;
  private int length;

  /**
   * Create a zero-size sequence.
   */
  public Bytes() {
    super();
  }

  /**
   * Create a Bytes using the byte array as the initial value.
   * @param bytes This array becomes the backing storage for the object.
   */
  public Bytes(byte[] bytes) {
    this(bytes, 0, bytes.length);
  }

  /**
   * Set the new Bytes to the contents of the passed
   * <code>ibw</code>.
   * @param ibw the value to set this Bytes to.
   */
  public Bytes(final Bytes ibw) {
    this(ibw.get(), ibw.getOffset(), ibw.getLength());
  }

  /**
   * Set the value to a given byte range
   * @param bytes the new byte range to set to
   * @param offset the offset in newData to start at
   * @param length the number of bytes in the range
   */
  public Bytes(final byte[] bytes, final int offset,
               final int length) {
    this.bytes = bytes;
    this.offset = offset;
    this.length = length;
  }

  /**
   * Copy bytes from ByteString instance.
   * @param byteString copy from
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
   */
  @Deprecated
  public Bytes(final ByteString byteString) {
    this(byteString.toByteArray());
  }

  /**
   * Get the data from the Bytes.
   * @return The data is only valid between offset and offset+length.
   */
  public byte [] get() {
    if (this.bytes == null) {
      throw new IllegalStateException("Uninitialiized. Null constructor " +
          "called w/o accompaying readFields invocation");
    }
    return this.bytes;
  }

  /**
   * @param b Use passed bytes as backing array for this instance.
   */
  public void set(final byte [] b) {
    set(b, 0, b.length);
  }

  /**
   * @param b Use passed bytes as backing array for this instance.
   * @param offset
   * @param length
   */
  public void set(final byte [] b, final int offset, final int length) {
    this.bytes = b;
    this.offset = offset;
    this.length = length;
  }

  /**
   * @return the number of valid bytes in the buffer
   * @deprecated since 2.0.0 and will be removed in 3.0.0. Use {@link #getLength()} instead.
   * @see #getLength()
   * @see <a href="https://issues.apache.org/jira/browse/HBASE-11862">HBASE-11862</a>
   */
  @Deprecated
  public int getSize() {
    if (this.bytes == null) {
      throw new IllegalStateException("Uninitialiized. Null constructor " +
          "called w/o accompaying readFields invocation");
    }
    return this.length;
  }

  /**
   * @return the number of valid bytes in the buffer
   */
  public int getLength() {
    if (this.bytes == null) {
      throw new IllegalStateException("Uninitialiized. Null constructor " +
          "called w/o accompaying readFields invocation");
    }
    return this.length;
  }

  /**
   * @return offset
   */
  public int getOffset(){
    return this.offset;
  }

  /**
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
   */
  @Deprecated
  public ByteString toByteString() {
    return ByteString.copyFrom(this.bytes, this.offset, this.length);
  }

  @Override
  public int hashCode() {
    return Bytes.hashCode(bytes, offset, length);
  }

  /**
   * Define the sort order of the Bytes.
   * @param that The other bytes writable
   * @return Positive if left is bigger than right, 0 if they are equal, and
   *         negative if left is smaller than right.
   */
  @Override
  public int compareTo(Bytes that) {
    return BYTES_RAWCOMPARATOR.compare(
        this.bytes, this.offset, this.length,
        that.bytes, that.offset, that.length);
  }

  /**
   * Compares the bytes in this object to the specified byte array
   * @param that
   * @return Positive if left is bigger than right, 0 if they are equal, and
   *         negative if left is smaller than right.
   */
  public int compareTo(final byte [] that) {
    return BYTES_RAWCOMPARATOR.compare(
        this.bytes, this.offset, this.length,
        that, 0, that.length);
  }

  /**
   * @see Object#equals(Object)
   */
  @Override
  public boolean equals(Object right_obj) {
    if (right_obj instanceof byte []) {
      return compareTo((byte [])right_obj) == 0;
    }
    if (right_obj instanceof Bytes) {
      return compareTo((Bytes)right_obj) == 0;
    }
    return false;
  }

  /**
   * @see Object#toString()
   */
  @Override
  public String toString() {
    return Bytes.toString(bytes, offset, length);
  }

  /**
   * @param array List of byte [].
   * @return Array of byte [].
   */
  public static byte [][] toArray(final List<byte []> array) {
    // List#toArray doesn't work on lists of byte [].
    byte[][] results = new byte[array.size()][];
    for (int i = 0; i < array.size(); i++) {
      results[i] = array.get(i);
    }
    return results;
  }

  /**
   * Returns a copy of the bytes referred to by this writable
   */
  public byte[] copyBytes() {
    return Arrays.copyOfRange(bytes, offset, offset+length);
  }
  /**
   * Byte array comparator class.
   */
  @InterfaceAudience.Public
  public static class ByteArrayComparator implements RawComparator<byte []> {
    /**
     * Constructor
     */
    public ByteArrayComparator() {
      super();
    }
    @Override
    public int compare(byte [] left, byte [] right) {
      return compareTo(left, right);
    }
    @Override
    public int compare(byte [] b1, int s1, int l1, byte [] b2, int s2, int l2) {
      return LexicographicalComparerHolder.BEST_COMPARER.
          compareTo(b1, s1, l1, b2, s2, l2);
    }
  }

  /**
   * A {@link ByteArrayComparator} that treats the empty array as the largest value.
   * This is useful for comparing row end keys for regions.
   */
  // TODO: unfortunately, HBase uses byte[0] as both start and end keys for region
  // boundaries. Thus semantically, we should treat empty byte array as the smallest value
  // while comparing row keys, start keys etc; but as the largest value for comparing
  // region boundaries for endKeys.
  @InterfaceAudience.Public
  public static class RowEndKeyComparator extends ByteArrayComparator {
    @Override
    public int compare(byte[] left, byte[] right) {
      return compare(left, 0, left.length, right, 0, right.length);
    }
    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      if (b1 == b2 && s1 == s2 && l1 == l2) {
        return 0;
      }
      if (l1 == 0) {
        return l2; //0 or positive
      }
      if (l2 == 0) {
        return -1;
      }
      return super.compare(b1, s1, l1, b2, s2, l2);
    }
  }

  /**
   * Pass this to TreeMaps where byte [] are keys.
   */
  public final static Comparator<byte []> BYTES_COMPARATOR = new ByteArrayComparator();

  /**
   * Use comparing byte arrays, byte-by-byte
   */
  public final static RawComparator<byte []> BYTES_RAWCOMPARATOR = new ByteArrayComparator();

  /**
   * Read byte-array written with a WritableableUtils.vint prefix.
   * @param in Input to read from.
   * @return byte array read off <code>in</code>
   * @throws IOException e
   */
  public static byte [] readByteArray(final DataInput in)
      throws IOException {
    int len = WritableUtils.readVInt(in);
    if (len < 0) {
      throw new NegativeArraySizeException(Integer.toString(len));
    }
    byte [] result = new byte[len];
    in.readFully(result, 0, len);
    return result;
  }

  /**
   * Read byte-array written with a WritableableUtils.vint prefix.
   * IOException is converted to a RuntimeException.
   * @param in Input to read from.
   * @return byte array read off <code>in</code>
   */
  public static byte [] readByteArrayThrowsRuntime(final DataInput in) {
    try {
      return readByteArray(in);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Write byte-array with a WritableableUtils.vint prefix.
   * @param out output stream to be written to
   * @param b array to write
   * @throws IOException e
   */
  public static void writeByteArray(final DataOutput out, final byte [] b)
      throws IOException {
    if(b == null) {
      WritableUtils.writeVInt(out, 0);
    } else {
      writeByteArray(out, b, 0, b.length);
    }
  }

  /**
   * Write byte-array to out with a vint length prefix.
   * @param out output stream
   * @param b array
   * @param offset offset into array
   * @param length length past offset
   * @throws IOException e
   */
  public static void writeByteArray(final DataOutput out, final byte [] b,
                                    final int offset, final int length)
      throws IOException {
    WritableUtils.writeVInt(out, length);
    out.write(b, offset, length);
  }

  /**
   * Write byte-array from src to tgt with a vint length prefix.
   * @param tgt target array
   * @param tgtOffset offset into target array
   * @param src source array
   * @param srcOffset source offset
   * @param srcLength source length
   * @return New offset in src array.
   */
  public static int writeByteArray(final byte [] tgt, final int tgtOffset,
                                   final byte [] src, final int srcOffset, final int srcLength) {
    byte [] vint = vintToBytes(srcLength);
    System.arraycopy(vint, 0, tgt, tgtOffset, vint.length);
    int offset = tgtOffset + vint.length;
    System.arraycopy(src, srcOffset, tgt, offset, srcLength);
    return offset + srcLength;
  }

  /**
   * Put bytes at the specified byte array position.
   * @param tgtBytes the byte array
   * @param tgtOffset position in the array
   * @param srcBytes array to write out
   * @param srcOffset source offset
   * @param srcLength source length
   * @return incremented offset
   */
  public static int putBytes(byte[] tgtBytes, int tgtOffset, byte[] srcBytes,
                             int srcOffset, int srcLength) {
    System.arraycopy(srcBytes, srcOffset, tgtBytes, tgtOffset, srcLength);
    return tgtOffset + srcLength;
  }

  /**
   * Write a single byte out to the specified byte array position.
   * @param bytes the byte array
   * @param offset position in the array
   * @param b byte to write out
   * @return incremented offset
   */
  public static int putByte(byte[] bytes, int offset, byte b) {
    bytes[offset] = b;
    return offset + 1;
  }

  /**
   * Add the whole content of the ByteBuffer to the bytes arrays. The ByteBuffer is modified.
   * @param bytes the byte array
   * @param offset position in the array
   * @param buf ByteBuffer to write out
   * @return incremented offset
   */
  public static int putByteBuffer(byte[] bytes, int offset, ByteBuffer buf) {
    int len = buf.remaining();
    buf.get(bytes, offset, len);
    return offset + len;
  }

  /**
   * Returns a new byte array, copied from the given {@code buf},
   * from the index 0 (inclusive) to the limit (exclusive),
   * regardless of the current position.
   * The position and the other index parameters are not changed.
   *
   * @param buf a byte buffer
   * @return the byte array
   * @see #getBytes(ByteBuffer)
   */
  public static byte[] toBytes(ByteBuffer buf) {
    ByteBuffer dup = buf.duplicate();
    dup.position(0);
    return readBytes(dup);
  }

  private static byte[] readBytes(ByteBuffer buf) {
    byte [] result = new byte[buf.remaining()];
    buf.get(result);
    return result;
  }

  /**
   * @param b Presumed UTF-8 encoded byte array.
   * @return String made from <code>b</code>
   */
  public static String toString(final byte [] b) {
    if (b == null) {
      return null;
    }
    return toString(b, 0, b.length);
  }

  /**
   * Joins two byte arrays together using a separator.
   * @param b1 The first byte array.
   * @param sep The separator to use.
   * @param b2 The second byte array.
   */
  public static String toString(final byte [] b1,
                                String sep,
                                final byte [] b2) {
    return toString(b1, 0, b1.length) + sep + toString(b2, 0, b2.length);
  }

  /**
   * This method will convert utf8 encoded bytes into a string. If
   * the given byte array is null, this method will return null.
   *
   * @param b Presumed UTF-8 encoded byte array.
   * @param off offset into array
   * @return String made from <code>b</code> or null
   */
  public static String toString(final byte[] b, int off) {
    if (b == null) {
      return null;
    }
    int len = b.length - off;
    if (len <= 0) {
      return "";
    }
    try {
      return new String(b, off, len, UTF8_CSN);
    } catch (UnsupportedEncodingException e) {
      // should never happen!
      throw new IllegalArgumentException("UTF8 encoding is not supported", e);
    }
  }

  /**
   * This method will convert utf8 encoded bytes into a string. If
   * the given byte array is null, this method will return null.
   *
   * @param b Presumed UTF-8 encoded byte array.
   * @param off offset into array
   * @param len length of utf-8 sequence
   * @return String made from <code>b</code> or null
   */
  public static String toString(final byte[] b, int off, int len) {
    if (b == null) {
      return null;
    }
    if (len == 0) {
      return "";
    }
    try {
      return new String(b, off, len, UTF8_CSN);
    } catch (UnsupportedEncodingException e) {
      // should never happen!
      throw new IllegalArgumentException("UTF8 encoding is not supported", e);
    }
  }

  /**
   * Write a printable representation of a byte array.
   *
   * @param b byte array
   * @return string
   * @see #toStringBinary(byte[], int, int)
   */
  public static String toStringBinary(final byte [] b) {
    if (b == null)
      return "null";
    return toStringBinary(b, 0, b.length);
  }

  /**
   * Converts the given byte buffer to a printable representation,
   * from the index 0 (inclusive) to the limit (exclusive),
   * regardless of the current position.
   * The position and the other index parameters are not changed.
   *
   * @param buf a byte buffer
   * @return a string representation of the buffer's binary contents
   * @see #toBytes(ByteBuffer)
   * @see #getBytes(ByteBuffer)
   */
  public static String toStringBinary(ByteBuffer buf) {
    if (buf == null)
      return "null";
    if (buf.hasArray()) {
      return toStringBinary(buf.array(), buf.arrayOffset(), buf.limit());
    }
    return toStringBinary(toBytes(buf));
  }

  private static final char[] HEX_CHARS_UPPER = {
      '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'
  };

  /**
   * Write a printable representation of a byte array. Non-printable
   * characters are hex escaped in the format \\x%02X, eg:
   * \x00 \x05 etc
   *
   * @param b array to write out
   * @param off offset to start at
   * @param len length to write
   * @return string output
   */
  public static String toStringBinary(final byte [] b, int off, int len) {
    StringBuilder result = new StringBuilder();
    // Just in case we are passed a 'len' that is > buffer length...
    if (off >= b.length) return result.toString();
    if (off + len > b.length) len = b.length - off;
    for (int i = off; i < off + len ; ++i) {
      int ch = b[i] & 0xFF;
      if (ch >= ' ' && ch <= '~' && ch != '\\') {
        result.append((char)ch);
      } else {
        result.append("\\x");
        result.append(HEX_CHARS_UPPER[ch / 0x10]);
        result.append(HEX_CHARS_UPPER[ch % 0x10]);
      }
    }
    return result.toString();
  }

  private static boolean isHexDigit(char c) {
    return
        (c >= 'A' && c <= 'F') ||
            (c >= '0' && c <= '9');
  }

  /**
   * Takes a ASCII digit in the range A-F0-9 and returns
   * the corresponding integer/ordinal value.
   * @param ch  The hex digit.
   * @return The converted hex value as a byte.
   */
  public static byte toBinaryFromHex(byte ch) {
    if (ch >= 'A' && ch <= 'F')
      return (byte) ((byte)10 + (byte) (ch - 'A'));
    // else
    return (byte) (ch - '0');
  }

  public static byte [] toBytesBinary(String in) {
    // this may be bigger than we need, but let's be safe.
    byte [] b = new byte[in.length()];
    int size = 0;
    for (int i = 0; i < in.length(); ++i) {
      char ch = in.charAt(i);
      if (ch == '\\' && in.length() > i+1 && in.charAt(i+1) == 'x') {
        // ok, take next 2 hex digits.
        char hd1 = in.charAt(i+2);
        char hd2 = in.charAt(i+3);

        // they need to be A-F0-9:
        if (!isHexDigit(hd1) ||
            !isHexDigit(hd2)) {
          // bogus escape code, ignore:
          continue;
        }
        // turn hex ASCII digit -> number
        byte d = (byte) ((toBinaryFromHex((byte)hd1) << 4) + toBinaryFromHex((byte)hd2));

        b[size++] = d;
        i += 3; // skip 3
      } else {
        b[size++] = (byte) ch;
      }
    }
    // resize:
    byte [] b2 = new byte[size];
    System.arraycopy(b, 0, b2, 0, size);
    return b2;
  }

  /**
   * Converts a string to a UTF-8 byte array.
   * @param s string
   * @return the byte array
   */
  public static byte[] toBytes(String s) {
    try {
      return s.getBytes(UTF8_CSN);
    } catch (UnsupportedEncodingException e) {
      // should never happen!
      throw new IllegalArgumentException("UTF8 decoding is not supported", e);
    }
  }

  /**
   * Convert a boolean to a byte array. True becomes -1
   * and false becomes 0.
   *
   * @param b value
   * @return <code>b</code> encoded in a byte array.
   */
  public static byte [] toBytes(final boolean b) {
    return new byte[] { b ? (byte) -1 : (byte) 0 };
  }

  /**
   * Reverses {@link #toBytes(boolean)}
   * @param b array
   * @return True or false.
   */
  public static boolean toBoolean(final byte [] b) {
    if (b.length != 1) {
      throw new IllegalArgumentException("Array has wrong size: " + b.length);
    }
    return b[0] != (byte) 0;
  }

  /**
   * Convert a long value to a byte array using big-endian.
   *
   * @param val value to convert
   * @return the byte array
   */
  public static byte[] toBytes(long val) {
    byte [] b = new byte[8];
    for (int i = 7; i > 0; i--) {
      b[i] = (byte) val;
      val >>>= 8;
    }
    b[0] = (byte) val;
    return b;
  }

  /**
   * Converts a byte array to a long value. Reverses
   * {@link #toBytes(long)}
   * @param bytes array
   * @return the long value
   */
  public static long toLong(byte[] bytes) {
    return toLong(bytes, 0, SIZEOF_LONG);
  }

  /**
   * Converts a byte array to a long value. Assumes there will be
   * {@link #SIZEOF_LONG} bytes available.
   *
   * @param bytes bytes
   * @param offset offset
   * @return the long value
   */
  public static long toLong(byte[] bytes, int offset) {
    return toLong(bytes, offset, SIZEOF_LONG);
  }

  /**
   * Converts a byte array to a long value.
   *
   * @param bytes array of bytes
   * @param offset offset into array
   * @param length length of data (must be {@link #SIZEOF_LONG})
   * @return the long value
   * @throws IllegalArgumentException if length is not {@link #SIZEOF_LONG} or
   * if there's not enough room in the array at the offset indicated.
   */
  public static long toLong(byte[] bytes, int offset, final int length) {
    if (length != SIZEOF_LONG || offset + length > bytes.length) {
      throw explainWrongLengthOrOffset(bytes, offset, length, SIZEOF_LONG);
    }
    return ConverterHolder.BEST_CONVERTER.toLong(bytes, offset, length);
  }

  private static IllegalArgumentException
  explainWrongLengthOrOffset(final byte[] bytes,
                             final int offset,
                             final int length,
                             final int expectedLength) {
    String reason;
    if (length != expectedLength) {
      reason = "Wrong length: " + length + ", expected " + expectedLength;
    } else {
      reason = "offset (" + offset + ") + length (" + length + ") exceed the"
          + " capacity of the array: " + bytes.length;
    }
    return new IllegalArgumentException(reason);
  }

  /**
   * Put a long value out to the specified byte array position.
   * @param bytes the byte array
   * @param offset position in the array
   * @param val long to write out
   * @return incremented offset
   * @throws IllegalArgumentException if the byte array given doesn't have
   * enough room at the offset specified.
   */
  public static int putLong(byte[] bytes, int offset, long val) {
    if (bytes.length - offset < SIZEOF_LONG) {
      throw new IllegalArgumentException("Not enough room to put a long at"
          + " offset " + offset + " in a " + bytes.length + " byte array");
    }
    return ConverterHolder.BEST_CONVERTER.putLong(bytes, offset, val);
  }

  /**
   * Put a long value out to the specified byte array position (Unsafe).
   * @param bytes the byte array
   * @param offset position in the array
   * @param val long to write out
   * @return incremented offset
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
   */
  @Deprecated
  public static int putLongUnsafe(byte[] bytes, int offset, long val) {
    return UnsafeAccess.putLong(bytes, offset, val);
  }

  /**
   * Presumes float encoded as IEEE 754 floating-point "single format"
   * @param bytes byte array
   * @return Float made from passed byte array.
   */
  public static float toFloat(byte [] bytes) {
    return toFloat(bytes, 0);
  }

  /**
   * Presumes float encoded as IEEE 754 floating-point "single format"
   * @param bytes array to convert
   * @param offset offset into array
   * @return Float made from passed byte array.
   */
  public static float toFloat(byte [] bytes, int offset) {
    return Float.intBitsToFloat(toInt(bytes, offset, SIZEOF_INT));
  }

  /**
   * @param bytes byte array
   * @param offset offset to write to
   * @param f float value
   * @return New offset in <code>bytes</code>
   */
  public static int putFloat(byte [] bytes, int offset, float f) {
    return putInt(bytes, offset, Float.floatToRawIntBits(f));
  }

  /**
   * @param f float value
   * @return the float represented as byte []
   */
  public static byte [] toBytes(final float f) {
    // Encode it as int
    return Bytes.toBytes(Float.floatToRawIntBits(f));
  }

  /**
   * @param bytes byte array
   * @return Return double made from passed bytes.
   */
  public static double toDouble(final byte [] bytes) {
    return toDouble(bytes, 0);
  }

  /**
   * @param bytes byte array
   * @param offset offset where double is
   * @return Return double made from passed bytes.
   */
  public static double toDouble(final byte [] bytes, final int offset) {
    return Double.longBitsToDouble(toLong(bytes, offset, SIZEOF_LONG));
  }

  /**
   * @param bytes byte array
   * @param offset offset to write to
   * @param d value
   * @return New offset into array <code>bytes</code>
   */
  public static int putDouble(byte [] bytes, int offset, double d) {
    return putLong(bytes, offset, Double.doubleToLongBits(d));
  }

  /**
   * Serialize a double as the IEEE 754 double format output. The resultant
   * array will be 8 bytes long.
   *
   * @param d value
   * @return the double represented as byte []
   */
  public static byte [] toBytes(final double d) {
    // Encode it as a long
    return Bytes.toBytes(Double.doubleToRawLongBits(d));
  }

  /**
   * Convert an int value to a byte array.  Big-endian.  Same as what DataOutputStream.writeInt
   * does.
   *
   * @param val value
   * @return the byte array
   */
  public static byte[] toBytes(int val) {
    byte [] b = new byte[4];
    for(int i = 3; i > 0; i--) {
      b[i] = (byte) val;
      val >>>= 8;
    }
    b[0] = (byte) val;
    return b;
  }

  /**
   * Converts a byte array to an int value
   * @param bytes byte array
   * @return the int value
   */
  public static int toInt(byte[] bytes) {
    return toInt(bytes, 0, SIZEOF_INT);
  }

  /**
   * Converts a byte array to an int value
   * @param bytes byte array
   * @param offset offset into array
   * @return the int value
   */
  public static int toInt(byte[] bytes, int offset) {
    return toInt(bytes, offset, SIZEOF_INT);
  }

  /**
   * Converts a byte array to an int value
   * @param bytes byte array
   * @param offset offset into array
   * @param length length of int (has to be {@link #SIZEOF_INT})
   * @return the int value
   * @throws IllegalArgumentException if length is not {@link #SIZEOF_INT} or
   * if there's not enough room in the array at the offset indicated.
   */
  public static int toInt(byte[] bytes, int offset, final int length) {
    if (length != SIZEOF_INT || offset + length > bytes.length) {
      throw explainWrongLengthOrOffset(bytes, offset, length, SIZEOF_INT);
    }
    return ConverterHolder.BEST_CONVERTER.toInt(bytes, offset, length);
  }

  /**
   * Converts a byte array to an int value (Unsafe version)
   * @param bytes byte array
   * @param offset offset into array
   * @return the int value
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
   */
  @Deprecated
  public static int toIntUnsafe(byte[] bytes, int offset) {
    return UnsafeAccess.toInt(bytes, offset);
  }

  /**
   * Converts a byte array to an short value (Unsafe version)
   * @param bytes byte array
   * @param offset offset into array
   * @return the short value
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
   */
  @Deprecated
  public static short toShortUnsafe(byte[] bytes, int offset) {
    return UnsafeAccess.toShort(bytes, offset);
  }

  /**
   * Converts a byte array to an long value (Unsafe version)
   * @param bytes byte array
   * @param offset offset into array
   * @return the long value
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
   */
  @Deprecated
  public static long toLongUnsafe(byte[] bytes, int offset) {
    return UnsafeAccess.toLong(bytes, offset);
  }

  /**
   * Converts a byte array to an int value
   * @param bytes byte array
   * @param offset offset into array
   * @param length how many bytes should be considered for creating int
   * @return the int value
   * @throws IllegalArgumentException if there's not enough room in the array at the offset
   * indicated.
   */
  public static int readAsInt(byte[] bytes, int offset, final int length) {
    if (offset + length > bytes.length) {
      throw new IllegalArgumentException("offset (" + offset + ") + length (" + length
          + ") exceed the" + " capacity of the array: " + bytes.length);
    }
    int n = 0;
    for(int i = offset; i < (offset + length); i++) {
      n <<= 8;
      n ^= bytes[i] & 0xFF;
    }
    return n;
  }

  /**
   * Put an int value out to the specified byte array position.
   * @param bytes the byte array
   * @param offset position in the array
   * @param val int to write out
   * @return incremented offset
   * @throws IllegalArgumentException if the byte array given doesn't have
   * enough room at the offset specified.
   */
  public static int putInt(byte[] bytes, int offset, int val) {
    if (bytes.length - offset < SIZEOF_INT) {
      throw new IllegalArgumentException("Not enough room to put an int at"
          + " offset " + offset + " in a " + bytes.length + " byte array");
    }
    return ConverterHolder.BEST_CONVERTER.putInt(bytes, offset, val);
  }

  /**
   * Put an int value out to the specified byte array position (Unsafe).
   * @param bytes the byte array
   * @param offset position in the array
   * @param val int to write out
   * @return incremented offset
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
   */
  @Deprecated
  public static int putIntUnsafe(byte[] bytes, int offset, int val) {
    return UnsafeAccess.putInt(bytes, offset, val);
  }

  /**
   * Convert a short value to a byte array of {@link #SIZEOF_SHORT} bytes long.
   * @param val value
   * @return the byte array
   */
  public static byte[] toBytes(short val) {
    byte[] b = new byte[SIZEOF_SHORT];
    b[1] = (byte) val;
    val >>= 8;
    b[0] = (byte) val;
    return b;
  }

  /**
   * Converts a byte array to a short value
   * @param bytes byte array
   * @return the short value
   */
  public static short toShort(byte[] bytes) {
    return toShort(bytes, 0, SIZEOF_SHORT);
  }

  /**
   * Converts a byte array to a short value
   * @param bytes byte array
   * @param offset offset into array
   * @return the short value
   */
  public static short toShort(byte[] bytes, int offset) {
    return toShort(bytes, offset, SIZEOF_SHORT);
  }

  /**
   * Converts a byte array to a short value
   * @param bytes byte array
   * @param offset offset into array
   * @param length length, has to be {@link #SIZEOF_SHORT}
   * @return the short value
   * @throws IllegalArgumentException if length is not {@link #SIZEOF_SHORT}
   * or if there's not enough room in the array at the offset indicated.
   */
  public static short toShort(byte[] bytes, int offset, final int length) {
    if (length != SIZEOF_SHORT || offset + length > bytes.length) {
      throw explainWrongLengthOrOffset(bytes, offset, length, SIZEOF_SHORT);
    }
    return ConverterHolder.BEST_CONVERTER.toShort(bytes, offset, length);
  }

  /**
   * Returns a new byte array, copied from the given {@code buf},
   * from the position (inclusive) to the limit (exclusive).
   * The position and the other index parameters are not changed.
   *
   * @param buf a byte buffer
   * @return the byte array
   * @see #toBytes(ByteBuffer)
   */
  public static byte[] getBytes(ByteBuffer buf) {
    return readBytes(buf.duplicate());
  }

  /**
   * Put a short value out to the specified byte array position.
   * @param bytes the byte array
   * @param offset position in the array
   * @param val short to write out
   * @return incremented offset
   * @throws IllegalArgumentException if the byte array given doesn't have
   * enough room at the offset specified.
   */
  public static int putShort(byte[] bytes, int offset, short val) {
    if (bytes.length - offset < SIZEOF_SHORT) {
      throw new IllegalArgumentException("Not enough room to put a short at"
          + " offset " + offset + " in a " + bytes.length + " byte array");
    }
    return ConverterHolder.BEST_CONVERTER.putShort(bytes, offset, val);
  }

  /**
   * Put a short value out to the specified byte array position (Unsafe).
   * @param bytes the byte array
   * @param offset position in the array
   * @param val short to write out
   * @return incremented offset
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
   */
  @Deprecated
  public static int putShortUnsafe(byte[] bytes, int offset, short val) {
    return UnsafeAccess.putShort(bytes, offset, val);
  }

  /**
   * Put an int value as short out to the specified byte array position. Only the lower 2 bytes of
   * the short will be put into the array. The caller of the API need to make sure they will not
   * loose the value by doing so. This is useful to store an unsigned short which is represented as
   * int in other parts.
   * @param bytes the byte array
   * @param offset position in the array
   * @param val value to write out
   * @return incremented offset
   * @throws IllegalArgumentException if the byte array given doesn't have
   * enough room at the offset specified.
   */
  public static int putAsShort(byte[] bytes, int offset, int val) {
    if (bytes.length - offset < SIZEOF_SHORT) {
      throw new IllegalArgumentException("Not enough room to put a short at"
          + " offset " + offset + " in a " + bytes.length + " byte array");
    }
    bytes[offset+1] = (byte) val;
    val >>= 8;
    bytes[offset] = (byte) val;
    return offset + SIZEOF_SHORT;
  }

  /**
   * Convert a BigDecimal value to a byte array
   *
   * @param val
   * @return the byte array
   */
  public static byte[] toBytes(BigDecimal val) {
    byte[] valueBytes = val.unscaledValue().toByteArray();
    byte[] result = new byte[valueBytes.length + SIZEOF_INT];
    int offset = putInt(result, 0, val.scale());
    putBytes(result, offset, valueBytes, 0, valueBytes.length);
    return result;
  }


  /**
   * Converts a byte array to a BigDecimal
   *
   * @param bytes
   * @return the char value
   */
  public static BigDecimal toBigDecimal(byte[] bytes) {
    return toBigDecimal(bytes, 0, bytes.length);
  }

  /**
   * Converts a byte array to a BigDecimal value
   *
   * @param bytes
   * @param offset
   * @param length
   * @return the char value
   */
  public static BigDecimal toBigDecimal(byte[] bytes, int offset, final int length) {
    if (bytes == null || length < SIZEOF_INT + 1 ||
        (offset + length > bytes.length)) {
      return null;
    }

    int scale = toInt(bytes, offset);
    byte[] tcBytes = new byte[length - SIZEOF_INT];
    System.arraycopy(bytes, offset + SIZEOF_INT, tcBytes, 0, length - SIZEOF_INT);
    return new BigDecimal(new BigInteger(tcBytes), scale);
  }

  /**
   * Put a BigDecimal value out to the specified byte array position.
   *
   * @param bytes  the byte array
   * @param offset position in the array
   * @param val    BigDecimal to write out
   * @return incremented offset
   */
  public static int putBigDecimal(byte[] bytes, int offset, BigDecimal val) {
    if (bytes == null) {
      return offset;
    }

    byte[] valueBytes = val.unscaledValue().toByteArray();
    byte[] result = new byte[valueBytes.length + SIZEOF_INT];
    offset = putInt(result, offset, val.scale());
    return putBytes(result, offset, valueBytes, 0, valueBytes.length);
  }

  /**
   * @param vint Integer to make a vint of.
   * @return Vint as bytes array.
   */
  public static byte [] vintToBytes(final long vint) {
    long i = vint;
    int size = WritableUtils.getVIntSize(i);
    byte [] result = new byte[size];
    int offset = 0;
    if (i >= -112 && i <= 127) {
      result[offset] = (byte) i;
      return result;
    }

    int len = -112;
    if (i < 0) {
      i ^= -1L; // take one's complement'
      len = -120;
    }

    long tmp = i;
    while (tmp != 0) {
      tmp = tmp >> 8;
      len--;
    }

    result[offset++] = (byte) len;

    len = (len < -120) ? -(len + 120) : -(len + 112);

    for (int idx = len; idx != 0; idx--) {
      int shiftbits = (idx - 1) * 8;
      long mask = 0xFFL << shiftbits;
      result[offset++] = (byte)((i & mask) >> shiftbits);
    }
    return result;
  }

  /**
   * @param buffer buffer to convert
   * @return vint bytes as an integer.
   */
  public static long bytesToVint(final byte [] buffer) {
    int offset = 0;
    byte firstByte = buffer[offset++];
    int len = WritableUtils.decodeVIntSize(firstByte);
    if (len == 1) {
      return firstByte;
    }
    long i = 0;
    for (int idx = 0; idx < len-1; idx++) {
      byte b = buffer[offset++];
      i = i << 8;
      i = i | (b & 0xFF);
    }
    return (WritableUtils.isNegativeVInt(firstByte) ? ~i : i);
  }

  /**
   * Reads a zero-compressed encoded long from input buffer and returns it.
   * @param buffer Binary array
   * @param offset Offset into array at which vint begins.
   * @throws java.io.IOException e
   * @return deserialized long from buffer.
   * @deprecated since 0.98.12. Use {@link #readAsVLong(byte[],int)} instead.
   * @see #readAsVLong(byte[], int)
   * @see <a href="https://issues.apache.org/jira/browse/HBASE-6919">HBASE-6919</a>
   */
  @Deprecated
  public static long readVLong(final byte [] buffer, final int offset)
      throws IOException {
    return readAsVLong(buffer, offset);
  }

  /**
   * Reads a zero-compressed encoded long from input buffer and returns it.
   * @param buffer Binary array
   * @param offset Offset into array at which vint begins.
   * @return deserialized long from buffer.
   */
  public static long readAsVLong(final byte [] buffer, final int offset) {
    byte firstByte = buffer[offset];
    int len = WritableUtils.decodeVIntSize(firstByte);
    if (len == 1) {
      return firstByte;
    }
    long i = 0;
    for (int idx = 0; idx < len-1; idx++) {
      byte b = buffer[offset + 1 + idx];
      i = i << 8;
      i = i | (b & 0xFF);
    }
    return (WritableUtils.isNegativeVInt(firstByte) ? ~i : i);
  }

  /**
   * @param left left operand
   * @param right right operand
   * @return 0 if equal, &lt; 0 if left is less than right, etc.
   */
  public static int compareTo(final byte [] left, final byte [] right) {
    return LexicographicalComparerHolder.BEST_COMPARER.
        compareTo(left, 0, left == null? 0: left.length, right, 0, right == null? 0: right.length);
  }

  /**
   * Lexicographically compare two arrays.
   *
   * @param buffer1 left operand
   * @param buffer2 right operand
   * @param offset1 Where to start comparing in the left buffer
   * @param offset2 Where to start comparing in the right buffer
   * @param length1 How much to compare from the left buffer
   * @param length2 How much to compare from the right buffer
   * @return 0 if equal, &lt; 0 if left is less than right, etc.
   */
  public static int compareTo(byte[] buffer1, int offset1, int length1,
                              byte[] buffer2, int offset2, int length2) {
    return LexicographicalComparerHolder.BEST_COMPARER.
        compareTo(buffer1, offset1, length1, buffer2, offset2, length2);
  }

  interface Comparer<T> {
    int compareTo(
        T buffer1, int offset1, int length1, T buffer2, int offset2, int length2
    );
  }

  static abstract class Converter {
    abstract long toLong(byte[] bytes, int offset, int length);
    abstract int putLong(byte[] bytes, int offset, long val);

    abstract int toInt(byte[] bytes, int offset, final int length);
    abstract int putInt(byte[] bytes, int offset, int val);

    abstract short toShort(byte[] bytes, int offset, final int length);
    abstract int putShort(byte[] bytes, int offset, short val);

  }

  @InterfaceAudience.Private
  static Comparer<byte[]> lexicographicalComparerJavaImpl() {
    return LexicographicalComparerHolder.PureJavaComparer.INSTANCE;
  }

  static class ConverterHolder {
    static final String UNSAFE_CONVERTER_NAME =
        ConverterHolder.class.getName() + "$UnsafeConverter";

    static final Converter BEST_CONVERTER = getBestConverter();
    /**
     * Returns the Unsafe-using Converter, or falls back to the pure-Java
     * implementation if unable to do so.
     */
    static Converter getBestConverter() {
      try {
        Class<?> theClass = Class.forName(UNSAFE_CONVERTER_NAME);

        // yes, UnsafeComparer does implement Comparer<byte[]>
        @SuppressWarnings("unchecked")
        Converter converter = (Converter) theClass.getConstructor().newInstance();
        return converter;
      } catch (Throwable t) { // ensure we really catch *everything*
        return PureJavaConverter.INSTANCE;
      }
    }

    protected static final class PureJavaConverter extends Converter {
      static final PureJavaConverter INSTANCE = new PureJavaConverter();

      private PureJavaConverter() {}

      @Override
      long toLong(byte[] bytes, int offset, int length) {
        long l = 0;
        for(int i = offset; i < offset + length; i++) {
          l <<= 8;
          l ^= bytes[i] & 0xFF;
        }
        return l;
      }

      @Override
      int putLong(byte[] bytes, int offset, long val) {
        for(int i = offset + 7; i > offset; i--) {
          bytes[i] = (byte) val;
          val >>>= 8;
        }
        bytes[offset] = (byte) val;
        return offset + SIZEOF_LONG;
      }

      @Override
      int toInt(byte[] bytes, int offset, int length) {
        int n = 0;
        for(int i = offset; i < (offset + length); i++) {
          n <<= 8;
          n ^= bytes[i] & 0xFF;
        }
        return n;
      }

      @Override
      int putInt(byte[] bytes, int offset, int val) {
        for(int i= offset + 3; i > offset; i--) {
          bytes[i] = (byte) val;
          val >>>= 8;
        }
        bytes[offset] = (byte) val;
        return offset + SIZEOF_INT;
      }

      @Override
      short toShort(byte[] bytes, int offset, int length) {
        short n = 0;
        n = (short) ((n ^ bytes[offset]) & 0xFF);
        n = (short) (n << 8);
        n ^= (short) (bytes[offset+1] & 0xFF);
        return n;
      }

      @Override
      int putShort(byte[] bytes, int offset, short val) {
        bytes[offset+1] = (byte) val;
        val >>= 8;
        bytes[offset] = (byte) val;
        return offset + SIZEOF_SHORT;
      }
    }

    protected static final class UnsafeConverter extends Converter {

      static final Unsafe theUnsafe;

      public UnsafeConverter() {}

      static {
        if (UNSAFE_UNALIGNED) {
          theUnsafe = UnsafeAccess.theUnsafe;
        } else {
          // It doesn't matter what we throw;
          // it's swallowed in getBestComparer().
          throw new Error();
        }

        // sanity check - this should never fail
        if (theUnsafe.arrayIndexScale(byte[].class) != 1) {
          throw new AssertionError();
        }
      }

      @Override
      long toLong(byte[] bytes, int offset, int length) {
        return UnsafeAccess.toLong(bytes, offset);
      }

      @Override
      int putLong(byte[] bytes, int offset, long val) {
        return UnsafeAccess.putLong(bytes, offset, val);
      }

      @Override
      int toInt(byte[] bytes, int offset, int length) {
        return UnsafeAccess.toInt(bytes, offset);
      }

      @Override
      int putInt(byte[] bytes, int offset, int val) {
        return UnsafeAccess.putInt(bytes, offset, val);
      }

      @Override
      short toShort(byte[] bytes, int offset, int length) {
        return UnsafeAccess.toShort(bytes, offset);
      }

      @Override
      int putShort(byte[] bytes, int offset, short val) {
        return UnsafeAccess.putShort(bytes, offset, val);
      }
    }
  }

  /**
   * Provides a lexicographical comparer implementation; either a Java
   * implementation or a faster implementation based on {@link Unsafe}.
   *
   * <p>Uses reflection to gracefully fall back to the Java implementation if
   * {@code Unsafe} isn't available.
   */
  @InterfaceAudience.Private
  static class LexicographicalComparerHolder {
    static final String UNSAFE_COMPARER_NAME =
        LexicographicalComparerHolder.class.getName() + "$UnsafeComparer";

    static final Comparer<byte[]> BEST_COMPARER = getBestComparer();
    /**
     * Returns the Unsafe-using Comparer, or falls back to the pure-Java
     * implementation if unable to do so.
     */
    static Comparer<byte[]> getBestComparer() {
      try {
        Class<?> theClass = Class.forName(UNSAFE_COMPARER_NAME);

        // yes, UnsafeComparer does implement Comparer<byte[]>
        @SuppressWarnings("unchecked")
        Comparer<byte[]> comparer =
            (Comparer<byte[]>) theClass.getEnumConstants()[0];
        return comparer;
      } catch (Throwable t) { // ensure we really catch *everything*
        return lexicographicalComparerJavaImpl();
      }
    }

    enum PureJavaComparer implements Comparer<byte[]> {
      INSTANCE;

      @Override
      public int compareTo(byte[] buffer1, int offset1, int length1,
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
    }

    @InterfaceAudience.Private
    enum UnsafeComparer implements Comparer<byte[]> {
      INSTANCE;

      static final Unsafe theUnsafe;
      static {
        if (UNSAFE_UNALIGNED) {
          theUnsafe = UnsafeAccess.theUnsafe;
        } else {
          // It doesn't matter what we throw;
          // it's swallowed in getBestComparer().
          throw new Error();
        }

        // sanity check - this should never fail
        if (theUnsafe.arrayIndexScale(byte[].class) != 1) {
          throw new AssertionError();
        }
      }

      /**
       * Lexicographically compare two arrays.
       *
       * @param buffer1 left operand
       * @param buffer2 right operand
       * @param offset1 Where to start comparing in the left buffer
       * @param offset2 Where to start comparing in the right buffer
       * @param length1 How much to compare from the left buffer
       * @param length2 How much to compare from the right buffer
       * @return 0 if equal, < 0 if left is less than right, etc.
       */
      @Override
      public int compareTo(byte[] buffer1, int offset1, int length1,
                           byte[] buffer2, int offset2, int length2) {

        // Short circuit equal case
        if (buffer1 == buffer2 &&
            offset1 == offset2 &&
            length1 == length2) {
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
          long lw = theUnsafe.getLong(buffer1, offset1Adj + i);
          long rw = theUnsafe.getLong(buffer2, offset2Adj + i);
          if (lw != rw) {
            if(!UnsafeAccess.LITTLE_ENDIAN) {
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
    }
  }

  /**
   * @param left left operand
   * @param right right operand
   * @return True if equal
   */
  public static boolean equals(final byte [] left, final byte [] right) {
    // Could use Arrays.equals?
    //noinspection SimplifiableConditionalExpression
    if (left == right) return true;
    if (left == null || right == null) return false;
    if (left.length != right.length) return false;
    if (left.length == 0) return true;

    // Since we're often comparing adjacent sorted data,
    // it's usual to have equal arrays except for the very last byte
    // so check that first
    if (left[left.length - 1] != right[right.length - 1]) return false;

    return compareTo(left, right) == 0;
  }

  public static boolean equals(final byte[] left, int leftOffset, int leftLen,
                               final byte[] right, int rightOffset, int rightLen) {
    // short circuit case
    if (left == right &&
        leftOffset == rightOffset &&
        leftLen == rightLen) {
      return true;
    }
    // different lengths fast check
    if (leftLen != rightLen) {
      return false;
    }
    if (leftLen == 0) {
      return true;
    }

    // Since we're often comparing adjacent sorted data,
    // it's usual to have equal arrays except for the very last byte
    // so check that first
    if (left[leftOffset + leftLen - 1] != right[rightOffset + rightLen - 1]) return false;

    return LexicographicalComparerHolder.BEST_COMPARER.
        compareTo(left, leftOffset, leftLen, right, rightOffset, rightLen) == 0;
  }


  /**
   * @param a left operand
   * @param buf right operand
   * @return True if equal
   */
  public static boolean equals(byte[] a, ByteBuffer buf) {
    if (a == null) return buf == null;
    if (buf == null) return false;
    if (a.length != buf.remaining()) return false;

    // Thou shalt not modify the original byte buffer in what should be read only operations.
    ByteBuffer b = buf.duplicate();
    for (byte anA : a) {
      if (anA != b.get()) {
        return false;
      }
    }
    return true;
  }


  /**
   * Return true if the byte array on the right is a prefix of the byte
   * array on the left.
   */
  public static boolean startsWith(byte[] bytes, byte[] prefix) {
    return bytes != null && prefix != null &&
        bytes.length >= prefix.length &&
        LexicographicalComparerHolder.BEST_COMPARER.
            compareTo(bytes, 0, prefix.length, prefix, 0, prefix.length) == 0;
  }

  /**
   * @param b bytes to hash
   * @return Runs {@link WritableComparator#hashBytes(byte[], int)} on the
   * passed in array.  This method is what {@link org.apache.hadoop.io.Text}
   * use calculating hash code.
   */
  public static int hashCode(final byte [] b) {
    return hashCode(b, b.length);
  }

  /**
   * @param b value
   * @param length length of the value
   * @return Runs {@link WritableComparator#hashBytes(byte[], int)} on the
   * passed in array.  This method is what {@link org.apache.hadoop.io.Text}
   * use calculating hash code.
   */
  public static int hashCode(final byte [] b, final int length) {
    return WritableComparator.hashBytes(b, length);
  }

  /**
   * @param b bytes to hash
   * @return A hash of <code>b</code> as an Integer that can be used as key in
   * Maps.
   */
  public static Integer mapKey(final byte [] b) {
    return hashCode(b);
  }

  /**
   * @param b bytes to hash
   * @param length length to hash
   * @return A hash of <code>b</code> as an Integer that can be used as key in
   * Maps.
   */
  public static Integer mapKey(final byte [] b, final int length) {
    return hashCode(b, length);
  }

  /**
   * @param a lower half
   * @param b upper half
   * @return New array that has a in lower half and b in upper half.
   */
  public static byte [] add(final byte [] a, final byte [] b) {
    return add(a, b, EMPTY_BYTE_ARRAY);
  }

  /**
   * @param a first third
   * @param b second third
   * @param c third third
   * @return New array made from a, b and c
   */
  public static byte [] add(final byte [] a, final byte [] b, final byte [] c) {
    byte [] result = new byte[a.length + b.length + c.length];
    System.arraycopy(a, 0, result, 0, a.length);
    System.arraycopy(b, 0, result, a.length, b.length);
    System.arraycopy(c, 0, result, a.length + b.length, c.length);
    return result;
  }

  /**
   * @param arrays all the arrays to concatenate together.
   * @return New array made from the concatenation of the given arrays.
   */
  public static byte [] add(final byte [][] arrays) {
    int length = 0;
    for (int i = 0; i < arrays.length; i++) {
      length += arrays[i].length;
    }
    byte [] result = new byte[length];
    int index = 0;
    for (int i = 0; i < arrays.length; i++) {
      System.arraycopy(arrays[i], 0, result, index, arrays[i].length);
      index += arrays[i].length;
    }
    return result;
  }

  /**
   * @param a array
   * @param length amount of bytes to grab
   * @return First <code>length</code> bytes from <code>a</code>
   */
  public static byte [] head(final byte [] a, final int length) {
    if (a.length < length) {
      return null;
    }
    byte [] result = new byte[length];
    System.arraycopy(a, 0, result, 0, length);
    return result;
  }

  /**
   * @param a array
   * @param length amount of bytes to snarf
   * @return Last <code>length</code> bytes from <code>a</code>
   */
  public static byte [] tail(final byte [] a, final int length) {
    if (a.length < length) {
      return null;
    }
    byte [] result = new byte[length];
    System.arraycopy(a, a.length - length, result, 0, length);
    return result;
  }

  /**
   * @param a array
   * @param length new array size
   * @return Value in <code>a</code> plus <code>length</code> prepended 0 bytes
   */
  public static byte [] padHead(final byte [] a, final int length) {
    byte [] padding = new byte[length];
    for (int i = 0; i < length; i++) {
      padding[i] = 0;
    }
    return add(padding,a);
  }

  /**
   * @param a array
   * @param length new array size
   * @return Value in <code>a</code> plus <code>length</code> appended 0 bytes
   */
  public static byte [] padTail(final byte [] a, final int length) {
    byte [] padding = new byte[length];
    for (int i = 0; i < length; i++) {
      padding[i] = 0;
    }
    return add(a,padding);
  }

  /**
   * Split passed range.  Expensive operation relatively.  Uses BigInteger math.
   * Useful splitting ranges for MapReduce jobs.
   * @param a Beginning of range
   * @param b End of range
   * @param num Number of times to split range.  Pass 1 if you want to split
   * the range in two; i.e. one split.
   * @return Array of dividing values
   */
  public static byte [][] split(final byte [] a, final byte [] b, final int num) {
    return split(a, b, false, num);
  }

  /**
   * Split passed range.  Expensive operation relatively.  Uses BigInteger math.
   * Useful splitting ranges for MapReduce jobs.
   * @param a Beginning of range
   * @param b End of range
   * @param inclusive Whether the end of range is prefix-inclusive or is
   * considered an exclusive boundary.  Automatic splits are generally exclusive
   * and manual splits with an explicit range utilize an inclusive end of range.
   * @param num Number of times to split range.  Pass 1 if you want to split
   * the range in two; i.e. one split.
   * @return Array of dividing values
   */
  public static byte[][] split(final byte[] a, final byte[] b,
                               boolean inclusive, final int num) {
    byte[][] ret = new byte[num + 2][];
    int i = 0;
    Iterable<byte[]> iter = iterateOnSplits(a, b, inclusive, num);
    if (iter == null)
      return null;
    for (byte[] elem : iter) {
      ret[i++] = elem;
    }
    return ret;
  }

  /**
   * Iterate over keys within the passed range, splitting at an [a,b) boundary.
   */
  public static Iterable<byte[]> iterateOnSplits(final byte[] a,
                                                 final byte[] b, final int num)
  {
    return iterateOnSplits(a, b, false, num);
  }

  /**
   * Iterate over keys within the passed range.
   */
  public static Iterable<byte[]> iterateOnSplits(
      final byte[] a, final byte[]b, boolean inclusive, final int num)
  {
    byte [] aPadded;
    byte [] bPadded;
    if (a.length < b.length) {
      aPadded = padTail(a, b.length - a.length);
      bPadded = b;
    } else if (b.length < a.length) {
      aPadded = a;
      bPadded = padTail(b, a.length - b.length);
    } else {
      aPadded = a;
      bPadded = b;
    }
    if (compareTo(aPadded,bPadded) >= 0) {
      throw new IllegalArgumentException("b <= a");
    }
    if (num <= 0) {
      throw new IllegalArgumentException("num cannot be <= 0");
    }
    byte [] prependHeader = {1, 0};
    final BigInteger startBI = new BigInteger(add(prependHeader, aPadded));
    final BigInteger stopBI = new BigInteger(add(prependHeader, bPadded));
    BigInteger diffBI = stopBI.subtract(startBI);
    if (inclusive) {
      diffBI = diffBI.add(BigInteger.ONE);
    }
    final BigInteger splitsBI = BigInteger.valueOf(num + 1);
    //when diffBI < splitBI, use an additional byte to increase diffBI
    if(diffBI.compareTo(splitsBI) < 0) {
      byte[] aPaddedAdditional = new byte[aPadded.length+1];
      byte[] bPaddedAdditional = new byte[bPadded.length+1];
      for (int i = 0; i < aPadded.length; i++){
        aPaddedAdditional[i] = aPadded[i];
      }
      for (int j = 0; j < bPadded.length; j++){
        bPaddedAdditional[j] = bPadded[j];
      }
      aPaddedAdditional[aPadded.length] = 0;
      bPaddedAdditional[bPadded.length] = 0;
      return iterateOnSplits(aPaddedAdditional, bPaddedAdditional, inclusive,  num);
    }
    final BigInteger intervalBI;
    try {
      intervalBI = diffBI.divide(splitsBI);
    } catch(Exception e) {
      LOG.error("Exception caught during division", e);
      return null;
    }

    final Iterator<byte[]> iterator = new Iterator<byte[]>() {
      private int i = -1;

      @Override
      public boolean hasNext() {
        return i < num+1;
      }

      @Override
      public byte[] next() {
        i++;
        if (i == 0) return a;
        if (i == num + 1) return b;

        BigInteger curBI = startBI.add(intervalBI.multiply(BigInteger.valueOf(i)));
        byte [] padded = curBI.toByteArray();
        if (padded[1] == 0)
          padded = tail(padded, padded.length - 2);
        else
          padded = tail(padded, padded.length - 1);
        return padded;
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }

    };

    return new Iterable<byte[]>() {
      @Override
      public Iterator<byte[]> iterator() {
        return iterator;
      }
    };
  }

  /**
   * @param bytes array to hash
   * @param offset offset to start from
   * @param length length to hash
   * */
  public static int hashCode(byte[] bytes, int offset, int length) {
    int hash = 1;
    for (int i = offset; i < offset + length; i++)
      hash = (31 * hash) + bytes[i];
    return hash;
  }

  /**
   * @param t operands
   * @return Array of byte arrays made from passed array of Text
   */
  public static byte [][] toByteArrays(final String [] t) {
    byte [][] result = new byte[t.length][];
    for (int i = 0; i < t.length; i++) {
      result[i] = Bytes.toBytes(t[i]);
    }
    return result;
  }

  /**
   * @param t operands
   * @return Array of binary byte arrays made from passed array of binary strings
   */
  public static byte[][] toBinaryByteArrays(final String[] t) {
    byte[][] result = new byte[t.length][];
    for (int i = 0; i < t.length; i++) {
      result[i] = Bytes.toBytesBinary(t[i]);
    }
    return result;
  }

  /**
   * @param column operand
   * @return A byte array of a byte array where first and only entry is
   * <code>column</code>
   */
  public static byte [][] toByteArrays(final String column) {
    return toByteArrays(toBytes(column));
  }

  /**
   * @param column operand
   * @return A byte array of a byte array where first and only entry is
   * <code>column</code>
   */
  public static byte [][] toByteArrays(final byte [] column) {
    byte [][] result = new byte[1][];
    result[0] = column;
    return result;
  }

  /**
   * Binary search for keys in indexes.
   *
   * @param arr array of byte arrays to search for
   * @param key the key you want to find
   * @param offset the offset in the key you want to find
   * @param length the length of the key
   * @param comparator a comparator to compare.
   * @return zero-based index of the key, if the key is present in the array.
   *         Otherwise, a value -(i + 1) such that the key is between arr[i -
   *         1] and arr[i] non-inclusively, where i is in [0, i], if we define
   *         arr[-1] = -Inf and arr[N] = Inf for an N-element array. The above
   *         means that this function can return 2N + 1 different values
   *         ranging from -(N + 1) to N - 1.
   * @deprecated since 2.0.0 and will be removed in 3.0.0. Use
   *   {@link #binarySearch(byte[][], byte[], int, int)} instead.
   * @see #binarySearch(byte[][], byte[], int, int)
   * @see <a href="https://issues.apache.org/jira/browse/HBASE-13450">HBASE-13450</a>
   */
  @Deprecated
  public static int binarySearch(byte [][]arr, byte []key, int offset,
                                 int length, RawComparator<?> comparator) {
    return binarySearch(arr, key, offset, length);
  }

  /**
   * Binary search for keys in indexes using Bytes.BYTES_RAWCOMPARATOR.
   *
   * @param arr array of byte arrays to search for
   * @param key the key you want to find
   * @param offset the offset in the key you want to find
   * @param length the length of the key
   * @return zero-based index of the key, if the key is present in the array.
   *         Otherwise, a value -(i + 1) such that the key is between arr[i -
   *         1] and arr[i] non-inclusively, where i is in [0, i], if we define
   *         arr[-1] = -Inf and arr[N] = Inf for an N-element array. The above
   *         means that this function can return 2N + 1 different values
   *         ranging from -(N + 1) to N - 1.
   */
  public static int binarySearch(byte[][] arr, byte[] key, int offset, int length) {
    int low = 0;
    int high = arr.length - 1;

    while (low <= high) {
      int mid = low + ((high - low) >> 1);
      // we have to compare in this order, because the comparator order
      // has special logic when the 'left side' is a special key.
      int cmp = Bytes.BYTES_RAWCOMPARATOR
          .compare(key, offset, length, arr[mid], 0, arr[mid].length);
      // key lives above the midpoint
      if (cmp > 0)
        low = mid + 1;
        // key lives below the midpoint
      else if (cmp < 0)
        high = mid - 1;
        // BAM. how often does this really happen?
      else
        return mid;
    }
    return -(low + 1);
  }

  /**
   * Binary search for keys in indexes.
   *
   * @param arr array of byte arrays to search for
   * @param key the key you want to find
   * @param comparator a comparator to compare.
   * @return zero-based index of the key, if the key is present in the array.
   *         Otherwise, a value -(i + 1) such that the key is between arr[i -
   *         1] and arr[i] non-inclusively, where i is in [0, i], if we define
   *         arr[-1] = -Inf and arr[N] = Inf for an N-element array. The above
   *         means that this function can return 2N + 1 different values
   *         ranging from -(N + 1) to N - 1.
   * @return the index of the block
   * @deprecated since 2.0.0 and will be removed in 3.0.0. Use
   *   {@link #binarySearch(Cell[], Cell, CellComparator)} instead.
   * @see #binarySearch(Cell[], Cell, CellComparator)
   * @see <a href="https://issues.apache.org/jira/browse/HBASE-13450">HBASE-13450</a>
   */
  @Deprecated
  public static int binarySearch(byte[][] arr, Cell key, RawComparator<Cell> comparator) {
    int low = 0;
    int high = arr.length - 1;
    KeyValue.KeyOnlyKeyValue r = new KeyValue.KeyOnlyKeyValue();
    while (low <= high) {
      int mid = low + ((high - low) >> 1);
      // we have to compare in this order, because the comparator order
      // has special logic when the 'left side' is a special key.
      r.setKey(arr[mid], 0, arr[mid].length);
      int cmp = comparator.compare(key, r);
      // key lives above the midpoint
      if (cmp > 0)
        low = mid + 1;
        // key lives below the midpoint
      else if (cmp < 0)
        high = mid - 1;
        // BAM. how often does this really happen?
      else
        return mid;
    }
    return - (low+1);
  }

  /**
   * Binary search for keys in indexes.
   *
   * @param arr array of byte arrays to search for
   * @param key the key you want to find
   * @param comparator a comparator to compare.
   * @return zero-based index of the key, if the key is present in the array.
   *         Otherwise, a value -(i + 1) such that the key is between arr[i -
   *         1] and arr[i] non-inclusively, where i is in [0, i], if we define
   *         arr[-1] = -Inf and arr[N] = Inf for an N-element array. The above
   *         means that this function can return 2N + 1 different values
   *         ranging from -(N + 1) to N - 1.
   * @return the index of the block
   */
  public static int binarySearch(Cell[] arr, Cell key, CellComparator comparator) {
    int low = 0;
    int high = arr.length - 1;
    while (low <= high) {
      int mid = low + ((high - low) >> 1);
      // we have to compare in this order, because the comparator order
      // has special logic when the 'left side' is a special key.
      int cmp = comparator.compare(key, arr[mid]);
      // key lives above the midpoint
      if (cmp > 0)
        low = mid + 1;
        // key lives below the midpoint
      else if (cmp < 0)
        high = mid - 1;
        // BAM. how often does this really happen?
      else
        return mid;
    }
    return - (low+1);
  }

  /**
   * Bytewise binary increment/deincrement of long contained in byte array
   * on given amount.
   *
   * @param value - array of bytes containing long (length &lt;= SIZEOF_LONG)
   * @param amount value will be incremented on (deincremented if negative)
   * @return array of bytes containing incremented long (length == SIZEOF_LONG)
   */
  public static byte [] incrementBytes(byte[] value, long amount)
  {
    byte[] val = value;
    if (val.length < SIZEOF_LONG) {
      // Hopefully this doesn't happen too often.
      byte [] newvalue;
      if (val[0] < 0) {
        newvalue = new byte[]{-1, -1, -1, -1, -1, -1, -1, -1};
      } else {
        newvalue = new byte[SIZEOF_LONG];
      }
      System.arraycopy(val, 0, newvalue, newvalue.length - val.length,
          val.length);
      val = newvalue;
    } else if (val.length > SIZEOF_LONG) {
      throw new IllegalArgumentException("Increment Bytes - value too big: " +
          val.length);
    }
    if(amount == 0) return val;
    if(val[0] < 0){
      return binaryIncrementNeg(val, amount);
    }
    return binaryIncrementPos(val, amount);
  }

  /* increment/deincrement for positive value */
  private static byte [] binaryIncrementPos(byte [] value, long amount) {
    long amo = amount;
    int sign = 1;
    if (amount < 0) {
      amo = -amount;
      sign = -1;
    }
    for(int i=0;i<value.length;i++) {
      int cur = ((int)amo % 256) * sign;
      amo = (amo >> 8);
      int val = value[value.length-i-1] & 0x0ff;
      int total = val + cur;
      if(total > 255) {
        amo += sign;
        total %= 256;
      } else if (total < 0) {
        amo -= sign;
      }
      value[value.length-i-1] = (byte)total;
      if (amo == 0) return value;
    }
    return value;
  }

  /* increment/deincrement for negative value */
  private static byte [] binaryIncrementNeg(byte [] value, long amount) {
    long amo = amount;
    int sign = 1;
    if (amount < 0) {
      amo = -amount;
      sign = -1;
    }
    for(int i=0;i<value.length;i++) {
      int cur = ((int)amo % 256) * sign;
      amo = (amo >> 8);
      int val = ((~value[value.length-i-1]) & 0x0ff) + 1;
      int total = cur - val;
      if(total >= 0) {
        amo += sign;
      } else if (total < -256) {
        amo -= sign;
        total %= 256;
      }
      value[value.length-i-1] = (byte)total;
      if (amo == 0) return value;
    }
    return value;
  }

  /**
   * Writes a string as a fixed-size field, padded with zeros.
   */
  public static void writeStringFixedSize(final DataOutput out, String s,
                                          int size) throws IOException {
    byte[] b = toBytes(s);
    if (b.length > size) {
      throw new IOException("Trying to write " + b.length + " bytes (" +
          toStringBinary(b) + ") into a field of length " + size);
    }

    out.writeBytes(s);
    for (int i = 0; i < size - s.length(); ++i)
      out.writeByte(0);
  }

  /**
   * Reads a fixed-size field and interprets it as a string padded with zeros.
   */
  public static String readStringFixedSize(final DataInput in, int size)
      throws IOException {
    byte[] b = new byte[size];
    in.readFully(b);
    int n = b.length;
    while (n > 0 && b[n - 1] == 0)
      --n;

    return toString(b, 0, n);
  }

  /**
   * Copy the byte array given in parameter and return an instance
   * of a new byte array with the same length and the same content.
   * @param bytes the byte array to duplicate
   * @return a copy of the given byte array
   */
  public static byte [] copy(byte [] bytes) {
    if (bytes == null) return null;
    byte [] result = new byte[bytes.length];
    System.arraycopy(bytes, 0, result, 0, bytes.length);
    return result;
  }

  /**
   * Copy the byte array given in parameter and return an instance
   * of a new byte array with the same length and the same content.
   * @param bytes the byte array to copy from
   * @return a copy of the given designated byte array
   * @param offset
   * @param length
   */
  public static byte [] copy(byte [] bytes, final int offset, final int length) {
    if (bytes == null) return null;
    byte [] result = new byte[length];
    System.arraycopy(bytes, offset, result, 0, length);
    return result;
  }

  /**
   * Search sorted array "a" for byte "key". I can't remember if I wrote this or copied it from
   * somewhere. (mcorgan)
   * @param a Array to search. Entries must be sorted and unique.
   * @param fromIndex First index inclusive of "a" to include in the search.
   * @param toIndex Last index exclusive of "a" to include in the search.
   * @param key The byte to search for.
   * @return The index of key if found. If not found, return -(index + 1), where negative indicates
   *         "not found" and the "index + 1" handles the "-0" case.
   */
  public static int unsignedBinarySearch(byte[] a, int fromIndex, int toIndex, byte key) {
    int unsignedKey = key & 0xff;
    int low = fromIndex;
    int high = toIndex - 1;

    while (low <= high) {
      int mid = low + ((high - low) >> 1);
      int midVal = a[mid] & 0xff;

      if (midVal < unsignedKey) {
        low = mid + 1;
      } else if (midVal > unsignedKey) {
        high = mid - 1;
      } else {
        return mid; // key found
      }
    }
    return -(low + 1); // key not found.
  }

  /**
   * Treat the byte[] as an unsigned series of bytes, most significant bits first.  Start by adding
   * 1 to the rightmost bit/byte and carry over all overflows to the more significant bits/bytes.
   *
   * @param input The byte[] to increment.
   * @return The incremented copy of "in".  May be same length or 1 byte longer.
   */
  public static byte[] unsignedCopyAndIncrement(final byte[] input) {
    byte[] copy = copy(input);
    if (copy == null) {
      throw new IllegalArgumentException("cannot increment null array");
    }
    for (int i = copy.length - 1; i >= 0; --i) {
      if (copy[i] == -1) {// -1 is all 1-bits, which is the unsigned maximum
        copy[i] = 0;
      } else {
        ++copy[i];
        return copy;
      }
    }
    // we maxed out the array
    byte[] out = new byte[copy.length + 1];
    out[0] = 1;
    System.arraycopy(copy, 0, out, 1, copy.length);
    return out;
  }

  public static boolean equals(List<byte[]> a, List<byte[]> b) {
    if (a == null) {
      if (b == null) {
        return true;
      }
      return false;
    }
    if (b == null) {
      return false;
    }
    if (a.size() != b.size()) {
      return false;
    }
    for (int i = 0; i < a.size(); ++i) {
      if (!Bytes.equals(a.get(i), b.get(i))) {
        return false;
      }
    }
    return true;
  }

  public static boolean isSorted(Collection<byte[]> arrays) {
    if (!CollectionUtils.isEmpty(arrays)) {
      byte[] previous = new byte[0];
      for (byte[] array : arrays) {
        if (Bytes.compareTo(previous, array) > 0) {
          return false;
        }
        previous = array;
      }
    }
    return true;
  }

  public static List<byte[]> getUtf8ByteArrays(List<String> strings) {
    if (CollectionUtils.isEmpty(strings)) {
      return Collections.emptyList();
    }
    List<byte[]> byteArrays = new ArrayList<>(strings.size());
    strings.forEach(s -> byteArrays.add(Bytes.toBytes(s)));
    return byteArrays;
  }

  /**
   * Returns the index of the first appearance of the value {@code target} in
   * {@code array}.
   *
   * @param array an array of {@code byte} values, possibly empty
   * @param target a primitive {@code byte} value
   * @return the least index {@code i} for which {@code array[i] == target}, or
   *     {@code -1} if no such index exists.
   */
  public static int indexOf(byte[] array, byte target) {
    for (int i = 0; i < array.length; i++) {
      if (array[i] == target) {
        return i;
      }
    }
    return -1;
  }

  /**
   * Returns the start position of the first occurrence of the specified {@code
   * target} within {@code array}, or {@code -1} if there is no such occurrence.
   *
   * <p>More formally, returns the lowest index {@code i} such that {@code
   * java.util.Arrays.copyOfRange(array, i, i + target.length)} contains exactly
   * the same elements as {@code target}.
   *
   * @param array the array to search for the sequence {@code target}
   * @param target the array to search for as a sub-sequence of {@code array}
   */
  public static int indexOf(byte[] array, byte[] target) {
    checkNotNull(array, "array");
    checkNotNull(target, "target");
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
   * @param array an array of {@code byte} values, possibly empty
   * @param target a primitive {@code byte} value
   * @return {@code true} if {@code target} is present as an element anywhere in {@code array}.
   */
  public static boolean contains(byte[] array, byte target) {
    return indexOf(array, target) > -1;
  }

  /**
   * @param array an array of {@code byte} values, possibly empty
   * @param target an array of {@code byte}
   * @return {@code true} if {@code target} is present anywhere in {@code array}
   */
  public static boolean contains(byte[] array, byte[] target) {
    return indexOf(array, target) > -1;
  }

  /**
   * Fill given array with zeros.
   * @param b array which needs to be filled with zeros
   */
  public static void zero(byte[] b) {
    zero(b, 0, b.length);
  }

  /**
   * Fill given array with zeros at the specified position.
   * @param b
   * @param offset
   * @param length
   */
  public static void zero(byte[] b, int offset, int length) {
    checkPositionIndex(offset, b.length, "offset");
    checkArgument(length > 0, "length must be greater than 0");
    checkPositionIndex(offset + length, b.length, "offset + length");
    Arrays.fill(b, offset, offset + length, (byte) 0);
  }

  private static final SecureRandom RNG = new SecureRandom();

  /**
   * Fill given array with random bytes.
   * @param b array which needs to be filled with random bytes
   */
  public static void random(byte[] b) {
    RNG.nextBytes(b);
  }

  /**
   * Fill given array with random bytes at the specified position.
   * @param b
   * @param offset
   * @param length
   */
  public static void random(byte[] b, int offset, int length) {
    checkPositionIndex(offset, b.length, "offset");
    checkArgument(length > 0, "length must be greater than 0");
    checkPositionIndex(offset + length, b.length, "offset + length");
    byte[] buf = new byte[length];
    RNG.nextBytes(buf);
    System.arraycopy(buf, 0, b, offset, length);
  }

  /**
   * Create a max byte array with the specified max byte count
   * @param maxByteCount the length of returned byte array
   * @return the created max byte array
   */
  public static byte[] createMaxByteArray(int maxByteCount) {
    byte[] maxByteArray = new byte[maxByteCount];
    for (int i = 0; i < maxByteArray.length; i++) {
      maxByteArray[i] = (byte) 0xff;
    }
    return maxByteArray;
  }

  /**
   * Create a byte array which is multiple given bytes
   * @param srcBytes
   * @param multiNum
   * @return byte array
   */
  public static byte[] multiple(byte[] srcBytes, int multiNum) {
    if (multiNum <= 0) {
      return new byte[0];
    }
    byte[] result = new byte[srcBytes.length * multiNum];
    for (int i = 0; i < multiNum; i++) {
      System.arraycopy(srcBytes, 0, result, i * srcBytes.length,
          srcBytes.length);
    }
    return result;
  }

  private static final char[] HEX_CHARS = {
      '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'
  };

  /**
   * Convert a byte range into a hex string
   */
  public static String toHex(byte[] b, int offset, int length) {
    checkArgument(length <= Integer.MAX_VALUE / 2);
    int numChars = length * 2;
    char[] ch = new char[numChars];
    for (int i = 0; i < numChars; i += 2)
    {
      byte d = b[offset + i/2];
      ch[i] = HEX_CHARS[(d >> 4) & 0x0F];
      ch[i+1] = HEX_CHARS[d & 0x0F];
    }
    return new String(ch);
  }

  /**
   * Convert a byte array into a hex string
   */
  public static String toHex(byte[] b) {
    return toHex(b, 0, b.length);
  }

  private static int hexCharToNibble(char ch) {
    if (ch <= '9' && ch >= '0') {
      return ch - '0';
    } else if (ch >= 'a' && ch <= 'f') {
      return ch - 'a' + 10;
    } else if (ch >= 'A' && ch <= 'F') {
      return ch - 'A' + 10;
    }
    throw new IllegalArgumentException("Invalid hex char: " + ch);
  }

  private static byte hexCharsToByte(char c1, char c2) {
    return (byte) ((hexCharToNibble(c1) << 4) | hexCharToNibble(c2));
  }

  /**
   * Create a byte array from a string of hash digits. The length of the
   * string must be a multiple of 2
   * @param hex
   */
  public static byte[] fromHex(String hex) {
    checkArgument(hex.length() % 2 == 0, "length must be a multiple of 2");
    int len = hex.length();
    byte[] b = new byte[len / 2];
    for (int i = 0; i < len; i += 2) {
      b[i / 2] = hexCharsToByte(hex.charAt(i),hex.charAt(i+1));
    }
    return b;
  }

  /**
   * @param b
   * @param delimiter
   * @return Index of delimiter having started from start of <code>b</code> moving rightward.
   */
  public static int searchDelimiterIndex(final byte[] b, int offset, final int length,
                                         final int delimiter) {
    if (b == null) {
      throw new IllegalArgumentException("Passed buffer is null");
    }
    int result = -1;
    for (int i = offset; i < length + offset; i++) {
      if (b[i] == delimiter) {
        result = i;
        break;
      }
    }
    return result;
  }

  /**
   * Find index of passed delimiter walking from end of buffer backwards.
   *
   * @param b
   * @param delimiter
   * @return Index of delimiter
   */
  public static int searchDelimiterIndexInReverse(final byte[] b, final int offset,
                                                  final int length, final int delimiter) {
    if (b == null) {
      throw new IllegalArgumentException("Passed buffer is null");
    }
    int result = -1;
    for (int i = (offset + length) - 1; i >= offset; i--) {
      if (b[i] == delimiter) {
        result = i;
        break;
      }
    }
    return result;
  }

  public static int findCommonPrefix(byte[] left, byte[] right, int leftLength, int rightLength,
                                     int leftOffset, int rightOffset) {
    int length = Math.min(leftLength, rightLength);
    int result = 0;

    while (result < length && left[leftOffset + result] == right[rightOffset + result]) {
      result++;
    }
    return result;
  }
}
