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

package org.apache.hudi.hbase;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.hudi.hbase.KeyValue.Type;
import org.apache.hudi.hbase.io.util.StreamUtils;
import org.apache.hudi.hbase.util.ByteBufferUtils;
import org.apache.hudi.hbase.util.Bytes;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.WritableUtils;

import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Function;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.org.apache.commons.collections4.IterableUtils;

/**
 * static convenience methods for dealing with KeyValues and collections of KeyValues
 */
@InterfaceAudience.Private
public class KeyValueUtil {

  private static final Logger LOG = LoggerFactory.getLogger(KeyValueUtil.class);

  /**************** length *********************/

  public static int length(short rlen, byte flen, int qlen, int vlen, int tlen, boolean withTags) {
    if (withTags) {
      return (int) (KeyValue.getKeyValueDataStructureSize(rlen, flen, qlen, vlen, tlen));
    }
    return (int) (KeyValue.getKeyValueDataStructureSize(rlen, flen, qlen, vlen));
  }

  /**
   * Returns number of bytes this cell's key part would have been used if serialized as in
   * {@link KeyValue}. Key includes rowkey, family, qualifier, timestamp and type.
   * @param cell
   * @return the key length
   */
  public static int keyLength(final Cell cell) {
    return keyLength(cell.getRowLength(), cell.getFamilyLength(), cell.getQualifierLength());
  }

  private static int keyLength(short rlen, byte flen, int qlen) {
    return (int) KeyValue.getKeyDataStructureSize(rlen, flen, qlen);
  }

  public static int lengthWithMvccVersion(final KeyValue kv, final boolean includeMvccVersion) {
    int length = kv.getLength();
    if (includeMvccVersion) {
      length += WritableUtils.getVIntSize(kv.getSequenceId());
    }
    return length;
  }

  public static int totalLengthWithMvccVersion(final Iterable<? extends KeyValue> kvs,
                                               final boolean includeMvccVersion) {
    int length = 0;
    for (KeyValue kv : IterableUtils.emptyIfNull(kvs)) {
      length += lengthWithMvccVersion(kv, includeMvccVersion);
    }
    return length;
  }


  /**************** copy the cell to create a new keyvalue *********************/

  public static KeyValue copyToNewKeyValue(final Cell cell) {
    byte[] bytes = copyToNewByteArray(cell);
    KeyValue kvCell = new KeyValue(bytes, 0, bytes.length);
    kvCell.setSequenceId(cell.getSequenceId());
    return kvCell;
  }

  /**
   * The position will be set to the beginning of the new ByteBuffer
   * @param cell
   * @return the Bytebuffer containing the key part of the cell
   */
  public static ByteBuffer copyKeyToNewByteBuffer(final Cell cell) {
    byte[] bytes = new byte[keyLength(cell)];
    appendKeyTo(cell, bytes, 0);
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    return buffer;
  }

  /**
   * Copies the key to a new KeyValue
   * @param cell
   * @return the KeyValue that consists only the key part of the incoming cell
   */
  public static KeyValue toNewKeyCell(final Cell cell) {
    byte[] bytes = new byte[keyLength(cell)];
    appendKeyTo(cell, bytes, 0);
    KeyValue kv = new KeyValue.KeyOnlyKeyValue(bytes, 0, bytes.length);
    // Set the seq id. The new key cell could be used in comparisons so it
    // is important that it uses the seqid also. If not the comparsion would fail
    kv.setSequenceId(cell.getSequenceId());
    return kv;
  }

  public static byte[] copyToNewByteArray(final Cell cell) {
    //Cell#getSerializedSize returns the serialized size of the Source cell, which may
    //not serialize all fields. We are constructing a KeyValue backing array here,
    //which does include all fields, and must allocate accordingly.
    int v1Length = length(cell.getRowLength(), cell.getFamilyLength(),
        cell.getQualifierLength(), cell.getValueLength(), cell.getTagsLength(), true);
    byte[] backingBytes = new byte[v1Length];
    appendToByteArray(cell, backingBytes, 0, true);
    return backingBytes;
  }

  public static int appendKeyTo(final Cell cell, final byte[] output,
                                final int offset) {
    int nextOffset = offset;
    nextOffset = Bytes.putShort(output, nextOffset, cell.getRowLength());
    nextOffset = CellUtil.copyRowTo(cell, output, nextOffset);
    nextOffset = Bytes.putByte(output, nextOffset, cell.getFamilyLength());
    nextOffset = CellUtil.copyFamilyTo(cell, output, nextOffset);
    nextOffset = CellUtil.copyQualifierTo(cell, output, nextOffset);
    nextOffset = Bytes.putLong(output, nextOffset, cell.getTimestamp());
    nextOffset = Bytes.putByte(output, nextOffset, cell.getTypeByte());
    return nextOffset;
  }

  /**************** copy key and value *********************/

  public static int appendToByteArray(Cell cell, byte[] output, int offset, boolean withTags) {
    int pos = offset;
    pos = Bytes.putInt(output, pos, keyLength(cell));
    pos = Bytes.putInt(output, pos, cell.getValueLength());
    pos = appendKeyTo(cell, output, pos);
    pos = CellUtil.copyValueTo(cell, output, pos);
    if (withTags && (cell.getTagsLength() > 0)) {
      pos = Bytes.putAsShort(output, pos, cell.getTagsLength());
      pos = PrivateCellUtil.copyTagsTo(cell, output, pos);
    }
    return pos;
  }

  /**
   * Copy the Cell content into the passed buf in KeyValue serialization format.
   */
  public static int appendTo(Cell cell, ByteBuffer buf, int offset, boolean withTags) {
    offset = ByteBufferUtils.putInt(buf, offset, keyLength(cell));// Key length
    offset = ByteBufferUtils.putInt(buf, offset, cell.getValueLength());// Value length
    offset = appendKeyTo(cell, buf, offset);
    offset = CellUtil.copyValueTo(cell, buf, offset);// Value bytes
    int tagsLength = cell.getTagsLength();
    if (withTags && (tagsLength > 0)) {
      offset = ByteBufferUtils.putAsShort(buf, offset, tagsLength);// Tags length
      offset = PrivateCellUtil.copyTagsTo(cell, buf, offset);// Tags bytes
    }
    return offset;
  }

  public static int appendKeyTo(Cell cell, ByteBuffer buf, int offset) {
    offset = ByteBufferUtils.putShort(buf, offset, cell.getRowLength());// RK length
    offset = CellUtil.copyRowTo(cell, buf, offset);// Row bytes
    offset = ByteBufferUtils.putByte(buf, offset, cell.getFamilyLength());// CF length
    offset = CellUtil.copyFamilyTo(cell, buf, offset);// CF bytes
    offset = CellUtil.copyQualifierTo(cell, buf, offset);// Qualifier bytes
    offset = ByteBufferUtils.putLong(buf, offset, cell.getTimestamp());// TS
    offset = ByteBufferUtils.putByte(buf, offset, cell.getTypeByte());// Type
    return offset;
  }

  public static void appendToByteBuffer(final ByteBuffer bb, final KeyValue kv,
                                        final boolean includeMvccVersion) {
    // keep pushing the limit out. assume enough capacity
    bb.limit(bb.position() + kv.getLength());
    bb.put(kv.getBuffer(), kv.getOffset(), kv.getLength());
    if (includeMvccVersion) {
      int numMvccVersionBytes = WritableUtils.getVIntSize(kv.getSequenceId());
      ByteBufferUtils.extendLimit(bb, numMvccVersionBytes);
      ByteBufferUtils.writeVLong(bb, kv.getSequenceId());
    }
  }


  /**************** iterating *******************************/

  /**
   * Creates a new KeyValue object positioned in the supplied ByteBuffer and sets the ByteBuffer's
   * position to the start of the next KeyValue. Does not allocate a new array or copy data.
   * @param bb
   * @param includesMvccVersion
   * @param includesTags
   */
  public static KeyValue nextShallowCopy(final ByteBuffer bb, final boolean includesMvccVersion,
                                         boolean includesTags) {
    if (bb.isDirect()) {
      throw new IllegalArgumentException("only supports heap buffers");
    }
    if (bb.remaining() < 1) {
      return null;
    }
    KeyValue keyValue = null;
    int underlyingArrayOffset = bb.arrayOffset() + bb.position();
    int keyLength = bb.getInt();
    int valueLength = bb.getInt();
    ByteBufferUtils.skip(bb, keyLength + valueLength);
    int tagsLength = 0;
    if (includesTags) {
      // Read short as unsigned, high byte first
      tagsLength = ((bb.get() & 0xff) << 8) ^ (bb.get() & 0xff);
      ByteBufferUtils.skip(bb, tagsLength);
    }
    int kvLength = (int) KeyValue.getKeyValueDataStructureSize(keyLength, valueLength, tagsLength);
    keyValue = new KeyValue(bb.array(), underlyingArrayOffset, kvLength);
    if (includesMvccVersion) {
      long mvccVersion = ByteBufferUtils.readVLong(bb);
      keyValue.setSequenceId(mvccVersion);
    }
    return keyValue;
  }


  /*************** next/previous **********************************/

  /**
   * Decrement the timestamp.  For tests (currently wasteful)
   *
   * Remember timestamps are sorted reverse chronologically.
   * @param in
   * @return previous key
   */
  public static KeyValue previousKey(final KeyValue in) {
    return createFirstOnRow(CellUtil.cloneRow(in), CellUtil.cloneFamily(in),
        CellUtil.cloneQualifier(in), in.getTimestamp() - 1);
  }


  /**
   * Create a KeyValue for the specified row, family and qualifier that would be
   * larger than or equal to all other possible KeyValues that have the same
   * row, family, qualifier. Used for reseeking. Should NEVER be returned to a client.
   *
   * @param row
   *          row key
   * @param roffset
   *         row offset
   * @param rlength
   *         row length
   * @param family
   *         family name
   * @param foffset
   *         family offset
   * @param flength
   *         family length
   * @param qualifier
   *        column qualifier
   * @param qoffset
   *        qualifier offset
   * @param qlength
   *        qualifier length
   * @return Last possible key on passed row, family, qualifier.
   */
  public static KeyValue createLastOnRow(final byte[] row, final int roffset, final int rlength,
                                         final byte[] family, final int foffset, final int flength, final byte[] qualifier,
                                         final int qoffset, final int qlength) {
    return new KeyValue(row, roffset, rlength, family, foffset, flength, qualifier, qoffset,
        qlength, HConstants.OLDEST_TIMESTAMP, Type.Minimum, null, 0, 0);
  }

  /**
   * Create a KeyValue that is smaller than all other possible KeyValues
   * for the given row. That is any (valid) KeyValue on 'row' would sort
   * _after_ the result.
   *
   * @param row - row key (arbitrary byte array)
   * @return First possible KeyValue on passed <code>row</code>
   */
  public static KeyValue createFirstOnRow(final byte [] row, int roffset, short rlength) {
    return new KeyValue(row, roffset, rlength,
        null, 0, 0, null, 0, 0, HConstants.LATEST_TIMESTAMP, Type.Maximum, null, 0, 0);
  }

  /**
   * Creates a KeyValue that is last on the specified row id. That is,
   * every other possible KeyValue for the given row would compareTo()
   * less than the result of this call.
   * @param row row key
   * @return Last possible KeyValue on passed <code>row</code>
   */
  public static KeyValue createLastOnRow(final byte[] row) {
    return new KeyValue(row, null, null, HConstants.LATEST_TIMESTAMP, Type.Minimum);
  }

  /**
   * Create a KeyValue that is smaller than all other possible KeyValues
   * for the given row. That is any (valid) KeyValue on 'row' would sort
   * _after_ the result.
   *
   * @param row - row key (arbitrary byte array)
   * @return First possible KeyValue on passed <code>row</code>
   */
  public static KeyValue createFirstOnRow(final byte [] row) {
    return createFirstOnRow(row, HConstants.LATEST_TIMESTAMP);
  }

  /**
   * Creates a KeyValue that is smaller than all other KeyValues that
   * are older than the passed timestamp.
   * @param row - row key (arbitrary byte array)
   * @param ts - timestamp
   * @return First possible key on passed <code>row</code> and timestamp.
   */
  public static KeyValue createFirstOnRow(final byte [] row,
                                          final long ts) {
    return new KeyValue(row, null, null, ts, Type.Maximum);
  }

  /**
   * Create a KeyValue for the specified row, family and qualifier that would be
   * smaller than all other possible KeyValues that have the same row,family,qualifier.
   * Used for seeking.
   * @param row - row key (arbitrary byte array)
   * @param family - family name
   * @param qualifier - column qualifier
   * @return First possible key on passed <code>row</code>, and column.
   */
  public static KeyValue createFirstOnRow(final byte [] row, final byte [] family,
                                          final byte [] qualifier) {
    return new KeyValue(row, family, qualifier, HConstants.LATEST_TIMESTAMP, Type.Maximum);
  }

  /**
   * @param row - row key (arbitrary byte array)
   * @param f - family name
   * @param q - column qualifier
   * @param ts - timestamp
   * @return First possible key on passed <code>row</code>, column and timestamp
   */
  public static KeyValue createFirstOnRow(final byte [] row, final byte [] f,
                                          final byte [] q, final long ts) {
    return new KeyValue(row, f, q, ts, Type.Maximum);
  }

  /**
   * Create a KeyValue for the specified row, family and qualifier that would be
   * smaller than all other possible KeyValues that have the same row,
   * family, qualifier.
   * Used for seeking.
   * @param row row key
   * @param roffset row offset
   * @param rlength row length
   * @param family family name
   * @param foffset family offset
   * @param flength family length
   * @param qualifier column qualifier
   * @param qoffset qualifier offset
   * @param qlength qualifier length
   * @return First possible key on passed Row, Family, Qualifier.
   */
  public static KeyValue createFirstOnRow(final byte [] row,
                                          final int roffset, final int rlength, final byte [] family,
                                          final int foffset, final int flength, final byte [] qualifier,
                                          final int qoffset, final int qlength) {
    return new KeyValue(row, roffset, rlength, family,
        foffset, flength, qualifier, qoffset, qlength,
        HConstants.LATEST_TIMESTAMP, Type.Maximum, null, 0, 0);
  }

  /**
   * Create a KeyValue for the specified row, family and qualifier that would be
   * smaller than all other possible KeyValues that have the same row,
   * family, qualifier.
   * Used for seeking.
   *
   * @param buffer the buffer to use for the new <code>KeyValue</code> object
   * @param row the value key
   * @param family family name
   * @param qualifier column qualifier
   *
   * @return First possible key on passed Row, Family, Qualifier.
   *
   * @throws IllegalArgumentException The resulting <code>KeyValue</code> object would be larger
   * than the provided buffer or than <code>Integer.MAX_VALUE</code>
   */
  public static KeyValue createFirstOnRow(byte [] buffer, final byte [] row,
                                          final byte [] family, final byte [] qualifier)
      throws IllegalArgumentException {
    return createFirstOnRow(buffer, 0, row, 0, row.length,
        family, 0, family.length,
        qualifier, 0, qualifier.length);
  }

  /**
   * Create a KeyValue for the specified row, family and qualifier that would be
   * smaller than all other possible KeyValues that have the same row,
   * family, qualifier.
   * Used for seeking.
   *
   * @param buffer the buffer to use for the new <code>KeyValue</code> object
   * @param boffset buffer offset
   * @param row the value key
   * @param roffset row offset
   * @param rlength row length
   * @param family family name
   * @param foffset family offset
   * @param flength family length
   * @param qualifier column qualifier
   * @param qoffset qualifier offset
   * @param qlength qualifier length
   *
   * @return First possible key on passed Row, Family, Qualifier.
   *
   * @throws IllegalArgumentException The resulting <code>KeyValue</code> object would be larger
   * than the provided buffer or than <code>Integer.MAX_VALUE</code>
   */
  public static KeyValue createFirstOnRow(byte[] buffer, final int boffset, final byte[] row,
                                          final int roffset, final int rlength, final byte[] family, final int foffset,
                                          final int flength, final byte[] qualifier, final int qoffset, final int qlength)
      throws IllegalArgumentException {

    long lLength = KeyValue.getKeyValueDataStructureSize(rlength, flength, qlength, 0);

    if (lLength > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("KeyValue length " + lLength + " > " + Integer.MAX_VALUE);
    }
    int iLength = (int) lLength;
    if (buffer.length - boffset < iLength) {
      throw new IllegalArgumentException("Buffer size " + (buffer.length - boffset) + " < "
          + iLength);
    }

    int len = KeyValue.writeByteArray(buffer, boffset, row, roffset, rlength, family, foffset,
        flength, qualifier, qoffset, qlength, HConstants.LATEST_TIMESTAMP, KeyValue.Type.Maximum,
        null, 0, 0, null);
    return new KeyValue(buffer, boffset, len);
  }

  /*************** misc **********************************/
  /**
   * @param cell
   * @return <code>cell</code> if it is an object of class {@link KeyValue} else we will return a
   *         new {@link KeyValue} instance made from <code>cell</code> Note: Even if the cell is an
   *         object of any of the subclass of {@link KeyValue}, we will create a new
   *         {@link KeyValue} object wrapping same buffer. This API is used only with MR based tools
   *         which expect the type to be exactly KeyValue. That is the reason for doing this way.
   * @deprecated without any replacement.
   */
  @Deprecated
  public static KeyValue ensureKeyValue(final Cell cell) {
    if (cell == null) return null;
    if (cell instanceof KeyValue) {
      if (cell.getClass().getName().equals(KeyValue.class.getName())) {
        return (KeyValue) cell;
      }
      // Cell is an Object of any of the sub classes of KeyValue. Make a new KeyValue wrapping the
      // same byte[]
      KeyValue kv = (KeyValue) cell;
      KeyValue newKv = new KeyValue(kv.bytes, kv.offset, kv.length);
      newKv.setSequenceId(kv.getSequenceId());
      return newKv;
    }
    return copyToNewKeyValue(cell);
  }

  @Deprecated
  public static List<KeyValue> ensureKeyValues(List<Cell> cells) {
    List<KeyValue> lazyList = Lists.transform(cells, new Function<Cell, KeyValue>() {
      @Override
      public KeyValue apply(Cell arg0) {
        return KeyValueUtil.ensureKeyValue(arg0);
      }
    });
    return new ArrayList<>(lazyList);
  }
  /**
   * Write out a KeyValue in the manner in which we used to when KeyValue was a
   * Writable.
   *
   * @param kv
   * @param out
   * @return Length written on stream
   * @throws IOException
   * @see #create(DataInput) for the inverse function
   */
  public static long write(final KeyValue kv, final DataOutput out) throws IOException {
    // This is how the old Writables write used to serialize KVs. Need to figure
    // way to make it
    // work for all implementations.
    int length = kv.getLength();
    out.writeInt(length);
    out.write(kv.getBuffer(), kv.getOffset(), length);
    return (long) length + Bytes.SIZEOF_INT;
  }

  static String bytesToHex(byte[] buf, int offset, int length) {
    String bufferContents = buf != null ? Bytes.toStringBinary(buf, offset, length) : "<null>";
    return ", KeyValueBytesHex=" + bufferContents + ", offset=" + offset + ", length=" + length;
  }

  static void checkKeyValueBytes(byte[] buf, int offset, int length, boolean withTags) {
    if (buf == null) {
      String msg = "Invalid to have null byte array in KeyValue.";
      LOG.warn(msg);
      throw new IllegalArgumentException(msg);
    }

    int pos = offset, endOffset = offset + length;
    // check the key
    if (pos + Bytes.SIZEOF_INT > endOffset) {
      String msg =
          "Overflow when reading key length at position=" + pos + bytesToHex(buf, offset, length);
      LOG.warn(msg);
      throw new IllegalArgumentException(msg);
    }
    int keyLen = Bytes.toInt(buf, pos, Bytes.SIZEOF_INT);
    pos += Bytes.SIZEOF_INT;
    if (keyLen <= 0 || pos + keyLen > endOffset) {
      String msg =
          "Invalid key length in KeyValue. keyLength=" + keyLen + bytesToHex(buf, offset, length);
      LOG.warn(msg);
      throw new IllegalArgumentException(msg);
    }
    // check the value
    if (pos + Bytes.SIZEOF_INT > endOffset) {
      String msg =
          "Overflow when reading value length at position=" + pos + bytesToHex(buf, offset, length);
      LOG.warn(msg);
      throw new IllegalArgumentException(msg);
    }
    int valLen = Bytes.toInt(buf, pos, Bytes.SIZEOF_INT);
    pos += Bytes.SIZEOF_INT;
    if (valLen < 0 || pos + valLen > endOffset) {
      String msg = "Invalid value length in KeyValue, valueLength=" + valLen +
          bytesToHex(buf, offset, length);
      LOG.warn(msg);
      throw new IllegalArgumentException(msg);
    }
    // check the row
    if (pos + Bytes.SIZEOF_SHORT > endOffset) {
      String msg =
          "Overflow when reading row length at position=" + pos + bytesToHex(buf, offset, length);
      LOG.warn(msg);
      throw new IllegalArgumentException(msg);
    }
    short rowLen = Bytes.toShort(buf, pos, Bytes.SIZEOF_SHORT);
    pos += Bytes.SIZEOF_SHORT;
    if (rowLen < 0 || pos + rowLen > endOffset) {
      String msg =
          "Invalid row length in KeyValue, rowLength=" + rowLen + bytesToHex(buf, offset, length);
      LOG.warn(msg);
      throw new IllegalArgumentException(msg);
    }
    pos += rowLen;
    // check the family
    if (pos + Bytes.SIZEOF_BYTE > endOffset) {
      String msg = "Overflow when reading family length at position=" + pos +
          bytesToHex(buf, offset, length);
      LOG.warn(msg);
      throw new IllegalArgumentException(msg);
    }
    int familyLen = buf[pos];
    pos += Bytes.SIZEOF_BYTE;
    if (familyLen < 0 || pos + familyLen > endOffset) {
      String msg = "Invalid family length in KeyValue, familyLength=" + familyLen +
          bytesToHex(buf, offset, length);
      LOG.warn(msg);
      throw new IllegalArgumentException(msg);
    }
    pos += familyLen;
    // check the qualifier
    int qualifierLen = keyLen - Bytes.SIZEOF_SHORT - rowLen - Bytes.SIZEOF_BYTE - familyLen
        - Bytes.SIZEOF_LONG - Bytes.SIZEOF_BYTE;
    if (qualifierLen < 0 || pos + qualifierLen > endOffset) {
      String msg = "Invalid qualifier length in KeyValue, qualifierLen=" + qualifierLen +
          bytesToHex(buf, offset, length);
      LOG.warn(msg);
      throw new IllegalArgumentException(msg);
    }
    pos += qualifierLen;
    // check the timestamp
    if (pos + Bytes.SIZEOF_LONG > endOffset) {
      String msg =
          "Overflow when reading timestamp at position=" + pos + bytesToHex(buf, offset, length);
      LOG.warn(msg);
      throw new IllegalArgumentException(msg);
    }
    long timestamp = Bytes.toLong(buf, pos, Bytes.SIZEOF_LONG);
    if (timestamp < 0) {
      String msg =
          "Timestamp cannot be negative, ts=" + timestamp + bytesToHex(buf, offset, length);
      LOG.warn(msg);
      throw new IllegalArgumentException(msg);
    }
    pos += Bytes.SIZEOF_LONG;
    // check the type
    if (pos + Bytes.SIZEOF_BYTE > endOffset) {
      String msg =
          "Overflow when reading type at position=" + pos + bytesToHex(buf, offset, length);
      LOG.warn(msg);
      throw new IllegalArgumentException(msg);
    }
    byte type = buf[pos];
    if (!Type.isValidType(type)) {
      String msg = "Invalid type in KeyValue, type=" + type + bytesToHex(buf, offset, length);
      LOG.warn(msg);
      throw new IllegalArgumentException(msg);
    }
    pos += Bytes.SIZEOF_BYTE;
    // check the value
    if (pos + valLen > endOffset) {
      String msg =
          "Overflow when reading value part at position=" + pos + bytesToHex(buf, offset, length);
      LOG.warn(msg);
      throw new IllegalArgumentException(msg);
    }
    pos += valLen;
    // check the tags
    if (withTags) {
      if (pos == endOffset) {
        // withTags is true but no tag in the cell.
        return;
      }
      pos = checkKeyValueTagBytes(buf, offset, length, pos, endOffset);
    }
    if (pos != endOffset) {
      String msg = "Some redundant bytes in KeyValue's buffer, startOffset=" + pos + ", endOffset="
          + endOffset + bytesToHex(buf, offset, length);
      LOG.warn(msg);
      throw new IllegalArgumentException(msg);
    }
  }

  private static int checkKeyValueTagBytes(byte[] buf, int offset, int length, int pos,
                                           int endOffset) {
    if (pos + Bytes.SIZEOF_SHORT > endOffset) {
      String msg = "Overflow when reading tags length at position=" + pos +
          bytesToHex(buf, offset, length);
      LOG.warn(msg);
      throw new IllegalArgumentException(msg);
    }
    short tagsLen = Bytes.toShort(buf, pos);
    pos += Bytes.SIZEOF_SHORT;
    if (tagsLen < 0 || pos + tagsLen > endOffset) {
      String msg = "Invalid tags length in KeyValue at position=" + (pos - Bytes.SIZEOF_SHORT)
          + bytesToHex(buf, offset, length);
      LOG.warn(msg);
      throw new IllegalArgumentException(msg);
    }
    int tagsEndOffset = pos + tagsLen;
    for (; pos < tagsEndOffset;) {
      if (pos + Tag.TAG_LENGTH_SIZE > endOffset) {
        String msg = "Overflow when reading tag length at position=" + pos +
            bytesToHex(buf, offset, length);
        LOG.warn(msg);
        throw new IllegalArgumentException(msg);
      }
      short tagLen = Bytes.toShort(buf, pos);
      pos += Tag.TAG_LENGTH_SIZE;
      // tagLen contains one byte tag type, so must be not less than 1.
      if (tagLen < 1 || pos + tagLen > endOffset) {
        String msg =
            "Invalid tag length at position=" + (pos - Tag.TAG_LENGTH_SIZE) + ", tagLength="
                + tagLen + bytesToHex(buf, offset, length);
        LOG.warn(msg);
        throw new IllegalArgumentException(msg);
      }
      pos += tagLen;
    }
    return pos;
  }

  /**
   * Create a KeyValue reading from the raw InputStream. Named
   * <code>createKeyValueFromInputStream</code> so doesn't clash with {@link #create(DataInput)}
   * @param in inputStream to read.
   * @param withTags whether the keyvalue should include tags are not
   * @return Created KeyValue OR if we find a length of zero, we will return null which can be
   *         useful marking a stream as done.
   * @throws IOException
   */
  public static KeyValue createKeyValueFromInputStream(InputStream in, boolean withTags)
      throws IOException {
    byte[] intBytes = new byte[Bytes.SIZEOF_INT];
    int bytesRead = 0;
    while (bytesRead < intBytes.length) {
      int n = in.read(intBytes, bytesRead, intBytes.length - bytesRead);
      if (n < 0) {
        if (bytesRead == 0) {
          throw new EOFException();
        }
        throw new IOException("Failed read of int, read " + bytesRead + " bytes");
      }
      bytesRead += n;
    }
    byte[] bytes = new byte[Bytes.toInt(intBytes)];
    IOUtils.readFully(in, bytes, 0, bytes.length);
    return withTags ? new KeyValue(bytes, 0, bytes.length)
        : new NoTagsKeyValue(bytes, 0, bytes.length);
  }

  /**
   * @param b
   * @return A KeyValue made of a byte array that holds the key-only part.
   *         Needed to convert hfile index members to KeyValues.
   */
  public static KeyValue createKeyValueFromKey(final byte[] b) {
    return createKeyValueFromKey(b, 0, b.length);
  }

  /**
   * @param bb
   * @return A KeyValue made of a byte buffer that holds the key-only part.
   *         Needed to convert hfile index members to KeyValues.
   */
  public static KeyValue createKeyValueFromKey(final ByteBuffer bb) {
    return createKeyValueFromKey(bb.array(), bb.arrayOffset(), bb.limit());
  }

  /**
   * @param b
   * @param o
   * @param l
   * @return A KeyValue made of a byte array that holds the key-only part.
   *         Needed to convert hfile index members to KeyValues.
   */
  public static KeyValue createKeyValueFromKey(final byte[] b, final int o, final int l) {
    byte[] newb = new byte[l + KeyValue.ROW_OFFSET];
    System.arraycopy(b, o, newb, KeyValue.ROW_OFFSET, l);
    Bytes.putInt(newb, 0, l);
    Bytes.putInt(newb, Bytes.SIZEOF_INT, 0);
    return new KeyValue(newb);
  }

  /**
   * @param in
   *          Where to read bytes from. Creates a byte array to hold the
   *          KeyValue backing bytes copied from the steam.
   * @return KeyValue created by deserializing from <code>in</code> OR if we
   *         find a length of zero, we will return null which can be useful
   *         marking a stream as done.
   * @throws IOException
   */
  public static KeyValue create(final DataInput in) throws IOException {
    return create(in.readInt(), in);
  }

  /**
   * Create a KeyValue reading <code>length</code> from <code>in</code>
   *
   * @param length
   * @param in
   * @return Created KeyValue OR if we find a length of zero, we will return
   *         null which can be useful marking a stream as done.
   * @throws IOException
   */
  public static KeyValue create(int length, final DataInput in) throws IOException {

    if (length <= 0) {
      if (length == 0)
        return null;
      throw new IOException("Failed read " + length + " bytes, stream corrupt?");
    }

    // This is how the old Writables.readFrom used to deserialize. Didn't even
    // vint.
    byte[] bytes = new byte[length];
    in.readFully(bytes);
    return new KeyValue(bytes, 0, length);
  }

  public static int getSerializedSize(Cell cell, boolean withTags) {
    if (withTags) {
      return cell.getSerializedSize();
    }
    if (cell instanceof ExtendedCell) {
      return ((ExtendedCell) cell).getSerializedSize(withTags);
    }
    return length(cell.getRowLength(), cell.getFamilyLength(), cell.getQualifierLength(),
        cell.getValueLength(), cell.getTagsLength(), withTags);
  }

  public static int oswrite(final Cell cell, final OutputStream out, final boolean withTags)
      throws IOException {
    if (cell instanceof ExtendedCell) {
      return ((ExtendedCell)cell).write(out, withTags);
    } else {
      short rlen = cell.getRowLength();
      byte flen = cell.getFamilyLength();
      int qlen = cell.getQualifierLength();
      int vlen = cell.getValueLength();
      int tlen = cell.getTagsLength();
      int size = 0;
      // write key length
      int klen = keyLength(rlen, flen, qlen);
      ByteBufferUtils.putInt(out, klen);
      // write value length
      ByteBufferUtils.putInt(out, vlen);
      // Write rowkey - 2 bytes rk length followed by rowkey bytes
      StreamUtils.writeShort(out, rlen);
      out.write(cell.getRowArray(), cell.getRowOffset(), rlen);
      // Write cf - 1 byte of cf length followed by the family bytes
      out.write(flen);
      out.write(cell.getFamilyArray(), cell.getFamilyOffset(), flen);
      // write qualifier
      out.write(cell.getQualifierArray(), cell.getQualifierOffset(), qlen);
      // write timestamp
      StreamUtils.writeLong(out, cell.getTimestamp());
      // write the type
      out.write(cell.getTypeByte());
      // write value
      out.write(cell.getValueArray(), cell.getValueOffset(), vlen);
      size = klen + vlen + KeyValue.KEYVALUE_INFRASTRUCTURE_SIZE;
      // write tags if we have to
      if (withTags && tlen > 0) {
        // 2 bytes tags length followed by tags bytes
        // tags length is serialized with 2 bytes only(short way) even if the
        // type is int. As this
        // is non -ve numbers, we save the sign bit. See HBASE-11437
        out.write((byte) (0xff & (tlen >> 8)));
        out.write((byte) (0xff & tlen));
        out.write(cell.getTagsArray(), cell.getTagsOffset(), tlen);
        size += tlen + KeyValue.TAGS_LENGTH_SIZE;
      }
      return size;
    }
  }
}
