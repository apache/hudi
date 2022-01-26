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

import static org.apache.hudi.hbase.util.Bytes.len;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hudi.hbase.util.ByteBufferUtils;
import org.apache.hudi.hbase.util.ClassSize;

import org.apache.hudi.hbase.util.Bytes;
import org.apache.hadoop.io.RawComparator;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An HBase Key/Value. This is the fundamental HBase Type.
 * <p>
 * HBase applications and users should use the Cell interface and avoid directly using KeyValue and
 * member functions not defined in Cell.
 * <p>
 * If being used client-side, the primary methods to access individual fields are
 * {@link #getRowArray()}, {@link #getFamilyArray()}, {@link #getQualifierArray()},
 * {@link #getTimestamp()}, and {@link #getValueArray()}. These methods allocate new byte arrays
 * and return copies. Avoid their use server-side.
 * <p>
 * Instances of this class are immutable. They do not implement Comparable but Comparators are
 * provided. Comparators change with context, whether user table or a catalog table comparison. Its
 * critical you use the appropriate comparator. There are Comparators for normal HFiles, Meta's
 * Hfiles, and bloom filter keys.
 * <p>
 * KeyValue wraps a byte array and takes offsets and lengths into passed array at where to start
 * interpreting the content as KeyValue. The KeyValue format inside a byte array is:
 * <code>&lt;keylength&gt; &lt;valuelength&gt; &lt;key&gt; &lt;value&gt;</code> Key is further
 * decomposed as: <code>&lt;rowlength&gt; &lt;row&gt; &lt;columnfamilylength&gt;
 * &lt;columnfamily&gt; &lt;columnqualifier&gt;
 * &lt;timestamp&gt; &lt;keytype&gt;</code> The <code>rowlength</code> maximum is
 * <code>Short.MAX_SIZE</code>, column family length maximum is <code>Byte.MAX_SIZE</code>, and
 * column qualifier + key length must be &lt; <code>Integer.MAX_SIZE</code>. The column does not
 * contain the family/qualifier delimiter, {@link #COLUMN_FAMILY_DELIMITER}<br>
 * KeyValue can optionally contain Tags. When it contains tags, it is added in the byte array after
 * the value part. The format for this part is: <code>&lt;tagslength&gt;&lt;tagsbytes&gt;</code>.
 * <code>tagslength</code> maximum is <code>Short.MAX_SIZE</code>. The <code>tagsbytes</code>
 * contain one or more tags where as each tag is of the form
 * <code>&lt;taglength&gt;&lt;tagtype&gt;&lt;tagbytes&gt;</code>. <code>tagtype</code> is one byte
 * and <code>taglength</code> maximum is <code>Short.MAX_SIZE</code> and it includes 1 byte type
 * length and actual tag bytes length.
 */
@InterfaceAudience.Private
public class KeyValue implements ExtendedCell, Cloneable {
  private static final ArrayList<Tag> EMPTY_ARRAY_LIST = new ArrayList<>();

  private static final Logger LOG = LoggerFactory.getLogger(KeyValue.class);

  public static final int FIXED_OVERHEAD = ClassSize.OBJECT + // the KeyValue object itself
      ClassSize.REFERENCE + // pointer to "bytes"
      2 * Bytes.SIZEOF_INT + // offset, length
      Bytes.SIZEOF_LONG;// memstoreTS

  /**
   * Colon character in UTF-8
   */
  public static final char COLUMN_FAMILY_DELIMITER = ':';

  public static final byte[] COLUMN_FAMILY_DELIM_ARRAY =
      new byte[]{COLUMN_FAMILY_DELIMITER};

  /**
   * Comparator for plain key/values; i.e. non-catalog table key/values. Works on Key portion
   * of KeyValue only.
   * @deprecated Use {@link CellComparator#getInstance()} instead. Deprecated for hbase 2.0, remove for hbase 3.0.
   */
  @Deprecated
  public static final KVComparator COMPARATOR = new KVComparator();
  /**
   * A {@link KVComparator} for <code>hbase:meta</code> catalog table
   * {@link KeyValue}s.
   * @deprecated Use {@link MetaCellComparator#META_COMPARATOR} instead.
   *   Deprecated for hbase 2.0, remove for hbase 3.0.
   */
  @Deprecated
  public static final KVComparator META_COMPARATOR = new MetaComparator();

  /** Size of the key length field in bytes*/
  public static final int KEY_LENGTH_SIZE = Bytes.SIZEOF_INT;

  /** Size of the key type field in bytes */
  public static final int TYPE_SIZE = Bytes.SIZEOF_BYTE;

  /** Size of the row length field in bytes */
  public static final int ROW_LENGTH_SIZE = Bytes.SIZEOF_SHORT;

  /** Size of the family length field in bytes */
  public static final int FAMILY_LENGTH_SIZE = Bytes.SIZEOF_BYTE;

  /** Size of the timestamp field in bytes */
  public static final int TIMESTAMP_SIZE = Bytes.SIZEOF_LONG;

  // Size of the timestamp and type byte on end of a key -- a long + a byte.
  public static final int TIMESTAMP_TYPE_SIZE = TIMESTAMP_SIZE + TYPE_SIZE;

  // Size of the length shorts and bytes in key.
  public static final int KEY_INFRASTRUCTURE_SIZE = ROW_LENGTH_SIZE
      + FAMILY_LENGTH_SIZE + TIMESTAMP_TYPE_SIZE;

  // How far into the key the row starts at. First thing to read is the short
  // that says how long the row is.
  public static final int ROW_OFFSET =
      Bytes.SIZEOF_INT /*keylength*/ +
          Bytes.SIZEOF_INT /*valuelength*/;

  public static final int ROW_KEY_OFFSET = ROW_OFFSET + ROW_LENGTH_SIZE;

  // Size of the length ints in a KeyValue datastructure.
  public static final int KEYVALUE_INFRASTRUCTURE_SIZE = ROW_OFFSET;

  /** Size of the tags length field in bytes */
  public static final int TAGS_LENGTH_SIZE = Bytes.SIZEOF_SHORT;

  public static final int KEYVALUE_WITH_TAGS_INFRASTRUCTURE_SIZE = ROW_OFFSET + TAGS_LENGTH_SIZE;

  /**
   * Computes the number of bytes that a <code>KeyValue</code> instance with the provided
   * characteristics would take up for its underlying data structure.
   *
   * @param rlength row length
   * @param flength family length
   * @param qlength qualifier length
   * @param vlength value length
   *
   * @return the <code>KeyValue</code> data structure length
   */
  public static long getKeyValueDataStructureSize(int rlength,
                                                  int flength, int qlength, int vlength) {
    return KeyValue.KEYVALUE_INFRASTRUCTURE_SIZE
        + getKeyDataStructureSize(rlength, flength, qlength) + vlength;
  }

  /**
   * Computes the number of bytes that a <code>KeyValue</code> instance with the provided
   * characteristics would take up for its underlying data structure.
   *
   * @param rlength row length
   * @param flength family length
   * @param qlength qualifier length
   * @param vlength value length
   * @param tagsLength total length of the tags
   *
   * @return the <code>KeyValue</code> data structure length
   */
  public static long getKeyValueDataStructureSize(int rlength, int flength, int qlength,
                                                  int vlength, int tagsLength) {
    if (tagsLength == 0) {
      return getKeyValueDataStructureSize(rlength, flength, qlength, vlength);
    }
    return KeyValue.KEYVALUE_WITH_TAGS_INFRASTRUCTURE_SIZE
        + getKeyDataStructureSize(rlength, flength, qlength) + vlength + tagsLength;
  }

  /**
   * Computes the number of bytes that a <code>KeyValue</code> instance with the provided
   * characteristics would take up for its underlying data structure.
   *
   * @param klength key length
   * @param vlength value length
   * @param tagsLength total length of the tags
   *
   * @return the <code>KeyValue</code> data structure length
   */
  public static long getKeyValueDataStructureSize(int klength, int vlength, int tagsLength) {
    if (tagsLength == 0) {
      return (long) KeyValue.KEYVALUE_INFRASTRUCTURE_SIZE + klength + vlength;
    }
    return (long) KeyValue.KEYVALUE_WITH_TAGS_INFRASTRUCTURE_SIZE + klength + vlength + tagsLength;
  }

  /**
   * Computes the number of bytes that a <code>KeyValue</code> instance with the provided
   * characteristics would take up in its underlying data structure for the key.
   *
   * @param rlength row length
   * @param flength family length
   * @param qlength qualifier length
   *
   * @return the key data structure length
   */
  public static long getKeyDataStructureSize(int rlength, int flength, int qlength) {
    return (long) KeyValue.KEY_INFRASTRUCTURE_SIZE + rlength + flength + qlength;
  }

  /**
   * Key type.
   * Has space for other key types to be added later.  Cannot rely on
   * enum ordinals . They change if item is removed or moved.  Do our own codes.
   */
  public static enum Type {
    Minimum((byte)0),
    Put((byte)4),

    Delete((byte)8),
    DeleteFamilyVersion((byte)10),
    DeleteColumn((byte)12),
    DeleteFamily((byte)14),

    // Maximum is used when searching; you look from maximum on down.
    Maximum((byte)255);

    private final byte code;

    Type(final byte c) {
      this.code = c;
    }

    public byte getCode() {
      return this.code;
    }

    private static Type[] codeArray = new Type[256];

    static {
      for (Type t : Type.values()) {
        codeArray[t.code & 0xff] = t;
      }
    }

    /**
     * True to indicate that the byte b is a valid type.
     * @param b byte to check
     * @return true or false
     */
    static boolean isValidType(byte b) {
      return codeArray[b & 0xff] != null;
    }

    /**
     * Cannot rely on enum ordinals . They change if item is removed or moved.
     * Do our own codes.
     * @param b
     * @return Type associated with passed code.
     */
    public static Type codeToType(final byte b) {
      Type t = codeArray[b & 0xff];
      if (t != null) {
        return t;
      }
      throw new RuntimeException("Unknown code " + b);
    }
  }

  /**
   * Lowest possible key.
   * Makes a Key with highest possible Timestamp, empty row and column.  No
   * key can be equal or lower than this one in memstore or in store file.
   */
  public static final KeyValue LOWESTKEY =
      new KeyValue(HConstants.EMPTY_BYTE_ARRAY, HConstants.LATEST_TIMESTAMP);

  ////
  // KeyValue core instance fields.
  protected byte [] bytes = null;  // an immutable byte array that contains the KV
  protected int offset = 0;  // offset into bytes buffer KV starts at
  protected int length = 0;  // length of the KV starting from offset.

  /** Here be dragons **/

  /**
   * used to achieve atomic operations in the memstore.
   */
  @Override
  public long getSequenceId() {
    return seqId;
  }

  @Override
  public void setSequenceId(long seqId) {
    this.seqId = seqId;
  }

  // multi-version concurrency control version.  default value is 0, aka do not care.
  private long seqId = 0;

  /** Dragon time over, return to normal business */


  /** Writable Constructor -- DO NOT USE */
  public KeyValue() {}

  /**
   * Creates a KeyValue from the start of the specified byte array.
   * Presumes <code>bytes</code> content is formatted as a KeyValue blob.
   * @param bytes byte array
   */
  public KeyValue(final byte [] bytes) {
    this(bytes, 0);
  }

  /**
   * Creates a KeyValue from the specified byte array and offset.
   * Presumes <code>bytes</code> content starting at <code>offset</code> is
   * formatted as a KeyValue blob.
   * @param bytes byte array
   * @param offset offset to start of KeyValue
   */
  public KeyValue(final byte [] bytes, final int offset) {
    this(bytes, offset, getLength(bytes, offset));
  }

  /**
   * Creates a KeyValue from the specified byte array, starting at offset, and
   * for length <code>length</code>.
   * @param bytes byte array
   * @param offset offset to start of the KeyValue
   * @param length length of the KeyValue
   */
  public KeyValue(final byte[] bytes, final int offset, final int length) {
    KeyValueUtil.checkKeyValueBytes(bytes, offset, length, true);
    this.bytes = bytes;
    this.offset = offset;
    this.length = length;
  }

  /**
   * Creates a KeyValue from the specified byte array, starting at offset, and
   * for length <code>length</code>.
   *
   * @param bytes  byte array
   * @param offset offset to start of the KeyValue
   * @param length length of the KeyValue
   * @param ts
   */
  public KeyValue(final byte[] bytes, final int offset, final int length, long ts) {
    this(bytes, offset, length, null, 0, 0, null, 0, 0, ts, Type.Maximum, null, 0, 0, null);
  }

  /** Constructors that build a new backing byte array from fields */

  /**
   * Constructs KeyValue structure filled with null value.
   * Sets type to {@link KeyValue.Type#Maximum}
   * @param row - row key (arbitrary byte array)
   * @param timestamp
   */
  public KeyValue(final byte [] row, final long timestamp) {
    this(row, null, null, timestamp, Type.Maximum, null);
  }

  /**
   * Constructs KeyValue structure filled with null value.
   * @param row - row key (arbitrary byte array)
   * @param timestamp
   */
  public KeyValue(final byte [] row, final long timestamp, Type type) {
    this(row, null, null, timestamp, type, null);
  }

  /**
   * Constructs KeyValue structure filled with null value.
   * Sets type to {@link KeyValue.Type#Maximum}
   * @param row - row key (arbitrary byte array)
   * @param family family name
   * @param qualifier column qualifier
   */
  public KeyValue(final byte [] row, final byte [] family,
                  final byte [] qualifier) {
    this(row, family, qualifier, HConstants.LATEST_TIMESTAMP, Type.Maximum);
  }

  /**
   * Constructs KeyValue structure as a put filled with specified values and
   * LATEST_TIMESTAMP.
   * @param row - row key (arbitrary byte array)
   * @param family family name
   * @param qualifier column qualifier
   */
  public KeyValue(final byte [] row, final byte [] family,
                  final byte [] qualifier, final byte [] value) {
    this(row, family, qualifier, HConstants.LATEST_TIMESTAMP, Type.Put, value);
  }

  /**
   * Constructs KeyValue structure filled with specified values.
   * @param row row key
   * @param family family name
   * @param qualifier column qualifier
   * @param timestamp version timestamp
   * @param type key type
   * @throws IllegalArgumentException
   */
  public KeyValue(final byte[] row, final byte[] family,
                  final byte[] qualifier, final long timestamp, Type type) {
    this(row, family, qualifier, timestamp, type, null);
  }

  /**
   * Constructs KeyValue structure filled with specified values.
   * @param row row key
   * @param family family name
   * @param qualifier column qualifier
   * @param timestamp version timestamp
   * @param value column value
   * @throws IllegalArgumentException
   */
  public KeyValue(final byte[] row, final byte[] family,
                  final byte[] qualifier, final long timestamp, final byte[] value) {
    this(row, family, qualifier, timestamp, Type.Put, value);
  }

  /**
   * Constructs KeyValue structure filled with specified values.
   * @param row row key
   * @param family family name
   * @param qualifier column qualifier
   * @param timestamp version timestamp
   * @param value column value
   * @param tags tags
   * @throws IllegalArgumentException
   */
  public KeyValue(final byte[] row, final byte[] family,
                  final byte[] qualifier, final long timestamp, final byte[] value,
                  final Tag[] tags) {
    this(row, family, qualifier, timestamp, value, tags != null ? Arrays.asList(tags) : null);
  }

  /**
   * Constructs KeyValue structure filled with specified values.
   * @param row row key
   * @param family family name
   * @param qualifier column qualifier
   * @param timestamp version timestamp
   * @param value column value
   * @param tags tags non-empty list of tags or null
   * @throws IllegalArgumentException
   */
  public KeyValue(final byte[] row, final byte[] family,
                  final byte[] qualifier, final long timestamp, final byte[] value,
                  final List<Tag> tags) {
    this(row, 0, row==null ? 0 : row.length,
        family, 0, family==null ? 0 : family.length,
        qualifier, 0, qualifier==null ? 0 : qualifier.length,
        timestamp, Type.Put,
        value, 0, value==null ? 0 : value.length, tags);
  }

  /**
   * Constructs KeyValue structure filled with specified values.
   * @param row row key
   * @param family family name
   * @param qualifier column qualifier
   * @param timestamp version timestamp
   * @param type key type
   * @param value column value
   * @throws IllegalArgumentException
   */
  public KeyValue(final byte[] row, final byte[] family,
                  final byte[] qualifier, final long timestamp, Type type,
                  final byte[] value) {
    this(row, 0, len(row),   family, 0, len(family),   qualifier, 0, len(qualifier),
        timestamp, type,   value, 0, len(value));
  }

  /**
   * Constructs KeyValue structure filled with specified values.
   * <p>
   * Column is split into two fields, family and qualifier.
   * @param row row key
   * @param family family name
   * @param qualifier column qualifier
   * @param timestamp version timestamp
   * @param type key type
   * @param value column value
   * @throws IllegalArgumentException
   */
  public KeyValue(final byte[] row, final byte[] family,
                  final byte[] qualifier, final long timestamp, Type type,
                  final byte[] value, final List<Tag> tags) {
    this(row, family, qualifier, 0, qualifier==null ? 0 : qualifier.length,
        timestamp, type, value, 0, value==null ? 0 : value.length, tags);
  }

  /**
   * Constructs KeyValue structure filled with specified values.
   * @param row row key
   * @param family family name
   * @param qualifier column qualifier
   * @param timestamp version timestamp
   * @param type key type
   * @param value column value
   * @throws IllegalArgumentException
   */
  public KeyValue(final byte[] row, final byte[] family,
                  final byte[] qualifier, final long timestamp, Type type,
                  final byte[] value, final byte[] tags) {
    this(row, family, qualifier, 0, qualifier==null ? 0 : qualifier.length,
        timestamp, type, value, 0, value==null ? 0 : value.length, tags);
  }

  /**
   * Constructs KeyValue structure filled with specified values.
   * @param row row key
   * @param family family name
   * @param qualifier column qualifier
   * @param qoffset qualifier offset
   * @param qlength qualifier length
   * @param timestamp version timestamp
   * @param type key type
   * @param value column value
   * @param voffset value offset
   * @param vlength value length
   * @throws IllegalArgumentException
   */
  public KeyValue(byte [] row, byte [] family,
                  byte [] qualifier, int qoffset, int qlength, long timestamp, Type type,
                  byte [] value, int voffset, int vlength, List<Tag> tags) {
    this(row, 0, row==null ? 0 : row.length,
        family, 0, family==null ? 0 : family.length,
        qualifier, qoffset, qlength, timestamp, type,
        value, voffset, vlength, tags);
  }

  /**
   * @param row
   * @param family
   * @param qualifier
   * @param qoffset
   * @param qlength
   * @param timestamp
   * @param type
   * @param value
   * @param voffset
   * @param vlength
   * @param tags
   */
  public KeyValue(byte [] row, byte [] family,
                  byte [] qualifier, int qoffset, int qlength, long timestamp, Type type,
                  byte [] value, int voffset, int vlength, byte[] tags) {
    this(row, 0, row==null ? 0 : row.length,
        family, 0, family==null ? 0 : family.length,
        qualifier, qoffset, qlength, timestamp, type,
        value, voffset, vlength, tags, 0, tags==null ? 0 : tags.length);
  }

  /**
   * Constructs KeyValue structure filled with specified values.
   * <p>
   * Column is split into two fields, family and qualifier.
   * @param row row key
   * @throws IllegalArgumentException
   */
  public KeyValue(final byte [] row, final int roffset, final int rlength,
                  final byte [] family, final int foffset, final int flength,
                  final byte [] qualifier, final int qoffset, final int qlength,
                  final long timestamp, final Type type,
                  final byte [] value, final int voffset, final int vlength) {
    this(row, roffset, rlength, family, foffset, flength, qualifier, qoffset,
        qlength, timestamp, type, value, voffset, vlength, null);
  }

  /**
   * Constructs KeyValue structure filled with specified values. Uses the provided buffer as the
   * data buffer.
   * <p>
   * Column is split into two fields, family and qualifier.
   *
   * @param buffer the bytes buffer to use
   * @param boffset buffer offset
   * @param row row key
   * @param roffset row offset
   * @param rlength row length
   * @param family family name
   * @param foffset family offset
   * @param flength family length
   * @param qualifier column qualifier
   * @param qoffset qualifier offset
   * @param qlength qualifier length
   * @param timestamp version timestamp
   * @param type key type
   * @param value column value
   * @param voffset value offset
   * @param vlength value length
   * @param tags non-empty list of tags or null
   * @throws IllegalArgumentException an illegal value was passed or there is insufficient space
   * remaining in the buffer
   */
  public KeyValue(byte [] buffer, final int boffset,
                  final byte [] row, final int roffset, final int rlength,
                  final byte [] family, final int foffset, final int flength,
                  final byte [] qualifier, final int qoffset, final int qlength,
                  final long timestamp, final Type type,
                  final byte [] value, final int voffset, final int vlength,
                  final Tag[] tags) {
    this.bytes  = buffer;
    this.length = writeByteArray(buffer, boffset,
        row, roffset, rlength,
        family, foffset, flength, qualifier, qoffset, qlength,
        timestamp, type, value, voffset, vlength, tags);
    this.offset = boffset;
  }

  /**
   * Constructs KeyValue structure filled with specified values.
   * <p>
   * Column is split into two fields, family and qualifier.
   * @param row row key
   * @param roffset row offset
   * @param rlength row length
   * @param family family name
   * @param foffset family offset
   * @param flength family length
   * @param qualifier column qualifier
   * @param qoffset qualifier offset
   * @param qlength qualifier length
   * @param timestamp version timestamp
   * @param type key type
   * @param value column value
   * @param voffset value offset
   * @param vlength value length
   * @param tags tags
   * @throws IllegalArgumentException
   */
  public KeyValue(final byte [] row, final int roffset, final int rlength,
                  final byte [] family, final int foffset, final int flength,
                  final byte [] qualifier, final int qoffset, final int qlength,
                  final long timestamp, final Type type,
                  final byte [] value, final int voffset, final int vlength,
                  final List<Tag> tags) {
    this.bytes = createByteArray(row, roffset, rlength,
        family, foffset, flength, qualifier, qoffset, qlength,
        timestamp, type, value, voffset, vlength, tags);
    this.length = bytes.length;
    this.offset = 0;
  }

  /**
   * @param row
   * @param roffset
   * @param rlength
   * @param family
   * @param foffset
   * @param flength
   * @param qualifier
   * @param qoffset
   * @param qlength
   * @param timestamp
   * @param type
   * @param value
   * @param voffset
   * @param vlength
   * @param tags
   */
  public KeyValue(final byte [] row, final int roffset, final int rlength,
                  final byte [] family, final int foffset, final int flength,
                  final byte [] qualifier, final int qoffset, final int qlength,
                  final long timestamp, final Type type,
                  final byte [] value, final int voffset, final int vlength,
                  final byte[] tags, final int tagsOffset, final int tagsLength) {
    this.bytes = createByteArray(row, roffset, rlength,
        family, foffset, flength, qualifier, qoffset, qlength,
        timestamp, type, value, voffset, vlength, tags, tagsOffset, tagsLength);
    this.length = bytes.length;
    this.offset = 0;
  }

  /**
   * Constructs an empty KeyValue structure, with specified sizes.
   * This can be used to partially fill up KeyValues.
   * <p>
   * Column is split into two fields, family and qualifier.
   * @param rlength row length
   * @param flength family length
   * @param qlength qualifier length
   * @param timestamp version timestamp
   * @param type key type
   * @param vlength value length
   * @throws IllegalArgumentException
   */
  public KeyValue(final int rlength,
                  final int flength,
                  final int qlength,
                  final long timestamp, final Type type,
                  final int vlength) {
    this(rlength, flength, qlength, timestamp, type, vlength, 0);
  }

  /**
   * Constructs an empty KeyValue structure, with specified sizes.
   * This can be used to partially fill up KeyValues.
   * <p>
   * Column is split into two fields, family and qualifier.
   * @param rlength row length
   * @param flength family length
   * @param qlength qualifier length
   * @param timestamp version timestamp
   * @param type key type
   * @param vlength value length
   * @param tagsLength
   * @throws IllegalArgumentException
   */
  public KeyValue(final int rlength,
                  final int flength,
                  final int qlength,
                  final long timestamp, final Type type,
                  final int vlength, final int tagsLength) {
    this.bytes = createEmptyByteArray(rlength, flength, qlength, timestamp, type, vlength,
        tagsLength);
    this.length = bytes.length;
    this.offset = 0;
  }


  public KeyValue(byte[] row, int roffset, int rlength,
                  byte[] family, int foffset, int flength,
                  ByteBuffer qualifier, long ts, Type type, ByteBuffer value, List<Tag> tags) {
    this.bytes = createByteArray(row, roffset, rlength, family, foffset, flength,
        qualifier, 0, qualifier == null ? 0 : qualifier.remaining(), ts, type,
        value, 0, value == null ? 0 : value.remaining(), tags);
    this.length = bytes.length;
    this.offset = 0;
  }

  public KeyValue(Cell c) {
    this(c.getRowArray(), c.getRowOffset(), c.getRowLength(),
        c.getFamilyArray(), c.getFamilyOffset(), c.getFamilyLength(),
        c.getQualifierArray(), c.getQualifierOffset(), c.getQualifierLength(),
        c.getTimestamp(), Type.codeToType(c.getTypeByte()), c.getValueArray(), c.getValueOffset(),
        c.getValueLength(), c.getTagsArray(), c.getTagsOffset(), c.getTagsLength());
    this.seqId = c.getSequenceId();
  }

  /**
   * Create an empty byte[] representing a KeyValue
   * All lengths are preset and can be filled in later.
   * @param rlength
   * @param flength
   * @param qlength
   * @param timestamp
   * @param type
   * @param vlength
   * @return The newly created byte array.
   */
  private static byte[] createEmptyByteArray(final int rlength, int flength,
                                             int qlength, final long timestamp, final Type type, int vlength, int tagsLength) {
    if (rlength > Short.MAX_VALUE) {
      throw new IllegalArgumentException("Row > " + Short.MAX_VALUE);
    }
    if (flength > Byte.MAX_VALUE) {
      throw new IllegalArgumentException("Family > " + Byte.MAX_VALUE);
    }
    // Qualifier length
    if (qlength > Integer.MAX_VALUE - rlength - flength) {
      throw new IllegalArgumentException("Qualifier > " + Integer.MAX_VALUE);
    }
    RawCell.checkForTagsLength(tagsLength);
    // Key length
    long longkeylength = getKeyDataStructureSize(rlength, flength, qlength);
    if (longkeylength > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("keylength " + longkeylength + " > " +
          Integer.MAX_VALUE);
    }
    int keylength = (int)longkeylength;
    // Value length
    if (vlength > HConstants.MAXIMUM_VALUE_LENGTH) { // FindBugs INT_VACUOUS_COMPARISON
      throw new IllegalArgumentException("Valuer > " +
          HConstants.MAXIMUM_VALUE_LENGTH);
    }

    // Allocate right-sized byte array.
    byte[] bytes= new byte[(int) getKeyValueDataStructureSize(rlength, flength, qlength, vlength,
        tagsLength)];
    // Write the correct size markers
    int pos = 0;
    pos = Bytes.putInt(bytes, pos, keylength);
    pos = Bytes.putInt(bytes, pos, vlength);
    pos = Bytes.putShort(bytes, pos, (short)(rlength & 0x0000ffff));
    pos += rlength;
    pos = Bytes.putByte(bytes, pos, (byte)(flength & 0x0000ff));
    pos += flength + qlength;
    pos = Bytes.putLong(bytes, pos, timestamp);
    pos = Bytes.putByte(bytes, pos, type.getCode());
    pos += vlength;
    if (tagsLength > 0) {
      pos = Bytes.putAsShort(bytes, pos, tagsLength);
    }
    return bytes;
  }

  /**
   * Checks the parameters passed to a constructor.
   *
   * @param row row key
   * @param rlength row length
   * @param family family name
   * @param flength family length
   * @param qlength qualifier length
   * @param vlength value length
   *
   * @throws IllegalArgumentException an illegal value was passed
   */
  static void checkParameters(final byte [] row, final int rlength,
                              final byte [] family, int flength, int qlength, int vlength)
      throws IllegalArgumentException {
    if (rlength > Short.MAX_VALUE) {
      throw new IllegalArgumentException("Row > " + Short.MAX_VALUE);
    }
    if (row == null) {
      throw new IllegalArgumentException("Row is null");
    }
    // Family length
    flength = family == null ? 0 : flength;
    if (flength > Byte.MAX_VALUE) {
      throw new IllegalArgumentException("Family > " + Byte.MAX_VALUE);
    }
    // Qualifier length
    if (qlength > Integer.MAX_VALUE - rlength - flength) {
      throw new IllegalArgumentException("Qualifier > " + Integer.MAX_VALUE);
    }
    // Key length
    long longKeyLength = getKeyDataStructureSize(rlength, flength, qlength);
    if (longKeyLength > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("keylength " + longKeyLength + " > " +
          Integer.MAX_VALUE);
    }
    // Value length
    if (vlength > HConstants.MAXIMUM_VALUE_LENGTH) { // FindBugs INT_VACUOUS_COMPARISON
      throw new IllegalArgumentException("Value length " + vlength + " > " +
          HConstants.MAXIMUM_VALUE_LENGTH);
    }
  }

  /**
   * Write KeyValue format into the provided byte array.
   *
   * @param buffer the bytes buffer to use
   * @param boffset buffer offset
   * @param row row key
   * @param roffset row offset
   * @param rlength row length
   * @param family family name
   * @param foffset family offset
   * @param flength family length
   * @param qualifier column qualifier
   * @param qoffset qualifier offset
   * @param qlength qualifier length
   * @param timestamp version timestamp
   * @param type key type
   * @param value column value
   * @param voffset value offset
   * @param vlength value length
   *
   * @return The number of useful bytes in the buffer.
   *
   * @throws IllegalArgumentException an illegal value was passed or there is insufficient space
   * remaining in the buffer
   */
  public static int writeByteArray(byte [] buffer, final int boffset,
                                   final byte [] row, final int roffset, final int rlength,
                                   final byte [] family, final int foffset, int flength,
                                   final byte [] qualifier, final int qoffset, int qlength,
                                   final long timestamp, final Type type,
                                   final byte [] value, final int voffset, int vlength, Tag[] tags) {

    checkParameters(row, rlength, family, flength, qlength, vlength);

    // Calculate length of tags area
    int tagsLength = 0;
    if (tags != null && tags.length > 0) {
      for (Tag t: tags) {
        tagsLength += t.getValueLength() + Tag.INFRASTRUCTURE_SIZE;
      }
    }
    RawCell.checkForTagsLength(tagsLength);
    int keyLength = (int) getKeyDataStructureSize(rlength, flength, qlength);
    int keyValueLength = (int) getKeyValueDataStructureSize(rlength, flength, qlength, vlength,
        tagsLength);
    if (keyValueLength > buffer.length - boffset) {
      throw new IllegalArgumentException("Buffer size " + (buffer.length - boffset) + " < " +
          keyValueLength);
    }

    // Write key, value and key row length.
    int pos = boffset;
    pos = Bytes.putInt(buffer, pos, keyLength);
    pos = Bytes.putInt(buffer, pos, vlength);
    pos = Bytes.putShort(buffer, pos, (short)(rlength & 0x0000ffff));
    pos = Bytes.putBytes(buffer, pos, row, roffset, rlength);
    pos = Bytes.putByte(buffer, pos, (byte) (flength & 0x0000ff));
    if (flength != 0) {
      pos = Bytes.putBytes(buffer, pos, family, foffset, flength);
    }
    if (qlength != 0) {
      pos = Bytes.putBytes(buffer, pos, qualifier, qoffset, qlength);
    }
    pos = Bytes.putLong(buffer, pos, timestamp);
    pos = Bytes.putByte(buffer, pos, type.getCode());
    if (value != null && value.length > 0) {
      pos = Bytes.putBytes(buffer, pos, value, voffset, vlength);
    }
    // Write the number of tags. If it is 0 then it means there are no tags.
    if (tagsLength > 0) {
      pos = Bytes.putAsShort(buffer, pos, tagsLength);
      for (Tag t : tags) {
        int tlen = t.getValueLength();
        pos = Bytes.putAsShort(buffer, pos, tlen + Tag.TYPE_LENGTH_SIZE);
        pos = Bytes.putByte(buffer, pos, t.getType());
        Tag.copyValueTo(t, buffer, pos);
        pos += tlen;
      }
    }
    return keyValueLength;
  }

  /**
   * Write KeyValue format into a byte array.
   * @param row row key
   * @param roffset row offset
   * @param rlength row length
   * @param family family name
   * @param foffset family offset
   * @param flength family length
   * @param qualifier column qualifier
   * @param qoffset qualifier offset
   * @param qlength qualifier length
   * @param timestamp version timestamp
   * @param type key type
   * @param value column value
   * @param voffset value offset
   * @param vlength value length
   * @return The newly created byte array.
   */
  private static byte [] createByteArray(final byte [] row, final int roffset,
                                         final int rlength, final byte [] family, final int foffset, int flength,
                                         final byte [] qualifier, final int qoffset, int qlength,
                                         final long timestamp, final Type type,
                                         final byte [] value, final int voffset,
                                         int vlength, byte[] tags, int tagsOffset, int tagsLength) {

    checkParameters(row, rlength, family, flength, qlength, vlength);
    RawCell.checkForTagsLength(tagsLength);
    // Allocate right-sized byte array.
    int keyLength = (int) getKeyDataStructureSize(rlength, flength, qlength);
    byte[] bytes = new byte[(int) getKeyValueDataStructureSize(rlength, flength, qlength, vlength,
        tagsLength)];
    // Write key, value and key row length.
    int pos = 0;
    pos = Bytes.putInt(bytes, pos, keyLength);
    pos = Bytes.putInt(bytes, pos, vlength);
    pos = Bytes.putShort(bytes, pos, (short)(rlength & 0x0000ffff));
    pos = Bytes.putBytes(bytes, pos, row, roffset, rlength);
    pos = Bytes.putByte(bytes, pos, (byte)(flength & 0x0000ff));
    if(flength != 0) {
      pos = Bytes.putBytes(bytes, pos, family, foffset, flength);
    }
    if(qlength != 0) {
      pos = Bytes.putBytes(bytes, pos, qualifier, qoffset, qlength);
    }
    pos = Bytes.putLong(bytes, pos, timestamp);
    pos = Bytes.putByte(bytes, pos, type.getCode());
    if (value != null && value.length > 0) {
      pos = Bytes.putBytes(bytes, pos, value, voffset, vlength);
    }
    // Add the tags after the value part
    if (tagsLength > 0) {
      pos = Bytes.putAsShort(bytes, pos, tagsLength);
      pos = Bytes.putBytes(bytes, pos, tags, tagsOffset, tagsLength);
    }
    return bytes;
  }

  /**
   * @param qualifier can be a ByteBuffer or a byte[], or null.
   * @param value can be a ByteBuffer or a byte[], or null.
   */
  private static byte [] createByteArray(final byte [] row, final int roffset,
                                         final int rlength, final byte [] family, final int foffset, int flength,
                                         final Object qualifier, final int qoffset, int qlength,
                                         final long timestamp, final Type type,
                                         final Object value, final int voffset, int vlength, List<Tag> tags) {

    checkParameters(row, rlength, family, flength, qlength, vlength);

    // Calculate length of tags area
    int tagsLength = 0;
    if (tags != null && !tags.isEmpty()) {
      for (Tag t : tags) {
        tagsLength += t.getValueLength() + Tag.INFRASTRUCTURE_SIZE;
      }
    }
    RawCell.checkForTagsLength(tagsLength);
    // Allocate right-sized byte array.
    int keyLength = (int) getKeyDataStructureSize(rlength, flength, qlength);
    byte[] bytes = new byte[(int) getKeyValueDataStructureSize(rlength, flength, qlength, vlength,
        tagsLength)];

    // Write key, value and key row length.
    int pos = 0;
    pos = Bytes.putInt(bytes, pos, keyLength);

    pos = Bytes.putInt(bytes, pos, vlength);
    pos = Bytes.putShort(bytes, pos, (short)(rlength & 0x0000ffff));
    pos = Bytes.putBytes(bytes, pos, row, roffset, rlength);
    pos = Bytes.putByte(bytes, pos, (byte)(flength & 0x0000ff));
    if(flength != 0) {
      pos = Bytes.putBytes(bytes, pos, family, foffset, flength);
    }
    if (qlength > 0) {
      if (qualifier instanceof ByteBuffer) {
        pos = Bytes.putByteBuffer(bytes, pos, (ByteBuffer) qualifier);
      } else {
        pos = Bytes.putBytes(bytes, pos, (byte[]) qualifier, qoffset, qlength);
      }
    }
    pos = Bytes.putLong(bytes, pos, timestamp);
    pos = Bytes.putByte(bytes, pos, type.getCode());
    if (vlength > 0) {
      if (value instanceof ByteBuffer) {
        pos = Bytes.putByteBuffer(bytes, pos, (ByteBuffer) value);
      } else {
        pos = Bytes.putBytes(bytes, pos, (byte[]) value, voffset, vlength);
      }
    }
    // Add the tags after the value part
    if (tagsLength > 0) {
      pos = Bytes.putAsShort(bytes, pos, tagsLength);
      for (Tag t : tags) {
        int tlen = t.getValueLength();
        pos = Bytes.putAsShort(bytes, pos, tlen + Tag.TYPE_LENGTH_SIZE);
        pos = Bytes.putByte(bytes, pos, t.getType());
        Tag.copyValueTo(t, bytes, pos);
        pos += tlen;
      }
    }
    return bytes;
  }

  /**
   * Needed doing 'contains' on List.  Only compares the key portion, not the value.
   */
  @Override
  public boolean equals(Object other) {
    if (!(other instanceof Cell)) {
      return false;
    }
    return CellUtil.equals(this, (Cell)other);
  }

  /**
   * In line with {@link #equals(Object)}, only uses the key portion, not the value.
   */
  @Override
  public int hashCode() {
    return calculateHashForKey(this);
  }

  private int calculateHashForKey(Cell cell) {
    // pre-calculate the 3 hashes made of byte ranges
    int rowHash = Bytes.hashCode(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
    int familyHash = Bytes.hashCode(cell.getFamilyArray(), cell.getFamilyOffset(),
        cell.getFamilyLength());
    int qualifierHash = Bytes.hashCode(cell.getQualifierArray(), cell.getQualifierOffset(),
        cell.getQualifierLength());

    // combine the 6 sub-hashes
    int hash = 31 * rowHash + familyHash;
    hash = 31 * hash + qualifierHash;
    hash = 31 * hash + (int) cell.getTimestamp();
    hash = 31 * hash + cell.getTypeByte();
    return hash;
  }

  //---------------------------------------------------------------------------
  //
  //  KeyValue cloning
  //
  //---------------------------------------------------------------------------

  /**
   * Clones a KeyValue.  This creates a copy, re-allocating the buffer.
   * @return Fully copied clone of this KeyValue
   * @throws CloneNotSupportedException
   */
  @Override
  public KeyValue clone() throws CloneNotSupportedException {
    super.clone();
    byte [] b = new byte[this.length];
    System.arraycopy(this.bytes, this.offset, b, 0, this.length);
    KeyValue ret = new KeyValue(b, 0, b.length);
    // Important to clone the memstoreTS as well - otherwise memstore's
    // update-in-place methods (eg increment) will end up creating
    // new entries
    ret.setSequenceId(seqId);
    return ret;
  }

  /**
   * Creates a shallow copy of this KeyValue, reusing the data byte buffer.
   * http://en.wikipedia.org/wiki/Object_copy
   * @return Shallow copy of this KeyValue
   */
  public KeyValue shallowCopy() {
    KeyValue shallowCopy = new KeyValue(this.bytes, this.offset, this.length);
    shallowCopy.setSequenceId(this.seqId);
    return shallowCopy;
  }

  //---------------------------------------------------------------------------
  //
  //  String representation
  //
  //---------------------------------------------------------------------------

  @Override
  public String toString() {
    if (this.bytes == null || this.bytes.length == 0) {
      return "empty";
    }
    return keyToString(this.bytes, this.offset + ROW_OFFSET, getKeyLength()) + "/vlen="
        + getValueLength() + "/seqid=" + seqId;
  }

  /**
   * @param k Key portion of a KeyValue.
   * @return Key as a String, empty string if k is null.
   */
  public static String keyToString(final byte [] k) {
    if (k == null) {
      return "";
    }
    return keyToString(k, 0, k.length);
  }

  /**
   * Produces a string map for this key/value pair. Useful for programmatic use
   * and manipulation of the data stored in an WALKey, for example, printing
   * as JSON. Values are left out due to their tendency to be large. If needed,
   * they can be added manually.
   *
   * @return the Map&lt;String,?&gt; containing data from this key
   */
  public Map<String, Object> toStringMap() {
    Map<String, Object> stringMap = new HashMap<>();
    stringMap.put("row", Bytes.toStringBinary(getRowArray(), getRowOffset(), getRowLength()));
    stringMap.put("family",
        Bytes.toStringBinary(getFamilyArray(), getFamilyOffset(), getFamilyLength()));
    stringMap.put("qualifier",
        Bytes.toStringBinary(getQualifierArray(), getQualifierOffset(), getQualifierLength()));
    stringMap.put("timestamp", getTimestamp());
    stringMap.put("vlen", getValueLength());
    Iterator<Tag> tags = getTags();
    if (tags != null) {
      List<String> tagsString = new ArrayList<String>();
      while (tags.hasNext()) {
        tagsString.add(tags.next().toString());
      }
      stringMap.put("tag", tagsString);
    }
    return stringMap;
  }

  /**
   * Use for logging.
   * @param b Key portion of a KeyValue.
   * @param o Offset to start of key
   * @param l Length of key.
   * @return Key as a String.
   */
  public static String keyToString(final byte [] b, final int o, final int l) {
    if (b == null) return "";
    int rowlength = Bytes.toShort(b, o);
    String row = Bytes.toStringBinary(b, o + Bytes.SIZEOF_SHORT, rowlength);
    int columnoffset = o + Bytes.SIZEOF_SHORT + 1 + rowlength;
    int familylength = b[columnoffset - 1];
    int columnlength = l - ((columnoffset - o) + TIMESTAMP_TYPE_SIZE);
    String family = familylength == 0? "":
        Bytes.toStringBinary(b, columnoffset, familylength);
    String qualifier = columnlength == 0? "":
        Bytes.toStringBinary(b, columnoffset + familylength,
            columnlength - familylength);
    long timestamp = Bytes.toLong(b, o + (l - TIMESTAMP_TYPE_SIZE));
    String timestampStr = humanReadableTimestamp(timestamp);
    byte type = b[o + l - 1];
    return row + "/" + family +
        (family != null && family.length() > 0? ":" :"") +
        qualifier + "/" + timestampStr + "/" + Type.codeToType(type);
  }

  public static String humanReadableTimestamp(final long timestamp) {
    if (timestamp == HConstants.LATEST_TIMESTAMP) {
      return "LATEST_TIMESTAMP";
    }
    if (timestamp == HConstants.OLDEST_TIMESTAMP) {
      return "OLDEST_TIMESTAMP";
    }
    return String.valueOf(timestamp);
  }

  //---------------------------------------------------------------------------
  //
  //  Public Member Accessors
  //
  //---------------------------------------------------------------------------

  /**
   * To be used only in tests where the Cells are clearly assumed to be of type KeyValue
   * and that we need access to the backing array to do some test case related assertions.
   * @return The byte array backing this KeyValue.
   */
  public byte [] getBuffer() {
    return this.bytes;
  }

  /**
   * @return Offset into {@link #getBuffer()} at which this KeyValue starts.
   */
  public int getOffset() {
    return this.offset;
  }

  /**
   * @return Length of bytes this KeyValue occupies in {@link #getBuffer()}.
   */
  public int getLength() {
    return length;
  }

  //---------------------------------------------------------------------------
  //
  //  Length and Offset Calculators
  //
  //---------------------------------------------------------------------------

  /**
   * Determines the total length of the KeyValue stored in the specified
   * byte array and offset.  Includes all headers.
   * @param bytes byte array
   * @param offset offset to start of the KeyValue
   * @return length of entire KeyValue, in bytes
   */
  private static int getLength(byte [] bytes, int offset) {
    int klength = ROW_OFFSET + Bytes.toInt(bytes, offset);
    int vlength = Bytes.toInt(bytes, offset + Bytes.SIZEOF_INT);
    return klength + vlength;
  }

  /**
   * @return Key offset in backing buffer..
   */
  public int getKeyOffset() {
    return this.offset + ROW_OFFSET;
  }

  public String getKeyString() {
    return Bytes.toStringBinary(getBuffer(), getKeyOffset(), getKeyLength());
  }

  /**
   * @return Length of key portion.
   */
  public int getKeyLength() {
    return Bytes.toInt(this.bytes, this.offset);
  }

  /**
   * @return the backing array of the entire KeyValue (all KeyValue fields are in a single array)
   */
  @Override
  public byte[] getValueArray() {
    return bytes;
  }

  /**
   * @return the value offset
   */
  @Override
  public int getValueOffset() {
    int voffset = getKeyOffset() + getKeyLength();
    return voffset;
  }

  /**
   * @return Value length
   */
  @Override
  public int getValueLength() {
    int vlength = Bytes.toInt(this.bytes, this.offset + Bytes.SIZEOF_INT);
    return vlength;
  }

  /**
   * @return the backing array of the entire KeyValue (all KeyValue fields are in a single array)
   */
  @Override
  public byte[] getRowArray() {
    return bytes;
  }

  /**
   * @return Row offset
   */
  @Override
  public int getRowOffset() {
    return this.offset + ROW_KEY_OFFSET;
  }

  /**
   * @return Row length
   */
  @Override
  public short getRowLength() {
    return Bytes.toShort(this.bytes, getKeyOffset());
  }

  /**
   * @return the backing array of the entire KeyValue (all KeyValue fields are in a single array)
   */
  @Override
  public byte[] getFamilyArray() {
    return bytes;
  }

  /**
   * @return Family offset
   */
  @Override
  public int getFamilyOffset() {
    return getFamilyOffset(getFamilyLengthPosition(getRowLength()));
  }

  /**
   * @return Family offset
   */
  int getFamilyOffset(int familyLenPosition) {
    return familyLenPosition + Bytes.SIZEOF_BYTE;
  }

  /**
   * @return Family length
   */
  @Override
  public byte getFamilyLength() {
    return getFamilyLength(getFamilyLengthPosition(getRowLength()));
  }

  /**
   * @return Family length
   */
  public byte getFamilyLength(int famLenPos) {
    return this.bytes[famLenPos];
  }

  int getFamilyLengthPosition(int rowLength) {
    return this.offset + KeyValue.ROW_KEY_OFFSET + rowLength;
  }

  /**
   * @return the backing array of the entire KeyValue (all KeyValue fields are in a single array)
   */
  @Override
  public byte[] getQualifierArray() {
    return bytes;
  }

  /**
   * @return Qualifier offset
   */
  @Override
  public int getQualifierOffset() {
    return getQualifierOffset(getFamilyOffset());
  }

  /**
   * @return Qualifier offset
   */
  private int getQualifierOffset(int foffset) {
    return getQualifierOffset(foffset, getFamilyLength());
  }

  /**
   * @return Qualifier offset
   */
  int getQualifierOffset(int foffset, int flength) {
    return foffset + flength;
  }

  /**
   * @return Qualifier length
   */
  @Override
  public int getQualifierLength() {
    return getQualifierLength(getRowLength(),getFamilyLength());
  }

  /**
   * @return Qualifier length
   */
  private int getQualifierLength(int rlength, int flength) {
    return getQualifierLength(getKeyLength(), rlength, flength);
  }

  /**
   * @return Qualifier length
   */
  int getQualifierLength(int keyLength, int rlength, int flength) {
    return keyLength - (int) getKeyDataStructureSize(rlength, flength, 0);
  }

  /**
   * @return Timestamp offset
   */
  public int getTimestampOffset() {
    return getTimestampOffset(getKeyLength());
  }

  /**
   * @param keylength Pass if you have it to save on a int creation.
   * @return Timestamp offset
   */
  private int getTimestampOffset(final int keylength) {
    return getKeyOffset() + keylength - TIMESTAMP_TYPE_SIZE;
  }

  /**
   * @return True if this KeyValue has a LATEST_TIMESTAMP timestamp.
   */
  public boolean isLatestTimestamp() {
    return Bytes.equals(getBuffer(), getTimestampOffset(), Bytes.SIZEOF_LONG,
        HConstants.LATEST_TIMESTAMP_BYTES, 0, Bytes.SIZEOF_LONG);
  }

  /**
   * @param now Time to set into <code>this</code> IFF timestamp ==
   * {@link HConstants#LATEST_TIMESTAMP} (else, its a noop).
   * @return True is we modified this.
   */
  public boolean updateLatestStamp(final byte [] now) {
    if (this.isLatestTimestamp()) {
      int tsOffset = getTimestampOffset();
      System.arraycopy(now, 0, this.bytes, tsOffset, Bytes.SIZEOF_LONG);
      // clear cache or else getTimestamp() possibly returns an old value
      return true;
    }
    return false;
  }

  @Override
  public void setTimestamp(long ts) {
    Bytes.putBytes(this.bytes, this.getTimestampOffset(), Bytes.toBytes(ts), 0, Bytes.SIZEOF_LONG);
  }

  @Override
  public void setTimestamp(byte[] ts) {
    Bytes.putBytes(this.bytes, this.getTimestampOffset(), ts, 0, Bytes.SIZEOF_LONG);
  }

  //---------------------------------------------------------------------------
  //
  //  Methods that return copies of fields
  //
  //---------------------------------------------------------------------------

  /**
   * Do not use unless you have to. Used internally for compacting and testing. Use
   * {@link #getRowArray()}, {@link #getFamilyArray()}, {@link #getQualifierArray()}, and
   * {@link #getValueArray()} if accessing a KeyValue client-side.
   * @return Copy of the key portion only.
   */
  public byte [] getKey() {
    int keylength = getKeyLength();
    byte [] key = new byte[keylength];
    System.arraycopy(getBuffer(), getKeyOffset(), key, 0, keylength);
    return key;
  }

  /**
   *
   * @return Timestamp
   */
  @Override
  public long getTimestamp() {
    return getTimestamp(getKeyLength());
  }

  /**
   * @param keylength Pass if you have it to save on a int creation.
   * @return Timestamp
   */
  long getTimestamp(final int keylength) {
    int tsOffset = getTimestampOffset(keylength);
    return Bytes.toLong(this.bytes, tsOffset);
  }

  /**
   * @return KeyValue.TYPE byte representation
   */
  @Override
  public byte getTypeByte() {
    return getTypeByte(getKeyLength());
  }

  byte getTypeByte(int keyLength) {
    return this.bytes[this.offset + keyLength - 1 + ROW_OFFSET];
  }

  /**
   * This returns the offset where the tag actually starts.
   */
  @Override
  public int getTagsOffset() {
    int tagsLen = getTagsLength();
    if (tagsLen == 0) {
      return this.offset + this.length;
    }
    return this.offset + this.length - tagsLen;
  }

  /**
   * This returns the total length of the tag bytes
   */
  @Override
  public int getTagsLength() {
    int tagsLen = this.length - (getKeyLength() + getValueLength() + KEYVALUE_INFRASTRUCTURE_SIZE);
    if (tagsLen > 0) {
      // There are some Tag bytes in the byte[]. So reduce 2 bytes which is added to denote the tags
      // length
      tagsLen -= TAGS_LENGTH_SIZE;
    }
    return tagsLen;
  }

  /**
   * @return the backing array of the entire KeyValue (all KeyValue fields are in a single array)
   */
  @Override
  public byte[] getTagsArray() {
    return bytes;
  }

  /**
   * Creates a new KeyValue that only contains the key portion (the value is
   * set to be null).
   *
   * TODO only used by KeyOnlyFilter -- move there.
   * @param lenAsVal replace value with the actual value length (false=empty)
   */
  public KeyValue createKeyOnly(boolean lenAsVal) {
    // KV format:  <keylen:4><valuelen:4><key:keylen><value:valuelen>
    // Rebuild as: <keylen:4><0:4><key:keylen>
    int dataLen = lenAsVal? Bytes.SIZEOF_INT : 0;
    byte [] newBuffer = new byte[getKeyLength() + ROW_OFFSET + dataLen];
    System.arraycopy(this.bytes, this.offset, newBuffer, 0,
        Math.min(newBuffer.length,this.length));
    Bytes.putInt(newBuffer, Bytes.SIZEOF_INT, dataLen);
    if (lenAsVal) {
      Bytes.putInt(newBuffer, newBuffer.length - dataLen, this.getValueLength());
    }
    return new KeyValue(newBuffer);
  }

  /**
   * @param b
   * @param delimiter
   * @return Index of delimiter having started from start of <code>b</code>
   * moving rightward.
   */
  public static int getDelimiter(final byte [] b, int offset, final int length,
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
   * @param b
   * @param delimiter
   * @return Index of delimiter
   */
  public static int getDelimiterInReverse(final byte [] b, final int offset,
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

  /**
   * A {@link KVComparator} for <code>hbase:meta</code> catalog table
   * {@link KeyValue}s.
   * @deprecated : {@link MetaCellComparator#META_COMPARATOR} to be used.
   *   Deprecated for hbase 2.0, remove for hbase 3.0.
   */
  @Deprecated
  public static class MetaComparator extends KVComparator {
    /**
     * Compare key portion of a {@link KeyValue} for keys in <code>hbase:meta</code>
     * table.
     */
    @Override
    public int compare(final Cell left, final Cell right) {
      return PrivateCellUtil.compareKeyIgnoresMvcc(MetaCellComparator.META_COMPARATOR, left,
          right);
    }

    @Override
    public int compareOnlyKeyPortion(Cell left, Cell right) {
      return compare(left, right);
    }

    @Override
    public int compareRows(byte [] left, int loffset, int llength,
                           byte [] right, int roffset, int rlength) {
      int leftDelimiter = getDelimiter(left, loffset, llength,
          HConstants.DELIMITER);
      int rightDelimiter = getDelimiter(right, roffset, rlength,
          HConstants.DELIMITER);
      // Compare up to the delimiter
      int lpart = (leftDelimiter < 0 ? llength :leftDelimiter - loffset);
      int rpart = (rightDelimiter < 0 ? rlength :rightDelimiter - roffset);
      int result = Bytes.compareTo(left, loffset, lpart, right, roffset, rpart);
      if (result != 0) {
        return result;
      } else {
        if (leftDelimiter < 0 && rightDelimiter >= 0) {
          return -1;
        } else if (rightDelimiter < 0 && leftDelimiter >= 0) {
          return 1;
        } else if (leftDelimiter < 0 && rightDelimiter < 0) {
          return 0;
        }
      }
      // Compare middle bit of the row.
      // Move past delimiter
      leftDelimiter++;
      rightDelimiter++;
      int leftFarDelimiter = getDelimiterInReverse(left, leftDelimiter,
          llength - (leftDelimiter - loffset), HConstants.DELIMITER);
      int rightFarDelimiter = getDelimiterInReverse(right,
          rightDelimiter, rlength - (rightDelimiter - roffset),
          HConstants.DELIMITER);
      // Now compare middlesection of row.
      lpart = (leftFarDelimiter < 0 ? llength + loffset: leftFarDelimiter) - leftDelimiter;
      rpart = (rightFarDelimiter < 0 ? rlength + roffset: rightFarDelimiter)- rightDelimiter;
      result = super.compareRows(left, leftDelimiter, lpart, right, rightDelimiter, rpart);
      if (result != 0) {
        return result;
      }  else {
        if (leftDelimiter < 0 && rightDelimiter >= 0) {
          return -1;
        } else if (rightDelimiter < 0 && leftDelimiter >= 0) {
          return 1;
        } else if (leftDelimiter < 0 && rightDelimiter < 0) {
          return 0;
        }
      }
      // Compare last part of row, the rowid.
      leftFarDelimiter++;
      rightFarDelimiter++;
      result = Bytes.compareTo(left, leftFarDelimiter, llength - (leftFarDelimiter - loffset),
          right, rightFarDelimiter, rlength - (rightFarDelimiter - roffset));
      return result;
    }

    /**
     * Don't do any fancy Block Index splitting tricks.
     */
    @Override
    public byte[] getShortMidpointKey(final byte[] leftKey, final byte[] rightKey) {
      return Arrays.copyOf(rightKey, rightKey.length);
    }

    /**
     * The HFileV2 file format's trailer contains this class name.  We reinterpret this and
     * instantiate the appropriate comparator.
     * TODO: With V3 consider removing this.
     * @return legacy class name for FileFileTrailer#comparatorClassName
     */
    @Override
    public String getLegacyKeyComparatorName() {
      return "org.apache.hudi.hbase.KeyValue$MetaKeyComparator";
    }

    @Override
    protected Object clone() throws CloneNotSupportedException {
      return new MetaComparator();
    }

    /**
     * Override the row key comparison to parse and compare the meta row key parts.
     */
    @Override
    protected int compareRowKey(final Cell l, final Cell r) {
      byte[] left = l.getRowArray();
      int loffset = l.getRowOffset();
      int llength = l.getRowLength();
      byte[] right = r.getRowArray();
      int roffset = r.getRowOffset();
      int rlength = r.getRowLength();
      return compareRows(left, loffset, llength, right, roffset, rlength);
    }
  }

  /**
   * Compare KeyValues.  When we compare KeyValues, we only compare the Key
   * portion.  This means two KeyValues with same Key but different Values are
   * considered the same as far as this Comparator is concerned.
   * @deprecated : Use {@link CellComparatorImpl}. Deprecated for hbase 2.0, remove for hbase 3.0.
   */
  @Deprecated
  public static class KVComparator implements RawComparator<Cell>, SamePrefixComparator<byte[]> {

    /**
     * The HFileV2 file format's trailer contains this class name.  We reinterpret this and
     * instantiate the appropriate comparator.
     * TODO: With V3 consider removing this.
     * @return legacy class name for FileFileTrailer#comparatorClassName
     */
    public String getLegacyKeyComparatorName() {
      return "org.apache.hudi.hbase.KeyValue$KeyComparator";
    }

    @Override // RawComparator
    public int compare(byte[] l, int loff, int llen, byte[] r, int roff, int rlen) {
      return compareFlatKey(l,loff,llen, r,roff,rlen);
    }


    /**
     * Compares the only the user specified portion of a Key.  This is overridden by MetaComparator.
     * @param left
     * @param right
     * @return 0 if equal, &lt;0 if left smaller, &gt;0 if right smaller
     */
    protected int compareRowKey(final Cell left, final Cell right) {
      return CellComparatorImpl.COMPARATOR.compareRows(left, right);
    }

    /**
     * Compares left to right assuming that left,loffset,llength and right,roffset,rlength are
     * full KVs laid out in a flat byte[]s.
     * @param left
     * @param loffset
     * @param llength
     * @param right
     * @param roffset
     * @param rlength
     * @return  0 if equal, &lt;0 if left smaller, &gt;0 if right smaller
     */
    public int compareFlatKey(byte[] left, int loffset, int llength,
                              byte[] right, int roffset, int rlength) {
      // Compare row
      short lrowlength = Bytes.toShort(left, loffset);
      short rrowlength = Bytes.toShort(right, roffset);
      int compare = compareRows(left, loffset + Bytes.SIZEOF_SHORT,
          lrowlength, right, roffset + Bytes.SIZEOF_SHORT, rrowlength);
      if (compare != 0) {
        return compare;
      }

      // Compare the rest of the two KVs without making any assumptions about
      // the common prefix. This function will not compare rows anyway, so we
      // don't need to tell it that the common prefix includes the row.
      return compareWithoutRow(0, left, loffset, llength, right, roffset,
          rlength, rrowlength);
    }

    public int compareFlatKey(byte[] left, byte[] right) {
      return compareFlatKey(left, 0, left.length, right, 0, right.length);
    }

    // compare a key against row/fam/qual/ts/type
    public int compareKey(Cell cell,
                          byte[] row, int roff, int rlen,
                          byte[] fam, int foff, int flen,
                          byte[] col, int coff, int clen,
                          long ts, byte type) {

      int compare = compareRows(
          cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(),
          row, roff, rlen);
      if (compare != 0) {
        return compare;
      }
      // If the column is not specified, the "minimum" key type appears the
      // latest in the sorted order, regardless of the timestamp. This is used
      // for specifying the last key/value in a given row, because there is no
      // "lexicographically last column" (it would be infinitely long). The
      // "maximum" key type does not need this behavior.
      if (cell.getFamilyLength() + cell.getQualifierLength() == 0
          && cell.getTypeByte() == Type.Minimum.getCode()) {
        // left is "bigger", i.e. it appears later in the sorted order
        return 1;
      }
      if (flen+clen == 0 && type == Type.Minimum.getCode()) {
        return -1;
      }

      compare = compareFamilies(
          cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength(),
          fam, foff, flen);
      if (compare != 0) {
        return compare;
      }
      compare = compareColumns(
          cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength(),
          col, coff, clen);
      if (compare != 0) {
        return compare;
      }
      // Next compare timestamps.
      compare = compareTimestamps(cell.getTimestamp(), ts);
      if (compare != 0) {
        return compare;
      }

      // Compare types. Let the delete types sort ahead of puts; i.e. types
      // of higher numbers sort before those of lesser numbers. Maximum (255)
      // appears ahead of everything, and minimum (0) appears after
      // everything.
      return (0xff & type) - (0xff & cell.getTypeByte());
    }

    public int compareOnlyKeyPortion(Cell left, Cell right) {
      return PrivateCellUtil.compareKeyIgnoresMvcc(CellComparatorImpl.COMPARATOR, left, right);
    }

    /**
     * Compares the Key of a cell -- with fields being more significant in this order:
     * rowkey, colfam/qual, timestamp, type, mvcc
     */
    @Override
    public int compare(final Cell left, final Cell right) {
      int compare = CellComparatorImpl.COMPARATOR.compare(left, right);
      return compare;
    }

    public int compareTimestamps(final Cell left, final Cell right) {
      return CellComparatorImpl.COMPARATOR.compareTimestamps(left, right);
    }

    /**
     * @param left
     * @param right
     * @return Result comparing rows.
     */
    public int compareRows(final Cell left, final Cell right) {
      return compareRows(left.getRowArray(),left.getRowOffset(), left.getRowLength(),
          right.getRowArray(), right.getRowOffset(), right.getRowLength());
    }

    /**
     * Get the b[],o,l for left and right rowkey portions and compare.
     * @param left
     * @param loffset
     * @param llength
     * @param right
     * @param roffset
     * @param rlength
     * @return 0 if equal, &lt;0 if left smaller, &gt;0 if right smaller
     */
    public int compareRows(byte[] left, int loffset, int llength, byte[] right, int roffset,
                           int rlength) {
      return Bytes.compareTo(left, loffset, llength, right, roffset, rlength);
    }

    int compareColumns(final Cell left, final short lrowlength, final Cell right,
                       final short rrowlength) {
      return CellComparatorImpl.COMPARATOR.compareColumns(left, right);
    }

    protected int compareColumns(
        byte [] left, int loffset, int llength, final int lfamilylength,
        byte [] right, int roffset, int rlength, final int rfamilylength) {
      // Compare family portion first.
      int diff = Bytes.compareTo(left, loffset, lfamilylength,
          right, roffset, rfamilylength);
      if (diff != 0) {
        return diff;
      }
      // Compare qualifier portion
      return Bytes.compareTo(left, loffset + lfamilylength,
          llength - lfamilylength,
          right, roffset + rfamilylength, rlength - rfamilylength);
    }

    static int compareTimestamps(final long ltimestamp, final long rtimestamp) {
      // The below older timestamps sorting ahead of newer timestamps looks
      // wrong but it is intentional. This way, newer timestamps are first
      // found when we iterate over a memstore and newer versions are the
      // first we trip over when reading from a store file.
      if (ltimestamp < rtimestamp) {
        return 1;
      } else if (ltimestamp > rtimestamp) {
        return -1;
      }
      return 0;
    }

    /**
     * Overridden
     * @param commonPrefix
     * @param left
     * @param loffset
     * @param llength
     * @param right
     * @param roffset
     * @param rlength
     * @return 0 if equal, &lt;0 if left smaller, &gt;0 if right smaller
     */
    @Override // SamePrefixComparator
    public int compareIgnoringPrefix(int commonPrefix, byte[] left,
                                     int loffset, int llength, byte[] right, int roffset, int rlength) {
      // Compare row
      short lrowlength = Bytes.toShort(left, loffset);
      short rrowlength;

      int comparisonResult = 0;
      if (commonPrefix < ROW_LENGTH_SIZE) {
        // almost nothing in common
        rrowlength = Bytes.toShort(right, roffset);
        comparisonResult = compareRows(left, loffset + ROW_LENGTH_SIZE,
            lrowlength, right, roffset + ROW_LENGTH_SIZE, rrowlength);
      } else { // the row length is the same
        rrowlength = lrowlength;
        if (commonPrefix < ROW_LENGTH_SIZE + rrowlength) {
          // The rows are not the same. Exclude the common prefix and compare
          // the rest of the two rows.
          int common = commonPrefix - ROW_LENGTH_SIZE;
          comparisonResult = compareRows(
              left, loffset + common + ROW_LENGTH_SIZE, lrowlength - common,
              right, roffset + common + ROW_LENGTH_SIZE, rrowlength - common);
        }
      }
      if (comparisonResult != 0) {
        return comparisonResult;
      }

      assert lrowlength == rrowlength;
      return compareWithoutRow(commonPrefix, left, loffset, llength, right,
          roffset, rlength, lrowlength);
    }

    /**
     * Compare columnFamily, qualifier, timestamp, and key type (everything
     * except the row). This method is used both in the normal comparator and
     * the "same-prefix" comparator. Note that we are assuming that row portions
     * of both KVs have already been parsed and found identical, and we don't
     * validate that assumption here.
     * @param commonPrefix
     *          the length of the common prefix of the two key-values being
     *          compared, including row length and row
     */
    private int compareWithoutRow(int commonPrefix, byte[] left, int loffset,
                                  int llength, byte[] right, int roffset, int rlength, short rowlength) {
      /***
       * KeyValue Format and commonLength:
       * |_keyLen_|_valLen_|_rowLen_|_rowKey_|_famiLen_|_fami_|_Quali_|....
       * ------------------|-------commonLength--------|--------------
       */
      int commonLength = ROW_LENGTH_SIZE + FAMILY_LENGTH_SIZE + rowlength;

      // commonLength + TIMESTAMP_TYPE_SIZE
      int commonLengthWithTSAndType = TIMESTAMP_TYPE_SIZE + commonLength;
      // ColumnFamily + Qualifier length.
      int lcolumnlength = llength - commonLengthWithTSAndType;
      int rcolumnlength = rlength - commonLengthWithTSAndType;

      byte ltype = left[loffset + (llength - 1)];
      byte rtype = right[roffset + (rlength - 1)];

      // If the column is not specified, the "minimum" key type appears the
      // latest in the sorted order, regardless of the timestamp. This is used
      // for specifying the last key/value in a given row, because there is no
      // "lexicographically last column" (it would be infinitely long). The
      // "maximum" key type does not need this behavior.
      if (lcolumnlength == 0 && ltype == Type.Minimum.getCode()) {
        // left is "bigger", i.e. it appears later in the sorted order
        return 1;
      }
      if (rcolumnlength == 0 && rtype == Type.Minimum.getCode()) {
        return -1;
      }

      int lfamilyoffset = commonLength + loffset;
      int rfamilyoffset = commonLength + roffset;

      // Column family length.
      int lfamilylength = left[lfamilyoffset - 1];
      int rfamilylength = right[rfamilyoffset - 1];
      // If left family size is not equal to right family size, we need not
      // compare the qualifiers.
      boolean sameFamilySize = (lfamilylength == rfamilylength);
      int common = 0;
      if (commonPrefix > 0) {
        common = Math.max(0, commonPrefix - commonLength);
        if (!sameFamilySize) {
          // Common should not be larger than Math.min(lfamilylength,
          // rfamilylength).
          common = Math.min(common, Math.min(lfamilylength, rfamilylength));
        } else {
          common = Math.min(common, Math.min(lcolumnlength, rcolumnlength));
        }
      }
      if (!sameFamilySize) {
        // comparing column family is enough.
        return Bytes.compareTo(left, lfamilyoffset + common, lfamilylength
            - common, right, rfamilyoffset + common, rfamilylength - common);
      }
      // Compare family & qualifier together.
      final int comparison = Bytes.compareTo(left, lfamilyoffset + common,
          lcolumnlength - common, right, rfamilyoffset + common,
          rcolumnlength - common);
      if (comparison != 0) {
        return comparison;
      }

      ////
      // Next compare timestamps.
      long ltimestamp = Bytes.toLong(left,
          loffset + (llength - TIMESTAMP_TYPE_SIZE));
      long rtimestamp = Bytes.toLong(right,
          roffset + (rlength - TIMESTAMP_TYPE_SIZE));
      int compare = compareTimestamps(ltimestamp, rtimestamp);
      if (compare != 0) {
        return compare;
      }

      // Compare types. Let the delete types sort ahead of puts; i.e. types
      // of higher numbers sort before those of lesser numbers. Maximum (255)
      // appears ahead of everything, and minimum (0) appears after
      // everything.
      return (0xff & rtype) - (0xff & ltype);
    }

    protected int compareFamilies(final byte[] left, final int loffset, final int lfamilylength,
                                  final byte[] right, final int roffset, final int rfamilylength) {
      int diff = Bytes.compareTo(left, loffset, lfamilylength, right, roffset, rfamilylength);
      return diff;
    }

    protected int compareColumns(final byte[] left, final int loffset, final int lquallength,
                                 final byte[] right, final int roffset, final int rquallength) {
      int diff = Bytes.compareTo(left, loffset, lquallength, right, roffset, rquallength);
      return diff;
    }
    /**
     * Compares the row and column of two keyvalues for equality
     * @param left
     * @param right
     * @return True if same row and column.
     */
    public boolean matchingRowColumn(final Cell left,
                                     final Cell right) {
      short lrowlength = left.getRowLength();
      short rrowlength = right.getRowLength();

      // TsOffset = end of column data. just comparing Row+CF length of each
      if ((left.getRowLength() + left.getFamilyLength() + left.getQualifierLength()) != (right
          .getRowLength() + right.getFamilyLength() + right.getQualifierLength())) {
        return false;
      }

      if (!matchingRows(left, lrowlength, right, rrowlength)) {
        return false;
      }

      int lfoffset = left.getFamilyOffset();
      int rfoffset = right.getFamilyOffset();
      int lclength = left.getQualifierLength();
      int rclength = right.getQualifierLength();
      int lfamilylength = left.getFamilyLength();
      int rfamilylength = right.getFamilyLength();
      int diff = compareFamilies(left.getFamilyArray(), lfoffset, lfamilylength,
          right.getFamilyArray(), rfoffset, rfamilylength);
      if (diff != 0) {
        return false;
      } else {
        diff = compareColumns(left.getQualifierArray(), left.getQualifierOffset(), lclength,
            right.getQualifierArray(), right.getQualifierOffset(), rclength);
        return diff == 0;
      }
    }

    /**
     * Compares the row of two keyvalues for equality
     * @param left
     * @param right
     * @return True if rows match.
     */
    public boolean matchingRows(final Cell left, final Cell right) {
      short lrowlength = left.getRowLength();
      short rrowlength = right.getRowLength();
      return matchingRows(left, lrowlength, right, rrowlength);
    }

    /**
     * @param left
     * @param lrowlength
     * @param right
     * @param rrowlength
     * @return True if rows match.
     */
    private boolean matchingRows(final Cell left, final short lrowlength,
                                 final Cell right, final short rrowlength) {
      return lrowlength == rrowlength &&
          matchingRows(left.getRowArray(), left.getRowOffset(), lrowlength,
              right.getRowArray(), right.getRowOffset(), rrowlength);
    }

    /**
     * Compare rows. Just calls Bytes.equals, but it's good to have this encapsulated.
     * @param left Left row array.
     * @param loffset Left row offset.
     * @param llength Left row length.
     * @param right Right row array.
     * @param roffset Right row offset.
     * @param rlength Right row length.
     * @return Whether rows are the same row.
     */
    public boolean matchingRows(final byte [] left, final int loffset, final int llength,
                                final byte [] right, final int roffset, final int rlength) {
      return Bytes.equals(left, loffset, llength, right, roffset, rlength);
    }

    public byte[] calcIndexKey(byte[] lastKeyOfPreviousBlock, byte[] firstKeyInBlock) {
      byte[] fakeKey = getShortMidpointKey(lastKeyOfPreviousBlock, firstKeyInBlock);
      if (compareFlatKey(fakeKey, firstKeyInBlock) > 0) {
        LOG.error("Unexpected getShortMidpointKey result, fakeKey:"
            + Bytes.toStringBinary(fakeKey) + ", firstKeyInBlock:"
            + Bytes.toStringBinary(firstKeyInBlock));
        return firstKeyInBlock;
      }
      if (lastKeyOfPreviousBlock != null && compareFlatKey(lastKeyOfPreviousBlock, fakeKey) >= 0) {
        LOG.error("Unexpected getShortMidpointKey result, lastKeyOfPreviousBlock:" +
            Bytes.toStringBinary(lastKeyOfPreviousBlock) + ", fakeKey:" +
            Bytes.toStringBinary(fakeKey));
        return firstKeyInBlock;
      }
      return fakeKey;
    }

    /**
     * This is a HFile block index key optimization.
     * @param leftKey
     * @param rightKey
     * @return 0 if equal, &lt;0 if left smaller, &gt;0 if right smaller
     * @deprecated Since 0.99.2;
     */
    @Deprecated
    public byte[] getShortMidpointKey(final byte[] leftKey, final byte[] rightKey) {
      if (rightKey == null) {
        throw new IllegalArgumentException("rightKey can not be null");
      }
      if (leftKey == null) {
        return Arrays.copyOf(rightKey, rightKey.length);
      }
      if (compareFlatKey(leftKey, rightKey) >= 0) {
        throw new IllegalArgumentException("Unexpected input, leftKey:" + Bytes.toString(leftKey)
            + ", rightKey:" + Bytes.toString(rightKey));
      }

      short leftRowLength = Bytes.toShort(leftKey, 0);
      short rightRowLength = Bytes.toShort(rightKey, 0);
      int leftCommonLength = ROW_LENGTH_SIZE + FAMILY_LENGTH_SIZE + leftRowLength;
      int rightCommonLength = ROW_LENGTH_SIZE + FAMILY_LENGTH_SIZE + rightRowLength;
      int leftCommonLengthWithTSAndType = TIMESTAMP_TYPE_SIZE + leftCommonLength;
      int rightCommonLengthWithTSAndType = TIMESTAMP_TYPE_SIZE + rightCommonLength;
      int leftColumnLength = leftKey.length - leftCommonLengthWithTSAndType;
      int rightColumnLength = rightKey.length - rightCommonLengthWithTSAndType;
      // rows are equal
      if (leftRowLength == rightRowLength && compareRows(leftKey, ROW_LENGTH_SIZE, leftRowLength,
          rightKey, ROW_LENGTH_SIZE, rightRowLength) == 0) {
        // Compare family & qualifier together.
        int comparison = Bytes.compareTo(leftKey, leftCommonLength, leftColumnLength, rightKey,
            rightCommonLength, rightColumnLength);
        // same with "row + family + qualifier", return rightKey directly
        if (comparison == 0) {
          return Arrays.copyOf(rightKey, rightKey.length);
        }
        // "family + qualifier" are different, generate a faked key per rightKey
        byte[] newKey = Arrays.copyOf(rightKey, rightKey.length);
        Bytes.putLong(newKey, rightKey.length - TIMESTAMP_TYPE_SIZE, HConstants.LATEST_TIMESTAMP);
        Bytes.putByte(newKey, rightKey.length - TYPE_SIZE, Type.Maximum.getCode());
        return newKey;
      }
      // rows are different
      short minLength = leftRowLength < rightRowLength ? leftRowLength : rightRowLength;
      short diffIdx = 0;
      while (diffIdx < minLength
          && leftKey[ROW_LENGTH_SIZE + diffIdx] == rightKey[ROW_LENGTH_SIZE + diffIdx]) {
        diffIdx++;
      }
      byte[] newRowKey = null;
      if (diffIdx >= minLength) {
        // leftKey's row is prefix of rightKey's.
        newRowKey = new byte[diffIdx + 1];
        System.arraycopy(rightKey, ROW_LENGTH_SIZE, newRowKey, 0, diffIdx + 1);
      } else {
        int diffByte = leftKey[ROW_LENGTH_SIZE + diffIdx];
        if ((0xff & diffByte) < 0xff && (diffByte + 1) <
            (rightKey[ROW_LENGTH_SIZE + diffIdx] & 0xff)) {
          newRowKey = new byte[diffIdx + 1];
          System.arraycopy(leftKey, ROW_LENGTH_SIZE, newRowKey, 0, diffIdx);
          newRowKey[diffIdx] = (byte) (diffByte + 1);
        } else {
          newRowKey = new byte[diffIdx + 1];
          System.arraycopy(rightKey, ROW_LENGTH_SIZE, newRowKey, 0, diffIdx + 1);
        }
      }
      return new KeyValue(newRowKey, null, null, HConstants.LATEST_TIMESTAMP,
          Type.Maximum).getKey();
    }

    @Override
    protected Object clone() throws CloneNotSupportedException {
      super.clone();
      return new KVComparator();
    }

  }

  /**
   * @param in Where to read bytes from.  Creates a byte array to hold the KeyValue
   * backing bytes copied from the steam.
   * @return KeyValue created by deserializing from <code>in</code> OR if we find a length
   * of zero, we will return null which can be useful marking a stream as done.
   * @throws IOException
   */
  public static KeyValue create(final DataInput in) throws IOException {
    return create(in.readInt(), in);
  }

  /**
   * Create a KeyValue reading <code>length</code> from <code>in</code>
   * @param length
   * @param in
   * @return Created KeyValue OR if we find a length of zero, we will return null which
   * can be useful marking a stream as done.
   * @throws IOException
   */
  public static KeyValue create(int length, final DataInput in) throws IOException {

    if (length <= 0) {
      if (length == 0) return null;
      throw new IOException("Failed read " + length + " bytes, stream corrupt?");
    }

    // This is how the old Writables.readFrom used to deserialize.  Didn't even vint.
    byte [] bytes = new byte[length];
    in.readFully(bytes);
    return new KeyValue(bytes, 0, length);
  }

  /**
   * Write out a KeyValue in the manner in which we used to when KeyValue was a Writable.
   * @param kv
   * @param out
   * @return Length written on stream
   * @throws IOException
   * @see #create(DataInput) for the inverse function
   */
  public static long write(final KeyValue kv, final DataOutput out) throws IOException {
    // This is how the old Writables write used to serialize KVs.  Need to figure way to make it
    // work for all implementations.
    int length = kv.getLength();
    out.writeInt(length);
    out.write(kv.getBuffer(), kv.getOffset(), length);
    return (long) length + Bytes.SIZEOF_INT;
  }

  /**
   * Write out a KeyValue in the manner in which we used to when KeyValue was a Writable but do
   * not require a {@link DataOutput}, just take plain {@link OutputStream}
   * Named <code>oswrite</code> so does not clash with {@link #write(KeyValue, DataOutput)}
   * @param kv
   * @param out
   * @param withTags
   * @return Length written on stream
   * @throws IOException
   * @see #create(DataInput) for the inverse function
   * @see #write(KeyValue, DataOutput)
   * @see KeyValueUtil#oswrite(Cell, OutputStream, boolean)
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
   *             Instead use {@link #write(OutputStream, boolean)}
   */
  @Deprecated
  public static long oswrite(final KeyValue kv, final OutputStream out, final boolean withTags)
      throws IOException {
    ByteBufferUtils.putInt(out, kv.getSerializedSize(withTags));
    return (long) kv.write(out, withTags) + Bytes.SIZEOF_INT;
  }

  @Override
  public int write(OutputStream out, boolean withTags) throws IOException {
    int len = getSerializedSize(withTags);
    out.write(this.bytes, this.offset, len);
    return len;
  }

  @Override
  public int getSerializedSize(boolean withTags) {
    if (withTags) {
      return this.length;
    }
    return this.getKeyLength() + this.getValueLength() + KEYVALUE_INFRASTRUCTURE_SIZE;
  }

  @Override
  public int getSerializedSize() {
    return this.length;
  }

  @Override
  public void write(ByteBuffer buf, int offset) {
    ByteBufferUtils.copyFromArrayToBuffer(buf, offset, this.bytes, this.offset, this.length);
  }

  /**
   * Avoids redundant comparisons for better performance.
   *
   * TODO get rid of this wart
   */
  public interface SamePrefixComparator<T> {
    /**
     * Compare two keys assuming that the first n bytes are the same.
     * @param commonPrefix How many bytes are the same.
     */
    int compareIgnoringPrefix(int commonPrefix, byte[] left, int loffset, int llength,
                              byte[] right, int roffset, int rlength
    );
  }

  /**
   * HeapSize implementation
   *
   * We do not count the bytes in the rowCache because it should be empty for a KeyValue in the
   * MemStore.
   */
  @Override
  public long heapSize() {
    /*
     * Deep object overhead for this KV consists of two parts. The first part is the KV object
     * itself, while the second part is the backing byte[]. We will only count the array overhead
     * from the byte[] only if this is the first KV in there.
     */
    return ClassSize.align(FIXED_OVERHEAD) +
        (offset == 0
            ? ClassSize.sizeOfByteArray(length)  // count both length and object overhead
            : length);                           // only count the number of bytes
  }

  /**
   * A simple form of KeyValue that creates a keyvalue with only the key part of the byte[]
   * Mainly used in places where we need to compare two cells.  Avoids copying of bytes
   * In places like block index keys, we need to compare the key byte[] with a cell.
   * Hence create a Keyvalue(aka Cell) that would help in comparing as two cells
   */
  public static class KeyOnlyKeyValue extends KeyValue {
    private short rowLen = -1;
    public KeyOnlyKeyValue() {

    }
    public KeyOnlyKeyValue(byte[] b) {
      this(b, 0, b.length);
    }

    public KeyOnlyKeyValue(byte[] b, int offset, int length) {
      this.bytes = b;
      this.length = length;
      this.offset = offset;
      this.rowLen = Bytes.toShort(this.bytes, this.offset);
    }

    public void set(KeyOnlyKeyValue keyOnlyKeyValue) {
      this.bytes = keyOnlyKeyValue.bytes;
      this.length = keyOnlyKeyValue.length;
      this.offset = keyOnlyKeyValue.offset;
      this.rowLen = keyOnlyKeyValue.rowLen;
    }

    public void clear() {
      rowLen = -1;
      bytes = null;
      offset = 0;
      length = 0;
    }

    @Override
    public int getKeyOffset() {
      return this.offset;
    }

    /**
     * A setter that helps to avoid object creation every time and whenever
     * there is a need to create new KeyOnlyKeyValue.
     * @param key
     * @param offset
     * @param length
     */
    public void setKey(byte[] key, int offset, int length) {
      this.bytes = key;
      this.offset = offset;
      this.length = length;
      this.rowLen = Bytes.toShort(this.bytes, this.offset);
    }

    @Override
    public byte[] getKey() {
      int keylength = getKeyLength();
      byte[] key = new byte[keylength];
      System.arraycopy(this.bytes, getKeyOffset(), key, 0, keylength);
      return key;
    }

    @Override
    public byte[] getRowArray() {
      return bytes;
    }

    @Override
    public int getRowOffset() {
      return getKeyOffset() + Bytes.SIZEOF_SHORT;
    }

    @Override
    public byte[] getFamilyArray() {
      return bytes;
    }

    @Override
    public byte getFamilyLength() {
      return this.bytes[getFamilyOffset() - 1];
    }

    int getFamilyLengthPosition(int rowLength) {
      return this.offset + Bytes.SIZEOF_SHORT + rowLength;
    }

    @Override
    public int getFamilyOffset() {
      return this.offset + Bytes.SIZEOF_SHORT + getRowLength() + Bytes.SIZEOF_BYTE;
    }

    @Override
    public byte[] getQualifierArray() {
      return bytes;
    }

    @Override
    public int getQualifierLength() {
      return getQualifierLength(getRowLength(), getFamilyLength());
    }

    @Override
    public int getQualifierOffset() {
      return getFamilyOffset() + getFamilyLength();
    }

    @Override
    public int getKeyLength() {
      return length;
    }

    @Override
    public short getRowLength() {
      return rowLen;
    }

    @Override
    public byte getTypeByte() {
      return getTypeByte(getKeyLength());
    }

    byte getTypeByte(int keyLength) {
      return this.bytes[this.offset + keyLength - 1];
    }


    private int getQualifierLength(int rlength, int flength) {
      return getKeyLength() - (int) getKeyDataStructureSize(rlength, flength, 0);
    }

    @Override
    public long getTimestamp() {
      int tsOffset = getTimestampOffset();
      return Bytes.toLong(this.bytes, tsOffset);
    }

    @Override
    public int getTimestampOffset() {
      return getKeyOffset() + getKeyLength() - TIMESTAMP_TYPE_SIZE;
    }

    @Override
    public byte[] getTagsArray() {
      return HConstants.EMPTY_BYTE_ARRAY;
    }

    @Override
    public int getTagsOffset() {
      return 0;
    }

    @Override
    public byte[] getValueArray() {
      throw new IllegalArgumentException("KeyOnlyKeyValue does not work with values.");
    }

    @Override
    public int getValueOffset() {
      throw new IllegalArgumentException("KeyOnlyKeyValue does not work with values.");
    }

    @Override
    public int getValueLength() {
      throw new IllegalArgumentException("KeyOnlyKeyValue does not work with values.");
    }

    @Override
    public int getTagsLength() {
      return 0;
    }

    @Override
    public String toString() {
      if (this.bytes == null || this.bytes.length == 0) {
        return "empty";
      }
      return keyToString(this.bytes, this.offset, getKeyLength()) + "/vlen=0/mvcc=0";
    }

    @Override
    public int hashCode() {
      return super.hashCode();
    }

    @Override
    public boolean equals(Object other) {
      return super.equals(other);
    }

    @Override
    public long heapSize() {
      return super.heapSize() + Bytes.SIZEOF_SHORT;
    }

    @Override
    public int write(OutputStream out, boolean withTags) throws IOException {
      // This type of Cell is used only to maintain some internal states. We never allow this type
      // of Cell to be returned back over the RPC
      throw new IllegalStateException("A reader should never return this type of a Cell");
    }
  }

  @Override
  public ExtendedCell deepClone() {
    byte[] copy = Bytes.copy(this.bytes, this.offset, this.length);
    KeyValue kv = new KeyValue(copy, 0, copy.length);
    kv.setSequenceId(this.getSequenceId());
    return kv;
  }
}
