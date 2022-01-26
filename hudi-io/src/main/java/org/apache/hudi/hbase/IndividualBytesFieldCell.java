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

import org.apache.commons.lang3.ArrayUtils;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hudi.hbase.util.Bytes;
import org.apache.hudi.hbase.util.ClassSize;

@InterfaceAudience.Private
public class IndividualBytesFieldCell implements ExtendedCell, Cloneable {
  // do alignment(padding gap)
  private static final long FIXED_OVERHEAD = ClassSize.align(ClassSize.OBJECT // object header
      // timestamp and type
      + KeyValue.TIMESTAMP_TYPE_SIZE
      // sequence id
      + Bytes.SIZEOF_LONG
      // references to all byte arrays: row, family, qualifier, value, tags
      + 5 * ClassSize.REFERENCE);

  // The following fields are backed by individual byte arrays
  private final byte[] row;
  private final int rOffset;
  private final int rLength;
  private final byte[] family;
  private final int fOffset;
  private final int fLength;
  private final byte[] qualifier;
  private final int qOffset;
  private final int qLength;
  private final byte[] value;
  private final int vOffset;
  private final int vLength;
  private final byte[] tags;  // A byte array, rather than an array of org.apache.hudi.hbase.Tag
  private final int tagsOffset;
  private final int tagsLength;

  // Other fields
  private long timestamp;
  private final byte type;  // A byte, rather than org.apache.hudi.hbase.KeyValue.Type
  private long seqId;

  public IndividualBytesFieldCell(byte[] row, byte[] family, byte[] qualifier, long timestamp,
                                  KeyValue.Type type,  byte[] value) {
    this(row, family, qualifier, timestamp, type, 0L /* sequence id */, value, null /* tags */);
  }

  public IndividualBytesFieldCell(byte[] row, byte[] family, byte[] qualifier, long timestamp,
                                  KeyValue.Type type, long seqId, byte[] value, byte[] tags) {
    this(row, 0, ArrayUtils.getLength(row),
        family, 0, ArrayUtils.getLength(family),
        qualifier, 0, ArrayUtils.getLength(qualifier),
        timestamp, type, seqId,
        value, 0, ArrayUtils.getLength(value),
        tags, 0, ArrayUtils.getLength(tags));
  }

  public IndividualBytesFieldCell(byte[] row, int rOffset, int rLength, byte[] family, int fOffset,
                                  int fLength, byte[] qualifier, int qOffset, int qLength, long timestamp, KeyValue.Type type,
                                  long seqId, byte[] value, int vOffset, int vLength, byte[] tags, int tagsOffset,
                                  int tagsLength) {
    // Check row, family, qualifier and value
    KeyValue.checkParameters(row, rLength,     // row and row length
        family, fLength,  // family and family length
        qLength,          // qualifier length
        vLength);         // value length

    // Check timestamp
    if (timestamp < 0) {
      throw new IllegalArgumentException("Timestamp cannot be negative. ts=" + timestamp);
    }

    // Check tags
    RawCell.checkForTagsLength(tagsLength);
    checkArrayBounds(row, rOffset, rLength);
    checkArrayBounds(family, fOffset, fLength);
    checkArrayBounds(qualifier, qOffset, qLength);
    checkArrayBounds(value, vOffset, vLength);
    checkArrayBounds(tags, tagsOffset, tagsLength);
    // No local copy is made, but reference to the input directly
    this.row        = row;
    this.rOffset    = rOffset;
    this.rLength    = rLength;
    this.family     = family;
    this.fOffset    = fOffset;
    this.fLength    = fLength;
    this.qualifier  = qualifier;
    this.qOffset    = qOffset;
    this.qLength    = qLength;
    this.value      = value;
    this.vOffset    = vOffset;
    this.vLength    = vLength;
    this.tags       = tags;
    this.tagsOffset = tagsOffset;
    this.tagsLength = tagsLength;

    // Set others
    this.timestamp  = timestamp;
    this.type       = type.getCode();
    this.seqId      = seqId;
  }

  private void checkArrayBounds(byte[] bytes, int offset, int length) {
    if (offset < 0 || length < 0) {
      throw new IllegalArgumentException("Negative number! offset=" + offset + "and length="
          + length);
    }
    if (bytes == null && (offset != 0 || length != 0)) {
      throw new IllegalArgumentException("Null bytes array but offset=" + offset + "and length="
          + length);
    }
    if (bytes != null && bytes.length < offset + length) {
      throw new IllegalArgumentException("Out of bounds! bytes.length=" + bytes.length
          + ", offset=" + offset + ", length=" + length);
    }
  }

  private long heapOverhead() {
    return FIXED_OVERHEAD
        + ClassSize.ARRAY                               // row      , can not be null
        + ((family    == null) ? 0 : ClassSize.ARRAY)   // family   , can be null
        + ((qualifier == null) ? 0 : ClassSize.ARRAY)   // qualifier, can be null
        + ((value     == null) ? 0 : ClassSize.ARRAY)   // value    , can be null
        + ((tags      == null) ? 0 : ClassSize.ARRAY);  // tags     , can be null
  }

  /**
   * Implement Cell interface
   */
  // 1) Row
  @Override
  public byte[] getRowArray() {
    // If row is null, the constructor will reject it, by {@link KeyValue#checkParameters()},
    // so it is safe to return row without checking.
    return row;
  }

  @Override
  public int getRowOffset() {
    return rOffset;
  }

  @Override
  public short getRowLength() {
    // If row is null or rLength is invalid, the constructor will reject it, by
    // {@link KeyValue#checkParameters()}, so it is safe to call rLength and make the type
    // conversion.
    return (short)(rLength);
  }

  // 2) Family
  @Override
  public byte[] getFamilyArray() {
    // Family could be null
    return (family == null) ? HConstants.EMPTY_BYTE_ARRAY : family;
  }

  @Override
  public int getFamilyOffset() {
    return fOffset;
  }

  @Override
  public byte getFamilyLength() {
    // If fLength is invalid, the constructor will reject it, by {@link KeyValue#checkParameters()},
    // so it is safe to make the type conversion.
    return (byte)(fLength);
  }

  // 3) Qualifier
  @Override
  public byte[] getQualifierArray() {
    // Qualifier could be null
    return (qualifier == null) ? HConstants.EMPTY_BYTE_ARRAY : qualifier;
  }

  @Override
  public int getQualifierOffset() {
    return qOffset;
  }

  @Override
  public int getQualifierLength() {
    return qLength;
  }

  // 4) Timestamp
  @Override
  public long getTimestamp() {
    return timestamp;
  }

  //5) Type
  @Override
  public byte getTypeByte() {
    return type;
  }

  //6) Sequence id
  @Override
  public long getSequenceId() {
    return seqId;
  }

  //7) Value
  @Override
  public byte[] getValueArray() {
    // Value could be null
    return (value == null) ? HConstants.EMPTY_BYTE_ARRAY : value;
  }

  @Override
  public int getValueOffset() {
    return vOffset;
  }

  @Override
  public int getValueLength() {
    return vLength;
  }

  // 8) Tags
  @Override
  public byte[] getTagsArray() {
    // Tags can could null
    return (tags == null) ? HConstants.EMPTY_BYTE_ARRAY : tags;
  }

  @Override
  public int getTagsOffset() {
    return tagsOffset;
  }

  @Override
  public int getTagsLength() {
    return tagsLength;
  }

  /**
   * Implement HeapSize interface
   */
  @Override
  public long heapSize() {
    // Size of array headers are already included into overhead, so do not need to include it for
    // each byte array
    return   heapOverhead()                         // overhead, with array headers included
        + ClassSize.align(getRowLength())        // row
        + ClassSize.align(getFamilyLength())     // family
        + ClassSize.align(getQualifierLength())  // qualifier
        + ClassSize.align(getValueLength())      // value
        + ClassSize.align(getTagsLength());      // tags
  }

  /**
   * Implement Cloneable interface
   */
  @Override
  public Object clone() throws CloneNotSupportedException {
    return super.clone();  // only a shadow copy
  }

  @Override
  public void setSequenceId(long seqId) {
    if (seqId < 0) {
      throw new IllegalArgumentException("Sequence Id cannot be negative. ts=" + seqId);
    }
    this.seqId = seqId;
  }

  @Override
  public void setTimestamp(long ts) {
    if (ts < 0) {
      throw new IllegalArgumentException("Timestamp cannot be negative. ts=" + ts);
    }
    this.timestamp = ts;
  }

  @Override
  public void setTimestamp(byte[] ts) {
    setTimestamp(Bytes.toLong(ts, 0));
  }

  @Override
  public String toString() {
    return CellUtil.toString(this, true);
  }
}
