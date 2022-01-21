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

import org.apache.hudi.hbase.io.HeapSize;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * The unit of storage in HBase consisting of the following fields:
 * <br>
 * <pre>
 * 1) row
 * 2) column family
 * 3) column qualifier
 * 4) timestamp
 * 5) type
 * 6) MVCC version
 * 7) value
 * </pre>
 * <p>
 * Uniqueness is determined by the combination of row, column family, column qualifier,
 * timestamp, and type.
 * </p>
 * <p>
 * The natural comparator will perform a bitwise comparison on row, column family, and column
 * qualifier. Less intuitively, it will then treat the greater timestamp as the lesser value with
 * the goal of sorting newer cells first.
 * </p>
 * <p>
 * Cell implements Comparable&lt;Cell&gt; which is only meaningful when
 * comparing to other keys in the
 * same table. It uses CellComparator which does not work on the -ROOT- and hbase:meta tables.
 * </p>
 * <p>
 * In the future, we may consider adding a boolean isOnHeap() method and a getValueBuffer() method
 * that can be used to pass a value directly from an off-heap ByteBuffer to the network without
 * copying into an on-heap byte[].
 * </p>
 * <p>
 * Historic note: the original Cell implementation (KeyValue) requires that all fields be encoded as
 * consecutive bytes in the same byte[], whereas this interface allows fields to reside in separate
 * byte[]'s.
 * </p>
 */
@InterfaceAudience.Public
public interface Cell extends HeapSize {

  //1) Row

  /**
   * Contiguous raw bytes that may start at any index in the containing array. Max length is
   * Short.MAX_VALUE which is 32,767 bytes.
   * @return The array containing the row bytes.
   */
  byte[] getRowArray();

  /**
   * @return Array index of first row byte
   */
  int getRowOffset();

  /**
   * @return Number of row bytes. Must be &lt; rowArray.length - offset.
   */
  short getRowLength();


  //2) Family

  /**
   * Contiguous bytes composed of legal HDFS filename characters which may start at any index in the
   * containing array. Max length is Byte.MAX_VALUE, which is 127 bytes.
   * @return the array containing the family bytes.
   */
  byte[] getFamilyArray();

  /**
   * @return Array index of first family byte
   */
  int getFamilyOffset();

  /**
   * @return Number of family bytes.  Must be &lt; familyArray.length - offset.
   */
  byte getFamilyLength();


  //3) Qualifier

  /**
   * Contiguous raw bytes that may start at any index in the containing array.
   * @return The array containing the qualifier bytes.
   */
  byte[] getQualifierArray();

  /**
   * @return Array index of first qualifier byte
   */
  int getQualifierOffset();

  /**
   * @return Number of qualifier bytes.  Must be &lt; qualifierArray.length - offset.
   */
  int getQualifierLength();


  //4) Timestamp

  /**
   * @return Long value representing time at which this cell was "Put" into the row.  Typically
   * represents the time of insertion, but can be any value from 0 to Long.MAX_VALUE.
   */
  long getTimestamp();


  //5) Type

  /**
   * @return The byte representation of the KeyValue.TYPE of this cell: one of Put, Delete, etc
   * @deprecated As of HBase-2.0. Will be removed in HBase-3.0. Use {@link #getType()}.
   */
  @Deprecated
  byte getTypeByte();


  //6) SequenceId

  /**
   * A region-specific unique monotonically increasing sequence ID given to each Cell. It always
   * exists for cells in the memstore but is not retained forever. It will be kept for
   * {@link HConstants#KEEP_SEQID_PERIOD} days, but generally becomes irrelevant after the cell's
   * row is no longer involved in any operations that require strict consistency.
   * @return seqId (always &gt; 0 if exists), or 0 if it no longer exists
   * @deprecated As of HBase-2.0. Will be removed in HBase-3.0.
   */
  @Deprecated
  long getSequenceId();

  //7) Value

  /**
   * Contiguous raw bytes that may start at any index in the containing array. Max length is
   * Integer.MAX_VALUE which is 2,147,483,647 bytes.
   * @return The array containing the value bytes.
   */
  byte[] getValueArray();

  /**
   * @return Array index of first value byte
   */
  int getValueOffset();

  /**
   * @return Number of value bytes.  Must be &lt; valueArray.length - offset.
   */
  int getValueLength();

  /**
   * @return Serialized size (defaults to include tag length if has some tags).
   */
  int getSerializedSize();

  /**
   * Contiguous raw bytes representing tags that may start at any index in the containing array.
   * @return the tags byte array
   * @deprecated As of HBase-2.0. Will be removed in HBase-3.0. Tags are are now internal.
   */
  @Deprecated
  byte[] getTagsArray();

  /**
   * @return the first offset where the tags start in the Cell
   * @deprecated As of HBase-2.0. Will be removed in HBase-3.0. Tags are are now internal.
   */
  @Deprecated
  int getTagsOffset();

  /**
   * HBase internally uses 2 bytes to store tags length in Cell.
   * As the tags length is always a non-negative number, to make good use of the sign bit,
   * the max of tags length is defined 2 * Short.MAX_VALUE + 1 = 65535.
   * As a result, the return type is int, because a short is not capable of handling that.
   * Please note that even if the return type is int, the max tags length is far
   * less than Integer.MAX_VALUE.
   *
   * @return the total length of the tags in the Cell.
   * @deprecated As of HBase-2.0. Will be removed in HBase-3.0. Tags are are now internal.
   */
  @Deprecated
  int getTagsLength();

  /**
   * Returns the type of cell in a human readable format using {@link Type}.
   * Note : This does not expose the internal types of Cells like {@link KeyValue.Type#Maximum} and
   * {@link KeyValue.Type#Minimum}
   * @return The data type this cell: one of Put, Delete, etc
   */
  default Type getType() {
    byte byteType = getTypeByte();
    Type t = Type.CODE_ARRAY[byteType & 0xff];
    if (t != null) {
      return t;
    }
    throw new UnsupportedOperationException("Invalid type of cell " + byteType);
  }

  /**
   * The valid types for user to build the cell. Currently, This is subset of {@link KeyValue.Type}.
   */
  enum Type {
    Put((byte) 4),

    Delete((byte) 8),

    DeleteFamilyVersion((byte) 10),

    DeleteColumn((byte) 12),

    DeleteFamily((byte) 14);

    private final byte code;

    Type(final byte c) {
      this.code = c;
    }

    public byte getCode() {
      return this.code;
    }

    private static final Type[] CODE_ARRAY = new Type[256];

    static {
      for (Type t : Type.values()) {
        CODE_ARRAY[t.code & 0xff] = t;
      }
    }
  }
}

