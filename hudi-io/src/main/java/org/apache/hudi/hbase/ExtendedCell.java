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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.apache.hudi.hbase.io.HeapSize;
import org.apache.hudi.hbase.util.ByteBufferUtils;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * Extension to {@link Cell} with server side required functions. Server side Cell implementations
 * must implement this.
 */
@InterfaceAudience.Private
public interface ExtendedCell extends RawCell, HeapSize {
  int CELL_NOT_BASED_ON_CHUNK = -1;

  /**
   * Write this cell to an OutputStream in a {@link KeyValue} format.
   * <br> KeyValue format <br>
   * <code>&lt;4 bytes keylength&gt; &lt;4 bytes valuelength&gt; &lt;2 bytes rowlength&gt;
   * &lt;row&gt; &lt;1 byte columnfamilylength&gt; &lt;columnfamily&gt; &lt;columnqualifier&gt;
   * &lt;8 bytes timestamp&gt; &lt;1 byte keytype&gt; &lt;value&gt; &lt;2 bytes tagslength&gt;
   * &lt;tags&gt;</code>
   * @param out Stream to which cell has to be written
   * @param withTags Whether to write tags.
   * @return how many bytes are written.
   * @throws IOException
   */
  // TODO remove the boolean param once HBASE-16706 is done.
  default int write(OutputStream out, boolean withTags) throws IOException {
    // Key length and then value length
    ByteBufferUtils.putInt(out, KeyValueUtil.keyLength(this));
    ByteBufferUtils.putInt(out, getValueLength());

    // Key
    PrivateCellUtil.writeFlatKey(this, out);

    if (getValueLength() > 0) {
      // Value
      out.write(getValueArray(), getValueOffset(), getValueLength());
    }

    // Tags length and tags byte array
    if (withTags && getTagsLength() > 0) {
      // Tags length
      out.write((byte)(0xff & (getTagsLength() >> 8)));
      out.write((byte)(0xff & getTagsLength()));

      // Tags byte array
      out.write(getTagsArray(), getTagsOffset(), getTagsLength());
    }

    return getSerializedSize(withTags);
  }

  /**
   * @param withTags Whether to write tags.
   * @return Bytes count required to serialize this Cell in a {@link KeyValue} format.
   * <br> KeyValue format <br>
   * <code>&lt;4 bytes keylength&gt; &lt;4 bytes valuelength&gt; &lt;2 bytes rowlength&gt;
   * &lt;row&gt; &lt;1 byte columnfamilylength&gt; &lt;columnfamily&gt; &lt;columnqualifier&gt;
   * &lt;8 bytes timestamp&gt; &lt;1 byte keytype&gt; &lt;value&gt; &lt;2 bytes tagslength&gt;
   * &lt;tags&gt;</code>
   */
  // TODO remove the boolean param once HBASE-16706 is done.
  default int getSerializedSize(boolean withTags) {
    return KeyValueUtil.length(getRowLength(), getFamilyLength(), getQualifierLength(),
        getValueLength(), getTagsLength(), withTags);
  }

  /**
   * @return Serialized size (defaults to include tag length).
   */
  @Override
  default int getSerializedSize() {
    return getSerializedSize(true);
  }

  /**
   * Write this Cell into the given buf's offset in a {@link KeyValue} format.
   * @param buf The buffer where to write the Cell.
   * @param offset The offset within buffer, to write the Cell.
   */
  default void write(ByteBuffer buf, int offset) {
    KeyValueUtil.appendTo(this, buf, offset, true);
  }

  /**
   * Does a deep copy of the contents to a new memory area and returns it as a new cell.
   * @return The deep cloned cell
   */
  default ExtendedCell deepClone() {
    // When being added to the memstore, deepClone() is called and KeyValue has less heap overhead.
    return new KeyValue(this);
  }

  /**
   * Extracts the id of the backing bytebuffer of this cell if it was obtained from fixed sized
   * chunks as in case of MemstoreLAB
   * @return the chunk id if the cell is backed by fixed sized Chunks, else return
   * {@link #CELL_NOT_BASED_ON_CHUNK}; i.e. -1.
   */
  default int getChunkId() {
    return CELL_NOT_BASED_ON_CHUNK;
  }

  /**
   * Sets with the given seqId.
   * @param seqId sequence ID
   */
  void setSequenceId(long seqId) throws IOException;

  /**
   * Sets with the given timestamp.
   * @param ts timestamp
   */
  void setTimestamp(long ts) throws IOException;

  /**
   * Sets with the given timestamp.
   * @param ts buffer containing the timestamp value
   */
  void setTimestamp(byte[] ts) throws IOException;

  /**
   * A region-specific unique monotonically increasing sequence ID given to each Cell. It always
   * exists for cells in the memstore but is not retained forever. It will be kept for
   * {@link HConstants#KEEP_SEQID_PERIOD} days, but generally becomes irrelevant after the cell's
   * row is no longer involved in any operations that require strict consistency.
   * @return seqId (always &gt; 0 if exists), or 0 if it no longer exists
   */
  long getSequenceId();

  /**
   * Contiguous raw bytes representing tags that may start at any index in the containing array.
   * @return the tags byte array
   */
  byte[] getTagsArray();

  /**
   * @return the first offset where the tags start in the Cell
   */
  int getTagsOffset();

  /**
   * HBase internally uses 2 bytes to store tags length in Cell. As the tags length is always a
   * non-negative number, to make good use of the sign bit, the max of tags length is defined 2 *
   * Short.MAX_VALUE + 1 = 65535. As a result, the return type is int, because a short is not
   * capable of handling that. Please note that even if the return type is int, the max tags length
   * is far less than Integer.MAX_VALUE.
   * @return the total length of the tags in the Cell.
   */
  int getTagsLength();

  /**
   * @return The byte representation of the KeyValue.TYPE of this cell: one of Put, Delete, etc
   */
  byte getTypeByte();
}
