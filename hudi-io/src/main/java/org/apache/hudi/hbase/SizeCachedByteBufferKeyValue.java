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

import java.nio.ByteBuffer;

import org.apache.hudi.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * This Cell is an implementation of {@link ByteBufferExtendedCell} where the data resides in
 * off heap/ on heap ByteBuffer
 */
@InterfaceAudience.Private
public class SizeCachedByteBufferKeyValue extends ByteBufferKeyValue {

  public static final int FIXED_OVERHEAD = Bytes.SIZEOF_SHORT + Bytes.SIZEOF_INT;
  private short rowLen;
  private int keyLen;

  public SizeCachedByteBufferKeyValue(ByteBuffer buf, int offset, int length, long seqId,
                                      int keyLen) {
    super(buf, offset, length);
    // We will read all these cached values at least once. Initialize now itself so that we can
    // avoid uninitialized checks with every time call
    this.rowLen = super.getRowLength();
    this.keyLen = keyLen;
    setSequenceId(seqId);
  }

  public SizeCachedByteBufferKeyValue(ByteBuffer buf, int offset, int length, long seqId,
                                      int keyLen, short rowLen) {
    super(buf, offset, length);
    // We will read all these cached values at least once. Initialize now itself so that we can
    // avoid uninitialized checks with every time call
    this.rowLen = rowLen;
    this.keyLen = keyLen;
    setSequenceId(seqId);
  }

  @Override
  public short getRowLength() {
    return rowLen;
  }

  @Override
  public int getKeyLength() {
    return this.keyLen;
  }

  @Override
  public long heapSize() {
    return super.heapSize() + FIXED_OVERHEAD;
  }

  /**
   * Override by just returning the length for saving cost of method dispatching. If not, it will
   * call {@link ExtendedCell#getSerializedSize()} firstly, then forward to
   * {@link SizeCachedKeyValue#getSerializedSize(boolean)}. (See HBASE-21657)
   */
  @Override
  public int getSerializedSize() {
    return this.length;
  }

  @Override
  public boolean equals(Object other) {
    return super.equals(other);
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }
}
