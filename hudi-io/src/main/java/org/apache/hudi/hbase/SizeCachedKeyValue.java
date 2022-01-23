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

import org.apache.hudi.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * This class is an extension to KeyValue where rowLen and keyLen are cached.
 * Parsing the backing byte[] every time to get these values will affect the performance.
 * In read path, we tend to read these values many times in Comparator, SQM etc.
 * Note: Please do not use these objects in write path as it will increase the heap space usage.
 * See https://issues.apache.org/jira/browse/HBASE-13448
 */
@InterfaceAudience.Private
public class SizeCachedKeyValue extends KeyValue {
  // Overhead in this class alone. Parent's overhead will be considered in usage places by calls to
  // super. methods
  private static final int FIXED_OVERHEAD = Bytes.SIZEOF_SHORT + Bytes.SIZEOF_INT;

  private short rowLen;
  private int keyLen;

  public SizeCachedKeyValue(byte[] bytes, int offset, int length, long seqId, int keyLen) {
    super(bytes, offset, length);
    // We will read all these cached values at least once. Initialize now itself so that we can
    // avoid uninitialized checks with every time call
    this.rowLen = super.getRowLength();
    this.keyLen = keyLen;
    setSequenceId(seqId);
  }

  public SizeCachedKeyValue(byte[] bytes, int offset, int length, long seqId, int keyLen,
                            short rowLen) {
    super(bytes, offset, length);
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
}
