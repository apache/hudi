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

import org.apache.hudi.hbase.util.ByteBufferUtils;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * An extension of the ByteBufferKeyValue where the tags length is always 0
 */
@InterfaceAudience.Private
public class NoTagsByteBufferKeyValue extends ByteBufferKeyValue {

  public NoTagsByteBufferKeyValue(ByteBuffer buf, int offset, int length) {
    super(buf, offset, length);
  }

  public NoTagsByteBufferKeyValue(ByteBuffer buf, int offset, int length, long seqId) {
    super(buf, offset, length, seqId);
  }

  @Override
  public byte[] getTagsArray() {
    return HConstants.EMPTY_BYTE_ARRAY;
  }

  @Override
  public int getTagsLength() {
    return 0;
  }

  @Override
  public int getSerializedSize(boolean withTags) {
    return this.length;
  }

  @Override
  public ExtendedCell deepClone() {
    byte[] copy = new byte[this.length];
    ByteBufferUtils.copyFromBufferToArray(copy, this.buf, this.offset, 0, this.length);
    KeyValue kv = new NoTagsKeyValue(copy, 0, copy.length);
    kv.setSequenceId(this.getSequenceId());
    return kv;
  }
}
