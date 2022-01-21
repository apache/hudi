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
import org.apache.yetus.audience.InterfaceStability;

/**
 * This is a {@link Tag} implementation in which value is backed by
 * {@link java.nio.ByteBuffer}
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ByteBufferTag implements Tag {

  private ByteBuffer buffer;
  private int offset, length;
  private byte type;

  public ByteBufferTag(ByteBuffer buffer, int offset, int length) {
    this.buffer = buffer;
    this.offset = offset;
    this.length = length;
    this.type = ByteBufferUtils.toByte(buffer, offset + TAG_LENGTH_SIZE);
  }

  @Override
  public byte getType() {
    return this.type;
  }

  @Override
  public int getValueOffset() {
    return this.offset + INFRASTRUCTURE_SIZE;
  }

  @Override
  public int getValueLength() {
    return this.length - INFRASTRUCTURE_SIZE;
  }

  @Override
  public boolean hasArray() {
    return false;
  }

  @Override
  public byte[] getValueArray() {
    throw new UnsupportedOperationException(
        "Tag is backed by an off heap buffer. Use getValueByteBuffer()");
  }

  @Override
  public ByteBuffer getValueByteBuffer() {
    return this.buffer;
  }

  @Override
  public String toString() {
    return "[Tag type : " + this.type + ", value : "
        + ByteBufferUtils.toStringBinary(buffer, getValueOffset(), getValueLength()) + "]";
  }
}
