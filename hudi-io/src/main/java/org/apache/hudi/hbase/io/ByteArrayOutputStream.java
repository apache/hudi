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

package org.apache.hudi.hbase.io;

import java.io.OutputStream;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.hudi.hbase.nio.ByteBuff;
import org.apache.hudi.hbase.util.ByteBufferUtils;
import org.apache.hudi.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Our own implementation of ByteArrayOutputStream where all methods are NOT synchronized and
 * supports writing ByteBuffer directly to it.
 */
@InterfaceAudience.Private
public class ByteArrayOutputStream extends OutputStream implements ByteBufferWriter {

  // Borrowed from openJDK:
  // http://grepcode.com/file/repository.grepcode.com/java/root/jdk/openjdk/8-b132/java/util/ArrayList.java#221
  private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

  private byte[] buf;
  private int pos = 0;

  public ByteArrayOutputStream() {
    this(32);
  }

  public ByteArrayOutputStream(int capacity) {
    this.buf = new byte[capacity];
  }

  @Override
  public void write(ByteBuffer b, int off, int len) {
    checkSizeAndGrow(len);
    ByteBufferUtils.copyFromBufferToArray(this.buf, b, off, this.pos, len);
    this.pos += len;
  }

  @Override
  public void writeInt(int i) {
    checkSizeAndGrow(Bytes.SIZEOF_INT);
    Bytes.putInt(this.buf, this.pos, i);
    this.pos += Bytes.SIZEOF_INT;
  }

  @Override
  public void write(int b) {
    checkSizeAndGrow(Bytes.SIZEOF_BYTE);
    buf[this.pos] = (byte) b;
    this.pos++;
  }

  @Override
  public void write(byte[] b, int off, int len) {
    checkSizeAndGrow(len);
    System.arraycopy(b, off, this.buf, this.pos, len);
    this.pos += len;
  }

  private void checkSizeAndGrow(int extra) {
    long capacityNeeded = this.pos + (long) extra;
    if (capacityNeeded > this.buf.length) {
      // guarantee it's possible to fit
      if (capacityNeeded > MAX_ARRAY_SIZE) {
        throw new BufferOverflowException();
      }
      // double until hit the cap
      long nextCapacity = Math.min(this.buf.length << 1, MAX_ARRAY_SIZE);
      // but make sure there is enough if twice the existing capacity is still too small
      nextCapacity = Math.max(nextCapacity, capacityNeeded);
      if (nextCapacity > MAX_ARRAY_SIZE) {
        throw new BufferOverflowException();
      }
      byte[] newBuf = new byte[(int) nextCapacity];
      System.arraycopy(buf, 0, newBuf, 0, buf.length);
      buf = newBuf;
    }
  }

  /**
   * Resets the <code>pos</code> field of this byte array output stream to zero. The output stream
   * can be used again.
   */
  public void reset() {
    this.pos = 0;
  }

  /**
   * Copies the content of this Stream into a new byte array.
   * @return  the contents of this output stream, as new byte array.
   */
  public byte[] toByteArray() {
    return Arrays.copyOf(buf, pos);
  }

  public void toByteBuff(ByteBuff buff) {
    buff.put(buf, 0, pos);
  }

  /**
   * @return the underlying array where the data gets accumulated
   */
  public byte[] getBuffer() {
    return this.buf;
  }

  /**
   * @return The current size of the buffer.
   */
  public int size() {
    return this.pos;
  }
}
