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

package org.apache.hudi.hbase.nio;

import static org.apache.hudi.hbase.io.ByteBuffAllocator.NONE;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;

import org.apache.hudi.hbase.io.ByteBuffAllocator.Recycler;
import org.apache.hudi.hbase.util.ByteBufferUtils;
import org.apache.hudi.hbase.util.ObjectIntPair;
import org.apache.hudi.hbase.util.UnsafeAccess;
import org.apache.hudi.hbase.util.UnsafeAvailChecker;

import org.apache.yetus.audience.InterfaceAudience;
import sun.nio.ch.DirectBuffer;

/**
 * An implementation of ByteBuff where a single BB backs the BBI. This just acts as a wrapper over a
 * normal BB - offheap or onheap
 */
@InterfaceAudience.Private
public class SingleByteBuff extends ByteBuff {

  private static final boolean UNSAFE_AVAIL = UnsafeAvailChecker.isAvailable();
  private static final boolean UNSAFE_UNALIGNED = UnsafeAvailChecker.unaligned();

  // Underlying BB
  private final ByteBuffer buf;

  // To access primitive values from underlying ByteBuffer using Unsafe
  private long unsafeOffset;
  private Object unsafeRef = null;

  public SingleByteBuff(ByteBuffer buf) {
    this(NONE, buf);
  }

  public SingleByteBuff(Recycler recycler, ByteBuffer buf) {
    this(new RefCnt(recycler), buf);
  }

  SingleByteBuff(RefCnt refCnt, ByteBuffer buf) {
    this.refCnt = refCnt;
    this.buf = buf;
    if (buf.hasArray()) {
      this.unsafeOffset = UnsafeAccess.BYTE_ARRAY_BASE_OFFSET + buf.arrayOffset();
      this.unsafeRef = buf.array();
    } else {
      this.unsafeOffset = ((DirectBuffer) buf).address();
    }
  }

  @Override
  public int position() {
    checkRefCount();
    return this.buf.position();
  }

  @Override
  public SingleByteBuff position(int position) {
    checkRefCount();
    this.buf.position(position);
    return this;
  }

  @Override
  public SingleByteBuff skip(int len) {
    checkRefCount();
    this.buf.position(this.buf.position() + len);
    return this;
  }

  @Override
  public SingleByteBuff moveBack(int len) {
    checkRefCount();
    this.buf.position(this.buf.position() - len);
    return this;
  }

  @Override
  public int capacity() {
    checkRefCount();
    return this.buf.capacity();
  }

  @Override
  public int limit() {
    checkRefCount();
    return this.buf.limit();
  }

  @Override
  public SingleByteBuff limit(int limit) {
    checkRefCount();
    this.buf.limit(limit);
    return this;
  }

  @Override
  public SingleByteBuff rewind() {
    checkRefCount();
    this.buf.rewind();
    return this;
  }

  @Override
  public SingleByteBuff mark() {
    checkRefCount();
    this.buf.mark();
    return this;
  }

  @Override
  public ByteBuffer asSubByteBuffer(int length) {
    checkRefCount();
    // Just return the single BB that is available
    return this.buf;
  }

  @Override
  public void asSubByteBuffer(int offset, int length, ObjectIntPair<ByteBuffer> pair) {
    checkRefCount();
    // Just return the single BB that is available
    pair.setFirst(this.buf);
    pair.setSecond(offset);
  }

  @Override
  public int remaining() {
    checkRefCount();
    return this.buf.remaining();
  }

  @Override
  public boolean hasRemaining() {
    checkRefCount();
    return buf.hasRemaining();
  }

  @Override
  public SingleByteBuff reset() {
    checkRefCount();
    this.buf.reset();
    return this;
  }

  @Override
  public SingleByteBuff slice() {
    checkRefCount();
    return new SingleByteBuff(this.refCnt, this.buf.slice());
  }

  @Override
  public SingleByteBuff duplicate() {
    checkRefCount();
    return new SingleByteBuff(this.refCnt, this.buf.duplicate());
  }

  @Override
  public byte get() {
    checkRefCount();
    return buf.get();
  }

  @Override
  public byte get(int index) {
    checkRefCount();
    if (UNSAFE_AVAIL) {
      return UnsafeAccess.toByte(this.unsafeRef, this.unsafeOffset + index);
    }
    return this.buf.get(index);
  }

  @Override
  public byte getByteAfterPosition(int offset) {
    checkRefCount();
    return get(this.buf.position() + offset);
  }

  @Override
  public SingleByteBuff put(byte b) {
    checkRefCount();
    this.buf.put(b);
    return this;
  }

  @Override
  public SingleByteBuff put(int index, byte b) {
    checkRefCount();
    buf.put(index, b);
    return this;
  }

  @Override
  public void get(byte[] dst, int offset, int length) {
    checkRefCount();
    ByteBufferUtils.copyFromBufferToArray(dst, buf, buf.position(), offset, length);
    buf.position(buf.position() + length);
  }

  @Override
  public void get(int sourceOffset, byte[] dst, int offset, int length) {
    checkRefCount();
    ByteBufferUtils.copyFromBufferToArray(dst, buf, sourceOffset, offset, length);
  }

  @Override
  public void get(byte[] dst) {
    get(dst, 0, dst.length);
  }

  @Override
  public SingleByteBuff put(int offset, ByteBuff src, int srcOffset, int length) {
    checkRefCount();
    if (src instanceof SingleByteBuff) {
      ByteBufferUtils.copyFromBufferToBuffer(((SingleByteBuff) src).buf, this.buf, srcOffset,
          offset, length);
    } else {
      // TODO we can do some optimization here? Call to asSubByteBuffer might
      // create a copy.
      ObjectIntPair<ByteBuffer> pair = new ObjectIntPair<>();
      src.asSubByteBuffer(srcOffset, length, pair);
      if (pair.getFirst() != null) {
        ByteBufferUtils.copyFromBufferToBuffer(pair.getFirst(), this.buf, pair.getSecond(), offset,
            length);
      }
    }
    return this;
  }

  @Override
  public SingleByteBuff put(byte[] src, int offset, int length) {
    checkRefCount();
    ByteBufferUtils.copyFromArrayToBuffer(this.buf, src, offset, length);
    return this;
  }

  @Override
  public SingleByteBuff put(byte[] src) {
    checkRefCount();
    return put(src, 0, src.length);
  }

  @Override
  public boolean hasArray() {
    checkRefCount();
    return this.buf.hasArray();
  }

  @Override
  public byte[] array() {
    checkRefCount();
    return this.buf.array();
  }

  @Override
  public int arrayOffset() {
    checkRefCount();
    return this.buf.arrayOffset();
  }

  @Override
  public short getShort() {
    checkRefCount();
    return this.buf.getShort();
  }

  @Override
  public short getShort(int index) {
    checkRefCount();
    if (UNSAFE_UNALIGNED) {
      return UnsafeAccess.toShort(unsafeRef, unsafeOffset + index);
    }
    return this.buf.getShort(index);
  }

  @Override
  public short getShortAfterPosition(int offset) {
    checkRefCount();
    return getShort(this.buf.position() + offset);
  }

  @Override
  public int getInt() {
    checkRefCount();
    return this.buf.getInt();
  }

  @Override
  public SingleByteBuff putInt(int value) {
    checkRefCount();
    ByteBufferUtils.putInt(this.buf, value);
    return this;
  }

  @Override
  public int getInt(int index) {
    checkRefCount();
    if (UNSAFE_UNALIGNED) {
      return UnsafeAccess.toInt(unsafeRef, unsafeOffset + index);
    }
    return this.buf.getInt(index);
  }

  @Override
  public int getIntAfterPosition(int offset) {
    checkRefCount();
    return getInt(this.buf.position() + offset);
  }

  @Override
  public long getLong() {
    checkRefCount();
    return this.buf.getLong();
  }

  @Override
  public SingleByteBuff putLong(long value) {
    checkRefCount();
    ByteBufferUtils.putLong(this.buf, value);
    return this;
  }

  @Override
  public long getLong(int index) {
    checkRefCount();
    if (UNSAFE_UNALIGNED) {
      return UnsafeAccess.toLong(unsafeRef, unsafeOffset + index);
    }
    return this.buf.getLong(index);
  }

  @Override
  public long getLongAfterPosition(int offset) {
    checkRefCount();
    return getLong(this.buf.position() + offset);
  }

  @Override
  public byte[] toBytes(int offset, int length) {
    checkRefCount();
    byte[] output = new byte[length];
    ByteBufferUtils.copyFromBufferToArray(output, buf, offset, 0, length);
    return output;
  }

  @Override
  public void get(ByteBuffer out, int sourceOffset, int length) {
    checkRefCount();
    ByteBufferUtils.copyFromBufferToBuffer(buf, out, sourceOffset, length);
  }

  @Override
  public int read(ReadableByteChannel channel) throws IOException {
    checkRefCount();
    return read(channel, buf, 0, CHANNEL_READER);
  }

  @Override
  public int read(FileChannel channel, long offset) throws IOException {
    checkRefCount();
    return read(channel, buf, offset, FILE_READER);
  }

  @Override
  public int write(FileChannel channel, long offset) throws IOException {
    checkRefCount();
    int total = 0;
    while(buf.hasRemaining()) {
      int len = channel.write(buf, offset);
      total += len;
      offset += len;
    }
    return total;
  }

  @Override
  public ByteBuffer[] nioByteBuffers() {
    checkRefCount();
    return new ByteBuffer[] { this.buf };
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof SingleByteBuff)) {
      return false;
    }
    return this.buf.equals(((SingleByteBuff) obj).buf);
  }

  @Override
  public int hashCode() {
    return this.buf.hashCode();
  }

  @Override
  public SingleByteBuff retain() {
    refCnt.retain();
    return this;
  }
}
