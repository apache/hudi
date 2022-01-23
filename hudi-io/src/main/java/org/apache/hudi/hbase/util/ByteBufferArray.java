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

package org.apache.hudi.hbase.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;

import org.apache.hudi.hbase.nio.ByteBuff;
import org.apache.hadoop.util.StringUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class manages an array of ByteBuffers with a default size 4MB. These buffers are sequential
 * and could be considered as a large buffer.It supports reading/writing data from this large buffer
 * with a position and offset
 */
@InterfaceAudience.Private
public class ByteBufferArray {
  private static final Logger LOG = LoggerFactory.getLogger(ByteBufferArray.class);

  public static final int DEFAULT_BUFFER_SIZE = 4 * 1024 * 1024;
  private final int bufferSize;
  private final int bufferCount;
  final ByteBuffer[] buffers;

  /**
   * We allocate a number of byte buffers as the capacity.
   * @param capacity total size of the byte buffer array
   * @param allocator the ByteBufferAllocator that will create the buffers
   * @throws IOException throws IOException if there is an exception thrown by the allocator
   */
  public ByteBufferArray(long capacity, ByteBufferAllocator allocator) throws IOException {
    this(getBufferSize(capacity), getBufferCount(capacity),
        Runtime.getRuntime().availableProcessors(), capacity, allocator);
  }

  ByteBufferArray(int bufferSize, int bufferCount, int threadCount, long capacity,
                  ByteBufferAllocator alloc) throws IOException {
    this.bufferSize = bufferSize;
    this.bufferCount = bufferCount;
    LOG.info("Allocating buffers total={}, sizePerBuffer={}, count={}",
        StringUtils.byteDesc(capacity), StringUtils.byteDesc(bufferSize), bufferCount);
    this.buffers = new ByteBuffer[bufferCount];
    createBuffers(threadCount, alloc);
  }

  private void createBuffers(int threadCount, ByteBufferAllocator alloc) throws IOException {
    ExecutorService pool = Executors.newFixedThreadPool(threadCount);
    int perThreadCount = bufferCount / threadCount;
    int reminder = bufferCount % threadCount;
    try {
      List<Future<ByteBuffer[]>> futures = new ArrayList<>(threadCount);
      // Dispatch the creation task to each thread.
      for (int i = 0; i < threadCount; i++) {
        final int chunkSize = perThreadCount + ((i == threadCount - 1) ? reminder : 0);
        futures.add(pool.submit(() -> {
          ByteBuffer[] chunk = new ByteBuffer[chunkSize];
          for (int k = 0; k < chunkSize; k++) {
            chunk[k] = alloc.allocate(bufferSize);
          }
          return chunk;
        }));
      }
      // Append the buffers created by each thread.
      int bufferIndex = 0;
      try {
        for (Future<ByteBuffer[]> f : futures) {
          for (ByteBuffer b : f.get()) {
            this.buffers[bufferIndex++] = b;
          }
        }
        assert bufferIndex == bufferCount;
      } catch (Exception e) {
        LOG.error("Buffer creation interrupted", e);
        throw new IOException(e);
      }
    } finally {
      pool.shutdownNow();
    }
  }

  static int getBufferSize(long capacity) {
    int bufferSize = DEFAULT_BUFFER_SIZE;
    if (bufferSize > (capacity / 16)) {
      bufferSize = (int) roundUp(capacity / 16, 32768);
    }
    return bufferSize;
  }

  private static int getBufferCount(long capacity) {
    int bufferSize = getBufferSize(capacity);
    return (int) (roundUp(capacity, bufferSize) / bufferSize);
  }

  private static long roundUp(long n, long to) {
    return ((n + to - 1) / to) * to;
  }

  /**
   * Transfers bytes from this buffers array into the given destination {@link ByteBuff}
   * @param offset start position in this big logical array.
   * @param dst the destination ByteBuff. Notice that its position will be advanced.
   * @return number of bytes read
   */
  public int read(long offset, ByteBuff dst) {
    return internalTransfer(offset, dst, READER);
  }

  /**
   * Transfers bytes from the given source {@link ByteBuff} into this buffer array
   * @param offset start offset of this big logical array.
   * @param src the source ByteBuff. Notice that its position will be advanced.
   * @return number of bytes write
   */
  public int write(long offset, ByteBuff src) {
    return internalTransfer(offset, src, WRITER);
  }

  /**
   * Transfer bytes from source {@link ByteBuff} to destination {@link ByteBuffer}. Position of both
   * source and destination will be advanced.
   */
  private static final BiConsumer<ByteBuffer, ByteBuff> WRITER = (dst, src) -> {
    int off = src.position(), len = dst.remaining();
    src.get(dst, off, len);
    src.position(off + len);
  };

  /**
   * Transfer bytes from source {@link ByteBuffer} to destination {@link ByteBuff}, Position of both
   * source and destination will be advanced.
   */
  private static final BiConsumer<ByteBuffer, ByteBuff> READER = (src, dst) -> {
    int off = dst.position(), len = src.remaining(), srcOff = src.position();
    dst.put(off, ByteBuff.wrap(src), srcOff, len);
    src.position(srcOff + len);
    dst.position(off + len);
  };

  /**
   * Transferring all remaining bytes from b to the buffers array starting at offset, or
   * transferring bytes from the buffers array at offset to b until b is filled. Notice that
   * position of ByteBuff b will be advanced.
   * @param offset where we start in the big logical array.
   * @param b the ByteBuff to transfer from or to
   * @param transfer the transfer interface.
   * @return the length of bytes we transferred.
   */
  private int internalTransfer(long offset, ByteBuff b, BiConsumer<ByteBuffer, ByteBuff> transfer) {
    int expectedTransferLen = b.remaining();
    if (expectedTransferLen == 0) {
      return 0;
    }
    BufferIterator it = new BufferIterator(offset, expectedTransferLen);
    while (it.hasNext()) {
      ByteBuffer a = it.next();
      transfer.accept(a, b);
      assert !a.hasRemaining();
    }
    assert expectedTransferLen == it.getSum() : "Expected transfer length (=" + expectedTransferLen
        + ") don't match the actual transfer length(=" + it.getSum() + ")";
    return expectedTransferLen;
  }

  /**
   * Creates a sub-array from a given array of ByteBuffers from the given offset to the length
   * specified. For eg, if there are 4 buffers forming an array each with length 10 and if we call
   * asSubByteBuffers(5, 10) then we will create an sub-array consisting of two BBs and the first
   * one be a BB from 'position' 5 to a 'length' 5 and the 2nd BB will be from 'position' 0 to
   * 'length' 5.
   * @param offset the position in the whole array which is composited by multiple byte buffers.
   * @param len the length of bytes
   * @return the underlying ByteBuffers, each ByteBuffer is a slice from the backend and will have a
   *         zero position.
   */
  public ByteBuffer[] asSubByteBuffers(long offset, final int len) {
    BufferIterator it = new BufferIterator(offset, len);
    ByteBuffer[] mbb = new ByteBuffer[it.getBufferCount()];
    for (int i = 0; i < mbb.length; i++) {
      assert it.hasNext();
      mbb[i] = it.next();
    }
    assert it.getSum() == len;
    return mbb;
  }

  /**
   * Iterator to fetch ByteBuffers from offset with given length in this big logical array.
   */
  private class BufferIterator implements Iterator<ByteBuffer> {
    private final int len;
    private int startBuffer, startOffset, endBuffer, endOffset;
    private int curIndex, sum = 0;

    private int index(long pos) {
      return (int) (pos / bufferSize);
    }

    private int offset(long pos) {
      return (int) (pos % bufferSize);
    }

    public BufferIterator(long offset, int len) {
      assert len >= 0 && offset >= 0;
      this.len = len;

      this.startBuffer = index(offset);
      this.startOffset = offset(offset);

      this.endBuffer = index(offset + len);
      this.endOffset = offset(offset + len);
      if (startBuffer < endBuffer && endOffset == 0) {
        endBuffer--;
        endOffset = bufferSize;
      }
      assert startBuffer >= 0 && startBuffer < bufferCount;
      assert endBuffer >= 0 && endBuffer < bufferCount;

      // initialize the index to the first buffer index.
      this.curIndex = startBuffer;
    }

    @Override
    public boolean hasNext() {
      return this.curIndex <= endBuffer;
    }

    /**
     * The returned ByteBuffer is an sliced one, it won't affect the position or limit of the
     * original one.
     */
    @Override
    public ByteBuffer next() {
      ByteBuffer bb = buffers[curIndex].duplicate();
      if (curIndex == startBuffer) {
        bb.position(startOffset).limit(Math.min(bufferSize, startOffset + len));
      } else if (curIndex == endBuffer) {
        bb.position(0).limit(endOffset);
      } else {
        bb.position(0).limit(bufferSize);
      }
      curIndex++;
      sum += bb.remaining();
      // Make sure that its pos is zero, it's important because MBB will count from zero for all nio
      // ByteBuffers.
      return bb.slice();
    }

    int getSum() {
      return sum;
    }

    int getBufferCount() {
      return this.endBuffer - this.startBuffer + 1;
    }
  }
}
