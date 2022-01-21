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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.hbase.HConstants;
import org.apache.hudi.hbase.nio.ByteBuff;
import org.apache.hudi.hbase.nio.SingleByteBuff;
import org.apache.hudi.hbase.util.ReflectionUtils;

import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.nio.ch.DirectBuffer;

import org.apache.hbase.thirdparty.com.google.common.collect.Sets;

/**
 * ByteBuffAllocator is used for allocating/freeing the ByteBuffers from/to NIO ByteBuffer pool, and
 * it provide high-level interfaces for upstream. when allocating desired memory size, it will
 * return {@link ByteBuff}, if we are sure that those ByteBuffers have reached the end of life
 * cycle, we must do the {@link ByteBuff#release()} to return back the buffers to the pool,
 * otherwise ByteBuffers leak will happen, and the NIO ByteBuffer pool may be exhausted. there's
 * possible that the desired memory size is large than ByteBufferPool has, we'll downgrade to
 * allocate ByteBuffers from heap which meaning the GC pressure may increase again. Of course, an
 * better way is increasing the ByteBufferPool size if we detected this case. <br/>
 * <br/>
 * On the other hand, for better memory utilization, we have set an lower bound named
 * minSizeForReservoirUse in this allocator, and if the desired size is less than
 * minSizeForReservoirUse, the allocator will just allocate the ByteBuffer from heap and let the JVM
 * free its memory, because it's too wasting to allocate a single fixed-size ByteBuffer for some
 * small objects. <br/>
 * <br/>
 * We recommend to use this class to allocate/free {@link ByteBuff} in the RPC layer or the entire
 * read/write path, because it hide the details of memory management and its APIs are more friendly
 * to the upper layer.
 */
@InterfaceAudience.Private
public class ByteBuffAllocator {

  private static final Logger LOG = LoggerFactory.getLogger(ByteBuffAllocator.class);

  // The on-heap allocator is mostly used for testing, but also some non-test usage, such as
  // scanning snapshot, we won't have an RpcServer to initialize the allocator, so just use the
  // default heap allocator, it will just allocate ByteBuffers from heap but wrapped by an ByteBuff.
  public static final ByteBuffAllocator HEAP = ByteBuffAllocator.createOnHeap();

  public static final String ALLOCATOR_POOL_ENABLED_KEY = "hbase.server.allocator.pool.enabled";

  public static final String MAX_BUFFER_COUNT_KEY = "hbase.server.allocator.max.buffer.count";

  public static final String BUFFER_SIZE_KEY = "hbase.server.allocator.buffer.size";

  public static final String MIN_ALLOCATE_SIZE_KEY = "hbase.server.allocator.minimal.allocate.size";

  /**
   * Set an alternate bytebuffallocator by setting this config,
   * e.g. we can config {@link DeallocateRewriteByteBuffAllocator} to find out
   * prematurely release issues
   */
  public static final String BYTEBUFF_ALLOCATOR_CLASS = "hbase.bytebuff.allocator.class";

  /**
   * @deprecated since 2.3.0 and will be removed in 4.0.0. Use
   *   {@link ByteBuffAllocator#ALLOCATOR_POOL_ENABLED_KEY} instead.
   */
  @Deprecated
  public static final String DEPRECATED_ALLOCATOR_POOL_ENABLED_KEY =
      "hbase.ipc.server.reservoir.enabled";

  /**
   * @deprecated since 2.3.0 and will be removed in 4.0.0. Use
   *   {@link ByteBuffAllocator#MAX_BUFFER_COUNT_KEY} instead.
   */
  @Deprecated
  static final String DEPRECATED_MAX_BUFFER_COUNT_KEY = "hbase.ipc.server.reservoir.initial.max";

  /**
   * @deprecated since 2.3.0 and will be removed in 4.0.0. Use
   *   {@link ByteBuffAllocator#BUFFER_SIZE_KEY} instead.
   */
  @Deprecated
  static final String DEPRECATED_BUFFER_SIZE_KEY = "hbase.ipc.server.reservoir.initial.buffer.size";

  /**
   * The hbase.ipc.server.reservoir.initial.max and hbase.ipc.server.reservoir.initial.buffer.size
   * were introduced in HBase2.0.0, while in HBase3.0.0 the two config keys will be replaced by
   * {@link ByteBuffAllocator#MAX_BUFFER_COUNT_KEY} and {@link ByteBuffAllocator#BUFFER_SIZE_KEY}.
   * Also the hbase.ipc.server.reservoir.enabled will be replaced by
   * hbase.server.allocator.pool.enabled. Keep the three old config keys here for HBase2.x
   * compatibility.
   */
  static {
    Configuration.addDeprecation(DEPRECATED_ALLOCATOR_POOL_ENABLED_KEY, ALLOCATOR_POOL_ENABLED_KEY);
    Configuration.addDeprecation(DEPRECATED_MAX_BUFFER_COUNT_KEY, MAX_BUFFER_COUNT_KEY);
    Configuration.addDeprecation(DEPRECATED_BUFFER_SIZE_KEY, BUFFER_SIZE_KEY);
  }

  /**
   * There're some reasons why better to choose 65KB(rather than 64KB) as the default buffer size:
   * <p>
   * 1. Almost all of the data blocks have the block size: 64KB + delta, whose delta is very small,
   * depends on the size of lastKeyValue. If we set buffer.size=64KB, then each block will be
   * allocated as a MultiByteBuff: one 64KB DirectByteBuffer and delta bytes HeapByteBuffer, the
   * HeapByteBuffer will increase the GC pressure. Ideally, we should let the data block to be
   * allocated as a SingleByteBuff, it has simpler data structure, faster access speed, less heap
   * usage.
   * <p>
   * 2. Since the blocks are MultiByteBuff when using buffer.size=64KB, so we have to calculate the
   * checksum by an temp heap copying (see HBASE-21917), while if it's a SingleByteBuff, we can
   * speed the checksum by calling the hadoop' checksum in native lib, which is more faster.
   * <p>
   * For performance comparison, please see HBASE-22483.
   */
  public static final int DEFAULT_BUFFER_SIZE = 65 * 1024;

  public static final Recycler NONE = () -> {
  };

  public interface Recycler {
    void free();
  }

  protected final boolean reservoirEnabled;
  protected final int bufSize;
  private final int maxBufCount;
  private final AtomicInteger usedBufCount = new AtomicInteger(0);

  private boolean maxPoolSizeInfoLevelLogged = false;

  // If the desired size is at least this size, it'll allocated from ByteBufferPool, otherwise it'll
  // allocated from heap for better utilization. We make this to be 1/6th of the pool buffer size.
  private final int minSizeForReservoirUse;

  private final Queue<ByteBuffer> buffers = new ConcurrentLinkedQueue<>();

  // Metrics to track the pool allocation bytes and heap allocation bytes. If heap allocation
  // bytes is increasing so much, then we may need to increase the max.buffer.count .
  private final LongAdder poolAllocationBytes = new LongAdder();
  private final LongAdder heapAllocationBytes = new LongAdder();
  private long lastPoolAllocationBytes = 0;
  private long lastHeapAllocationBytes = 0;

  /**
   * Initialize an {@link ByteBuffAllocator} which will try to allocate ByteBuffers from off-heap if
   * reservoir is enabled and the reservoir has enough buffers, otherwise the allocator will just
   * allocate the insufficient buffers from on-heap to meet the requirement.
   * @param conf which get the arguments to initialize the allocator.
   * @param reservoirEnabled indicate whether the reservoir is enabled or disabled. NOTICE: if
   *          reservoir is enabled, then we will use the pool allocator to allocate off-heap
   *          ByteBuffers and use the HEAP allocator to allocate heap ByteBuffers. Otherwise if
   *          reservoir is disabled then all allocations will happen in HEAP instance.
   * @return ByteBuffAllocator to manage the byte buffers.
   */
  public static ByteBuffAllocator create(Configuration conf, boolean reservoirEnabled) {
    if (conf.get(DEPRECATED_BUFFER_SIZE_KEY) != null
        || conf.get(DEPRECATED_MAX_BUFFER_COUNT_KEY) != null) {
      LOG.warn("The config keys {} and {} are deprecated now, instead please use {} and {}. In "
              + "future release we will remove the two deprecated configs.",
          DEPRECATED_BUFFER_SIZE_KEY, DEPRECATED_MAX_BUFFER_COUNT_KEY, BUFFER_SIZE_KEY,
          MAX_BUFFER_COUNT_KEY);
    }
    int poolBufSize = conf.getInt(BUFFER_SIZE_KEY, DEFAULT_BUFFER_SIZE);
    if (reservoirEnabled) {
      // The max number of buffers to be pooled in the ByteBufferPool. The default value been
      // selected based on the #handlers configured. When it is read request, 2 MB is the max size
      // at which we will send back one RPC request. Means max we need 2 MB for creating the
      // response cell block. (Well it might be much lesser than this because in 2 MB size calc, we
      // include the heap size overhead of each cells also.) Considering 2 MB, we will need
      // (2 * 1024 * 1024) / poolBufSize buffers to make the response cell block. Pool buffer size
      // is by default 64 KB.
      // In case of read request, at the end of the handler process, we will make the response
      // cellblock and add the Call to connection's response Q and a single Responder thread takes
      // connections and responses from that one by one and do the socket write. So there is chances
      // that by the time a handler originated response is actually done writing to socket and so
      // released the BBs it used, the handler might have processed one more read req. On an avg 2x
      // we consider and consider that also for the max buffers to pool
      int bufsForTwoMB = (2 * 1024 * 1024) / poolBufSize;
      int maxBuffCount =
          conf.getInt(MAX_BUFFER_COUNT_KEY, conf.getInt(HConstants.REGION_SERVER_HANDLER_COUNT,
              HConstants.DEFAULT_REGION_SERVER_HANDLER_COUNT) * bufsForTwoMB * 2);
      int minSizeForReservoirUse = conf.getInt(MIN_ALLOCATE_SIZE_KEY, poolBufSize / 6);
      Class<?> clazz = conf.getClass(BYTEBUFF_ALLOCATOR_CLASS, ByteBuffAllocator.class);
      return (ByteBuffAllocator) ReflectionUtils
          .newInstance(clazz, true, maxBuffCount, poolBufSize, minSizeForReservoirUse);
    } else {
      return HEAP;
    }
  }

  /**
   * Initialize an {@link ByteBuffAllocator} which only allocate ByteBuffer from on-heap, it's
   * designed for testing purpose or disabled reservoir case.
   * @return allocator to allocate on-heap ByteBuffer.
   */
  private static ByteBuffAllocator createOnHeap() {
    return new ByteBuffAllocator(false, 0, DEFAULT_BUFFER_SIZE, Integer.MAX_VALUE);
  }

  protected ByteBuffAllocator(boolean reservoirEnabled, int maxBufCount, int bufSize,
                              int minSizeForReservoirUse) {
    this.reservoirEnabled = reservoirEnabled;
    this.maxBufCount = maxBufCount;
    this.bufSize = bufSize;
    this.minSizeForReservoirUse = minSizeForReservoirUse;
  }

  public boolean isReservoirEnabled() {
    return reservoirEnabled;
  }

  public long getHeapAllocationBytes() {
    return heapAllocationBytes.sum();
  }

  public long getPoolAllocationBytes() {
    return poolAllocationBytes.sum();
  }

  public int getBufferSize() {
    return this.bufSize;
  }

  public int getUsedBufferCount() {
    return this.usedBufCount.intValue();
  }

  /**
   * The {@link ConcurrentLinkedQueue#size()} is O(N) complexity and time-consuming, so DO NOT use
   * the method except in UT.
   */
  public int getFreeBufferCount() {
    return this.buffers.size();
  }

  public int getTotalBufferCount() {
    return maxBufCount;
  }

  public static long getHeapAllocationBytes(ByteBuffAllocator... allocators) {
    long heapAllocBytes = 0;
    for (ByteBuffAllocator alloc : Sets.newHashSet(allocators)) {
      heapAllocBytes += alloc.getHeapAllocationBytes();
    }
    return heapAllocBytes;
  }

  public static double getHeapAllocationRatio(ByteBuffAllocator... allocators) {
    double heapDelta = 0.0, poolDelta = 0.0;
    long heapAllocBytes, poolAllocBytes;
    // If disabled the pool allocator, then we use the global HEAP allocator. otherwise we use
    // the pool allocator to allocate offheap ByteBuffers and use the HEAP to allocate heap
    // ByteBuffers. So here we use a HashSet to remove the duplicated allocator object in disable
    // case.
    for (ByteBuffAllocator alloc : Sets.newHashSet(allocators)) {
      heapAllocBytes = alloc.heapAllocationBytes.sum();
      poolAllocBytes = alloc.poolAllocationBytes.sum();
      heapDelta += (heapAllocBytes - alloc.lastHeapAllocationBytes);
      poolDelta += (poolAllocBytes - alloc.lastPoolAllocationBytes);
      alloc.lastHeapAllocationBytes = heapAllocBytes;
      alloc.lastPoolAllocationBytes = poolAllocBytes;
    }
    // Calculate the heap allocation ratio.
    if (Math.abs(heapDelta + poolDelta) < 1e-3) {
      return 0.0;
    }
    return heapDelta / (heapDelta + poolDelta);
  }

  /**
   * Allocate an buffer with buffer size from ByteBuffAllocator, Note to call the
   * {@link ByteBuff#release()} if no need any more, otherwise the memory leak happen in NIO
   * ByteBuffer pool.
   * @return an ByteBuff with the buffer size.
   */
  public SingleByteBuff allocateOneBuffer() {
    if (isReservoirEnabled()) {
      ByteBuffer bb = getBuffer();
      if (bb != null) {
        return new SingleByteBuff(() -> putbackBuffer(bb), bb);
      }
    }
    // Allocated from heap, let the JVM free its memory.
    return (SingleByteBuff) ByteBuff.wrap(allocateOnHeap(bufSize));
  }

  private ByteBuffer allocateOnHeap(int size) {
    heapAllocationBytes.add(size);
    return ByteBuffer.allocate(size);
  }

  /**
   * Allocate size bytes from the ByteBufAllocator, Note to call the {@link ByteBuff#release()} if
   * no need any more, otherwise the memory leak happen in NIO ByteBuffer pool.
   * @param size to allocate
   * @return an ByteBuff with the desired size.
   */
  public ByteBuff allocate(int size) {
    if (size < 0) {
      throw new IllegalArgumentException("size to allocate should >=0");
    }
    // If disabled the reservoir, just allocate it from on-heap.
    if (!isReservoirEnabled() || size == 0) {
      return ByteBuff.wrap(allocateOnHeap(size));
    }
    int reminder = size % bufSize;
    int len = size / bufSize + (reminder > 0 ? 1 : 0);
    List<ByteBuffer> bbs = new ArrayList<>(len);
    // Allocate from ByteBufferPool until the remaining is less than minSizeForReservoirUse or
    // reservoir is exhausted.
    int remain = size;
    while (remain >= minSizeForReservoirUse) {
      ByteBuffer bb = this.getBuffer();
      if (bb == null) {
        break;
      }
      bbs.add(bb);
      remain -= bufSize;
    }
    int lenFromReservoir = bbs.size();
    if (remain > 0) {
      // If the last ByteBuffer is too small or the reservoir can not provide more ByteBuffers, we
      // just allocate the ByteBuffer from on-heap.
      bbs.add(allocateOnHeap(remain));
    }
    ByteBuff bb = ByteBuff.wrap(bbs, () -> {
      for (int i = 0; i < lenFromReservoir; i++) {
        this.putbackBuffer(bbs.get(i));
      }
    });
    bb.limit(size);
    return bb;
  }

  /**
   * Free all direct buffers if allocated, mainly used for testing.
   */
  public void clean() {
    while (!buffers.isEmpty()) {
      ByteBuffer b = buffers.poll();
      if (b instanceof DirectBuffer) {
        DirectBuffer db = (DirectBuffer) b;
        if (db.cleaner() != null) {
          db.cleaner().clean();
        }
      }
    }
    this.usedBufCount.set(0);
    this.maxPoolSizeInfoLevelLogged = false;
    this.poolAllocationBytes.reset();
    this.heapAllocationBytes.reset();
    this.lastPoolAllocationBytes = 0;
    this.lastHeapAllocationBytes = 0;
  }

  /**
   * @return One free DirectByteBuffer from the pool. If no free ByteBuffer and we have not reached
   *         the maximum pool size, it will create a new one and return. In case of max pool size
   *         also reached, will return null. When pool returned a ByteBuffer, make sure to return it
   *         back to pool after use.
   */
  private ByteBuffer getBuffer() {
    ByteBuffer bb = buffers.poll();
    if (bb != null) {
      // To reset the limit to capacity and position to 0, must clear here.
      bb.clear();
      poolAllocationBytes.add(bufSize);
      return bb;
    }
    while (true) {
      int c = this.usedBufCount.intValue();
      if (c >= this.maxBufCount) {
        if (!maxPoolSizeInfoLevelLogged) {
          LOG.info("Pool already reached its max capacity : {} and no free buffers now. Consider "
                  + "increasing the value for '{}' ?",
              maxBufCount, MAX_BUFFER_COUNT_KEY);
          maxPoolSizeInfoLevelLogged = true;
        }
        return null;
      }
      if (!this.usedBufCount.compareAndSet(c, c + 1)) {
        continue;
      }
      poolAllocationBytes.add(bufSize);
      return ByteBuffer.allocateDirect(bufSize);
    }
  }

  /**
   * Return back a ByteBuffer after its use. Don't read/write the ByteBuffer after the returning.
   * @param buf ByteBuffer to return.
   */
  protected void putbackBuffer(ByteBuffer buf) {
    if (buf.capacity() != bufSize || (reservoirEnabled ^ buf.isDirect())) {
      LOG.warn("Trying to put a buffer, not created by this pool! Will be just ignored");
      return;
    }
    buffers.offer(buf);
  }
}
