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

package org.apache.hudi.hbase.io.hfile.bucket;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

import org.apache.hudi.hbase.io.ByteBuffAllocator;
import org.apache.hudi.hbase.io.ByteBuffAllocator.Recycler;
import org.apache.hudi.hbase.io.hfile.BlockPriority;
import org.apache.hudi.hbase.io.hfile.Cacheable;
import org.apache.hudi.hbase.io.hfile.CacheableDeserializer;
import org.apache.hudi.hbase.io.hfile.CacheableDeserializerIdManager;
import org.apache.hudi.hbase.nio.ByteBuff;
import org.apache.hudi.hbase.nio.HBaseReferenceCounted;
import org.apache.hudi.hbase.nio.RefCnt;
import org.apache.hudi.hbase.util.IdReadWriteLock;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Item in cache. We expect this to be where most memory goes. Java uses 8 bytes just for object
 * headers; after this, we want to use as little as possible - so we only use 8 bytes, but in order
 * to do so we end up messing around with all this Java casting stuff. Offset stored as 5 bytes that
 * make up the long. Doubt we'll see devices this big for ages. Offsets are divided by 256. So 5
 * bytes gives us 256TB or so.
 */
@InterfaceAudience.Private
class BucketEntry implements HBaseReferenceCounted {
  // access counter comparator, descending order
  static final Comparator<BucketEntry> COMPARATOR =
      Comparator.comparingLong(BucketEntry::getAccessCounter).reversed();

  private int offsetBase;
  private int length;
  private byte offset1;

  /**
   * The index of the deserializer that can deserialize this BucketEntry content. See
   * {@link CacheableDeserializerIdManager} for hosting of index to serializers.
   */
  byte deserializerIndex;

  private volatile long accessCounter;
  private BlockPriority priority;

  /**
   * <pre>
   * The RefCnt means how many paths are referring the {@link BucketEntry}, there are two cases:
   * 1.If the {@link IOEngine#usesSharedMemory()} is false(eg.{@link FileIOEngine}),the refCnt is
   *   always 1 until this {@link BucketEntry} is evicted from {@link BucketCache#backingMap}.Even
   *   if the corresponding {@link HFileBlock} is referenced by RPC reading, the refCnt should not
   *   increase.
   *
   * 2.If the {@link IOEngine#usesSharedMemory()} is true(eg.{@link ByteBufferIOEngine}),each RPC
   *   reading path is considering as one path, the {@link BucketCache#backingMap} reference is
   *   also considered a path. NOTICE that if two read RPC path hit the same {@link BucketEntry},
   *   then the {@link HFileBlock}s the two RPC referred will share the same refCnt instance with
   *   the {@link BucketEntry},so the refCnt will increase or decrease as the following:
   *   (1) when writerThread flush the block into IOEngine and add the bucketEntry into backingMap,
   *       the refCnt ++;
   *   (2) If BucketCache evict the block and move the bucketEntry out of backingMap, the refCnt--;
   *       it usually happen when HFile is closing or someone call the clearBucketCache by force.
   *   (3) The read RPC path start to refer the block which is backend by the memory area in
   *       bucketEntry, then refCnt ++ ;
   *   (4) The read RPC patch shipped the response, and release the block. then refCnt--;
   *    Once the refCnt decrease to zero, then the {@link BucketAllocator} will free the block area.
   * </pre>
   */
  private final RefCnt refCnt;
  final AtomicBoolean markedAsEvicted;
  final ByteBuffAllocator allocator;

  /**
   * Time this block was cached. Presumes we are created just before we are added to the cache.
   */
  private final long cachedTime = System.nanoTime();

  /**
   * @param createRecycler used to free this {@link BucketEntry} when {@link BucketEntry#refCnt}
   *          becoming 0. NOTICE that {@link ByteBuffAllocator#NONE} could only be used for test.
   */
  BucketEntry(long offset, int length, long accessCounter, boolean inMemory,
              Function<BucketEntry, Recycler> createRecycler,
              ByteBuffAllocator allocator) {
    if (createRecycler == null) {
      throw new IllegalArgumentException("createRecycler could not be null!");
    }
    setOffset(offset);
    this.length = length;
    this.accessCounter = accessCounter;
    this.priority = inMemory ? BlockPriority.MEMORY : BlockPriority.MULTI;
    this.refCnt = RefCnt.create(createRecycler.apply(this));

    this.markedAsEvicted = new AtomicBoolean(false);
    this.allocator = allocator;
  }

  long offset() {
    // Java has no unsigned numbers, so this needs the L cast otherwise it will be sign extended
    // as a negative number.
    long o = ((long) offsetBase) & 0xFFFFFFFFL;
    // The 0xFF here does not need the L cast because it is treated as a positive int.
    o += (((long) (offset1)) & 0xFF) << 32;
    return o << 8;
  }

  private void setOffset(long value) {
    assert (value & 0xFF) == 0;
    value >>= 8;
    offsetBase = (int) value;
    offset1 = (byte) (value >> 32);
  }

  public int getLength() {
    return length;
  }

  CacheableDeserializer<Cacheable> deserializerReference() {
    return CacheableDeserializerIdManager.getDeserializer(deserializerIndex);
  }

  void setDeserializerReference(CacheableDeserializer<Cacheable> deserializer) {
    this.deserializerIndex = (byte) deserializer.getDeserializerIdentifier();
  }

  long getAccessCounter() {
    return accessCounter;
  }

  /**
   * Block has been accessed. Update its local access counter.
   */
  void access(long accessCounter) {
    this.accessCounter = accessCounter;
    if (this.priority == BlockPriority.SINGLE) {
      this.priority = BlockPriority.MULTI;
    }
  }

  public BlockPriority getPriority() {
    return this.priority;
  }

  long getCachedTime() {
    return cachedTime;
  }

  /**
   * The {@link BucketCache} will try to release its reference to this BucketEntry many times. we
   * must make sure the idempotent, otherwise it'll decrease the RPC's reference count in advance,
   * then for RPC memory leak happen.
   * @return true if we deallocate this entry successfully.
   */
  boolean markAsEvicted() {
    if (markedAsEvicted.compareAndSet(false, true)) {
      return this.release();
    }
    return false;
  }

  /**
   * Check whether have some RPC patch referring this block.<br/>
   * For {@link IOEngine#usesSharedMemory()} is true(eg.{@link ByteBufferIOEngine}), there're two
   * case: <br>
   * 1. If current refCnt is greater than 1, there must be at least one referring RPC path; <br>
   * 2. If current refCnt is equal to 1 and the markedAtEvicted is true, the it means backingMap has
   * released its reference, the remaining reference can only be from RPC path. <br>
   * We use this check to decide whether we can free the block area: when cached size exceed the
   * acceptable size, our eviction policy will choose those stale blocks without any RPC reference
   * and the RPC referred block will be excluded. <br/>
   * <br/>
   * For {@link IOEngine#usesSharedMemory()} is false(eg.{@link FileIOEngine}),
   * {@link BucketEntry#refCnt} is always 1 until it is evicted from {@link BucketCache#backingMap},
   * so {@link BucketEntry#isRpcRef()} is always return false.
   * @return true to indicate there're some RPC referring the block.
   */
  boolean isRpcRef() {
    boolean evicted = markedAsEvicted.get();
    return this.refCnt() > 1 || (evicted && refCnt() == 1);
  }

  Cacheable wrapAsCacheable(ByteBuffer[] buffers) throws IOException {
    return wrapAsCacheable(ByteBuff.wrap(buffers, this.refCnt));
  }

  Cacheable wrapAsCacheable(ByteBuff buf) throws IOException {
    return this.deserializerReference().deserialize(buf, allocator);
  }

  interface BucketEntryHandler<T> {
    T handle();
  }

  <T> T withWriteLock(IdReadWriteLock<Long> offsetLock, BucketEntryHandler<T> handler) {
    ReentrantReadWriteLock lock = offsetLock.getLock(this.offset());
    try {
      lock.writeLock().lock();
      return handler.handle();
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public int refCnt() {
    return this.refCnt.refCnt();
  }

  @Override
  public BucketEntry retain() {
    refCnt.retain();
    return this;
  }

  /**
   * We've three cases to release refCnt now: <br>
   * 1. BucketCache#evictBlock, it will release the backingMap's reference by force because we're
   * closing file or clear the bucket cache or some corruption happen. when all rpc references gone,
   * then free the area in bucketAllocator. <br>
   * 2. BucketCache#returnBlock . when rpc shipped, we'll release the block, only when backingMap
   * also release its refCnt (case.1 will do this) and no other rpc reference, then it will free the
   * area in bucketAllocator. <br>
   * 3.evict those block without any rpc reference if cache size exceeded. we'll only free those
   * blocks with zero rpc reference count, as the {@link BucketEntry#markStaleAsEvicted()} do.
   * @return true to indicate we've decreased to zero and do the de-allocation.
   */
  @Override
  public boolean release() {
    return refCnt.release();
  }
}
