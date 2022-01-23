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

import java.util.Comparator;
import java.util.Map;

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hudi.hbase.io.hfile.BlockCacheKey;

import org.apache.hbase.thirdparty.com.google.common.collect.MinMaxPriorityQueue;

/**
 * A memory-bound queue that will grow until an element brings total size larger
 * than maxSize. From then on, only entries that are sorted larger than the
 * smallest current entry will be inserted/replaced.
 *
 * <p>
 * Use this when you want to find the largest elements (according to their
 * ordering, not their heap size) that consume as close to the specified maxSize
 * as possible. Default behavior is to grow just above rather than just below
 * specified max.
 */
@InterfaceAudience.Private
public class CachedEntryQueue {

  private static final Comparator<Map.Entry<BlockCacheKey, BucketEntry>> COMPARATOR =
      (a, b) -> BucketEntry.COMPARATOR.compare(a.getValue(), b.getValue());

  private MinMaxPriorityQueue<Map.Entry<BlockCacheKey, BucketEntry>> queue;

  private long cacheSize;
  private long maxSize;

  /**
   * @param maxSize the target size of elements in the queue
   * @param blockSize expected average size of blocks
   */
  public CachedEntryQueue(long maxSize, long blockSize) {
    int initialSize = (int) (maxSize / blockSize);
    if (initialSize == 0) {
      initialSize++;
    }
    queue = MinMaxPriorityQueue.orderedBy(COMPARATOR).expectedSize(initialSize).create();
    cacheSize = 0;
    this.maxSize = maxSize;
  }

  /**
   * Attempt to add the specified entry to this queue.
   * <p>
   * If the queue is smaller than the max size, or if the specified element is
   * ordered after the smallest element in the queue, the element will be added
   * to the queue. Otherwise, there is no side effect of this call.
   * @param entry a bucket entry with key to try to add to the queue
   */
  public void add(Map.Entry<BlockCacheKey, BucketEntry> entry) {
    if (cacheSize < maxSize) {
      queue.add(entry);
      cacheSize += entry.getValue().getLength();
    } else {
      BucketEntry head = queue.peek().getValue();
      if (BucketEntry.COMPARATOR.compare(entry.getValue(), head) > 0) {
        cacheSize += entry.getValue().getLength();
        cacheSize -= head.getLength();
        if (cacheSize > maxSize) {
          queue.poll();
        } else {
          cacheSize += head.getLength();
        }
        queue.add(entry);
      }
    }
  }

  /**
   * @return The next element in this queue, or {@code null} if the queue is
   *         empty.
   */
  public Map.Entry<BlockCacheKey, BucketEntry> poll() {
    return queue.poll();
  }

  /**
   * @return The last element in this queue, or {@code null} if the queue is
   *         empty.
   */
  public Map.Entry<BlockCacheKey, BucketEntry> pollLast() {
    return queue.pollLast();
  }
}
