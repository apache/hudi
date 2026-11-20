/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.utilities.unstructured.memory;

import com.sun.management.GarbageCollectionNotificationInfo;

import javax.management.Notification;
import javax.management.NotificationEmitter;
import javax.management.NotificationListener;
import javax.management.openmbean.CompositeData;

import java.io.PrintStream;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.MemoryUsage;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * The heap a memory cell's child holds after its garbage collections, a measure of its live heap (the sum of the heap pools'
 * peaks, {@code HEAP_POOL_PEAKS_SUM_BYTES}, counts garbage too, so it cannot tell a heap that grows with the input from one
 * that does not). Installed in the child, it listens to every garbage collector's notifications and keeps, over the
 * collections, the largest sum of the heap pools' used bytes after the collection; separately, the same over the full
 * collections only (after a full collection the used heap is the live heap). {@link #print} writes:
 * <ul>
 *   <li>{@code POST_GC_HEAP_PEAK_BYTES <n>} and {@code POST_GC_COUNT <n>}: over every collection;</li>
 *   <li>{@code POST_FULL_GC_HEAP_PEAK_BYTES <n>} and {@code POST_FULL_GC_COUNT <n>}: over the full collections.</li>
 * </ul>
 * After a young collection the old generation still holds whatever garbage was promoted since the last old collection, so the
 * first figure can grow with the run's length alone. With {@code -D}{@value #SAMPLE_PROPERTY}{@code =<ms>} the child also asks
 * for a full collection ({@link System#gc}) every that many milliseconds, so the full-collection figure samples the live heap
 * all along the round.
 */
final class PostGcHeap {

  /** The child property naming the period, in milliseconds, of the sampling full collections (none when absent). */
  static final String SAMPLE_PROPERTY = "memory.cell.gc.sample.ms";
  private static final String FULL_GC_ACTION = "end of major GC";

  private final Set<String> heapPools = new HashSet<>();
  private long peak;
  private long count;
  private long fullPeak;
  private long fullCount;

  private PostGcHeap() {
    for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
      if (pool.getType() == MemoryType.HEAP) {
        heapPools.add(pool.getName());
      }
    }
  }

  /** Listens to every garbage collector of this JVM from now on, and starts the sampling collections if asked for. */
  static PostGcHeap install() {
    PostGcHeap heap = new PostGcHeap();
    NotificationListener listener = heap::onNotification;
    for (GarbageCollectorMXBean collector : ManagementFactory.getGarbageCollectorMXBeans()) {
      if (collector instanceof NotificationEmitter) {
        ((NotificationEmitter) collector).addNotificationListener(listener, null, null);
      }
    }
    long periodMs = Long.getLong(SAMPLE_PROPERTY, 0L);
    if (periodMs > 0) {
      Thread sampler = new Thread(() -> {
        try {
          while (true) {
            TimeUnit.MILLISECONDS.sleep(periodMs);
            System.gc();
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }, "post-gc-heap-sampler");
      sampler.setDaemon(true);
      sampler.start();
    }
    return heap;
  }

  private void onNotification(Notification notification, Object handback) {
    if (!GarbageCollectionNotificationInfo.GARBAGE_COLLECTION_NOTIFICATION.equals(notification.getType())) {
      return;
    }
    GarbageCollectionNotificationInfo info = GarbageCollectionNotificationInfo.from((CompositeData) notification.getUserData());
    long used = 0;
    for (Map.Entry<String, MemoryUsage> pool : info.getGcInfo().getMemoryUsageAfterGc().entrySet()) {
      if (heapPools.contains(pool.getKey())) {
        used += pool.getValue().getUsed();
      }
    }
    synchronized (this) {
      count++;
      peak = Math.max(peak, used);
      if (FULL_GC_ACTION.equals(info.getGcAction())) {
        fullCount++;
        fullPeak = Math.max(fullPeak, used);
      }
    }
  }

  /**
   * Prints the four lines to {@code out}, first waiting (at most 5 s) for the notifications of the collections the collectors
   * have counted so far: notifications arrive on their own thread, after the collection.
   */
  void print(PrintStream out) throws InterruptedException {
    long collected = 0;
    List<GarbageCollectorMXBean> collectors = ManagementFactory.getGarbageCollectorMXBeans();
    for (GarbageCollectorMXBean collector : collectors) {
      collected += Math.max(0, collector.getCollectionCount());
    }
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (System.nanoTime() < deadline) {
      synchronized (this) {
        if (count >= collected) {
          break;
        }
      }
      TimeUnit.MILLISECONDS.sleep(20);
    }
    synchronized (this) {
      out.println("POST_GC_HEAP_PEAK_BYTES " + peak);
      out.println("POST_GC_COUNT " + count);
      out.println("POST_FULL_GC_HEAP_PEAK_BYTES " + fullPeak);
      out.println("POST_FULL_GC_COUNT " + fullCount);
    }
  }
}
