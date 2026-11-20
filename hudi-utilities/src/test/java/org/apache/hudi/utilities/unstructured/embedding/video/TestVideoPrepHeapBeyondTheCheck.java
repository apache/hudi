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

package org.apache.hudi.utilities.unstructured.embedding.video;

import org.apache.hudi.utilities.unstructured.storage.OutOfLineWritePool;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The heap a video prep task holds beside what the frame-heap start check counts. The check's slot term is
 * {@code 6 · maxPixels + 16 MiB + 10 MiB + 2 · prefetch}: data prep's read ahead is counted (2 buffers per task, admitted only
 * where they fit), and so are two bounded buffers, whose bounds are pinned here so a change to either is a change to this sum:
 * <ul>
 *   <li>the FFmpeg frame source's block reads: at most {@code BLOCKS × BLOCK_BYTES} = 2 MiB per clip, one clip per task;</li>
 *   <li>the background out-of-line writes: at most {@link OutOfLineWritePool#MAX_PENDING_BYTES} = 4 MiB pending per task, plus
 *       the one payload of at most that size the task holds while it waits for room.</li>
 * </ul>
 * Together 10 MiB per task slot, counted by the check as {@code VideoFrameHeapCheck.STREAMING_BUFFER_BYTES} beside its measured
 * 16 MiB per-slot overhead.
 */
class TestVideoPrepHeapBeyondTheCheck {

  private static final long MIB = 1024L * 1024;

  /**
   * The frame source keeps its blocks in a fixed array of {@code BLOCKS} slots of at most {@code BLOCK_BYTES} each, one source per
   * clip: 2 MiB.
   */
  @Test
  void blockReadsKeepAtMostTwoMiBPerClip() {
    assertEquals(2, BlockBufferedBlob.BLOCKS);
    assertEquals(MIB, BlockBufferedBlob.BLOCK_BYTES);
  }

  /**
   * With its write held, a task's 4 MiB payload fills the pending bound, and the task's next 4 MiB payload waits for room in
   * the task's hands: 8 MiB in all. A payload above the bound never reaches the pool (PreparedInputs streams it on the task
   * thread), so the waiting payload is at most the bound.
   */
  @Test
  void backgroundWritesHoldTheirBoundPlusTheWaitingPayload() throws Exception {
    assertEquals(OutOfLineWritePool.MAX_PENDING_BYTES, 4 * MIB);
    CountDownLatch release = new CountDownLatch(1);
    CountDownLatch firstAdmitted = new CountDownLatch(1);
    AtomicInteger admitted = new AtomicInteger();
    AtomicReference<Throwable> failure = new AtomicReference<>();
    try (OutOfLineWritePool pool = new OutOfLineWritePool()) {
      Thread task = new Thread(() -> {
        try {
          pool.submit(OutOfLineWritePool.MAX_PENDING_BYTES, release::await);
          admitted.incrementAndGet();
          firstAdmitted.countDown();
          pool.submit(OutOfLineWritePool.MAX_PENDING_BYTES, () -> { });
          admitted.incrementAndGet();
        } catch (Throwable t) {
          failure.set(t);
        }
      });
      task.start();
      assertTrue(firstAdmitted.await(30, TimeUnit.SECONDS));
      // the second submit can only park waiting for the pending bytes, up to the pool's bound on a wait
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
      while (task.getState() != Thread.State.TIMED_WAITING && System.nanoTime() < deadline) {
        task.join(10);
      }
      assertEquals(Thread.State.TIMED_WAITING, task.getState(), "the second payload waits for room");
      assertEquals(1, admitted.get());
      release.countDown();
      task.join(TimeUnit.SECONDS.toMillis(30));
      assertFalse(task.isAlive());
      assertNull(failure.get());
      assertEquals(2, admitted.get());
      pool.awaitAll();
    }
  }

  /** The heap per task slot the check does not count: the blocks of one clip, the pending writes and the waiting payload. */
  @Test
  void uncountedHeapPerSlotIsTenMiB() {
    long uncountedPerSlot = (long) BlockBufferedBlob.BLOCKS * BlockBufferedBlob.BLOCK_BYTES + 2L * OutOfLineWritePool.MAX_PENDING_BYTES;
    assertEquals(10 * MIB, uncountedPerSlot);
  }
}
