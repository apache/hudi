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
 * "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.storage;

import org.apache.hudi.io.SeekableDataInputStream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TestBoundedRangeReadHandle {
  private ExecutorService executor;

  @AfterEach
  void tearDown() throws InterruptedException {
    if (executor != null) {
      executor.shutdownNow();
      executor.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  @Test
  void readsRangesThroughIndependentStreamsWithBoundedConcurrency() throws Exception {
    byte[] file = new byte[64];
    for (int i = 0; i < file.length; i++) {
      file[i] = (byte) i;
    }
    AtomicInteger streamsOpened = new AtomicInteger();
    AtomicInteger streamsClosed = new AtomicInteger();
    AtomicInteger activeStreams = new AtomicInteger();
    AtomicInteger maxActiveStreams = new AtomicInteger();
    HoodieStorage storage = storage(file, 40, false, streamsOpened, streamsClosed,
        activeStreams, maxActiveStreams);
    executor = Executors.newFixedThreadPool(4);

    List<HoodieRangeReadHandle.ByteRange> ranges = Arrays.asList(
        new HoodieRangeReadHandle.ByteRange(0, 4),
        new HoodieRangeReadHandle.ByteRange(8, 4),
        new HoodieRangeReadHandle.ByteRange(16, 4),
        new HoodieRangeReadHandle.ByteRange(24, 4),
        new HoodieRangeReadHandle.ByteRange(32, 4),
        new HoodieRangeReadHandle.ByteRange(40, 4));

    try (BoundedRangeReadHandle handle = new BoundedRangeReadHandle(storage, 8, 4, 0, 4096, executor)) {
      HoodieRangeReadHandle.RangeReadBatch batch = handle.readRanges(
          new StoragePath("file:///vectors.parquet"), ranges,
          System.nanoTime() + TimeUnit.SECONDS.toNanos(5), 1024);

      assertEquals(ranges.size(), streamsOpened.get());
      assertEquals(streamsOpened.get(), streamsClosed.get());
      assertTrue(maxActiveStreams.get() > 1, "reads should overlap across independent streams");
      assertTrue(maxActiveStreams.get() <= 4, "transport cap must bound active range reads");
      assertEquals(4, batch.getMetrics().getEffectiveConcurrency());
      assertEquals(maxActiveStreams.get(), batch.getMetrics().getMaxRangesInFlight());
      assertEquals(24, batch.getMetrics().getByteCount());
      assertArrayEquals(new byte[] {8, 9, 10, 11}, bytes(batch.getResults().get(1).getData()));
    }
  }

  @Test
  void rejectsPlannedBytesBeforeOpeningOrAllocating() throws Exception {
    HoodieStorage storage = mock(HoodieStorage.class);
    when(storage.getDefaultBufferSize()).thenReturn(4096);
    when(storage.getScheme()).thenReturn("file");
    executor = Executors.newSingleThreadExecutor();

    IOException failure;
    try (BoundedRangeReadHandle handle = new BoundedRangeReadHandle(storage, 1, 1, 0, 4096, executor)) {
      failure = assertThrows(IOException.class, () -> handle.readRanges(
          new StoragePath("file:///vectors.parquet"),
          Arrays.asList(new HoodieRangeReadHandle.ByteRange(0, 8)),
          System.nanoTime() + TimeUnit.SECONDS.toNanos(5), 4));
    }

    assertTrue(failure.getMessage().contains("plannedBytes=8"));
    assertTrue(failure.getMessage().contains("maxPlannedBytes=4"));
    verify(storage, never()).openSeekable(any(), anyInt(), anyBoolean());
  }

  @Test
  void retriesOneRangeWithANewStreamAndClosesBothAttempts() throws Exception {
    byte[] file = new byte[] {1, 2, 3, 4};
    AtomicInteger streamsOpened = new AtomicInteger();
    AtomicInteger streamsClosed = new AtomicInteger();
    AtomicInteger activeStreams = new AtomicInteger();
    AtomicInteger maxActiveStreams = new AtomicInteger();
    HoodieStorage storage = storage(file, 0, true, streamsOpened, streamsClosed,
        activeStreams, maxActiveStreams);
    executor = Executors.newSingleThreadExecutor();

    try (BoundedRangeReadHandle handle = new BoundedRangeReadHandle(storage, 1, 1, 1, 4096, executor)) {
      HoodieRangeReadHandle.RangeReadBatch batch = handle.readRanges(
          new StoragePath("file:///vectors.parquet"),
          Arrays.asList(new HoodieRangeReadHandle.ByteRange(0, 4)),
          System.nanoTime() + TimeUnit.SECONDS.toNanos(5), 4);

      assertEquals(1, batch.getMetrics().getRetryCount());
      assertEquals(2, streamsOpened.get());
      assertEquals(2, streamsClosed.get());
      assertArrayEquals(file, bytes(batch.getResults().get(0).getData()));
    }
  }

  private static HoodieStorage storage(byte[] file,
                                       long readDelayMs,
                                       boolean failFirstRead,
                                       AtomicInteger streamsOpened,
                                       AtomicInteger streamsClosed,
                                       AtomicInteger activeStreams,
                                       AtomicInteger maxActiveStreams) throws IOException {
    HoodieStorage storage = mock(HoodieStorage.class);
    when(storage.getDefaultBufferSize()).thenReturn(4096);
    when(storage.getScheme()).thenReturn("test");
    AtomicBoolean fail = new AtomicBoolean(failFirstRead);
    when(storage.openSeekable(any(), anyInt(), anyBoolean())).thenAnswer(ignored -> {
      streamsOpened.incrementAndGet();
      int active = activeStreams.incrementAndGet();
      maxActiveStreams.accumulateAndGet(active, Math::max);
      TrackingInputStream input = new TrackingInputStream(
          file, readDelayMs, fail.compareAndSet(true, false), streamsClosed, activeStreams);
      return new SeekableDataInputStream(input) {
        @Override
        public long getPos() {
          return input.position;
        }

        @Override
        public void seek(long pos) {
          input.position = Math.toIntExact(pos);
        }
      };
    });
    return storage;
  }

  private static byte[] bytes(ByteBuffer buffer) {
    byte[] bytes = new byte[buffer.remaining()];
    buffer.get(bytes);
    return bytes;
  }

  private static final class TrackingInputStream extends InputStream {
    private final byte[] file;
    private final long delayMs;
    private final boolean fail;
    private final AtomicInteger streamsClosed;
    private final AtomicInteger activeStreams;
    private boolean closed;
    private int position;

    private TrackingInputStream(byte[] file,
                                long delayMs,
                                boolean fail,
                                AtomicInteger streamsClosed,
                                AtomicInteger activeStreams) {
      this.file = file;
      this.delayMs = delayMs;
      this.fail = fail;
      this.streamsClosed = streamsClosed;
      this.activeStreams = activeStreams;
    }

    @Override
    public int read() throws IOException {
      byte[] one = new byte[1];
      return read(one, 0, 1) < 0 ? -1 : one[0] & 0xff;
    }

    @Override
    public int read(byte[] target, int offset, int length) throws IOException {
      if (fail) {
        throw new IOException("transient read failure");
      }
      if (delayMs > 0) {
        try {
          Thread.sleep(delayMs);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new InterruptedIOException("interrupted test read");
        }
      }
      if (position >= file.length) {
        return -1;
      }
      int copied = Math.min(length, file.length - position);
      System.arraycopy(file, position, target, offset, copied);
      position += copied;
      return copied;
    }

    @Override
    public void close() {
      if (!closed) {
        closed = true;
        streamsClosed.incrementAndGet();
        activeStreams.decrementAndGet();
      }
    }
  }
}
