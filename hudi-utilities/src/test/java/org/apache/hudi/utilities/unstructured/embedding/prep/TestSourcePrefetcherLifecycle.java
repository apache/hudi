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

package org.apache.hudi.utilities.unstructured.embedding.prep;

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobBytesSource;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobColumns;

import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Cancellation releases the consumer even while a source read has not yet observed interruption. */
class TestSourcePrefetcherLifecycle {
  @Test
  void closeReleasesAConsumerWaitingForAReadThatIgnoresInterruption() throws Exception {
    BlockingInput input = new BlockingInput();
    BlobBytesSource source = source(input);
    Row row = row();
    SourcePrefetcher prefetch = new SourcePrefetcher(source, 1024);
    ExecutorService consumer = Executors.newSingleThreadExecutor();
    CountDownLatch waiting = new CountDownLatch(1);
    CountDownLatch returned = new CountDownLatch(1);
    try {
      prefetch.start(row, 1, 1024);
      assertTrue(input.reading.await(5, TimeUnit.SECONDS));
      Field field = SourcePrefetcher.class.getDeclaredField("pending");
      field.setAccessible(true);
      @SuppressWarnings("unchecked")
      Future<SourcePrefetcher.CachedRead> original = (Future<SourcePrefetcher.CachedRead>) field.get(prefetch);
      field.set(prefetch, new WaitingFuture(original, waiting));
      Future<Option<SourcePrefetcher.CachedRead>> taken = consumer.submit(() -> {
        try {
          return prefetch.take(row);
        } finally {
          returned.countDown();
        }
      });
      assertTrue(waiting.await(5, TimeUnit.SECONDS));
      prefetch.close();
      assertTrue(returned.await(2, TimeUnit.SECONDS), "close releases the consumer without waiting for the backend read");
      assertTrue(taken.get(5, TimeUnit.SECONDS).isEmpty());
      assertEquals(0, input.closes.get(), "the backend worker still owns its uncompleted stream");
    } finally {
      input.release.countDown();
      prefetch.close();
      consumer.shutdownNow();
      assertTrue(consumer.awaitTermination(5, TimeUnit.SECONDS));
      assertTrue(input.closed.await(5, TimeUnit.SECONDS));
    }
    assertEquals(1, input.closes.get(), "the late worker closes the stream exactly once");
  }

  @Test
  void completedReadTransfersBytesAndClosesItsStreamExactlyOnce() throws Exception {
    BlockingInput input = new BlockingInput();
    input.release.countDown();
    Row row = row();
    try (SourcePrefetcher prefetch = new SourcePrefetcher(source(input), 1024)) {
      prefetch.start(row, 1, 1024);
      assertArrayEquals(new byte[] {7}, prefetch.take(row).get().bytes());
    }
    assertEquals(1, input.closes.get());
  }

  @Test
  void interruptedWaitFailsTheTaskInsteadOfBecomingACacheMiss() throws Exception {
    BlockingInput input = new BlockingInput();
    Row row = row();
    try (SourcePrefetcher prefetch = new SourcePrefetcher(source(input), 1024)) {
      prefetch.start(row, 1, 1024);
      assertTrue(input.reading.await(5, TimeUnit.SECONDS));
      Thread.currentThread().interrupt();
      try {
        org.junit.jupiter.api.Assertions.assertThrows(org.apache.hudi.exception.HoodieException.class,
            () -> prefetch.take(row));
        assertTrue(Thread.currentThread().isInterrupted());
      } finally {
        Thread.interrupted();
        input.release.countDown();
      }
    }
    assertTrue(input.closed.await(5, TimeUnit.SECONDS));
  }

  private static BlobBytesSource source(InputStream input) throws Exception {
    BlobBytesSource source = mock(BlobBytesSource.class);
    when(source.withoutOpenRetry()).thenReturn(source);
    when(source.checkedVersionCheck(any(Row.class))).thenReturn(consumer -> { });
    when(source.open(any(Row.class), eq(1L))).thenReturn(input);
    return source;
  }

  private static Row row() {
    Row row = mock(Row.class);
    Row blob = mock(Row.class);
    Row reference = mock(Row.class);
    when(row.getAs(BlobColumns.SOURCE_MODIFICATION_TIME)).thenReturn(0L);
    when(row.fieldIndex(BlobColumns.BLOB)).thenReturn(0);
    when(row.getStruct(0)).thenReturn(blob);
    when(blob.getAs(HoodieSchema.Blob.TYPE)).thenReturn(HoodieSchema.Blob.OUT_OF_LINE);
    when(blob.getAs(HoodieSchema.Blob.EXTERNAL_REFERENCE)).thenReturn(reference);
    return row;
  }

  private static final class BlockingInput extends InputStream {
    private final CountDownLatch reading = new CountDownLatch(1);
    private final CountDownLatch release = new CountDownLatch(1);
    private final CountDownLatch closed = new CountDownLatch(1);
    private final AtomicInteger closes = new AtomicInteger();
    private boolean sent;

    @Override
    public int read() {
      if (sent) {
        return -1;
      }
      reading.countDown();
      boolean interrupted = false;
      while (release.getCount() != 0) {
        try {
          release.await();
        } catch (InterruptedException ignored) {
          interrupted = true;
        }
      }
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
      sent = true;
      return 7;
    }

    @Override
    public void close() {
      closes.incrementAndGet();
      closed.countDown();
    }
  }

  /** Signals the precise Future.get boundary, avoiding a timing-only race in the cancellation test. */
  private static final class WaitingFuture implements Future<SourcePrefetcher.CachedRead> {
    private final Future<SourcePrefetcher.CachedRead> delegate;
    private final CountDownLatch waiting;

    private WaitingFuture(Future<SourcePrefetcher.CachedRead> delegate, CountDownLatch waiting) {
      this.delegate = delegate;
      this.waiting = waiting;
    }

    @Override
    public boolean cancel(boolean interrupt) {
      return delegate.cancel(interrupt);
    }

    @Override
    public boolean isCancelled() {
      return delegate.isCancelled();
    }

    @Override
    public boolean isDone() {
      return delegate.isDone();
    }

    @Override
    public SourcePrefetcher.CachedRead get() throws InterruptedException, ExecutionException {
      waiting.countDown();
      return delegate.get();
    }

    @Override
    public SourcePrefetcher.CachedRead get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
      return delegate.get(timeout, unit);
    }
  }
}
