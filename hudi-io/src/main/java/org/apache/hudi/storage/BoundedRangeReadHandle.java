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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Portable bounded range reader built on {@link HoodieStorage#openSeekable}. A fresh stream is
 * opened for each attempt because object-store streams commonly serialize positioned reads.
 * Executor-local pools are shared by handles so Spark tasks do not each create dozens of threads.
 */
public final class BoundedRangeReadHandle implements HoodieRangeReadHandle {
  private static final Logger LOG = LoggerFactory.getLogger(BoundedRangeReadHandle.class);
  private static final Map<Integer, ExecutorService> EXECUTORS = new ConcurrentHashMap<>();
  private static final AtomicInteger THREAD_ID = new AtomicInteger();

  private final HoodieStorage storage;
  private final int requestedConcurrency;
  private final int transportMaxConnections;
  private final int effectiveConcurrency;
  private final int maxRetries;
  private final int bufferSize;
  private final ExecutorService executor;
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private volatile List<Future<RangeReadResult>> activeFutures = Collections.emptyList();

  public BoundedRangeReadHandle(HoodieStorage storage,
                                int requestedConcurrency,
                                int transportMaxConnections,
                                int maxRetries) {
    this(storage, requestedConcurrency, transportMaxConnections, maxRetries,
        storage.getDefaultBufferSize(), null);
  }

  BoundedRangeReadHandle(HoodieStorage storage,
                         int requestedConcurrency,
                         int transportMaxConnections,
                         int maxRetries,
                         int bufferSize,
                         ExecutorService executor) {
    if (requestedConcurrency <= 0) {
      throw new IllegalArgumentException("requestedConcurrency must be positive");
    }
    if (transportMaxConnections <= 0) {
      throw new IllegalArgumentException("transportMaxConnections must be positive");
    }
    if (maxRetries < 0) {
      throw new IllegalArgumentException("maxRetries must not be negative");
    }
    this.storage = storage;
    this.requestedConcurrency = requestedConcurrency;
    this.transportMaxConnections = transportMaxConnections;
    this.effectiveConcurrency = Math.min(requestedConcurrency, transportMaxConnections);
    this.maxRetries = maxRetries;
    this.bufferSize = bufferSize;
    this.executor = executor == null
        ? EXECUTORS.computeIfAbsent(effectiveConcurrency, BoundedRangeReadHandle::newExecutor)
        : executor;
    LOG.info("Created range read handle: scheme={}, requestedConcurrency={}, "
            + "transportMaxConnections={}, effectiveConcurrency={}",
        storage.getScheme(), requestedConcurrency, transportMaxConnections, effectiveConcurrency);
  }

  @Override
  public RangeReadBatch readRanges(StoragePath path,
                                   List<ByteRange> ranges,
                                   long deadlineNanos,
                                   long maxPlannedBytes) throws IOException {
    if (closed.get()) {
      throw new IOException("Range read handle is closed");
    }
    if (deadlineNanos <= System.nanoTime()) {
      throw deadlineExceeded(path);
    }

    long plannedBytes = plannedBytes(ranges);
    if (plannedBytes > maxPlannedBytes) {
      throw new IOException("Vector range prefetch rejected before allocation: plannedBytes="
          + plannedBytes + ", maxPlannedBytes=" + maxPlannedBytes + ", path=" + path);
    }
    if (ranges.isEmpty()) {
      return new RangeReadBatch(Collections.emptyList(),
          new RangeReadMetrics(0, 0, 0, 0, 0, 0, 0, effectiveConcurrency));
    }

    AtomicLong retries = new AtomicLong();
    AtomicLong openNanos = new AtomicLong();
    AtomicLong readNanos = new AtomicLong();
    AtomicInteger inFlight = new AtomicInteger();
    AtomicInteger maxInFlight = new AtomicInteger();
    List<Future<RangeReadResult>> futures = new ArrayList<>(ranges.size());
    long waitStart = System.nanoTime();

    try {
      for (ByteRange range : ranges) {
        ensureBeforeDeadline(path, deadlineNanos);
        futures.add(executor.submit(() -> {
          int current = inFlight.incrementAndGet();
          maxInFlight.accumulateAndGet(current, Math::max);
          try {
            return readRange(path, range, deadlineNanos, retries, openNanos, readNanos);
          } finally {
            inFlight.decrementAndGet();
          }
        }));
      }
      activeFutures = futures;

      List<RangeReadResult> results = new ArrayList<>(ranges.size());
      for (Future<RangeReadResult> future : futures) {
        long remaining = deadlineNanos - System.nanoTime();
        if (remaining <= 0) {
          throw deadlineExceeded(path);
        }
        try {
          results.add(future.get(remaining, TimeUnit.NANOSECONDS));
        } catch (java.util.concurrent.TimeoutException e) {
          throw deadlineExceeded(path);
        } catch (ExecutionException e) {
          throw asIOException(e.getCause());
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          InterruptedIOException ioe = new InterruptedIOException("Interrupted while reading ranges for " + path);
          ioe.initCause(e);
          throw ioe;
        }
      }

      return new RangeReadBatch(results, new RangeReadMetrics(
          ranges.size(), plannedBytes, retries.get(), maxInFlight.get(), openNanos.get(),
          readNanos.get(), System.nanoTime() - waitStart, effectiveConcurrency));
    } catch (IOException | RuntimeException e) {
      futures.forEach(future -> future.cancel(true));
      throw e;
    } finally {
      activeFutures = Collections.emptyList();
    }
  }

  private RangeReadResult readRange(StoragePath path,
                                    ByteRange range,
                                    long deadlineNanos,
                                    AtomicLong retries,
                                    AtomicLong openNanos,
                                    AtomicLong readNanos) throws IOException {
    List<IOException> priorFailures = new ArrayList<>();
    for (int attempt = 0; ; attempt++) {
      ensureBeforeDeadline(path, deadlineNanos);
      byte[] bytes = new byte[range.getLength()];
      try {
        long openStart = System.nanoTime();
        try (SeekableDataInputStream stream = storage.openSeekable(path, bufferSize, false)) {
          openNanos.addAndGet(System.nanoTime() - openStart);
          long readStart = System.nanoTime();
          stream.seek(range.getOffset());
          stream.readFully(bytes);
          readNanos.addAndGet(System.nanoTime() - readStart);
        }
        return new RangeReadResult(range, ByteBuffer.wrap(bytes));
      } catch (IOException failure) {
        if (!isRetryable(failure) || attempt >= maxRetries) {
          priorFailures.forEach(failure::addSuppressed);
          throw failure;
        }
        priorFailures.add(failure);
        retries.incrementAndGet();
        sleepWithJitter(path, deadlineNanos, attempt);
      }
    }
  }

  private static long plannedBytes(List<ByteRange> ranges) throws IOException {
    long total = 0;
    try {
      for (ByteRange range : ranges) {
        total = Math.addExact(total, range.getLength());
      }
    } catch (ArithmeticException e) {
      throw new IOException("Range byte count overflow", e);
    }
    return total;
  }

  private static boolean isRetryable(IOException failure) {
    return !(failure instanceof EOFException) && !(failure instanceof InterruptedIOException);
  }

  private static void sleepWithJitter(StoragePath path, long deadlineNanos, int attempt) throws IOException {
    long ceilingMs = Math.min(1000L, 50L << Math.min(attempt, 4));
    long sleepMs = ThreadLocalRandom.current().nextLong(Math.max(1L, ceilingMs / 2), ceilingMs + 1);
    if (System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(sleepMs) >= deadlineNanos) {
      throw deadlineExceeded(path);
    }
    try {
      Thread.sleep(sleepMs);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      InterruptedIOException ioe = new InterruptedIOException("Interrupted during range-read retry for " + path);
      ioe.initCause(e);
      throw ioe;
    }
  }

  private static void ensureBeforeDeadline(StoragePath path, long deadlineNanos) throws SocketTimeoutException {
    if (System.nanoTime() >= deadlineNanos) {
      throw deadlineExceeded(path);
    }
  }

  private static SocketTimeoutException deadlineExceeded(StoragePath path) {
    return new SocketTimeoutException("Vector range-read deadline exceeded for " + path);
  }

  private static IOException asIOException(Throwable failure) {
    if (failure instanceof IOException) {
      return (IOException) failure;
    }
    return new IOException("Range read failed", failure);
  }

  private static ExecutorService newExecutor(int concurrency) {
    int queueCapacity = Math.max(64, concurrency * 4);
    ThreadFactory threadFactory = runnable -> {
      Thread thread = new Thread(runnable, "hudi-range-reader-" + THREAD_ID.incrementAndGet());
      thread.setDaemon(true);
      return thread;
    };
    ThreadPoolExecutor executor = new ThreadPoolExecutor(
        concurrency, concurrency, 0L, TimeUnit.MILLISECONDS,
        new ArrayBlockingQueue<>(queueCapacity), threadFactory,
        (task, pool) -> {
          if (pool.isShutdown()) {
            throw new java.util.concurrent.RejectedExecutionException("Range read executor is shut down");
          }
          try {
            pool.getQueue().put(task);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new java.util.concurrent.RejectedExecutionException(e);
          }
        });
    executor.prestartAllCoreThreads();
    return executor;
  }

  int getRequestedConcurrency() {
    return requestedConcurrency;
  }

  int getTransportMaxConnections() {
    return transportMaxConnections;
  }

  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) {
      activeFutures.forEach(future -> future.cancel(true));
    }
  }
}
