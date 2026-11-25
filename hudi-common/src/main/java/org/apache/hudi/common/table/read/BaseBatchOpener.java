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

package org.apache.hudi.common.table.read;

import org.apache.hudi.common.util.collection.ClosableIterator;

import lombok.extern.slf4j.Slf4j;

import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Opens a base file as batches and reads its first batch on a separate thread, so the open overlaps the log scan of
 * the same file slice. The pool holds one thread per core, matching the tasks an executor runs at once.
 */
@Slf4j
final class BaseBatchOpener {

  private static final AtomicInteger THREAD_ID = new AtomicInteger();
  private static final ThreadPoolExecutor POOL = createPool();

  private BaseBatchOpener() {
  }

  @FunctionalInterface
  interface Opener<B> {
    ClosableIterator<B> open() throws Exception;
  }

  static <B> CompletableFuture<ClosableIterator<B>> open(Opener<B> opener) {
    return CompletableFuture.supplyAsync(() -> {
      ClosableIterator<B> batches;
      try {
        batches = opener.open();
      } catch (Exception e) {
        throw new CompletionException(e);
      }
      try {
        return new PeekedIterator<>(batches);
      } catch (RuntimeException e) {
        batches.close();
        throw e;
      }
    }, POOL);
  }

  /**
   * The opened batches, or null when the open failed and the caller should open the file again.
   */
  static <B> ClosableIterator<B> await(CompletableFuture<ClosableIterator<B>> future) {
    try {
      return future.join();
    } catch (CompletionException e) {
      log.warn("Opening a base file ahead of the log scan failed; opening it again", e.getCause());
      return null;
    }
  }

  /**
   * Closes batches opened ahead that the read does not use.
   */
  static <B> void discard(CompletableFuture<ClosableIterator<B>> future) {
    if (future != null) {
      ClosableIterator<B> batches = await(future);
      if (batches != null) {
        batches.close();
      }
    }
  }

  private static ThreadPoolExecutor createPool() {
    int threads = Runtime.getRuntime().availableProcessors();
    ThreadPoolExecutor pool = new ThreadPoolExecutor(threads, threads, 30, TimeUnit.SECONDS, new LinkedBlockingQueue<>(),
        runnable -> {
          Thread thread = new Thread(runnable, "hudi-base-batch-open-" + THREAD_ID.incrementAndGet());
          thread.setDaemon(true);
          return thread;
        });
    pool.allowCoreThreadTimeOut(true);
    return pool;
  }

  /**
   * Batches whose first one has been read.
   */
  private static final class PeekedIterator<B> implements ClosableIterator<B> {
    private final ClosableIterator<B> batches;
    private B head;
    private boolean hasHead;

    private PeekedIterator(ClosableIterator<B> batches) {
      this.batches = batches;
      if (batches.hasNext()) {
        head = batches.next();
        hasHead = true;
      }
    }

    @Override
    public boolean hasNext() {
      return hasHead || batches.hasNext();
    }

    @Override
    public B next() {
      if (hasHead) {
        hasHead = false;
        B batch = head;
        head = null;
        return batch;
      }
      if (!batches.hasNext()) {
        throw new NoSuchElementException();
      }
      return batches.next();
    }

    @Override
    public void close() {
      batches.close();
    }
  }
}
