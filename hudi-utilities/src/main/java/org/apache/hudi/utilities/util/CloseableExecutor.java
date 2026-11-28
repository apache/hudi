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

package org.apache.hudi.utilities.util;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * A fixed thread pool a try-with-resources block owns.
 *
 * <p>{@link ExecutorService} only became {@code AutoCloseable} in Java 19. A pool created on the
 * line before a {@code try} and shut down in its {@code finally} is closed, but any statement
 * inserted between the creation and the {@code try} leaks the pool if it throws. Owning the pool
 * through try-with-resources removes that gap.
 *
 * <p>The pool is built inside the factory methods rather than handed in, so a caller cannot hold an
 * unowned {@code ExecutorService} even briefly.
 *
 * <p>{@link #close()} calls {@code shutdownNow()}, which is what every call site did before: each
 * waits for its own work to finish inside the block, so by the time close runs there is nothing left
 * to interrupt, and an abnormal exit is exactly when the running tasks should be interrupted.
 */
public final class CloseableExecutor implements AutoCloseable {

  private final ExecutorService delegate;

  private CloseableExecutor(ExecutorService delegate) {
    this.delegate = delegate;
  }

  /** A fixed pool of {@code threads} threads, at least one however small the caller's bound is. */
  public static CloseableExecutor fixedPool(int threads) {
    return new CloseableExecutor(Executors.newFixedThreadPool(Math.max(1, threads)));
  }

  /** As {@link #fixedPool(int)}, for a caller that needs its threads named or daemonised. */
  public static CloseableExecutor fixedPool(int threads, ThreadFactory threadFactory) {
    return new CloseableExecutor(Executors.newFixedThreadPool(Math.max(1, threads), threadFactory));
  }

  public ExecutorService service() {
    return delegate;
  }

  @Override
  public void close() {
    delegate.shutdownNow();
  }
}
