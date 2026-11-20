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

package org.apache.hudi.utilities.unstructured.embedding.source;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Ownership of an asynchronous object open abandoned by an interrupted task. */
class TestSourceObjectOpenerLifecycle {
  @Test
  void interruptedOpenClosesTheStreamThatArrivesLater() throws Exception {
    CountDownLatch waiting = new CountDownLatch(1);
    CompletableFuture<FSDataInputStream> future = new CompletableFuture<FSDataInputStream>() {
      @Override
      public FSDataInputStream get() throws InterruptedException, ExecutionException {
        waiting.countDown();
        return super.get();
      }
    };
    CompletableFuture<Throwable> result = new CompletableFuture<>();
    AtomicBoolean interrupted = new AtomicBoolean();
    Thread reader = new Thread(() -> {
      try {
        await(future);
        result.complete(new AssertionError("open unexpectedly completed"));
      } catch (Throwable failure) {
        interrupted.set(Thread.currentThread().isInterrupted());
        result.complete(failure);
      }
    }, "test-source-open-owner");
    TrackedInput input = new TrackedInput();
    FSDataInputStream stream = new FSDataInputStream(input);
    try {
      reader.start();
      assertTrue(waiting.await(5, TimeUnit.SECONDS));
      reader.interrupt();
      Throwable failure = result.get(5, TimeUnit.SECONDS);
      assertInstanceOf(InterruptedIOException.class, failure);
      assertInstanceOf(InterruptedException.class, failure.getCause());
      assertTrue(interrupted.get(), "the task retains its interrupt status");
      future.complete(stream);
      assertEquals(1, input.closes.get(), "the abandoned open owns and closes its late stream");
    } finally {
      reader.interrupt();
      reader.join(5000);
      future.complete(stream);
      if (input.closes.get() == 0) {
        input.close();
      }
    }
  }

  @Test
  void completedOpenTransfersStreamOwnershipToItsCaller() throws Exception {
    TrackedInput input = new TrackedInput();
    FSDataInputStream stream = new FSDataInputStream(input);
    assertSame(stream, await(CompletableFuture.completedFuture(stream)));
    assertEquals(0, input.closes.get());
    stream.close();
    assertEquals(1, input.closes.get());
  }

  @Test
  void failedOpenPreservesItsIoFailure() throws Exception {
    IOException failure = new IOException("object open failed");
    CompletableFuture<FSDataInputStream> future = new CompletableFuture<>();
    future.completeExceptionally(failure);
    assertSame(failure, assertThrows(IOException.class, () -> await(future)));
  }

  private static FSDataInputStream await(CompletableFuture<?> future) throws Exception {
    Method method = SourceObjectOpener.OpenFileApi.class.getDeclaredMethod("await", CompletableFuture.class, Path.class);
    method.setAccessible(true);
    try {
      return (FSDataInputStream) method.invoke(null, future, new Path("file:///test/open"));
    } catch (InvocationTargetException failure) {
      throw (Exception) failure.getCause();
    }
  }

  private static final class TrackedInput extends FSInputStream {
    private final AtomicInteger closes = new AtomicInteger();

    @Override
    public int read() {
      return -1;
    }

    @Override
    public void seek(long position) {
    }

    @Override
    public long getPos() {
      return 0;
    }

    @Override
    public boolean seekToNewSource(long position) {
      return false;
    }

    @Override
    public void close() {
      closes.incrementAndGet();
    }
  }
}
