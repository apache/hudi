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

package org.apache.hudi.utilities.unstructured.retry;

import org.apache.hadoop.net.ConnectTimeoutException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.SocketTimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** {@link TransientRetry}: bounded, backoff doubling and capped, never retries an interruption or a non-transient failure. */
public class TestTransientRetry {

  @AfterEach
  public void clearInterrupt() {
    Thread.interrupted();
  }

  @Test
  public void transientFailureIsRetriedUntilItSucceeds() throws IOException {
    AtomicInteger tries = new AtomicInteger();
    String value = new TransientRetry(3, 0).call("step", () -> {
      if (tries.incrementAndGet() < 3) {
        throw new IOException("503 Slow Down");
      }
      return "ok";
    }, failure -> true);
    assertEquals("ok", value);
    assertEquals(3, tries.get());
  }

  @Test
  public void lastFailureCarriesTheFirstAsSuppressedWhenTheBoundIsSpent() {
    AtomicInteger tries = new AtomicInteger();
    IOException last = assertThrows(IOException.class, () -> new TransientRetry(3, 0).call("step", () -> {
      throw new IOException("try " + tries.incrementAndGet());
    }, failure -> true));
    assertEquals("try 3", last.getMessage());
    assertEquals("try 1", last.getSuppressed()[0].getMessage());
    assertEquals(3, tries.get());
  }

  @Test
  public void nonTransientFailureIsNeverRetried() {
    AtomicInteger tries = new AtomicInteger();
    IOException denied = new IOException("403 Forbidden");
    assertSame(denied, assertThrows(IOException.class, () -> new TransientRetry(3, 0).call("step", () -> {
      tries.incrementAndGet();
      throw denied;
    }, failure -> false)));
    assertEquals(1, tries.get());
  }

  /** A killed task is never retried, whatever the stage's classifier answers. */
  @Test
  public void interruptionIsNeverRetried() {
    AtomicInteger tries = new AtomicInteger();
    assertThrows(InterruptedIOException.class, () -> new TransientRetry(3, 0).call("step", () -> {
      tries.incrementAndGet();
      throw new InterruptedIOException("read interrupted");
    }, failure -> true));
    assertEquals(1, tries.get());

    AtomicInteger flagged = new AtomicInteger();
    Thread.currentThread().interrupt();
    assertThrows(IOException.class, () -> new TransientRetry(3, 0).call("step", () -> {
      flagged.incrementAndGet();
      throw new IOException("503 Slow Down");
    }, failure -> true));
    assertEquals(1, flagged.get(), "an interrupted thread does not retry");
    assertTrue(Thread.currentThread().isInterrupted(), "the interrupt flag is kept");
  }

  /** A timed-out request is an InterruptedIOException by type, but transient: it is retried within the bound. */
  @Test
  public void timeoutsAreRetried() throws IOException {
    for (IOException timeout : new IOException[] {new SocketTimeoutException("read timed out"),
        new ConnectTimeoutException("connect timed out")}) {
      AtomicInteger tries = new AtomicInteger();
      String value = new TransientRetry(3, 0).call("step", () -> {
        if (tries.incrementAndGet() < 3) {
          throw timeout;
        }
        return "ok";
      }, failure -> true);
      assertEquals("ok", value, timeout.toString());
      assertEquals(3, tries.get(), timeout.toString());
    }
  }

  @Test
  public void backoffDoublesAndIsCapped() {
    TransientRetry retry = new TransientRetry(10, 1000);
    assertEquals(1000, retry.waitBefore(2));
    assertEquals(2000, retry.waitBefore(3));
    assertEquals(TransientRetry.MAX_BACKOFF_MS, retry.waitBefore(10));
  }

  @Test
  public void boundsAreValidated() {
    assertThrows(IllegalArgumentException.class, () -> new TransientRetry(0, 0));
    assertThrows(IllegalArgumentException.class, () -> new TransientRetry(1, -1));
  }
}
