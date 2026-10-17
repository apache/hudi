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

package org.apache.hudi.client.utils;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.function.IntConsumer;

/**
 * Halts the executor JVM when a write task failed because the storage client's HTTP connection pool was shut down.
 *
 * <p>The pool belongs to the JVM-wide cached file system, so once it is shut down every later task on the executor
 * fails the same way while the executor keeps heartbeating. Halting the JVM lets Spark replace the executor.
 * {@link Runtime#halt} is used rather than {@link System#exit} because shutdown hooks (such as the Hadoop file
 * system cache close) can block on the same dead client.
 *
 * <p>Detection matches the exact exception type and message the HTTP client raises, so it is a best-effort
 * safeguard rather than a contract: if that message is ever reworded the failure mode is that this stops halting
 * and the executor behaves as it did before, never that an unrelated failure halts the JVM.
 */
@Slf4j
public class ConnectionPoolShutdownExecutorHalter {

  /** Message of the {@link IllegalStateException} Apache HttpClient throws when a shut-down pool is used. */
  static final String CONNECTION_POOL_SHUT_DOWN_MESSAGE = "Connection pool shut down";

  /** Distinct from the exit codes Spark itself uses (0-3, 11, 50-56, 127), so the cause is identifiable. */
  static final int HALT_EXIT_CODE = 57;

  /** Shared instance for the write path; the halter holds no per-failure state. */
  public static final ConnectionPoolShutdownExecutorHalter DEFAULT = new ConnectionPoolShutdownExecutorHalter();

  private final IntConsumer halter;

  public ConnectionPoolShutdownExecutorHalter() {
    this(exitCode -> Runtime.getRuntime().halt(exitCode));
  }

  ConnectionPoolShutdownExecutorHalter(IntConsumer halter) {
    this.halter = halter;
  }

  /**
   * Halts the JVM with {@link #HALT_EXIT_CODE} if {@code failure} or any exception in its cause chain is the
   * connection pool shutdown; otherwise returns normally so the caller can rethrow.
   */
  public void haltIfConnectionPoolShutDown(Throwable failure) {
    if (isConnectionPoolShutDown(failure)) {
      log.error("Halting the executor JVM with exit code {}: the storage client's connection pool is shut down, "
          + "so every later task on this executor would fail the same way", HALT_EXIT_CODE);
      halter.accept(HALT_EXIT_CODE);
    }
  }

  static boolean isConnectionPoolShutDown(Throwable failure) {
    Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
    Deque<Throwable> pending = new ArrayDeque<>();
    if (failure != null) {
      pending.push(failure);
    }
    while (!pending.isEmpty()) {
      Throwable current = pending.pop();
      if (!visited.add(current)) {
        continue;
      }
      if (current instanceof IllegalStateException && CONNECTION_POOL_SHUT_DOWN_MESSAGE.equals(current.getMessage())) {
        return true;
      }
      if (current.getCause() != null) {
        pending.push(current.getCause());
      }
      // A close() that failed on the dead pool reaches the caller as a suppressed exception, so the
      // chain alone would miss it.
      for (Throwable suppressed : current.getSuppressed()) {
        pending.push(suppressed);
      }
    }
    return false;
  }
}
