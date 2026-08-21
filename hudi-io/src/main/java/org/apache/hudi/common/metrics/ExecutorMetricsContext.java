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

package org.apache.hudi.common.metrics;

import java.util.Collections;
import java.util.Map;

/**
 * Registries {@link Registry#getRegistry(String)} resolves against on this thread. Scoped per thread
 * because a name cannot separate two tables writing in one JVM, whereas a task has only one table.
 */
public class ExecutorMetricsContext {

  private static final ThreadLocal<Map<String, Registry>> BOUND = new ThreadLocal<>();

  private ExecutorMetricsContext() {
  }

  /** Returns the previous binding, which callers must pass to {@link #unbind} in a finally block. */
  public static Map<String, Registry> bind(Map<String, Registry> registries) {
    Map<String, Registry> previous = BOUND.get();
    BOUND.set(registries == null ? Collections.emptyMap() : registries);
    return previous;
  }

  /** Removes rather than empties: Spark reuses task threads across tables. */
  public static void unbind(Map<String, Registry> previous) {
    if (previous == null) {
      BOUND.remove();
    } else {
      BOUND.set(previous);
    }
  }

  /** For propagating onto pool threads, which a thread-local does not reach. */
  public static Map<String, Registry> capture() {
    Map<String, Registry> bound = BOUND.get();
    return bound == null ? Collections.emptyMap() : bound;
  }

  public static Registry lookup(String registryName) {
    Map<String, Registry> bound = BOUND.get();
    return bound == null ? null : bound.get(registryName);
  }

  public static boolean isBound() {
    return BOUND.get() != null;
  }
}
