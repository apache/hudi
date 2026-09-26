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
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.common.data;

import java.io.Serializable;

/**
 * A read-only value shared with the tasks of an engine, created by
 * {@link org.apache.hudi.common.engine.HoodieEngineContext#broadcast}.
 *
 * <p>Capture the handle, not the value, in a task function: the handle is small, and a distributed
 * engine ships the value once per worker instead of once per task.
 *
 * <p>What callers can rely on:
 * <ul>
 *   <li>The tasks running in the same worker share one instance, so any state the value builds lazily
 *   (caches, readers) must be thread-safe.</li>
 *   <li>An engine that runs tasks in the caller's JVM may hand back the caller's own instance.</li>
 *   <li>A distributed engine hands tasks a deserialized copy; changes made to the value after
 *   {@code broadcast} do not reach it.</li>
 * </ul>
 *
 * @param <T> type of the value
 */
public interface HoodieBroadcast<T> extends Serializable {

  /**
   * @return the broadcast value.
   */
  T value();

  /**
   * Releases the value on the workers. Call it once no task that reads the value can still run (for lazy
   * {@link HoodieData}, after the data is computed); the handle must not be used afterwards.
   */
  default void destroy() {
  }
}
