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

package org.apache.hudi.metadata;

import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.function.SerializableFunctionUnchecked;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manager for tracking and cleaning up persisted HoodieData/HoodiePairData on exceptions.
 * This class provides thread-safe tracking of persisted data objects and ensures they are
 * properly cleaned up when exceptions occur.
 */
@Slf4j
public class HoodieDataCleanupManager implements Serializable {

  // Thread-local tracking of persisted data for cleanup on exceptions
  @Getter(AccessLevel.PACKAGE)
  private final ConcurrentHashMap<Long, List<Object>> threadPersistedData = new ConcurrentHashMap<>();
  
  /**
   * Track a persisted data object for the current thread.
   *
   * @param data The HoodiePairData to track
   */
  public void trackPersistedData(HoodiePairData<?, ?> data) {
    long threadId = Thread.currentThread().getId();
    threadPersistedData.computeIfAbsent(threadId, k -> new ArrayList<>()).add(data);
  }
  
  /**
   * Track a persisted data object for the current thread.
   *
   * @param data The HoodieData to track
   */
  public void trackPersistedData(HoodieData<?> data) {
    long threadId = Thread.currentThread().getId();
    threadPersistedData.computeIfAbsent(threadId, k -> new ArrayList<>()).add(data);
  }
  
  /**
   * Executes the given operation with automatic cleanup of persisted data on exception.
   * This method ensures that any data persisted by the current thread are properly unpersisted
   * if an exception occurs, and thread-local tracking is always cleared.
   *
   * @param operation The operation to execute
   * @param <T> The return type of the operation
   * @return The result of the operation
   */
  public <T> T ensureDataCleanupOnException(SerializableFunctionUnchecked<Void, T> operation) {
    try {
      return operation.apply(null);
    } catch (Exception e) {
      // Clean up any persisted data from this thread on exception
      cleanupPersistedData();
      throw (RuntimeException) e;
    } finally {
      // Clear thread-local tracking
      clearThreadTracking();
    }
  }
  
  /**
   * Clean up persisted data for the current thread (called on exception).
   */
  private void cleanupPersistedData() {
    long threadId = Thread.currentThread().getId();
    List<Object> dataObjects = threadPersistedData.get(threadId);
    if (dataObjects != null) {
      for (Object data : dataObjects) {
        try {
          if (data instanceof HoodiePairData) {
            ((HoodiePairData<?, ?>) data).unpersistWithDependencies();
          } else if (data instanceof HoodieData) {
            ((HoodieData<?>) data).unpersistWithDependencies();
          }
        } catch (Exception e) {
          log.warn("Failed to unpersist data on exception cleanup", e);
        }
      }
    }
  }
  
  /**
   * Clear thread-local tracking (called in finally block).
   */
  private void clearThreadTracking() {
    long threadId = Thread.currentThread().getId();
    threadPersistedData.remove(threadId);
  }
}