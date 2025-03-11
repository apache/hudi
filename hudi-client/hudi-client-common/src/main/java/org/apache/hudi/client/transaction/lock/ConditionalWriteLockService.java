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

package org.apache.hudi.client.transaction.lock;

import org.apache.hudi.client.transaction.lock.models.ConditionalWriteLockData;
import org.apache.hudi.client.transaction.lock.models.ConditionalWriteLockFile;
import org.apache.hudi.common.util.collection.Pair;

import java.util.function.Supplier;

/**
 * Defines a contract for a service which should be able to perform conditional writes to object storage.
 * It expects to be interacting with a single lock file per context (table), and will be competing with other instances
 * to perform writes, so it should handle these cases accordingly (using conditional writes).
 */
public interface ConditionalWriteLockService extends AutoCloseable {
  /**
   * Tries once to create or update a lock file.
   * @param newLockData The new data to update the lock file with.
   * @param previousLockFile The previous lock file, use this to conditionally update the lock file.
   * @return A pair containing the result state and the new lock file (if successful)
   */
  Pair<LockUpdateResult, ConditionalWriteLockFile> tryCreateOrUpdateLockFile(
      ConditionalWriteLockData newLockData,
      ConditionalWriteLockFile previousLockFile);

  /**
   * Tries to create or update a lock file while retrying N times.
   * All non pre-condition failure related errors should be retried.
   * @param newLockDataSupplier The new data supplier
   * @param previousLockFile The previous lock file
   * @param retryCount Number of retries to attempt
   * @return A pair containing the result state and the new lock file (if successful)
   */
  Pair<LockUpdateResult, ConditionalWriteLockFile> tryCreateOrUpdateLockFileWithRetry(
      Supplier<ConditionalWriteLockData> newLockDataSupplier,
      ConditionalWriteLockFile previousLockFile,
      long retryCount);

  /**
   * Gets the current lock file.
   * @return The lock retrieve result and the current lock file if successfully retrieved.
   * */
  Pair<LockGetResult, ConditionalWriteLockFile> getCurrentLockFile();
}
