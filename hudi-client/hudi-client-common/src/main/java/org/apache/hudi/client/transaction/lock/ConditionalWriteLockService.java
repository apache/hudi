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
import org.apache.hudi.common.util.Option;

import java.util.function.Supplier;

/**
 * Defines a contract for a service which should be able to perform conditional writes to object storage.
 * It expects to be interacting with a single lock file per context (table), and will be competing with other instances
 * to perform writes, so it should handle these cases accordingly (using conditional writes).
 */
public interface ConditionalWriteLockService extends AutoCloseable {
  /**
   * Tries once to create or update a lock file. If previousLockFile is null, this indicates we are creating the lock file.
   * @param newLockData The new data to update the lock file with.
   * @param previousLockFile The previous lock file, use this to conditionally update the lock file.
   *                         If null, there is no lockfile yet, we attempt to create it.
   * @return The new lock file if it was successfully created, otherwise null.
   */
  ConditionalWriteLockFile tryCreateOrUpdateLockFile(ConditionalWriteLockData newLockData, ConditionalWriteLockFile previousLockFile);

  /**
   * Tries to create or update a lock file while retrying N times. If previousLockFile is null, this indicates we are creating the lock file.
   * All non pre-condition failure related errors should be retried.
   * @param newLockDataSupplier The new data to update the lock file with, passed as supplier so the lease can be extended properly.
   * @param previousLockFile The previous lock file, use this to conditionally update the lock file. If null, there is no lockfile yet, we attempt to create it.
   * @param retryCount Number of retries to attempt.
   * @return The new lock file if it was successfully created, null if we encounter a precondition failure.
   * Empty option if we are unsure about the state of the current lock file, but we were unable to write the new lock file.
   */
  Option<ConditionalWriteLockFile> tryCreateOrUpdateLockFileWithRetry(Supplier<ConditionalWriteLockData> newLockDataSupplier, ConditionalWriteLockFile previousLockFile, long retryCount);

  /**
   * Gets the current lock file.
   * @return The current lock file, deserialized into ConditionalWriteLockFile.
   * Empty option if it does not exist. Null if we were unable to retrieve it (unknown state).
   */
  Option<ConditionalWriteLockFile> getCurrentLockFile();
}
