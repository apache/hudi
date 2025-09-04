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

import org.apache.hudi.client.transaction.lock.models.LockUpsertResult;
import org.apache.hudi.client.transaction.lock.models.StorageLockData;
import org.apache.hudi.client.transaction.lock.models.StorageLockFile;
import org.apache.hudi.client.transaction.lock.models.LockGetResult;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;

/**
 * Defines a contract for a service which should be able to perform conditional writes to object storage.
 * It expects to be interacting with a single lock file per context (table), and will be competing with other instances
 * to perform writes, so it should handle these cases accordingly (using conditional writes).
 */
public interface StorageLockClient extends AutoCloseable {
  /**
   * Tries to create or update a lock file.
   * All non pre-condition failure related errors should be returned as UNKNOWN_ERROR.
   * @param newLockData The new data to write to the lock file
   * @param previousLockFile The previous lock file
   * @return A pair containing the result state and the new lock file (if successful)
   */
  Pair<LockUpsertResult, Option<StorageLockFile>> tryUpsertLockFile(
      StorageLockData newLockData,
      Option<StorageLockFile> previousLockFile);

  /**
   * Reads the current lock file.
   * @return The lock retrieve result and the current lock file if successfully retrieved.
   * */
  Pair<LockGetResult, Option<StorageLockFile>> readCurrentLockFile();
  
  /**
   * Reads a small JSON configuration file from the specified path.
   * This method is intended for reading small configuration files (e.g., audit config, feature flags)
   * and loads the entire file into memory. Do not use for large files.
   * 
   * @param filePath The path to the small JSON config file to read
   * @param checkExistsFirst If true, performs a lightweight existence check before attempting to read.
   *                         Recommended when the file is unlikely to exist (e.g., optional configs).
   * @return An Option containing the JSON content as a string if successful, Option.empty() otherwise
   */
  Option<String> readSmallJsonConfig(String filePath, boolean checkExistsFirst);
}
