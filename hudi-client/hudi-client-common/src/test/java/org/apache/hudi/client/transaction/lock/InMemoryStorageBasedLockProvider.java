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

package org.apache.hudi.client.transaction.lock;

import org.apache.hudi.common.config.LockConfiguration;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.storage.StorageConfiguration;

import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * {@link StorageBasedLockProvider} with unchanged locking logic, backed by in-memory storage that
 * keeps the conditional-write semantics of S3, GCS and Azure: a write succeeds only if the lock file
 * still has the version the writer last saw, and a create only if no lock file exists.
 */
public class InMemoryStorageBasedLockProvider extends StorageBasedLockProvider {

  private static final Map<String, StorageLockFile> LOCK_FILES = new ConcurrentHashMap<>();

  public InMemoryStorageBasedLockProvider(LockConfiguration lockConfiguration, StorageConfiguration<?> conf) {
    super(
        UUID.randomUUID().toString(),
        lockConfiguration.getConfig(),
        LockProviderHeartbeatManager::new,
        (ownerId, lockFilePath, props) -> new InMemoryStorageLockClient(lockFilePath),
        LoggerFactory.getLogger(InMemoryStorageBasedLockProvider.class),
        null);
  }

  public static void reset() {
    LOCK_FILES.clear();
  }

  /**
   * Returns the lock file currently stored for a table, as another writer would read it.
   */
  public static Option<StorageLockFile> storedLock(String lockFilePath) {
    return Option.ofNullable(LOCK_FILES.get(lockFilePath));
  }

  static class InMemoryStorageLockClient implements StorageLockClient {
    private final String lockFilePath;

    InMemoryStorageLockClient(String lockFilePath) {
      this.lockFilePath = lockFilePath;
    }

    @Override
    public Pair<LockUpsertResult, Option<StorageLockFile>> tryUpsertLockFile(
        StorageLockData newLockData, Option<StorageLockFile> previousLockFile) {
      synchronized (LOCK_FILES) {
        StorageLockFile current = LOCK_FILES.get(lockFilePath);
        boolean preconditionHolds = previousLockFile.isPresent()
            ? current != null && current.getVersionId().equals(previousLockFile.get().getVersionId())
            : current == null;
        if (!preconditionHolds) {
          return Pair.of(LockUpsertResult.ACQUIRED_BY_OTHERS, Option.empty());
        }
        StorageLockFile written = new StorageLockFile(newLockData, UUID.randomUUID().toString());
        LOCK_FILES.put(lockFilePath, written);
        return Pair.of(LockUpsertResult.SUCCESS, Option.of(written));
      }
    }

    @Override
    public Pair<LockGetResult, Option<StorageLockFile>> readCurrentLockFile() {
      StorageLockFile current = LOCK_FILES.get(lockFilePath);
      return current == null
          ? Pair.of(LockGetResult.NOT_EXISTS, Option.empty())
          : Pair.of(LockGetResult.SUCCESS, Option.of(current));
    }

    @Override
    public Option<String> readObject(String filePath, boolean checkExistsFirst) {
      return Option.empty();
    }

    @Override
    public boolean writeObject(String filePath, String content) {
      return true;
    }

    @Override
    public void close() {
    }
  }
}
