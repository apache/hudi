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

package org.apache.hudi.client.transaction;

import org.apache.hudi.common.config.LockConfiguration;
import org.apache.hudi.common.lock.LockProvider;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieLockException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.storage.StorageConfiguration;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.TimeUnit;

import static org.apache.hudi.common.config.LockConfiguration.FILESYSTEM_LOCK_PATH_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.LOCK_ACQUIRE_NUM_RETRIES_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.LOCK_ACQUIRE_RETRY_WAIT_TIME_IN_MILLIS_PROP_KEY;

/**
 * This lock provider is used for testing purposes only. It provides a simple file system based lock
 * using filesystem's atomic create operation. This lock does not support cleaning/expiring the lock
 * after a failed write. Must not be used in production environments.
 */
public class FileSystemBasedLockProviderTestClass implements LockProvider<String>, Serializable {

  private static final String LOCK = "lock";

  private final int retryMaxCount;
  private final int retryWaitTimeMs;
  private transient FileSystem fs;
  private transient Path lockFile;
  protected LockConfiguration lockConfiguration;

  public FileSystemBasedLockProviderTestClass(final LockConfiguration lockConfiguration, final StorageConfiguration<?> configuration) {
    this.lockConfiguration = lockConfiguration;
    final String lockDirectory = lockConfiguration.getConfig().getString(FILESYSTEM_LOCK_PATH_PROP_KEY);
    this.retryWaitTimeMs = lockConfiguration.getConfig().getInteger(LOCK_ACQUIRE_RETRY_WAIT_TIME_IN_MILLIS_PROP_KEY);
    this.retryMaxCount = lockConfiguration.getConfig().getInteger(LOCK_ACQUIRE_NUM_RETRIES_PROP_KEY);
    this.lockFile = new Path(lockDirectory + "/" + LOCK);
    this.fs = HadoopFSUtils.getFs(this.lockFile.toString(), configuration);
  }

  @Override
  public void close() {
    synchronized (LOCK) {
      try {
        fs.delete(this.lockFile, true);
      } catch (IOException e) {
        throw new HoodieLockException("Unable to release lock: " + getLock(), e);
      }
    }
  }

  @Override
  public boolean tryLock(long time, TimeUnit unit) {
    try {
      int numRetries = 0;
      synchronized (LOCK) {
        while (fs.exists(this.lockFile)) {
          LOCK.wait(retryWaitTimeMs);
          numRetries++;
          if (numRetries > retryMaxCount) {
            return false;
          }
        }
        acquireLock();
        return fs.exists(this.lockFile);
      }
    } catch (IOException | InterruptedException e) {
      throw new HoodieLockException("Failed to acquire lock: " + getLock(), e);
    }
  }

  @Override
  public void unlock() {
    synchronized (LOCK) {
      try {
        if (fs.exists(this.lockFile)) {
          fs.delete(this.lockFile, true);
        }
      } catch (IOException io) {
        throw new HoodieIOException("Unable to delete lock " + getLock() + "on disk", io);
      }
    }
  }

  @Override
  public String getLock() {
    return this.lockFile.toString();
  }

  private void acquireLock() {
    try {
      fs.create(this.lockFile, false).close();
    } catch (IOException e) {
      throw new HoodieIOException("Failed to acquire lock: " + getLock(), e);
    }
  }
}
