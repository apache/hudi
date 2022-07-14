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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.config.LockConfiguration;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.lock.LockProvider;
import org.apache.hudi.common.lock.LockState;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieLockException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.TimeUnit;

import static org.apache.hudi.common.config.LockConfiguration.FILESYSTEM_LOCK_EXPIRE_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.FILESYSTEM_LOCK_PATH_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.LOCK_ACQUIRE_NUM_RETRIES_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.LOCK_ACQUIRE_RETRY_WAIT_TIME_IN_MILLIS_PROP_KEY;

/**
 * A FileSystem based lock. This {@link LockProvider} implementation allows to lock table operations
 * using DFS. Users might need to manually clean the Locker's path if writeClient crash and never run again.
 * NOTE: This only works for DFS with atomic create/delete operation
 */
public class FileSystemBasedLockProvider implements LockProvider<String>, Serializable {

  private static final Logger LOG = LogManager.getLogger(FileSystemBasedLockProvider.class);

  private static final String LOCK_FILE_NAME = "lock";

  private final int retryMaxCount;
  private final int retryWaitTimeMs;
  private final int lockTimeoutMinutes;
  private transient FileSystem fs;
  private transient Path lockFile;
  protected LockConfiguration lockConfiguration;

  public FileSystemBasedLockProvider(final LockConfiguration lockConfiguration, final Configuration configuration) {
    checkRequiredProps(lockConfiguration);
    this.lockConfiguration = lockConfiguration;
    String lockDirectory = lockConfiguration.getConfig().getString(FILESYSTEM_LOCK_PATH_PROP_KEY, null);
    if (StringUtils.isNullOrEmpty(lockDirectory)) {
      lockDirectory = lockConfiguration.getConfig().getString(HoodieWriteConfig.BASE_PATH.key(), null);
    }
    this.retryWaitTimeMs = lockConfiguration.getConfig().getInteger(LOCK_ACQUIRE_RETRY_WAIT_TIME_IN_MILLIS_PROP_KEY);
    this.retryMaxCount = lockConfiguration.getConfig().getInteger(LOCK_ACQUIRE_NUM_RETRIES_PROP_KEY);
    this.lockTimeoutMinutes = lockConfiguration.getConfig().getInteger(FILESYSTEM_LOCK_EXPIRE_PROP_KEY);
    this.lockFile = new Path(lockDirectory + Path.SEPARATOR + LOCK_FILE_NAME);
    this.fs = FSUtils.getFs(this.lockFile.toString(), configuration);
  }

  @Override
  public void close() {
    synchronized (LOCK_FILE_NAME) {
      try {
        fs.delete(this.lockFile, true);
      } catch (IOException e) {
        throw new HoodieLockException(generateLogStatement(LockState.FAILED_TO_RELEASE), e);
      }
    }
  }

  @Override
  public boolean tryLock(long time, TimeUnit unit) {
    try {
      synchronized (LOCK_FILE_NAME) {
        if (fs.exists(this.lockFile)) {
          // Check whether lock is already expired or not, if so try to delete lock file
          if (lockTimeoutMinutes != 0 && checkIfExpired()) {
            fs.delete(this.lockFile, true);
          }
        }
        acquireLock();
        return fs.exists(this.lockFile);
      }
    } catch (IOException | HoodieIOException e) {
      LOG.info(generateLogStatement(LockState.FAILED_TO_ACQUIRE), e);
      return false;
    }
  }

  @Override
  public void unlock() {
    synchronized (LOCK_FILE_NAME) {
      try {
        if (fs.exists(this.lockFile)) {
          fs.delete(this.lockFile, true);
        }
      } catch (IOException io) {
        throw new HoodieIOException(generateLogStatement(LockState.FAILED_TO_RELEASE), io);
      }
    }
  }

  @Override
  public String getLock() {
    return this.lockFile.toString();
  }

  private boolean checkIfExpired() {
    try {
      long modificationTime = fs.getFileStatus(this.lockFile).getModificationTime();
      if (System.currentTimeMillis() - modificationTime > lockTimeoutMinutes * 60 * 1000) {
        return true;
      }
    } catch (IOException | HoodieIOException e) {
      LOG.error(generateLogStatement(LockState.ALREADY_RELEASED) + " failed to get lockFile's modification time", e);
    }
    return false;
  }

  private void acquireLock() {
    try {
      fs.create(this.lockFile, false).close();
    } catch (IOException e) {
      throw new HoodieIOException(generateLogStatement(LockState.FAILED_TO_ACQUIRE), e);
    }
  }

  protected String generateLogStatement(LockState state) {
    return StringUtils.join(state.name(), " lock at: ", getLock());
  }

  private void checkRequiredProps(final LockConfiguration config) {
    ValidationUtils.checkArgument(config.getConfig().getString(FILESYSTEM_LOCK_PATH_PROP_KEY, null) != null
          || config.getConfig().getString(HoodieWriteConfig.BASE_PATH.key(), null) != null);
    ValidationUtils.checkArgument(config.getConfig().getInteger(LOCK_ACQUIRE_RETRY_WAIT_TIME_IN_MILLIS_PROP_KEY) > 0);
    ValidationUtils.checkArgument(config.getConfig().getInteger(LOCK_ACQUIRE_NUM_RETRIES_PROP_KEY) > 0);
    ValidationUtils.checkArgument(config.getConfig().getInteger(FILESYSTEM_LOCK_EXPIRE_PROP_KEY) >= 0);
  }
}
