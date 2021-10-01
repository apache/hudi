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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.config.LockConfiguration;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.lock.LockProvider;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieLockException;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.TimeUnit;

import static org.apache.hudi.common.config.LockConfiguration.FILESYSTEM_LOCK_PATH_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.LOCK_ACQUIRE_NUM_RETRIES_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.LOCK_ACQUIRE_RETRY_WAIT_TIME_IN_MILLIS_PROP_KEY;

/**
 * This lock provider is used for testing purposes only. It provides a simple file system based lock using HDFS atomic
 * create operation. This lock does not support cleaning/expiring the lock after a failed write hence cannot be used
 * in production environments.
 */
public class FileSystemBasedLockProviderTestClass implements LockProvider<String>, Serializable {

  private static final String LOCK_NAME = "acquired";

  private String lockPath;
  private transient FileSystem fs;
  protected LockConfiguration lockConfiguration;

  public FileSystemBasedLockProviderTestClass(final LockConfiguration lockConfiguration, final Configuration configuration) {
    this.lockConfiguration = lockConfiguration;
    this.lockPath = lockConfiguration.getConfig().getString(FILESYSTEM_LOCK_PATH_PROP_KEY);
    this.fs = FSUtils.getFs(this.lockPath, configuration);
  }

  public void acquireLock() {
    try {
      fs.create(new Path(lockPath + "/" + LOCK_NAME), false).close();
    } catch (IOException e) {
      throw new HoodieIOException("Failed to acquire lock", e);
    }
  }

  @Override
  public void close() {
    try {
      fs.delete(new Path(lockPath + "/" + LOCK_NAME), true);
    } catch (IOException e) {
      throw new HoodieLockException("Unable to release lock", e);
    }
  }

  @Override
  public boolean tryLock(long time, TimeUnit unit) {
    try {
      int numRetries = 0;
      while (fs.exists(new Path(lockPath + "/" + LOCK_NAME))
          && (numRetries <= lockConfiguration.getConfig().getInteger(LOCK_ACQUIRE_NUM_RETRIES_PROP_KEY))) {
        Thread.sleep(lockConfiguration.getConfig().getInteger(LOCK_ACQUIRE_RETRY_WAIT_TIME_IN_MILLIS_PROP_KEY));
      }
      synchronized (LOCK_NAME) {
        if (fs.exists(new Path(lockPath + "/" + LOCK_NAME))) {
          return false;
        }
        acquireLock();
      }
      return true;
    } catch (IOException | InterruptedException e) {
      throw new HoodieLockException("Failed to acquire lock", e);
    }
  }

  @Override
  public void unlock() {
    try {
      if (fs.exists(new Path(lockPath + "/" + LOCK_NAME))) {
        fs.delete(new Path(lockPath + "/" + LOCK_NAME), true);
      }
    } catch (IOException io) {
      throw new HoodieIOException("Unable to delete lock on disk", io);
    }
  }

  @Override
  public String getLock() {
    try {
      return fs.listStatus(new Path(lockPath))[0].getPath().toString();
    } catch (Exception e) {
      throw new HoodieLockException("Failed to retrieve lock status from lock path " + lockPath);
    }
  }
}
