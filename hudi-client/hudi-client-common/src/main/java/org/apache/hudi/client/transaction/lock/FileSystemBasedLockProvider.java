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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.client.heartbeat.HoodieHeartbeatClient;
import org.apache.hudi.common.config.LockConfiguration;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.lock.LockProvider;
import org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieLockException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.Serializable;
import java.text.ParseException;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import static org.apache.hudi.common.config.LockConfiguration.LOCK_ACQUIRE_EXPIRE_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.LOCK_ACQUIRE_NUM_RETRIES_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.LOCK_ACQUIRE_RETRY_WAIT_TIME_IN_MILLIS_PROP_KEY;

public class FileSystemBasedLockProvider implements LockProvider<String>, Serializable {

  private static final Logger LOG = LogManager.getLogger(FileSystemBasedLockProvider.class);

  private static final String LOCK = "lock";

  private final int retryMaxCount;
  private final int retryWaitTimeMs;
  private final int expireTimeSec;
  private final long heartbeatInterval;
  private transient FileSystem fs;
  private transient Path lockFile;
  private transient String basePath;
  private transient String clientId;
  protected LockConfiguration lockConfiguration;

  public FileSystemBasedLockProvider(final LockConfiguration lockConfiguration, final HoodieWriteConfig writeConfig, final Configuration configuration) {
    this.lockConfiguration = lockConfiguration;
    this.retryWaitTimeMs = lockConfiguration.getConfig().getInteger(LOCK_ACQUIRE_RETRY_WAIT_TIME_IN_MILLIS_PROP_KEY);
    this.retryMaxCount = lockConfiguration.getConfig().getInteger(LOCK_ACQUIRE_NUM_RETRIES_PROP_KEY);
    this.expireTimeSec = lockConfiguration.getConfig().getInteger(LOCK_ACQUIRE_EXPIRE_PROP_KEY);
    this.heartbeatInterval = writeConfig.getHoodieClientHeartbeatIntervalInMs();
    this.basePath = writeConfig.getBasePath();
    this.clientId = writeConfig.getWriterClientId();
    this.lockFile = new Path(writeConfig.getBasePath() + "/.hoodie/" + LOCK);
    this.fs = FSUtils.getFs(this.lockFile.toString(), configuration);
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
    return tryLockWithInstant(time, unit, Option.empty());
  }

  @Override
  public boolean tryLockWithInstant(long time, TimeUnit unit, Option<String> timestamp) {
    try {
      int numRetries = 0;
      synchronized (LOCK) {
        while (fs.exists(this.lockFile)) {
          LOCK.wait(retryWaitTimeMs);
          numRetries++;
          if (numRetries > retryMaxCount) {
            return false;
          }
          if (isLockExpireOrBelongsToMe()) {
            fs.delete(this.lockFile, true);
          }
        }
        if (timestamp.isPresent()) {
          acquireLock(timestamp.get());
        } else {
          acquireLock();
        }
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
      fs.create(this.lockFile, false);
    } catch (IOException e) {
      throw new HoodieIOException("Failed to acquire lock: " + getLock(), e);
    }
  }

  /**
   * acquire lock, then set commit time and lock acquire time
   *
   * @param timestamp
   */
  private void acquireLock(String timestamp) {
    try {
      Date date = HoodieInstantTimeGenerator.parseDateFromInstantTime(timestamp);
      FSDataOutputStream stream = fs.create(this.lockFile, false);
      stream.writeLong(date.getTime());
      stream.writeLong(System.currentTimeMillis());
      stream.writeBytes(clientId);
      stream.close();
    } catch (IOException | ParseException e) {
      throw new HoodieException("Failed to acquire lock: " + getLock(), e);
    }
  }

  private boolean isLockExpireOrBelongsToMe() {
    try {
      FSDataInputStream stream = fs.open(lockFile);
      long instantTime = stream.readLong();
      long createTime = stream.readLong();
      String lockClientId = stream.readLine();

      String instant = HoodieInstantTimeGenerator.formatDate(new Date(instantTime));
      Long lastHeartbeatTime = HoodieHeartbeatClient.getLastHeartbeatTime(this.fs, this.basePath, instant);
      if (!StringUtils.isNullOrEmpty(lockClientId) && !this.clientId.isEmpty() && lockClientId.equals(this.clientId)) {
        return true;
      }

      // if the heartbeat of this instant already expired, check if lock is expire
      if (System.currentTimeMillis() - lastHeartbeatTime > this.heartbeatInterval + 1000) {
        if (System.currentTimeMillis() - createTime > this.expireTimeSec * 1000) {
          return true;
        }
      }
    } catch (IOException e) {
      LOG.warn("Failed to check expire: " + getLock(), e);
    }
    return false;
  }

}
