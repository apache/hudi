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

import org.apache.hudi.client.transaction.lock.FileSystemBasedLockProvider;
import org.apache.hudi.common.config.LockConfiguration;
import org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieLockException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.apache.hudi.common.config.LockConfiguration.LOCK_ACQUIRE_EXPIRE_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.LOCK_ACQUIRE_NUM_RETRIES_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.LOCK_ACQUIRE_RETRY_WAIT_TIME_IN_MILLIS_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY;

public class TestFileSystemBasedLockProvider extends HoodieCommonTestHarness {

  private static LockConfiguration lockConfiguration;
  private static Properties properties;

  @BeforeEach
  public void init() throws IOException {
    initMetaClient();
    properties = new Properties();
    properties.setProperty(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY, "1000");
    properties.setProperty(LOCK_ACQUIRE_RETRY_WAIT_TIME_IN_MILLIS_PROP_KEY, "1000");
    properties.setProperty(LOCK_ACQUIRE_NUM_RETRIES_PROP_KEY, "6");
    properties.setProperty(LOCK_ACQUIRE_EXPIRE_PROP_KEY, "5");
    lockConfiguration = new LockConfiguration(properties);
    properties.setProperty("hoodie.base.path", this.metaClient.getBasePath());
    properties.setProperty("hoodie.client.heartbeat.interval_in_ms", "60000");
  }

  @Test
  public void testAcquireLock() {
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder().withProperties(properties).build();
    FileSystemBasedLockProvider fileSystemBasedLockProvider = new FileSystemBasedLockProvider(lockConfiguration, writeConfig, this.metaClient.getHadoopConf());
    String timestamp = HoodieInstantTimeGenerator.createNewInstantTime(1);
    Assertions.assertTrue(fileSystemBasedLockProvider.tryLockWithInstant(lockConfiguration.getConfig()
          .getLong(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY), TimeUnit.MILLISECONDS, Option.of(timestamp)));
    fileSystemBasedLockProvider.unlock();
  }

  @Test
  public void testUnLock() {
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder().withProperties(properties).build();
    FileSystemBasedLockProvider fileSystemBasedLockProvider = new FileSystemBasedLockProvider(lockConfiguration, writeConfig, this.metaClient.getHadoopConf());
    Assertions.assertTrue(fileSystemBasedLockProvider.tryLock(lockConfiguration.getConfig()
          .getLong(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY), TimeUnit.MILLISECONDS));
    fileSystemBasedLockProvider.unlock();
    fileSystemBasedLockProvider.tryLock(lockConfiguration.getConfig()
          .getLong(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY), TimeUnit.MILLISECONDS);
  }

  @Test
  public void testReentrantLock() {
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder().withProperties(properties).build();
    FileSystemBasedLockProvider fileSystemBasedLockProvider = new FileSystemBasedLockProvider(lockConfiguration, writeConfig, this.metaClient.getHadoopConf());
    fileSystemBasedLockProvider.unlock();
    Assertions.assertTrue(fileSystemBasedLockProvider.tryLock(lockConfiguration.getConfig()
          .getLong(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY), TimeUnit.MILLISECONDS));
    try {
      boolean lockHold = fileSystemBasedLockProvider.tryLock(lockConfiguration.getConfig()
            .getLong(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY), TimeUnit.MILLISECONDS);
      Assertions.assertFalse(lockHold);
    } catch (HoodieLockException e) {
      // pass
    }
    fileSystemBasedLockProvider.unlock();
  }

  @Test
  public void testReentrantLockWithExpire() {
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder().withProperties(properties).build();
    writeConfig.setValue(writeConfig.WRITER_CLIENT_ID.key(), "1");
    FileSystemBasedLockProvider fileSystemBasedLockProvider = new FileSystemBasedLockProvider(lockConfiguration, writeConfig, this.metaClient.getHadoopConf());
    String timestamp = HoodieInstantTimeGenerator.createNewInstantTime(1);
    Assertions.assertTrue(fileSystemBasedLockProvider.tryLockWithInstant(lockConfiguration.getConfig()
          .getLong(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY), TimeUnit.MILLISECONDS, Option.of(timestamp)));
    try {
      boolean lockHold = fileSystemBasedLockProvider.tryLockWithInstant(lockConfiguration.getConfig()
            .getLong(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY), TimeUnit.MILLISECONDS, Option.of(timestamp));
      Assertions.assertTrue(lockHold);
    } catch (HoodieLockException e) {
      //pass
    }
    fileSystemBasedLockProvider.unlock();
  }

  @Test
  public void testReentrantLockWithDiffWriters() {
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder().withProperties(properties).build();
    writeConfig.setValue(writeConfig.WRITER_CLIENT_ID.key(), "1");
    FileSystemBasedLockProvider fileSystemBasedLockProvider1 = new FileSystemBasedLockProvider(lockConfiguration, writeConfig, this.metaClient.getHadoopConf());
    String timestamp = HoodieInstantTimeGenerator.createNewInstantTime(1);
    Assertions.assertTrue(fileSystemBasedLockProvider1.tryLockWithInstant(lockConfiguration.getConfig()
          .getLong(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY), TimeUnit.MILLISECONDS, Option.of(timestamp)));

    writeConfig.setValue(writeConfig.WRITER_CLIENT_ID.key(), "2");
    lockConfiguration.getConfig().setProperty(LOCK_ACQUIRE_EXPIRE_PROP_KEY, "100");
    FileSystemBasedLockProvider fileSystemBasedLockProvider2 = new FileSystemBasedLockProvider(lockConfiguration, writeConfig, this.metaClient.getHadoopConf());
    try {
      boolean lockHold = fileSystemBasedLockProvider2.tryLockWithInstant(lockConfiguration.getConfig()
            .getLong(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY), TimeUnit.MILLISECONDS, Option.of(timestamp));
      Assertions.assertFalse(lockHold);
    } catch (HoodieLockException e) {
      //pass
    }
    fileSystemBasedLockProvider1.unlock();
    fileSystemBasedLockProvider2.unlock();
  }

  @Test
  public void testUnlockWithoutLock() {
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder().withProperties(properties).build();
    FileSystemBasedLockProvider fileSystemBasedLockProvider = new FileSystemBasedLockProvider(lockConfiguration, writeConfig, this.metaClient.getHadoopConf());
    fileSystemBasedLockProvider.unlock();
  }

}
