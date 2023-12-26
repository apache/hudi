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

package org.apache.hudi.client;

import org.apache.hudi.client.transaction.lock.FileSystemBasedLockProvider;
import org.apache.hudi.client.transaction.lock.LockInfo;
import org.apache.hudi.common.config.LockConfiguration;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieWriteConfig;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.apache.hudi.common.config.LockConfiguration.FILESYSTEM_LOCK_EXPIRE_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.FILESYSTEM_LOCK_PATH_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.LOCK_ACQUIRE_NUM_RETRIES_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.LOCK_ACQUIRE_RETRY_WAIT_TIME_IN_MILLIS_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestFileBasedLockProvider {

  @TempDir
  Path tempDir;
  String basePath;
  LockConfiguration lockConfiguration;
  Configuration hadoopConf;

  @BeforeEach
  public void setUp() throws IOException {
    basePath = tempDir.toUri().getPath();
    Properties properties = new Properties();
    properties.setProperty(FILESYSTEM_LOCK_PATH_PROP_KEY, basePath);
    properties.setProperty(FILESYSTEM_LOCK_EXPIRE_PROP_KEY, "1");
    properties.setProperty(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY, "1000");
    properties.setProperty(LOCK_ACQUIRE_RETRY_WAIT_TIME_IN_MILLIS_PROP_KEY, "1000");
    properties.setProperty(LOCK_ACQUIRE_NUM_RETRIES_PROP_KEY, "3");
    lockConfiguration = new LockConfiguration(properties);
    hadoopConf = new Configuration();
  }

  @Test
  public void testAcquireLock() {
    FileSystemBasedLockProvider fileBasedLockProvider = new FileSystemBasedLockProvider(lockConfiguration, hadoopConf);
    assertTrue(fileBasedLockProvider.tryLock(lockConfiguration.getConfig()
        .getLong(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY), TimeUnit.MILLISECONDS));
    fileBasedLockProvider.unlock();
  }

  @Test
  public void testAcquireLockWithDefaultPath() {
    lockConfiguration.getConfig().remove(FILESYSTEM_LOCK_PATH_PROP_KEY);
    lockConfiguration.getConfig().setProperty(HoodieWriteConfig.BASE_PATH.key(), basePath);
    FileSystemBasedLockProvider fileBasedLockProvider = new FileSystemBasedLockProvider(lockConfiguration, hadoopConf);
    assertTrue(fileBasedLockProvider.tryLock(lockConfiguration.getConfig()
        .getLong(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY), TimeUnit.MILLISECONDS));
    fileBasedLockProvider.unlock();
    lockConfiguration.getConfig().setProperty(FILESYSTEM_LOCK_PATH_PROP_KEY, basePath);
  }

  @Test
  public void testUnLock() {
    FileSystemBasedLockProvider fileBasedLockProvider = new FileSystemBasedLockProvider(lockConfiguration, hadoopConf);
    assertTrue(fileBasedLockProvider.tryLock(lockConfiguration.getConfig()
        .getLong(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY), TimeUnit.MILLISECONDS));
    fileBasedLockProvider.unlock();
    assertTrue(fileBasedLockProvider.tryLock(lockConfiguration.getConfig()
        .getLong(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY), TimeUnit.MILLISECONDS));
  }

  @Test
  public void testReentrantLock() {
    FileSystemBasedLockProvider fileBasedLockProvider = new FileSystemBasedLockProvider(lockConfiguration, hadoopConf);
    assertTrue(fileBasedLockProvider.tryLock(lockConfiguration.getConfig()
        .getLong(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY), TimeUnit.MILLISECONDS));
    assertFalse(fileBasedLockProvider.tryLock(lockConfiguration.getConfig()
        .getLong(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY), TimeUnit.MILLISECONDS));
    fileBasedLockProvider.unlock();
  }

  @Test
  public void testUnlockWithoutLock() {
    assertDoesNotThrow(() -> {
      FileSystemBasedLockProvider fileBasedLockProvider = new FileSystemBasedLockProvider(lockConfiguration, hadoopConf);
      fileBasedLockProvider.unlock();
    });
  }

  @Test
  public void testUnlockInMultiThread() throws IOException {

    String lockDirectory = lockConfiguration.getConfig().getString(FILESYSTEM_LOCK_PATH_PROP_KEY, null);
    if (StringUtils.isNullOrEmpty(lockDirectory)) {
      lockDirectory = lockConfiguration.getConfig().getString(HoodieWriteConfig.BASE_PATH.key())
          + org.apache.hadoop.fs.Path.SEPARATOR + HoodieTableMetaClient.METAFOLDER_NAME;
    }
    org.apache.hadoop.fs.Path lockFile = new org.apache.hadoop.fs.Path(lockDirectory + org.apache.hadoop.fs.Path.SEPARATOR + "lock");
    FileSystem fs = FSUtils.getFs(lockFile.toString(), hadoopConf);
    // lockOne create lock first
    FileSystemBasedLockProvider fileBasedLockProviderOne = new FileSystemBasedLockProvider(lockConfiguration, hadoopConf);
    assertTrue(fileBasedLockProviderOne.tryLock(lockConfiguration.getConfig()
        .getLong(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY), TimeUnit.MILLISECONDS));
    assertTrue(fs.exists(lockFile));

    FileSystemBasedLockProvider fileBasedLockProviderTwo = new FileSystemBasedLockProvider(lockConfiguration, hadoopConf);
    // lockTwo lock fail
    assertFalse(fileBasedLockProviderTwo.tryLock(lockConfiguration.getConfig()
        .getLong(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY), TimeUnit.MILLISECONDS));
    // lockTwo can not release lock
    fileBasedLockProviderTwo.unlock();
    assertTrue(fs.exists(lockFile));
    fileBasedLockProviderTwo.close();
    assertTrue(fs.exists(lockFile));

    // the same lockTwo
    FileSystemBasedLockProvider fileBasedLockProviderThree = new FileSystemBasedLockProvider(lockConfiguration, hadoopConf);
    assertFalse(fileBasedLockProviderThree.tryLock(lockConfiguration.getConfig()
        .getLong(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY), TimeUnit.MILLISECONDS));
    fileBasedLockProviderThree.unlock();
    assertTrue(fs.exists(lockFile));
    fileBasedLockProviderThree.close();
    assertTrue(fs.exists(lockFile));

    // lockOne release lock
    fileBasedLockProviderOne.unlock();
    assertFalse(fs.exists(lockFile));

    // test close release lock
    assertTrue(fileBasedLockProviderOne.tryLock(lockConfiguration.getConfig()
        .getLong(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY), TimeUnit.MILLISECONDS));
    assertTrue(fs.exists(lockFile));
    fileBasedLockProviderOne.close();
    assertFalse(fs.exists(lockFile));

    // lockTwo can get lock
    assertTrue(fileBasedLockProviderTwo.tryLock(lockConfiguration.getConfig()
        .getLong(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY), TimeUnit.MILLISECONDS));
    assertTrue(fs.exists(lockFile));
  }

  @Test
  public void testLockExpire() throws IOException, InterruptedException {
    String lockDirectory = lockConfiguration.getConfig().getString(FILESYSTEM_LOCK_PATH_PROP_KEY, null);
    if (StringUtils.isNullOrEmpty(lockDirectory)) {
      lockDirectory = lockConfiguration.getConfig().getString(HoodieWriteConfig.BASE_PATH.key())
          + org.apache.hadoop.fs.Path.SEPARATOR + HoodieTableMetaClient.METAFOLDER_NAME;
    }
    org.apache.hadoop.fs.Path lockFile = new org.apache.hadoop.fs.Path(lockDirectory + org.apache.hadoop.fs.Path.SEPARATOR + "lock");
    FileSystem fs = FSUtils.getFs(lockFile.toString(), hadoopConf);
    FileSystemBasedLockProvider fileBasedLockProviderOne = new FileSystemBasedLockProvider(lockConfiguration, hadoopConf);
    FileSystemBasedLockProvider fileBasedLockProviderTwo = new FileSystemBasedLockProvider(lockConfiguration, hadoopConf);

    // if lock expire, current lock will be released, and current lock can not release others
    assertTrue(fileBasedLockProviderOne.tryLock(lockConfiguration.getConfig()
        .getLong(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY), TimeUnit.MILLISECONDS));
    Thread.sleep(60000);
    assertTrue(fs.exists(lockFile));
    FSDataInputStream fis = fs.open(lockFile);
    String lockJson = FileIOUtils.readAsUTFString(fis);
    fis.close();
    LockInfo lockInfoBaseOne = LockInfo.getLockInfoByJson(lockJson);
    LockInfo lockInfoOne = fileBasedLockProviderOne.getLockInfo();
    assertEquals(lockInfoBaseOne.getLockCreateTime(), lockInfoOne.getLockCreateTime());

    assertTrue(fileBasedLockProviderTwo.tryLock(lockConfiguration.getConfig()
        .getLong(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY), TimeUnit.MILLISECONDS));
    fis = fs.open(lockFile);
    lockJson = FileIOUtils.readAsUTFString(fis);
    LockInfo lockInfoBaseTwo = LockInfo.getLockInfoByJson(lockJson);
    LockInfo lockInfoTwo = fileBasedLockProviderTwo.getLockInfo();
    assertEquals(lockInfoBaseTwo.getLockCreateTime(), lockInfoTwo.getLockCreateTime());
    assertNotEquals(lockInfoBaseOne.getLockCreateTime(), lockInfoBaseTwo.getLockCreateTime());

    // lockOne has expired, can not release other lock
    fileBasedLockProviderOne.unlock();
    assertTrue(fs.exists(lockFile));
    fileBasedLockProviderOne.close();
    assertTrue(fs.exists(lockFile));
  }
}
