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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hudi.client.transaction.lock.FileSystemBasedLockProvider;
import org.apache.hudi.common.config.LockConfiguration;
import org.apache.hudi.common.testutils.minicluster.HdfsTestService;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieLockException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.apache.hudi.common.config.LockConfiguration.FILESYSTEM_LOCK_EXPIRE_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.FILESYSTEM_LOCK_PATH_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.LOCK_ACQUIRE_NUM_RETRIES_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.LOCK_ACQUIRE_RETRY_WAIT_TIME_IN_MILLIS_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY;

public class TestFileBasedLockProvider {
  private static HdfsTestService hdfsTestService;
  private static MiniDFSCluster dfsCluster;
  private static LockConfiguration lockConfiguration;
  private static Configuration hadoopConf;

  @BeforeAll
  public static void setup() throws IOException {
    hdfsTestService = new HdfsTestService();
    dfsCluster = hdfsTestService.start(true);
    hadoopConf = dfsCluster.getFileSystem().getConf();

    Properties properties = new Properties();
    properties.setProperty(FILESYSTEM_LOCK_PATH_PROP_KEY, "/tmp/");
    properties.setProperty(FILESYSTEM_LOCK_EXPIRE_PROP_KEY, "1");
    properties.setProperty(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY, "1000");
    properties.setProperty(LOCK_ACQUIRE_RETRY_WAIT_TIME_IN_MILLIS_PROP_KEY, "1000");
    properties.setProperty(LOCK_ACQUIRE_NUM_RETRIES_PROP_KEY, "3");
    lockConfiguration = new LockConfiguration(properties);
  }

  @AfterAll
  public static void cleanUpAfterAll() throws IOException {
    Path workDir = dfsCluster.getFileSystem().getWorkingDirectory();
    FileSystem fs = workDir.getFileSystem(hdfsTestService.getHadoopConf());
    fs.delete(new Path("/tmp"), true);
    if (hdfsTestService != null) {
      hdfsTestService.stop();
      hdfsTestService = null;
    }
  }

  @AfterEach
  public void cleanUpAfterEach() throws IOException {
    Path workDir = dfsCluster.getFileSystem().getWorkingDirectory();
    FileSystem fs = workDir.getFileSystem(hdfsTestService.getHadoopConf());
    fs.delete(new Path("/tmp/lock"), true);
  }

  @Test
  public void testAcquireLock() {
    FileSystemBasedLockProvider fileBasedLockProvider = new FileSystemBasedLockProvider(lockConfiguration, hadoopConf);
    Assertions.assertTrue(fileBasedLockProvider.tryLock(lockConfiguration.getConfig()
          .getLong(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY), TimeUnit.MILLISECONDS));
    fileBasedLockProvider.unlock();
  }

  @Test
  public void testAcquireLockWithDefaultPath() {
    lockConfiguration.getConfig().remove(FILESYSTEM_LOCK_PATH_PROP_KEY);
    lockConfiguration.getConfig().setProperty(HoodieWriteConfig.BASE_PATH.key(), "/tmp/");
    FileSystemBasedLockProvider fileBasedLockProvider = new FileSystemBasedLockProvider(lockConfiguration, hadoopConf);
    Assertions.assertTrue(fileBasedLockProvider.tryLock(lockConfiguration.getConfig()
          .getLong(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY), TimeUnit.MILLISECONDS));
    fileBasedLockProvider.unlock();
    lockConfiguration.getConfig().setProperty(FILESYSTEM_LOCK_PATH_PROP_KEY, "/tmp/");
  }

  @Test
  public void testUnLock() {
    FileSystemBasedLockProvider fileBasedLockProvider = new FileSystemBasedLockProvider(lockConfiguration, hadoopConf);
    Assertions.assertTrue(fileBasedLockProvider.tryLock(lockConfiguration.getConfig()
          .getLong(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY), TimeUnit.MILLISECONDS));
    fileBasedLockProvider.unlock();
    Assertions.assertTrue(fileBasedLockProvider.tryLock(lockConfiguration.getConfig()
          .getLong(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY), TimeUnit.MILLISECONDS));
  }

  @Test
  public void testReentrantLock() {
    FileSystemBasedLockProvider fileBasedLockProvider = new FileSystemBasedLockProvider(lockConfiguration, hadoopConf);
    Assertions.assertTrue(fileBasedLockProvider.tryLock(lockConfiguration.getConfig()
          .getLong(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY), TimeUnit.MILLISECONDS));
    Assertions.assertFalse(fileBasedLockProvider.tryLock(lockConfiguration.getConfig()
            .getLong(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY), TimeUnit.MILLISECONDS));
    fileBasedLockProvider.unlock();
  }

  @Test
  public void testUnlockWithoutLock() {
    try {
      FileSystemBasedLockProvider fileBasedLockProvider = new FileSystemBasedLockProvider(lockConfiguration, hadoopConf);
      fileBasedLockProvider.unlock();
    } catch (HoodieLockException e) {
      Assertions.fail();
    }
  }

}
