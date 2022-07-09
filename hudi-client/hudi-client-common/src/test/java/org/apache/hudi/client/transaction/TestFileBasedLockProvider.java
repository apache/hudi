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

package org.apache.hudi.client.transaction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hudi.client.transaction.lock.FileBasedLockProvider;
import org.apache.hudi.common.config.LockConfiguration;
import org.apache.hudi.common.testutils.minicluster.HdfsTestService;
import org.apache.hudi.exception.HoodieLockException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.apache.hudi.common.config.LockConfiguration.FILESYSTEM_LOCK_EXPIRE_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.FILESYSTEM_LOCK_PATH_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.LOCK_ACQUIRE_NUM_RETRIES_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY;

public class TestFileBasedLockProvider {
  private static final Logger LOG = LogManager.getLogger(TestFileBasedLockProvider.class);
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
    properties.setProperty(LOCK_ACQUIRE_NUM_RETRIES_PROP_KEY, "3");
    lockConfiguration = new LockConfiguration(properties);
  }

  @Test
  public void testAcquireLock() {
    FileBasedLockProvider fileBasedLockProvider = new FileBasedLockProvider(lockConfiguration, hadoopConf);
    Assertions.assertTrue(fileBasedLockProvider.tryLock(lockConfiguration.getConfig()
          .getLong(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY), TimeUnit.MILLISECONDS));
    fileBasedLockProvider.unlock();
  }

  @Test
  public void testUnLock() {
    FileBasedLockProvider fileBasedLockProvider = new FileBasedLockProvider(lockConfiguration, hadoopConf);
    Assertions.assertTrue(fileBasedLockProvider.tryLock(lockConfiguration.getConfig()
          .getLong(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY), TimeUnit.MILLISECONDS));
    fileBasedLockProvider.unlock();
    fileBasedLockProvider.tryLock(lockConfiguration.getConfig()
          .getLong(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY), TimeUnit.MILLISECONDS);
  }

  @Test
  public void testReentrantLock() {
    FileBasedLockProvider fileBasedLockProvider = new FileBasedLockProvider(lockConfiguration, hadoopConf);
    Assertions.assertTrue(fileBasedLockProvider.tryLock(lockConfiguration.getConfig()
          .getLong(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY), TimeUnit.MILLISECONDS));
    try {
      fileBasedLockProvider.tryLock(lockConfiguration.getConfig()
            .getLong(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY), TimeUnit.MILLISECONDS);
      Assertions.fail();
    } catch (HoodieLockException e) {
      // expected
    }
    fileBasedLockProvider.unlock();
  }

  @Test
  public void testUnlockWithoutLock() {
    FileBasedLockProvider fileBasedLockProvider = new FileBasedLockProvider(lockConfiguration, hadoopConf);
    fileBasedLockProvider.unlock();
  }

}
