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
import org.apache.hudi.exception.HoodieLockException;
import org.apache.hudi.storage.StorageConfiguration;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Test LockProvider that verifies configuration properties are accessible.
 * This provider reads a specific test property from the StorageConfiguration
 * to verify that resources added via addResource() are accessible.
 */
public class ConfigurationVerifyingLockProvider implements LockProvider<ReentrantReadWriteLock>, Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(ConfigurationVerifyingLockProvider.class);
  private static final String TEST_CONFIG_KEY = "hudi.test.lock.config.verification";
  private static final String TEST_CONFIG_VALUE = "verified";

  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private final String verifiedConfigValue;
  private final StorageConfiguration<?> storageConf;

  public ConfigurationVerifyingLockProvider(final LockConfiguration lockConfiguration,
                                           final StorageConfiguration<?> conf) {
    this.storageConf = conf;
    // Verify that the configuration property is accessible
    Configuration hadoopConf = conf.unwrapAs(Configuration.class);
    this.verifiedConfigValue = hadoopConf.get(TEST_CONFIG_KEY);

    if (TEST_CONFIG_VALUE.equals(verifiedConfigValue)) {
      LOG.info("Successfully verified configuration property {} = {}", TEST_CONFIG_KEY, verifiedConfigValue);
    } else {
      LOG.warn("Configuration property {} not found or has unexpected value: {}", TEST_CONFIG_KEY, verifiedConfigValue);
    }
  }

  /**
   * Returns the verified configuration value for testing purposes.
   */
  public String getVerifiedConfigValue() {
    return verifiedConfigValue;
  }

  /**
   * Returns the StorageConfiguration for testing purposes.
   */
  public StorageConfiguration<?> getStorageConf() {
    return storageConf;
  }

  @Override
  public void lock() {
    LOG.info("Lock acquired, config verification: {}", verifiedConfigValue);
    lock.writeLock().lock();
  }

  @Override
  public boolean tryLock() {
    return tryLock(1000, TimeUnit.MILLISECONDS);
  }

  @Override
  public boolean tryLock(long time, TimeUnit unit) {
    try {
      boolean acquired = lock.writeLock().tryLock(time, unit);
      if (acquired) {
        LOG.info("Lock acquired, config verification: {}", verifiedConfigValue);
      }
      return acquired;
    } catch (InterruptedException e) {
      throw new HoodieLockException("Failed to acquire lock", e);
    }
  }

  @Override
  public void unlock() {
    if (lock.isWriteLockedByCurrentThread()) {
      lock.writeLock().unlock();
      LOG.info("Lock released");
    }
  }

  @Override
  public ReentrantReadWriteLock getLock() {
    return lock;
  }

  @Override
  public void close() {
    if (lock.isWriteLockedByCurrentThread()) {
      lock.writeLock().unlock();
    }
  }
}
