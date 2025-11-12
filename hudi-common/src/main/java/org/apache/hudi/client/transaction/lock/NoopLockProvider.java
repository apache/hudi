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

import org.apache.hudi.common.config.LockConfiguration;
import org.apache.hudi.common.lock.LockProvider;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.storage.StorageConfiguration;

import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * NoopLockProvider as the name suggests, is a no op lock provider. Any caller asking for a lock will be able to get hold of the lock.
 * This is not meant to be used a production grade lock providers. This is meant to be used for Hudi's internal operations.
 * For eg: During upgrade, we have nested lock situations, and we leverage this {@code NoopLockProvider} for any operations we
 * might want to do within the upgradeHandler blocks to avoid re-entrant situations. Not all lock providers support re-entrance and during upgrade,
 * it is expected to have a single writer to the Hudi table of interest.
 */
public class NoopLockProvider implements LockProvider<ReentrantReadWriteLock>, Serializable {

  public NoopLockProvider(final LockConfiguration lockConfiguration, final StorageConfiguration<?> conf) {
    // no op.
  }

  @Override
  public boolean tryLock(long time, @NotNull TimeUnit unit) throws InterruptedException {
    return true;
  }

  @Override
  public void unlock() {
    // no op.
  }

  @Override
  public void lockInterruptibly() {
    // no op.
  }

  @Override
  public void lock() {
    // no op.
  }

  @Override
  public boolean tryLock() {
    return true;
  }

  @Override
  public ReentrantReadWriteLock getLock() {
    return new ReentrantReadWriteLock();
  }

  @Override
  public String getCurrentOwnerLockInfo() {
    return StringUtils.EMPTY_STRING;
  }

  @Override
  public void close() {
    // no op.
  }
}
