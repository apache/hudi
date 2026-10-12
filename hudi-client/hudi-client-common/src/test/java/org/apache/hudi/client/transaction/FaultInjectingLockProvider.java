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

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

/**
 * Reentrant test lock provider that records every instance it creates and can be told to fail
 * {@link #tryLock} or {@link #unlock}.
 */
public class FaultInjectingLockProvider implements LockProvider<String> {

  private static final List<FaultInjectingLockProvider> INSTANCES = new CopyOnWriteArrayList<>();
  private static volatile boolean failTryLock = false;
  private static volatile boolean failUnlock = false;

  private volatile boolean held = false;
  private volatile boolean closed = false;

  public FaultInjectingLockProvider(LockConfiguration lockConfiguration, StorageConfiguration<?> conf) {
    INSTANCES.add(this);
  }

  public static void reset() {
    INSTANCES.clear();
    failTryLock = false;
    failUnlock = false;
  }

  public static void setFailTryLock(boolean fail) {
    failTryLock = fail;
  }

  public static void setFailUnlock(boolean fail) {
    failUnlock = fail;
  }

  public static List<FaultInjectingLockProvider> getInstances() {
    return INSTANCES;
  }

  @Override
  public boolean tryLock(long time, TimeUnit unit) {
    if (closed) {
      throw new HoodieLockException("Lock provider already closed");
    }
    if (failTryLock) {
      return false;
    }
    held = true;
    return true;
  }

  @Override
  public void unlock() {
    if (failUnlock) {
      throw new HoodieLockException("Injected unlock failure");
    }
    held = false;
  }

  @Override
  public String getLock() {
    return held ? "held" : null;
  }

  @Override
  public void close() {
    closed = true;
  }

  public boolean isHeld() {
    return held;
  }

  public boolean isClosed() {
    return closed;
  }
}
