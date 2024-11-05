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

import org.apache.hudi.client.transaction.lock.InProcessLockProvider;
import org.apache.hudi.common.config.LockConfiguration;
import org.apache.hudi.storage.StorageConfiguration;

import java.util.concurrent.TimeUnit;

public class InProcessLockProviderWithRuntimeError extends InProcessLockProvider {

  public InProcessLockProviderWithRuntimeError(
      LockConfiguration lockConfiguration,
      StorageConfiguration<?> conf) {
    super(lockConfiguration, conf);
  }

  @Override
  public boolean tryLock(long time, TimeUnit unit) {
    throw new RuntimeException();
  }

  @Override
  public void unlock() {
    return;
  }
}
