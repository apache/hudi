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

package org.apache.hudi.common.lock;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * Pluggable lock implementations using this provider class.
 */
public interface LockProvider<T> extends Lock, AutoCloseable {

  @Override
  default void lockInterruptibly() {
    throw new UnsupportedOperationException();
  }

  @Override
  default void lock() {
    throw new UnsupportedOperationException();
  }

  @Override
  default boolean tryLock() {
    throw new UnsupportedOperationException();
  }

  @Override
  default Condition newCondition() {
    throw new UnsupportedOperationException();
  }

  default boolean tryLockWithInstant(long time, TimeUnit unit, String timestamp) {
    throw new UnsupportedOperationException();
  }

  default T getLock() {
    throw new IllegalArgumentException();
  }

  @Override
  default void close() {
  }
}
