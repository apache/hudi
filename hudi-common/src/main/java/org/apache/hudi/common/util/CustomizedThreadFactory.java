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

package org.apache.hudi.common.util;

import javax.annotation.Nonnull;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A thread factory for creation of threads
 */
public class CustomizedThreadFactory implements ThreadFactory {

  private static final AtomicLong POOL_NUM = new AtomicLong(1);
  private final AtomicLong threadNum = new AtomicLong(1);

  private final String threadName;
  private final boolean daemon;

  private Runnable preExecuteRunnable;

  public CustomizedThreadFactory() {
    this("pool-" + POOL_NUM.getAndIncrement(), false);
  }

  public CustomizedThreadFactory(String threadNamePrefix) {
    this(threadNamePrefix, false);
  }

  public CustomizedThreadFactory(String threadNamePrefix, Runnable preExecuteRunnable) {
    this(threadNamePrefix, false, preExecuteRunnable);
  }

  public CustomizedThreadFactory(String threadNamePrefix, boolean daemon, Runnable preExecuteRunnable) {
    this.threadName = threadNamePrefix + "-thread-";
    this.daemon = daemon;
    this.preExecuteRunnable = preExecuteRunnable;
  }

  public CustomizedThreadFactory(String threadNamePrefix, boolean daemon) {
    this.threadName = threadNamePrefix + "-thread-";
    this.daemon = daemon;
  }

  @Override
  public Thread newThread(@Nonnull Runnable r) {
    Thread runThread = preExecuteRunnable == null ? new Thread(r) : new Thread(new Runnable() {

      @Override
      public void run() {
        preExecuteRunnable.run();
        r.run();
      }
    });
    runThread.setDaemon(daemon);
    runThread.setName(threadName + threadNum.getAndIncrement());
    return runThread;
  }
}
