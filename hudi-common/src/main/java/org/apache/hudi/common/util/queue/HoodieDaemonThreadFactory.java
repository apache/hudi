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

package org.apache.hudi.common.util.queue;

import org.apache.hudi.common.util.Functions;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * Access to a ThreadFactory instance.
 * 1. All threads are created with setDaemon(true).
 * 2. All threads execute preExecuteRunnable func once.
 */
public class HoodieDaemonThreadFactory implements ThreadFactory {

  private final Runnable preExecuteRunnable;
  private final AtomicInteger threadsNum = new AtomicInteger();
  private final String namePattern;
  private static final String BASE_NAME = "Hoodie-daemon-thread";

  public HoodieDaemonThreadFactory() {
    this(BASE_NAME, Functions.noop());
  }

  public HoodieDaemonThreadFactory(String threadNamePrefix) {
    this(threadNamePrefix, Functions.noop());
  }

  public HoodieDaemonThreadFactory(Runnable preExecuteRunnable) {
    this(BASE_NAME, preExecuteRunnable);
  }

  public HoodieDaemonThreadFactory(String threadNamePrefix, Runnable preExecuteRunnable) {
    this.preExecuteRunnable = preExecuteRunnable;
    this.namePattern = threadNamePrefix + "-%d";
  }

  @Override
  public Thread newThread(Runnable r) {
    Thread t = new Thread(new Runnable() {

      @Override
      public void run() {
        preExecuteRunnable.run();
        r.run();
      }
    }, String.format(namePattern, threadsNum.addAndGet(1)));

    t.setDaemon(true);
    return t;
  }
}
