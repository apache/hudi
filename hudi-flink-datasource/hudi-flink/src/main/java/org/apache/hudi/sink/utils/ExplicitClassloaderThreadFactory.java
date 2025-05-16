/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.sink.utils;

import org.apache.flink.util.FatalExitExceptionHandler;

import java.util.concurrent.ThreadFactory;

/**
 * A thread factory with explicit classloader.
 */
public class ExplicitClassloaderThreadFactory implements ThreadFactory {
  private final String threadName;
  private final ClassLoader cl;
  private final Thread.UncaughtExceptionHandler errorHandler;

  private Thread t;

  public ExplicitClassloaderThreadFactory(
      final String threadName,
      final ClassLoader contextClassLoader) {
    this(threadName, contextClassLoader, FatalExitExceptionHandler.INSTANCE);
  }

  public ExplicitClassloaderThreadFactory(
      final String threadName,
      final ClassLoader contextClassLoader,
      final Thread.UncaughtExceptionHandler errorHandler) {
    this.threadName = threadName;
    this.cl = contextClassLoader;
    this.errorHandler = errorHandler;
  }

  @Override
  public synchronized Thread newThread(Runnable r) {
    if (t != null) {
      throw new Error(
          "This indicates that a fatal error has happened and caused the "
              + "coordinator executor thread to exit. Check the earlier logs"
              + "to see the root cause of the problem.");
    }
    t = new Thread(r, threadName);
    t.setContextClassLoader(cl);
    t.setUncaughtExceptionHandler(errorHandler);
    return t;
  }
}
