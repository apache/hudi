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

package org.apache.hudi.common.testutils;

import org.junit.jupiter.api.function.Executable;

import java.net.URL;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Counts the Hadoop default resources ({@code core-default.xml}, {@code core-site.xml}) loaded
 * while an action runs on the current thread.
 *
 * <p>A Hadoop {@code Configuration} captures the thread context class loader when it is created
 * and loads the default resources through it on first access, while a copy of an existing
 * configuration keeps the loader of its source. Running the action under a counting context
 * class loader therefore counts only the configurations the action creates from scratch.
 */
public final class HadoopDefaultResourceLoadCounter {

  private HadoopDefaultResourceLoadCounter() {
  }

  public static int countDefaultResourceLoads(Executable action) throws Throwable {
    Thread thread = Thread.currentThread();
    ClassLoader original = thread.getContextClassLoader();
    CountingClassLoader counting = new CountingClassLoader(original);
    thread.setContextClassLoader(counting);
    try {
      action.execute();
    } finally {
      thread.setContextClassLoader(original);
    }
    return counting.loads.get();
  }

  private static class CountingClassLoader extends ClassLoader {
    private final AtomicInteger loads = new AtomicInteger();

    CountingClassLoader(ClassLoader parent) {
      super(parent);
    }

    @Override
    public URL getResource(String name) {
      if ("core-default.xml".equals(name) || "core-site.xml".equals(name)) {
        loads.incrementAndGet();
      }
      return super.getResource(name);
    }
  }
}
