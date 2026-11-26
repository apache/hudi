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

package org.apache.hudi.io.vortex;

import dev.vortex.jni.NativeRuntime;

/**
 * The native worker threads of Vortex. Without workers, Vortex does its work only on the Java
 * thread that waits for it. All Vortex files in the JVM share the workers.
 */
final class VortexWorkerThreads {

  /** The number of workers that this class set. Other code in the JVM can also add workers. */
  private static int workerThreads = 0;

  private VortexWorkerThreads() {
  }

  /**
   * Increases the number of workers to at least {@code count}. The number does not decrease,
   * because other readers and writers in the JVM can use the workers.
   */
  static void ensureAtLeast(int count) {
    if (count < 0) {
      throw new IllegalArgumentException("The number of Vortex worker threads must not be negative: " + count);
    }

    synchronized (VortexWorkerThreads.class) {
      if (count <= workerThreads) {
        return;
      }

      if (count > NativeRuntime.workerCount()) {
        NativeRuntime.setWorkerThreads(count);
      }
      workerThreads = count;
    }
  }
}
