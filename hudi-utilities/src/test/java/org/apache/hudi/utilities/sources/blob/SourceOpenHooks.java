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

package org.apache.hudi.utilities.sources.blob;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * TEST-ONLY hooks on the source objects {@link BucketFolderFileSystem} serves: a test may fail an object's status, run code
 * before each open of an object (change or remove the object, fail the open) and count the opens. Objects are registered by the
 * absolute path of their local file; Spark runs local tasks in the test JVM.
 */
public final class SourceOpenHooks {

  private static final Map<String, RuntimeException> STATUS_FAILURES = new ConcurrentHashMap<>();
  private static final Map<String, AtomicInteger> OPENS = new ConcurrentHashMap<>();
  private static final Map<String, OpenHook> BEFORE_OPEN = new ConcurrentHashMap<>();

  /** What runs before each open of a file for reading: it may change the file, or fail the open. */
  @FunctionalInterface
  public interface OpenHook {
    void run() throws IOException;
  }

  private SourceOpenHooks() {
  }

  /** The status of the local file at {@code absolutePath} fails with {@code failure} from now on. */
  public static void failWith(String absolutePath, RuntimeException failure) {
    STATUS_FAILURES.put(absolutePath, failure);
  }

  /** Called by {@link BucketFolderFileSystem#getFileStatus} for every status of the local file at {@code absolutePath}. */
  static void checkStatus(String absolutePath) {
    RuntimeException failure = STATUS_FAILURES.get(absolutePath);
    if (failure != null) {
      throw failure;
    }
  }

  /** Called by {@link BucketFolderFileSystem#open} for every read of the local file at {@code absolutePath}. */
  static void opened(String absolutePath) {
    OPENS.computeIfAbsent(absolutePath, path -> new AtomicInteger()).incrementAndGet();
  }

  /** Runs {@code hook} before every open of the local file at {@code absolutePath} for reading, from now on. */
  public static void beforeOpen(String absolutePath, OpenHook hook) {
    BEFORE_OPEN.put(absolutePath, hook);
  }

  /** Called by {@link BucketFolderFileSystem#open} before every read of the local file at {@code absolutePath}. */
  static void opening(String absolutePath) throws IOException {
    OpenHook hook = BEFORE_OPEN.get(absolutePath);
    if (hook != null) {
      hook.run();
    }
  }

  /** How many times the local file at {@code absolutePath} has been opened for reading since the last {@link #clear}. */
  public static int opens(String absolutePath) {
    AtomicInteger opens = OPENS.get(absolutePath);
    return opens == null ? 0 : opens.get();
  }

  public static void clear() {
    STATUS_FAILURES.clear();
    OPENS.clear();
    BEFORE_OPEN.clear();
  }
}
