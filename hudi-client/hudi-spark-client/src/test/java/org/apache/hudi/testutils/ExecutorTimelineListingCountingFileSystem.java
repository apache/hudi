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

package org.apache.hudi.testutils;

import org.apache.hudi.storage.StorageConfiguration;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.spark.TaskContext;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A local file system that counts the listings of the table timeline directory made by Spark tasks.
 */
public class ExecutorTimelineListingCountingFileSystem extends RawLocalFileSystem {

  private static final AtomicInteger EXECUTOR_TIMELINE_LISTINGS = new AtomicInteger();

  /**
   * Returns a copy of {@code storageConf} that resolves the {@code file} scheme to this file system.
   */
  public static <T> StorageConfiguration<T> withCountingFileSystem(StorageConfiguration<T> storageConf) {
    StorageConfiguration<T> conf = storageConf.newInstance();
    conf.set("fs.file.impl", ExecutorTimelineListingCountingFileSystem.class.getName());
    conf.set("fs.file.impl.disable.cache", "true");
    return conf;
  }

  public static void reset() {
    EXECUTOR_TIMELINE_LISTINGS.set(0);
  }

  public static int executorTimelineListings() {
    return EXECUTOR_TIMELINE_LISTINGS.get();
  }

  @Override
  public FileStatus[] listStatus(Path path) throws IOException {
    if (TaskContext.get() != null && path.toUri().getPath().endsWith("/.hoodie/timeline")) {
      EXECUTOR_TIMELINE_LISTINGS.incrementAndGet();
    }
    return super.listStatus(path);
  }
}
