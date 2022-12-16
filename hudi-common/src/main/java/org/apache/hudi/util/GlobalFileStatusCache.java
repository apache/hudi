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

package org.apache.hudi.util;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.util.Option;

public class GlobalFileStatusCache {

  private static final GlobalFileStatusCache INSTANCE = new GlobalFileStatusCache();

  private SharedInMemoryCache sharedCache = null;

  private GlobalFileStatusCache() {}

  /**
   * @return a new FileStatusCache based on session configuration. Cache memory quota is
   * shared across all clients.
   */
  private FileStatusCache createInternal() {
    // TODO add config to disable the cache
    long maxCacheSize = 250 * 1024 * 1024;
    long cacheTTL = -1;

    synchronized (this) {
      if (sharedCache == null) {
        sharedCache = new SharedInMemoryCache(maxCacheSize, cacheTTL);
      }
    }

    return sharedCache.createForNewClient();
  }

  private void resetInternal() {
    synchronized (this) {
      sharedCache = null;
    }
  }

  public static FileStatusCache create() {
    return INSTANCE.createInternal();
  }

  // For testing only
  public static void reset() {
    INSTANCE.resetInternal();
  }

  private static class NoopCache implements FileStatusCache {
    @Override
    public Option<FileStatus[]> get(Path path) {
      return Option.empty();
    }

    @Override
    public void put(Path path, FileStatus[] leafFiles) {
      // no-op
    }

    @Override
    public void invalidate() {
      // no-op
    }
  }
}
