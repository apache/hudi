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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.Weigher;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.util.ObjectSizeCalculator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Implementation of the shared cache table's per partition list of {@link FileStatus} in memory
 */
class SharedInMemoryCache {

  private static final Logger LOG = LogManager.getLogger(SharedInMemoryCache.class);

  private final Long maxSizeInBytes;

  private final Cache<Pair<Object, Path>, FileStatus[]> cache;

  private final AtomicBoolean warnedAboutEviction = new AtomicBoolean(false);

  public SharedInMemoryCache(Long maxSizeInBytes, Long cacheTTL) {
    this.maxSizeInBytes = maxSizeInBytes;

    this.cache = composeCache(maxSizeInBytes, cacheTTL, this::onRemoval);
  }

  private static Cache<Pair<Object, Path>, FileStatus[]> composeCache(Long maxSizeInBytes,
                                                                      Long cacheTTL,
                                                                      RemovalListener<Pair<Object, Path>, FileStatus[]> removalListener) {
    // [[Weigher]].weigh returns Int so we could only cache objects < 2GB
    // instead, the weight is divided by this factor (which is smaller
    // than the size of one [[FileStatus]]).
    // so it will support objects up to 64GB in size.
    int weightScale = 32;
    Weigher<Pair<Object, Path>, FileStatus[]> weigher = new Weigher<Pair<Object, Path>, FileStatus[]>() {
      @Override
      public int weigh(Pair<Object, Path> key, FileStatus[] value) {
        long estimate = (estimateSize(key) + estimateSize(value)) / weightScale;
        if (estimate > Integer.MAX_VALUE) {
          LOG.warn(String.format("Cached table partition file-list is too big (%d)", estimate));
          return Integer.MAX_VALUE;
        } else {
          return (int) estimate;
        }
      }
    };

    Caffeine<Pair<Object, Path>, FileStatus[]> builder = Caffeine.newBuilder()
        .weigher(weigher)
        .removalListener(removalListener)
        .maximumWeight(maxSizeInBytes / weightScale);

    if (cacheTTL > 0) {
      builder = builder.expireAfterWrite(cacheTTL, TimeUnit.SECONDS);
    }

    return builder.build();
  }

  private void onRemoval(Pair<Object, Path> key, FileStatus[] value, RemovalCause cause) {
    if (cause == RemovalCause.SIZE && warnedAboutEviction.compareAndSet(false, true)) {
      LOG.warn(String.format("Evicting cached table's partition file-list from memory due to memory pressure " +
              "(%d bytes threshold). This may impact query planning performance", maxSizeInBytes));
    }
  }

  /**
   * @return a FileStatusCache that does not share any entries with any other client, but does
   *         share memory resources for the purpose of cache eviction.
   */
  public FileStatusCache createForNewClient() {
    return new FileStatusCache() {
      private final Object clientToken = new Object();

      @Override
      public Option<FileStatus[]> get(Path path) {
        return Option.ofNullable(cache.getIfPresent(Pair.of(clientToken, path)));
      }

      @Override
      public void put(Path path, FileStatus[] leafFiles) {
        cache.put(Pair.of(clientToken, path), leafFiles);
      }

      public void invalidate() {
        cache.asMap().forEach((key, value) -> {
          if (key.getLeft() == clientToken) {
            cache.invalidate(key);
          }
        });
      }
    };
  }

  private static long estimateSize(Object o) {
    return ObjectSizeCalculator.getObjectSize(o);
  }
}

