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

package org.apache.hudi.io;

import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.table.view.TableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Triple;
import org.apache.hudi.metadata.MetadataPartitionType;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * File slice cache for Latest file slice. This is mainly used for RLI partition in Metadata table so that each task pertaining to RLI file group
 * don't need to build the FileSystemView repeatedly. Here, the latest file slice for each file group is populated just once upfront, shared by the same jvm and is used
 * by different tasks being spun up in the same node.
 */
public class LatestFileSliceCache {
  private static final Logger LOG = LoggerFactory.getLogger(LatestFileSliceCache.class);

  private static String RLI_PARTITION_PATH = MetadataPartitionType.RECORD_INDEX.getPartitionPath();
  private static AtomicReference<Cache<Triple<String, String, String>, Option<FileSlice>>> LATEST_FILE_SLICE_CACHE = null;
  private static volatile String INSTANT_TIME_CACHED = null;

  public static Cache<Triple<String, String, String>, Option<FileSlice>> getCache(TableFileSystemView.SliceView sliceView, String instantTime,
                                                                                  int maxCacheSize, int expirationInMins) {
    if (LATEST_FILE_SLICE_CACHE == null || INSTANT_TIME_CACHED == null || (!INSTANT_TIME_CACHED.equals(instantTime))) {
      synchronized (LatestFileSliceCache.class) {
        if (LATEST_FILE_SLICE_CACHE == null || INSTANT_TIME_CACHED == null || (!INSTANT_TIME_CACHED.equals(instantTime))) {
          if (LATEST_FILE_SLICE_CACHE != null) {
            LATEST_FILE_SLICE_CACHE.get().cleanUp();
          }
          LOG.warn("Instantiating LatestFileSliceCache");
          LATEST_FILE_SLICE_CACHE = new AtomicReference<>(Caffeine.newBuilder()
              .maximumSize(maxCacheSize)
              .expireAfterWrite(Duration.of(expirationInMins, ChronoUnit.MINUTES))
              .build());
          LOG.warn("Populating entries into Latest file slice cache with instant time {} : Started ", instantTime);
          // populate cache w/ latest file slice for all file groups
          sliceView.getLatestMergedFileSlicesBeforeOrOn(RLI_PARTITION_PATH, instantTime).forEach(fileSlice -> {
            LATEST_FILE_SLICE_CACHE.get().put(Triple.of(RLI_PARTITION_PATH, fileSlice.getFileId(), instantTime), Option.of(fileSlice));
          });
          INSTANT_TIME_CACHED = instantTime;
          LOG.warn("Populating entries into Latest file slice cache with instant time {} : Completed. Total entries {}", instantTime, LATEST_FILE_SLICE_CACHE.get().estimatedSize());
        } else {
          LOG.warn("Already some other concurrent task populated the entries. Hence, skipping to populate entries. Total entries " + LATEST_FILE_SLICE_CACHE.get().estimatedSize());
        }
      }
    }
    return LATEST_FILE_SLICE_CACHE.get();
  }
}