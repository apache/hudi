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

package org.apache.hudi.timeline.service.handlers;

import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.timeline.service.TimelineService;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class RemotePartitionerHandler extends Handler {

  // cache Map<PartitionPath, BucketStartIndex>
  private final ConcurrentHashMap<String, Integer> cache;
  private final AtomicInteger nextIndex = new AtomicInteger(0);

  public RemotePartitionerHandler(StorageConfiguration<?> conf, TimelineService.Config timelineServiceConfig,
                                  FileSystemViewManager viewManager) {
    super(conf, timelineServiceConfig, viewManager);
    this.cache = new ConcurrentHashMap<>();
  }

  public int gePartitionIndex(String numBuckets, String partitionPath, String partitionNum) {
    int num = Integer.parseInt(numBuckets);
    int partNum = Integer.parseInt(partitionNum);

    return cache.computeIfAbsent(partitionPath, key -> {
      int current;
      int newNext;
      int res;
      do {
        current = nextIndex.get();
        res = current;
        newNext = current + num;
        if (newNext >= partNum) {
          newNext = newNext % partNum;
        }
      } while (!nextIndex.compareAndSet(current, newNext));
      return res;
    });
  }
}
