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

package org.apache.hudi.source.split;

import org.apache.hudi.common.util.Option;
import org.apache.hudi.source.assign.HoodieSplitAssigner;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.stream.Collectors;

/**
 *  Default split provider caches pending splits in queue and return split sequentially.
 */
public class DefaultHoodieSplitProvider implements HoodieSplitProvider {
  public static final int DEFAULT_SPLIT_QUEUE_SIZE = 20;

  private final Map<Integer, Queue<HoodieSourceSplit>> pendingSplits;
  private final HoodieSplitAssigner splitAssigner;
  private final SerializableComparator<HoodieSourceSplit> comparator;
  private CompletableFuture<Void> availableFuture;

  /**
   * Creates a DefaultHoodieSplitProvider.
   *
   * @param splitAssigner the assigner that assigns each split to a task ID (must not be null)
   */
  public DefaultHoodieSplitProvider(HoodieSplitAssigner splitAssigner) {
    this.comparator = new HoodieSourceSplitComparator();
    this.pendingSplits = new ConcurrentHashMap<>();
    this.splitAssigner = splitAssigner;
  }

  @Override
  public Option<HoodieSourceSplit> getNext(int subTaskId, @Nullable String hostname) {
    if (pendingSplits.containsKey(subTaskId)) {
      Queue<HoodieSourceSplit> splits = pendingSplits.get(subTaskId);
      if (!splits.isEmpty()) {
        return Option.of(splits.poll());
      }
    }

    return Option.empty();
  }

  @Override
  public void onDiscoveredSplits(Collection<HoodieSourceSplit> splits) {
    addSplits(splits);
  }

  @Override
  public void onUnassignedSplits(Collection<HoodieSourceSplit> splits) {
    addSplits(splits);
  }

  private void addSplits(Collection<HoodieSourceSplit> splits) {
    for (HoodieSourceSplit split : splits) {
      int taskId = splitAssigner.assign(split);
      Queue<HoodieSourceSplit> queue = pendingSplits.computeIfAbsent(
          taskId, k -> new PriorityBlockingQueue<>(DEFAULT_SPLIT_QUEUE_SIZE, comparator));
      queue.add(split);

      // complete pending future if new splits are discovered
      completeAvailableFuturesIfNeeded();
    }
  }

  @Override
  public Collection<HoodieSourceSplitState> state() {
    List<HoodieSourceSplitState> splitList = new ArrayList<>();

    for (Queue<HoodieSourceSplit> queue: pendingSplits.values()) {
      splitList.addAll(queue.stream()
          .map(split -> new HoodieSourceSplitState(split, HoodieSourceSplitStatus.UNASSIGNED))
          .collect(Collectors.toList()));
    }

    return splitList;
  }

  @Override
  public CompletableFuture<Void> isAvailable() {
    if (availableFuture == null) {
      availableFuture = new CompletableFuture<>();
    }
    return availableFuture;
  }

  @Override
  public int pendingSplitCount() {
    return pendingSplits.values().stream().mapToInt(Collection::size).sum();
  }

  @Override
  public long pendingRecords() {
    throw new UnsupportedOperationException("Pending records is not supported in DefaultSplitProvider.");
  }

  private synchronized void completeAvailableFuturesIfNeeded() {
    if (availableFuture != null && !pendingSplits.isEmpty()) {
      availableFuture.complete(null);
    }
    availableFuture = null;
  }
}
