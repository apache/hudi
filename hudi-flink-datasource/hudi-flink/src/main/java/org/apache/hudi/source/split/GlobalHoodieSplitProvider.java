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

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.stream.Collectors;

/**
 * Split provider that serves splits from a single shared pool, ignoring the requesting subtask id
 * (work stealing): whichever reader asks next gets the next pending split, so all readers stay busy
 * until the pool is fully drained.
 *
 * <p>Intended for BOUNDED (batch) reads driven by
 * {@link org.apache.hudi.source.enumerator.HoodieStaticSplitEnumerator}. There the complete split
 * set is known up front and every split is independent: exactly one split per file group, no
 * cross-commit continuation and no ordering relationship between splits. That holds for all bounded
 * query modes, including the CDC one, where a file group's changes are sorted inside a single split
 * rather than spread over several. Any reader can therefore safely read any split.
 *
 * <p>Contrast with {@link DefaultHoodieSplitProvider}, which pins each split to one subtask (by
 * hashing the file id, or round-robin on the split number) and never rebalances: a subtask that
 * drew a heavier share runs long while its peers sit idle. Because that assignment balances split
 * <em>count</em> rather than bytes or records, and cannot steal, even small per-subtask skew is
 * unrecoverable and shows up as a declining tail at the end of a bounded read.
 *
 * <p>NOT used for streaming reads: the continuous enumerator keeps per-subtask assignment (via
 * {@link DefaultHoodieSplitProvider}) so that a file id's successive incremental splits stay affine
 * to one reader, and so bucket id to subtask alignment is preserved for bucket index tables.
 *
 * <p>Splits are served oldest-commit-first via {@link HoodieSourceSplitComparator}, the same
 * ordering the per-subtask queues use. Thread safe: a {@link PriorityBlockingQueue} backs the pool,
 * so {@link #pendingSplitCount()} can be read from the I/O threads for the unassigned splits gauge
 * while the coordinator thread assigns.
 */
public class GlobalHoodieSplitProvider implements HoodieSplitProvider {
  public static final int INITIAL_POOL_CAPACITY = 20;

  // Shared pool of unassigned splits, ordered by commit time (oldest first).
  private final Queue<HoodieSourceSplit> pendingSplits;
  private CompletableFuture<Void> availableFuture;

  public GlobalHoodieSplitProvider() {
    this.pendingSplits =
        new PriorityBlockingQueue<>(INITIAL_POOL_CAPACITY, new HoodieSourceSplitComparator());
  }

  @Override
  public Option<HoodieSourceSplit> getNext(int taskId, @Nullable String hostname) {
    // Work stealing: the subtask id and hostname are intentionally ignored, so any requesting
    // reader gets the next split from the shared pool. Empty means the pool is globally drained;
    // for the static enumerator (shouldWaitForMoreSplits() == false) that correctly triggers
    // signalNoMoreSplits for the requesting reader.
    HoodieSourceSplit next = pendingSplits.poll();
    return next == null ? Option.empty() : Option.of(next);
  }

  @Override
  public void onDiscoveredSplits(Collection<HoodieSourceSplit> splits) {
    addSplits(splits);
  }

  @Override
  public void onUnassignedSplits(Collection<HoodieSourceSplit> splits) {
    // Splits handed back by a failed reader (addSplitsBack) return to the shared pool and are
    // picked up by whichever reader asks next, which need not be the failed subtask. Readers that
    // already received no-more-splits are done, but any reader still asking can claim them.
    addSplits(splits);
  }

  private void addSplits(Collection<HoodieSourceSplit> splits) {
    if (splits.isEmpty()) {
      return;
    }
    pendingSplits.addAll(splits);
    completeAvailableFuturesIfNeeded();
  }

  @Override
  public Collection<HoodieSourceSplitState> state() {
    return pendingSplits.stream()
        .map(split -> new HoodieSourceSplitState(split, HoodieSourceSplitStatus.UNASSIGNED))
        .collect(Collectors.toList());
  }

  @Override
  public synchronized CompletableFuture<Void> isAvailable() {
    if (availableFuture == null) {
      availableFuture = new CompletableFuture<>();
    }
    return availableFuture;
  }

  @Override
  public int pendingSplitCount() {
    return pendingSplits.size();
  }

  @Override
  public long pendingRecords() {
    throw new UnsupportedOperationException(
        "Pending records is not supported in GlobalHoodieSplitProvider.");
  }

  private synchronized void completeAvailableFuturesIfNeeded() {
    if (availableFuture != null && !pendingSplits.isEmpty()) {
      availableFuture.complete(null);
      // Cleared only once completed, so a waiter never loses the future it is blocked on.
      availableFuture = null;
    }
  }
}
