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
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.stream.Collectors;

/**
 *  Default split provider caches pending splits in queue and return split sequentially.
 */
public class DefaultHoodieSplitProvider implements HoodieSplitProvider {
  private Queue<HoodieSourceSplit> pendingSplits;

  public DefaultHoodieSplitProvider() {
    this.pendingSplits = new ConcurrentLinkedDeque<>();
  }

  @Override
  public Option<HoodieSourceSplit> getNext(@Nullable String hostname) {
    if (!pendingSplits.isEmpty()) {
      return Option.of(pendingSplits.poll());
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
    this.pendingSplits.addAll(splits);
  }

  @Override
  public Collection<HoodieSourceSplitState> state() {
    return pendingSplits.stream().map(
        split -> new HoodieSourceSplitState(split, HoodieSourceSplitStatus.UNASSIGNED))
        .collect(Collectors.toList());
  }

  @Override
  public CompletableFuture<Void> isAvailable() {
    return new CompletableFuture<>();
  }

  @Override
  public int pendingSplitCount() {
    return pendingSplits.size();
  }

  @Override
  public long pendingRecords() {
    throw new UnsupportedOperationException("Pending records is not supported in DefaultSplitProvider.");
  }
}
