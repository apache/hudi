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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * SplitProvider interface is to define split assignment strategy for different requirements. E.g.
 *
 * <ul>
 *   <li>Simple assigner with no ordering guarantee.
 *   <li>Instant time aware assigner that assign splits based on the order they are committed.
 * </ul>
 *
 * <p>SplitProvider implementation needs to be thread safe. Enumerator call the assigner APIs mostly from
 * the coordinator thread. But enumerator may call the {@link HoodieSplitProvider#pendingSplitCount()}
 * from the I/O threads.
 */
@Internal
public interface HoodieSplitProvider extends Closeable {

  /**
   * Some SplitProviders may need to start background threads or perform other activity such as
   * registering as listeners to updates from other event sources e.g., watermark tracker.
   */
  default void start() {
  }

  /**
   * Some SplitProviders may need to perform certain actions when their corresponding enumerators are
   * closed
   */
  @Override
  default void close() {
  }

  /**
   * Request a new split from the assigner when enumerator trying to assign splits to awaiting
   * readers.
   *
   * <p>If enumerator wasn't able to assign the split (e.g., reader disconnected), enumerator should
   * call {@link HoodieSplitProvider#onUnassignedSplits} to return the split.
   */
  Option<HoodieSourceSplit> getNext(@Nullable String hostname);

  /** Add new splits discovered by enumerator */
  void onDiscoveredSplits(Collection<HoodieSourceSplit> splits);

  /** Forward addSplitsBack event (for failed reader) to assigner */
  void onUnassignedSplits(Collection<HoodieSourceSplit> splits);

  /**
   * Some assigner (like event time alignment) may track in-progress splits to advance watermark upon
   * completed splits
   */
  default void onCompletedSplits(Collection<String> completedSplitIds) {
  }

  /**
   * Get assigner state for checkpointing. This is a super-set API that works for all currently
   * imagined assigners.
   */
  Collection<HoodieSourceSplitState> state();

  /**
   * Enumerator can get a notification via CompletableFuture when the assigner has more splits
   * available later. Enumerator should schedule assignment in the thenAccept action of the future.
   *
   * <p>Assigner will return the same future if this method is called again before the previous
   * future is completed.
   *
   * <p>The future can be completed from other thread, e.g. the coordinator thread from another
   * thread for event time alignment.
   *
   * <p>If enumerator need to trigger action upon the future completion, it may want to run it in
   * the coordinator thread using {@link SplitEnumeratorContext#runInCoordinatorThread(Runnable)}.
   */
  CompletableFuture<Void> isAvailable();

  /**
   * Return the number of pending splits that haven't been assigned yet.
   *
   * <p>The enumerator can poll this API to publish a metric on the number of pending splits.
   *
   * <p>The enumerator can also use this information to throttle split discovery for streaming read.
   * If there are already many pending splits tracked by the assigner, it is undesirable to discover
   * more splits and track them in the assigner. That will increase the memory footprint and
   * enumerator checkpoint size.
   */
  int pendingSplitCount();

  /**
   * Return the number of pending records, which can act as a measure of the source lag. This value
   * could be an estimation if the exact number of records cannot be accurately computed.
   */
  long pendingRecords();
}
