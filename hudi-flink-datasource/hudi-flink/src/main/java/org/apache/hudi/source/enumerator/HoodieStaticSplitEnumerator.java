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

package org.apache.hudi.source.enumerator;

import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.source.split.HoodieSourceSplit;
import org.apache.hudi.source.split.HoodieSplitProvider;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.runtime.execution.SuppressRestartsException;

/**
 *  Static Hoodie split enumerator that only handles with a bounded number of hudi commits.
 */
@Slf4j
public class HoodieStaticSplitEnumerator extends AbstractHoodieSplitEnumerator {

  // The read.start-commit / read.end-commit bounds this bounded read was enumerated with, persisted
  // in the enumerator checkpoint so a later restore can detect that they changed. See
  // HoodieSource#checkBoundedCommitRangeUnchanged.
  private final Option<String> readStartCommit;
  private final Option<String> readEndCommit;
  // Deferred commit-range failure, present only when this enumerator was restored from a checkpoint
  // whose range differs from the configured one. Raised from start() rather than at restore time,
  // because on the initial restore from a savepoint the coordinator context is not yet initialized
  // and its failJob cannot terminate the job. See HoodieSource#checkBoundedCommitRangeUnchanged.
  private final Option<String> rangeFailure;

  public HoodieStaticSplitEnumerator(
      String tableName,
      SplitEnumeratorContext<HoodieSourceSplit> enumeratorContext,
      HoodieSplitProvider provider,
      Option<String> readStartCommit,
      Option<String> readEndCommit,
      Option<String> rangeFailure) {
    super(tableName, enumeratorContext, provider);
    this.readStartCommit = readStartCommit;
    this.readEndCommit = readEndCommit;
    this.rangeFailure = rangeFailure;
  }

  /**
   * Raises the deferred commit-range failure before starting split discovery. Flink's
   * single-threaded coordinator executor guarantees this runs before any other enumerator callback,
   * and by this point {@code OperatorCoordinatorHolder#start()} has asserted that the coordinator
   * context is initialized, so the throw reaches {@code context.failJob} and terminates the job.
   * {@link SuppressRestartsException} keeps the restart strategy from looping on it: the same
   * mismatch would recur on every restore from the same checkpoint.
   */
  @Override
  public void start() {
    if (rangeFailure.isPresent()) {
      log.error(rangeFailure.get());
      throw new SuppressRestartsException(new HoodieException(rangeFailure.get()));
    }
    super.start();
  }

  @Override
  public HoodieSplitEnumeratorState snapshotState(long checkpointId) {
    return new HoodieSplitEnumeratorState(
        splitProvider.state(), Option.empty(), Option.empty(), readStartCommit, readEndCommit);
  }

  @Override
  protected boolean shouldWaitForMoreSplits() {
    return false;
  }
}
