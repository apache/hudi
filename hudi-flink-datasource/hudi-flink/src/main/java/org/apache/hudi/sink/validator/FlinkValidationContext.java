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

package org.apache.hudi.sink.validator;

import org.apache.hudi.client.validator.ValidationContext;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;

import java.util.List;

/**
 * Flink implementation of {@link ValidationContext}.
 *
 * <p>Constructed from data available in {@code StreamWriteOperatorCoordinator.doCommit()}
 * before the commit is finalized. Provides validators with access to commit metadata,
 * write statistics, and previous commit information.</p>
 *
 * <p>Unlike Spark's ValidationContext (Phase 3), Flink's context does not have access to
 * the active timeline at validation time because the commit has not yet been written.
 * The previous commit metadata is provided directly from the coordinator's state.</p>
 */
public class FlinkValidationContext implements ValidationContext {

  private final String instantTime;
  private final Option<HoodieCommitMetadata> commitMetadata;
  private final Option<List<HoodieWriteStat>> writeStats;
  private final Option<HoodieCommitMetadata> previousCommitMetadata;

  /**
   * Create a Flink validation context.
   *
   * @param instantTime Current commit instant time
   * @param commitMetadata Current commit metadata (with extraMetadata including checkpoints)
   * @param writeStats Write statistics from all write operators
   * @param previousCommitMetadata Metadata from the previous completed commit
   */
  public FlinkValidationContext(String instantTime,
                                Option<HoodieCommitMetadata> commitMetadata,
                                Option<List<HoodieWriteStat>> writeStats,
                                Option<HoodieCommitMetadata> previousCommitMetadata) {
    this.instantTime = instantTime;
    this.commitMetadata = commitMetadata;
    this.writeStats = writeStats;
    this.previousCommitMetadata = previousCommitMetadata;
  }

  @Override
  public String getInstantTime() {
    return instantTime;
  }

  @Override
  public Option<HoodieCommitMetadata> getCommitMetadata() {
    return commitMetadata;
  }

  @Override
  public Option<List<HoodieWriteStat>> getWriteStats() {
    return writeStats;
  }

  /**
   * Not supported in Flink context. The active timeline is not available
   * at pre-commit validation time because the commit has not been written yet.
   *
   * @throws UnsupportedOperationException always
   */
  @Override
  public HoodieActiveTimeline getActiveTimeline() {
    throw new UnsupportedOperationException(
        "Active timeline is not available in Flink pre-commit validation context. "
            + "Use getPreviousCommitMetadata() to access previous commit information.");
  }

  /**
   * Not supported in Flink context. Use {@link #isFirstCommit()} or
   * {@link #getPreviousCommitMetadata()} instead.
   *
   * @throws UnsupportedOperationException always
   */
  @Override
  public Option<HoodieInstant> getPreviousCommitInstant() {
    throw new UnsupportedOperationException(
        "getPreviousCommitInstant() is not available in Flink pre-commit validation context. "
            + "Use isFirstCommit() or getPreviousCommitMetadata() instead.");
  }

  @Override
  public boolean isFirstCommit() {
    return !previousCommitMetadata.isPresent();
  }

  @Override
  public Option<HoodieCommitMetadata> getPreviousCommitMetadata() {
    return previousCommitMetadata;
  }
}
