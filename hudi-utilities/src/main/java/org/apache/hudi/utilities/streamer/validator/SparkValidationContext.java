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

package org.apache.hudi.utilities.streamer.validator;

import org.apache.hudi.client.validator.ValidationContext;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;

import java.util.List;

/**
 * Spark/HoodieStreamer implementation of {@link ValidationContext}.
 *
 * <p>Constructed from data available in {@code StreamSync.writeToSinkAndDoMetaSync()}
 * before the commit is finalized. Provides validators with access to commit metadata,
 * write statistics, and previous commit information for streaming offset validation.</p>
 *
 * <p>Unlike Flink's implementation, Spark can optionally provide active timeline access
 * via {@link HoodieTableMetaClient} for richer validation patterns.</p>
 */
public class SparkValidationContext implements ValidationContext {

  private final String instantTime;
  private final Option<HoodieCommitMetadata> commitMetadata;
  private final Option<List<HoodieWriteStat>> writeStats;
  private final Option<HoodieCommitMetadata> previousCommitMetadata;
  private final HoodieTableMetaClient metaClient;

  /**
   * Create a Spark validation context with full timeline access.
   *
   * @param instantTime Current commit instant time
   * @param commitMetadata Current commit metadata (with extraMetadata including checkpoints)
   * @param writeStats Write statistics from write operations
   * @param previousCommitMetadata Metadata from the previous completed commit
   * @param metaClient Table meta client for timeline access (may be null for testing)
   */
  public SparkValidationContext(String instantTime,
                                Option<HoodieCommitMetadata> commitMetadata,
                                Option<List<HoodieWriteStat>> writeStats,
                                Option<HoodieCommitMetadata> previousCommitMetadata,
                                HoodieTableMetaClient metaClient) {
    this.instantTime = instantTime;
    this.commitMetadata = commitMetadata;
    this.writeStats = writeStats;
    this.previousCommitMetadata = previousCommitMetadata;
    this.metaClient = metaClient;
  }

  /**
   * Create a Spark validation context without timeline access (for testing).
   *
   * @param instantTime Current commit instant time
   * @param commitMetadata Current commit metadata (with extraMetadata including checkpoints)
   * @param writeStats Write statistics from write operations
   * @param previousCommitMetadata Metadata from the previous completed commit
   */
  public SparkValidationContext(String instantTime,
                                Option<HoodieCommitMetadata> commitMetadata,
                                Option<List<HoodieWriteStat>> writeStats,
                                Option<HoodieCommitMetadata> previousCommitMetadata) {
    this(instantTime, commitMetadata, writeStats, previousCommitMetadata, null);
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
   * Get the active timeline. Available when metaClient is provided.
   *
   * @throws UnsupportedOperationException if metaClient was not provided
   */
  @Override
  public HoodieActiveTimeline getActiveTimeline() {
    if (metaClient == null) {
      throw new UnsupportedOperationException(
          "Active timeline is not available without HoodieTableMetaClient.");
    }
    return metaClient.getActiveTimeline();
  }

  /**
   * Not directly supported. Use {@link #isFirstCommit()} or
   * {@link #getPreviousCommitMetadata()} instead.
   *
   * @throws UnsupportedOperationException always
   */
  @Override
  public Option<HoodieInstant> getPreviousCommitInstant() {
    throw new UnsupportedOperationException(
        "getPreviousCommitInstant() is not available in HoodieStreamer pre-commit validation context. "
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
