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

package org.apache.hudi.estimator;

import org.apache.hudi.common.data.HoodieAtomicLongAccumulator;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.table.timeline.CommitMetadataSerDe;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.storage.StoragePath;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMPACTION_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.DELTA_COMMIT_ACTION;

/**
 * Default Implementation for the {@link RecordSizeEstimator}.
 * Estimates the average record sizes based on the stats from the previous X commits and deltacommits.
 * X is configured using hoodieWriteConfig.getRecordSizeEstimateMaxCommits().
 * <p>
 * Currently, we will estimate the avg record sizes only from candidate files from the commit metadata. Candidate
 * files are selective files that have a threshold size to avoid measurement errors. Optionally, we can
 * configure the expected metadata size of the file so that can be accounted for.
 */
public class AverageRecordSizeEstimator extends RecordSizeEstimator {
  private static final Logger LOG = LoggerFactory.getLogger(AverageRecordSizeEstimator.class);
  /*
   * NOTE: we only use commit instants to calculate average record size because replacecommit can be
   * created by clustering, which has smaller average record size, which affects assigning inserts and
   * may result in OOM by making spark underestimate the actual input record sizes.
   */
  private static final Set<String> RECORD_SIZE_ESTIMATE_ACTIONS = CollectionUtils.createSet(COMMIT_ACTION, DELTA_COMMIT_ACTION, COMPACTION_ACTION);

  public AverageRecordSizeEstimator(HoodieWriteConfig writeConfig) {
    super(writeConfig);
  }

  @Override
  public long averageBytesPerRecord(HoodieTimeline commitTimeline, CommitMetadataSerDe commitMetadataSerDe) {
    int maxCommits = hoodieWriteConfig.getRecordSizeEstimatorMaxCommits();
    final long commitSizeThreshold = (long) (hoodieWriteConfig.getRecordSizeEstimationThreshold() * hoodieWriteConfig.getParquetSmallFileLimit());
    final long metadataSizeEstimate = hoodieWriteConfig.getRecordSizeEstimatorAverageMetadataSize();
    try {
      if (!commitTimeline.empty()) {
        Iterator<HoodieInstant> instants = commitTimeline.filterCompletedInstants()
            .getReverseOrderedInstants()
            .filter(s -> RECORD_SIZE_ESTIMATE_ACTIONS.contains(s.getAction()))
            .limit(maxCommits).iterator();
        while (instants.hasNext()) {
          HoodieInstant instant = instants.next();
          try {
            HoodieCommitMetadata commitMetadata = commitTimeline.readCommitMetadata(instant);
            final HoodieAtomicLongAccumulator totalBytesWritten = HoodieAtomicLongAccumulator.create();
            final HoodieAtomicLongAccumulator totalRecordsWritten = HoodieAtomicLongAccumulator.create();
            if (instant.getAction().equals(DELTA_COMMIT_ACTION)) {
              // Only use base files for estimate
              commitMetadata.getWriteStats().stream()
                  .filter(hoodieWriteStat -> FSUtils.isBaseFile(new StoragePath(hoodieWriteStat.getPath())))
                  .forEach(hoodieWriteStat -> {
                    totalBytesWritten.add(hoodieWriteStat.getTotalWriteBytes() - metadataSizeEstimate);
                    totalRecordsWritten.add(hoodieWriteStat.getNumWrites());
                  });
            } else {
              totalBytesWritten.add(commitMetadata.fetchTotalBytesWritten() - (commitMetadata.fetchTotalFiles() * metadataSizeEstimate));
              totalRecordsWritten.add(commitMetadata.fetchTotalRecordsWritten());
            }
            if (totalBytesWritten.value() > commitSizeThreshold && totalRecordsWritten.value() > 0) {
              return (long) Math.ceil((1.0 * totalBytesWritten.value()) / totalRecordsWritten.value());
            }
          } catch (IOException ignore) {
            LOG.info("Failed to parse commit metadata", ignore);
          }
        }
      }
    } catch (Throwable t) {
      LOG.info("Got error while trying to compute average bytes/record but will proceed to use the computed value "
          + "or fallback to default config value ", t);
    }
    return hoodieWriteConfig.getCopyOnWriteRecordSizeEstimate();
  }
}