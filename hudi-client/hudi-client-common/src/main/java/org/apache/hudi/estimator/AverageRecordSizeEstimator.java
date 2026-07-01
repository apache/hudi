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

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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
@Slf4j
public class AverageRecordSizeEstimator extends RecordSizeEstimator {
  /*
   * NOTE: we only use commit instants to calculate average record size because replacecommit can be
   * created by clustering, which has smaller average record size, which affects assigning inserts and
   * may result in OOM by making spark underestimate the actual input record sizes.
   */
  private static final Set<String> RECORD_SIZE_ESTIMATE_ACTIONS = CollectionUtils.createSet(COMMIT_ACTION, DELTA_COMMIT_ACTION, COMPACTION_ACTION);
  private static final long ESTIMATOR_TIMEOUT_MINUTES = 5;
  private static final AtomicInteger THREAD_COUNTER = new AtomicInteger(0);
  // Dedicated static thread pool for parallel commit metadata deserialization. Uses a shared pool
  // instead of ForkJoinPool.commonPool() to avoid deadlocks
  private static final ExecutorService ESTIMATOR_POOL;

  static {
    int maxThreads = 2 * Runtime.getRuntime().availableProcessors();
    ESTIMATOR_POOL = new ThreadPoolExecutor(
        0, maxThreads,
        60L, TimeUnit.SECONDS,
        new LinkedBlockingQueue<>(),
        r -> new Thread(r, "hudi-record-size-estimator-" + THREAD_COUNTER.getAndIncrement()));
  }

  public AverageRecordSizeEstimator(HoodieWriteConfig writeConfig) {
    super(writeConfig);
  }

  @Override
  public long averageBytesPerRecord(HoodieTimeline commitTimeline, CommitMetadataSerDe commitMetadataSerDe) {
    int maxCommits = hoodieWriteConfig.getRecordSizeEstimatorMaxCommits();
    AverageRecordSizeStats stats = new AverageRecordSizeStats(hoodieWriteConfig);
    try {
      if (!commitTimeline.empty()) {
        // Go over the reverse ordered commits to get a more recent estimate of average record size.
        CompletableFuture<?>[] futures = commitTimeline.filterCompletedInstants()
            .getReverseOrderedInstants()
            .filter(s -> RECORD_SIZE_ESTIMATE_ACTIONS.contains(s.getAction()))
            .limit(maxCommits)
            .map(instant -> CompletableFuture.runAsync(() -> processInstant(commitTimeline, instant, stats), ESTIMATOR_POOL))
            .toArray(CompletableFuture[]::new);
        CompletableFuture.allOf(futures).get(ESTIMATOR_TIMEOUT_MINUTES, TimeUnit.MINUTES);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      log.warn("Interrupted while computing average bytes/record, will use partially computed average or default record size", e);
    } catch (Exception e) {
      log.warn("Error computing average bytes/record, will use partially computed average or default record size", e);
    }
    return stats.computeAverageRecordSize();
  }

  private void processInstant(HoodieTimeline commitTimeline, HoodieInstant instant, AverageRecordSizeStats stats) {
    try {
      HoodieCommitMetadata commitMetadata =
          commitTimeline.deserializeInstantContent(instant, HoodieCommitMetadata.class);
      if (instant.getAction().equals(DELTA_COMMIT_ACTION)) {
        // Only consider base files for delta commits
        commitMetadata.getWriteStats().stream()
            .filter(writeStat -> FSUtils.isBaseFile(new Path(writeStat.getPath())))
            .forEach(writeStat -> stats.updateStats(writeStat.getTotalWriteBytes(), writeStat.getNumWrites()));
      } else {
        stats.updateStats(commitMetadata.fetchTotalBytesWritten(), commitMetadata.fetchTotalRecordsWritten());
      }
    } catch (IOException ignore) {
      LOG.info("Failed to parse commit metadata", ignore);
    }
  }

  private static class AverageRecordSizeStats implements Serializable {
    private final HoodieAtomicLongAccumulator totalBytesWritten;
    private final HoodieAtomicLongAccumulator totalRecordsWritten;
    private final long fileSizeThreshold;
    private final long avgMetadataSize;
    private final int defaultRecordSize;

    public AverageRecordSizeStats(HoodieWriteConfig hoodieWriteConfig) {
      totalBytesWritten = HoodieAtomicLongAccumulator.create();
      totalRecordsWritten = HoodieAtomicLongAccumulator.create();
      fileSizeThreshold = (long) (hoodieWriteConfig.getRecordSizeEstimationThreshold() * hoodieWriteConfig.getParquetSmallFileLimit());
      avgMetadataSize = hoodieWriteConfig.getRecordSizeEstimatorAverageMetadataSize();
      defaultRecordSize = hoodieWriteConfig.getCopyOnWriteRecordSizeEstimate();
    }

    private void updateStats(long fileSizeInBytes, long recordWritten) {
      if (fileSizeInBytes > fileSizeThreshold && fileSizeInBytes > avgMetadataSize && recordWritten > 0) {
        totalBytesWritten.add(fileSizeInBytes - avgMetadataSize);
        totalRecordsWritten.add(recordWritten);
      }
    }

    private long computeAverageRecordSize() {
      if (totalBytesWritten.value() > 0 && totalRecordsWritten.value() > 0) {
        return totalBytesWritten.value() / totalRecordsWritten.value();
      }
      // Fallback to default implementation in the cases were we either got an exception before we could
      // compute the average record size or there are no eligible commits yet.
      return defaultRecordSize;
    }
  }
}
