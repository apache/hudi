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

package org.apache.hudi.table.action.deltacommit;

import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.WorkloadProfile;
import org.apache.hudi.table.action.commit.SmallFile;
import org.apache.hudi.table.action.commit.UpsertPartitioner;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * UpsertPartitioner for MergeOnRead table type, this allows auto correction of small parquet files to larger ones
 * without the need for an index in the logFile.
 */
public class SparkUpsertDeltaCommitPartitioner<T> extends UpsertPartitioner<T> {

  public SparkUpsertDeltaCommitPartitioner(WorkloadProfile profile, HoodieSparkEngineContext context, HoodieTable table,
                                           HoodieWriteConfig config, WriteOperationType operationType) {
    super(profile, context, table, config, operationType);
  }

  @Override
  protected List<SmallFile> getSmallFiles(String partitionPath) {
    // Init here since this class (and member variables) might not have been initialized
    HoodieTimeline commitTimeline = table.getCompletedCommitsTimeline();

    if (commitTimeline.empty()) {
      return Collections.emptyList();
    }

    HoodieInstant latestCommitTime = commitTimeline.lastInstant().get();

    // Find out all eligible small file slices, looking for
    // smallest file in the partition to append to
    List<FileSlice> smallFileSlicesCandidates = getSmallFileCandidates(partitionPath, latestCommitTime);
    List<SmallFile> smallFileLocations = new ArrayList<>();

    // Create SmallFiles from the eligible file slices
    for (FileSlice smallFileSlice : smallFileSlicesCandidates) {
      SmallFile sf = new SmallFile();
      if (smallFileSlice.getBaseFile().isPresent()) {
        HoodieBaseFile baseFile = smallFileSlice.getBaseFile().get();
        sf.location = new HoodieRecordLocation(baseFile.getCommitTime(), baseFile.getFileId());
        sf.sizeBytes = smallFileSlice.getTotalFileSizeAsParquetFormat(config.getLogFileToParquetCompressionRatio());
        smallFileLocations.add(sf);
      } else {
        HoodieLogFile logFile = smallFileSlice.getLogFiles().findFirst().get();
        sf.location = new HoodieRecordLocation(logFile.getDeltaCommitTime(),
            logFile.getFileId());
        sf.sizeBytes = smallFileSlice.getTotalFileSizeAsParquetFormat(config.getLogFileToParquetCompressionRatio());
        smallFileLocations.add(sf);
      }
    }
    return smallFileLocations;
  }

  @Nonnull
  private List<FileSlice> getSmallFileCandidates(String partitionPath, HoodieInstant latestCommitInstant) {
    // If we can index log files, we can add more inserts to log files for fileIds NOT including those under
    // pending compaction
    if (table.getIndex().canIndexLogFiles()) {
      return table.getSliceView()
              .getLatestFileSlicesBeforeOrOn(partitionPath, latestCommitInstant.requestedTime(), false)
              .filter(this::isSmallFile)
              .collect(Collectors.toList());
    }

    if (config.getParquetSmallFileLimit() <= 0) {
      return Collections.emptyList();
    }

    // If we cannot index log files, then we choose the smallest parquet file in the partition and add inserts to
    // it. Doing this overtime for a partition, we ensure that we handle small file issues
    return table.getSliceView()
          .getLatestFileSlicesBeforeOrOn(partitionPath, latestCommitInstant.requestedTime(), false)
          .filter(
              fileSlice ->
                  // NOTE: We can not pad slices with existing log-files w/o compacting these,
                  //       hence skipping
                  fileSlice.getLogFiles().count() < 1
                  && fileSlice.getBaseFile().get().getFileSize() < config.getParquetSmallFileLimit())
          .sorted(Comparator.comparing(fileSlice -> fileSlice.getBaseFile().get().getFileSize()))
          .limit(config.getSmallFileGroupCandidatesLimit())
          .collect(Collectors.toList());
  }

  public List<String> getSmallFileIds() {
    return smallFiles.stream().map(smallFile -> smallFile.location.getFileId())
        .collect(Collectors.toList());
  }

  private boolean isSmallFile(FileSlice fileSlice) {
    long totalSize = fileSlice.getTotalFileSizeAsParquetFormat(config.getLogFileToParquetCompressionRatio());
    return totalSize < config.getParquetMaxFileSize();
  }

}
