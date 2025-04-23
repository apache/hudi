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

package org.apache.hudi.sink.partitioner.profile;

import org.apache.hudi.client.common.HoodieFlinkEngineContext;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.action.commit.SmallFile;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * DeltaWriteProfile for MERGE_ON_READ table type, this allows auto correction of small parquet files to larger ones
 * without the need for an index in the logFile.
 *
 * <p>Note: assumes the index can always index log files for Flink write.
 */
public class DeltaWriteProfile extends WriteProfile {
  public DeltaWriteProfile(HoodieWriteConfig config, HoodieFlinkEngineContext context) {
    super(config, context);
  }

  @Override
  protected List<SmallFile> smallFilesProfile(String partitionPath) {
    // smallFiles only for partitionPath
    List<SmallFile> smallFileLocations = new ArrayList<>();

    // Init here since this class (and member variables) might not have been initialized
    HoodieTimeline commitTimeline = metaClient.getCommitsTimeline().filterCompletedAndCompactionInstants();

    // Find out all eligible small file slices
    if (!commitTimeline.empty()) {
      HoodieInstant latestCommitTime = commitTimeline.lastInstant().get();
      // find the smallest file in partition and append to it
      List<FileSlice> allSmallFileSlices = new ArrayList<>();
      // If we can index log files, we can add more inserts to log files for fileIds including those under
      // pending compaction.
      List<FileSlice> allFileSlices = fsView.getLatestMergedFileSlicesBeforeOrOn(partitionPath, latestCommitTime.requestedTime())
          .collect(Collectors.toList());
      for (FileSlice fileSlice : allFileSlices) {
        if (isSmallFile(fileSlice)) {
          allSmallFileSlices.add(fileSlice);
        }
      }
      // Create SmallFiles from the eligible file slices
      for (FileSlice smallFileSlice : allSmallFileSlices) {
        SmallFile sf = new SmallFile();
        if (smallFileSlice.getBaseFile().isPresent()) {
          HoodieBaseFile baseFile = smallFileSlice.getBaseFile().get();
          sf.location = new HoodieRecordLocation(baseFile.getCommitTime(), baseFile.getFileId());
          sf.sizeBytes = getTotalFileSize(smallFileSlice);
          smallFileLocations.add(sf);
        } else {
          smallFileSlice.getLogFiles().findFirst().ifPresent(logFile -> {
            // in case there is something error, and the file slice has no log file
            sf.location = new HoodieRecordLocation(logFile.getDeltaCommitTime(), logFile.getFileId());
            sf.sizeBytes = getTotalFileSize(smallFileSlice);
            smallFileLocations.add(sf);
          });
        }
      }
    }
    return smallFileLocations;
  }

  protected SyncableFileSystemView getFileSystemView() {
    return (SyncableFileSystemView) getTable().getSliceView();
  }

  private long getTotalFileSize(FileSlice fileSlice) {
    return fileSlice.getTotalFileSizeAsParquetFormat(config.getLogFileToParquetCompressionRatio());
  }

  private boolean isSmallFile(FileSlice fileSlice) {
    long totalSize = getTotalFileSize(fileSlice);
    return totalSize < config.getParquetMaxFileSize();
  }

}
