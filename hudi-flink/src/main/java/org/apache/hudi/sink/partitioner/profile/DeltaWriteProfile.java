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
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieLogFile;
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
 * WriteProfile for MERGE_ON_READ table type, this allows auto correction of small parquet files to larger ones
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
    HoodieTimeline commitTimeline = metaClient.getCommitsTimeline().filterCompletedInstants();

    // Find out all eligible small file slices
    if (!commitTimeline.empty()) {
      HoodieInstant latestCommitTime = commitTimeline.lastInstant().get();
      // find the smallest file in partition and append to it
      List<FileSlice> allSmallFileSlices = new ArrayList<>();
      // If we can index log files, we can add more inserts to log files for fileIds including those under
      // pending compaction.
      List<FileSlice> allFileSlices = fsView.getLatestFileSlicesBeforeOrOn(partitionPath, latestCommitTime.getTimestamp(), true)
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
          // TODO : Move logic of file name, file id, base commit time handling inside file slice
          String filename = smallFileSlice.getBaseFile().get().getFileName();
          sf.location = new HoodieRecordLocation(FSUtils.getCommitTime(filename), FSUtils.getFileId(filename));
          sf.sizeBytes = getTotalFileSize(smallFileSlice);
          smallFileLocations.add(sf);
        } else {
          smallFileSlice.getLogFiles().findFirst().ifPresent(logFile -> {
            // in case there is something error, and the file slice has no log file
            sf.location = new HoodieRecordLocation(FSUtils.getBaseCommitTimeFromLogPath(logFile.getPath()),
                FSUtils.getFileIdFromLogPath(logFile.getPath()));
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
    if (!fileSlice.getBaseFile().isPresent()) {
      return convertLogFilesSizeToExpectedParquetSize(fileSlice.getLogFiles().collect(Collectors.toList()));
    } else {
      return fileSlice.getBaseFile().get().getFileSize()
          + convertLogFilesSizeToExpectedParquetSize(fileSlice.getLogFiles().collect(Collectors.toList()));
    }
  }

  private boolean isSmallFile(FileSlice fileSlice) {
    long totalSize = getTotalFileSize(fileSlice);
    return totalSize < config.getParquetMaxFileSize();
  }

  // TODO (NA) : Make this static part of utility
  public long convertLogFilesSizeToExpectedParquetSize(List<HoodieLogFile> hoodieLogFiles) {
    long totalSizeOfLogFiles = hoodieLogFiles.stream().map(HoodieLogFile::getFileSize)
        .filter(size -> size > 0).reduce(Long::sum).orElse(0L);
    // Here we assume that if there is no base parquet file, all log files contain only inserts.
    // We can then just get the parquet equivalent size of these log files, compare that with
    // {@link config.getParquetMaxFileSize()} and decide if there is scope to insert more rows
    return (long) (totalSizeOfLogFiles * config.getLogFileToParquetCompressionRatio());
  }
}
