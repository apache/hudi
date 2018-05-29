/*
 *  Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.uber.hoodie.common.table;

import com.uber.hoodie.common.model.FileSlice;
import com.uber.hoodie.common.model.HoodieDataFile;
import com.uber.hoodie.common.model.HoodieFileGroup;
import java.util.List;
import java.util.stream.Stream;

/**
 * Interface for viewing the table file system.
 *
 * @since 0.3.0
 */
public interface TableFileSystemView {

  /**
   * ReadOptimizedView - methods to provide a view of columnar data files only.
   */
  interface ReadOptimizedView {

    /**
     * Stream all the latest data files in the given partition
     */
    Stream<HoodieDataFile> getLatestDataFiles(String partitionPath);

    /**
     * Stream all the latest data files, in the file system view
     */
    Stream<HoodieDataFile> getLatestDataFiles();

    /**
     * Stream all the latest version data files in the given partition with precondition that
     * commitTime(file) before maxCommitTime
     */
    Stream<HoodieDataFile> getLatestDataFilesBeforeOrOn(String partitionPath,
        String maxCommitTime);

    /**
     * Stream all the latest data files pass
     */
    Stream<HoodieDataFile> getLatestDataFilesInRange(List<String> commitsToReturn);

    /**
     * Stream all the data file versions grouped by FileId for a given partition
     */
    Stream<HoodieDataFile> getAllDataFiles(String partitionPath);
  }

  /**
   * RealtimeView - methods to access a combination of columnar data files + log files with real
   * time data.
   */
  interface RealtimeView {

    /**
     * Stream all the latest committed file slices in the given partition
     */
    Stream<FileSlice> getLatestFileSlices(String partitionPath);

    /**
     * Stream all the latest file slices in the given partition with precondition that
     * commitTime(file) before maxCommitTime
     */
    Stream<FileSlice> getLatestFileSlicesBeforeOrOn(String partitionPath,
        String maxCommitTime);

    /**
     * Stream all the latest file slices, in the given range
     */
    Stream<FileSlice> getLatestFileSliceInRange(List<String> commitsToReturn);

    /**
     * Stream all the committed file slices for a given partition, latest or not.
     */
    Stream<FileSlice> getAllFileSlices(String partitionPath);
  }

  /**
   * Stream all the file groups for a given partition
   */
  Stream<HoodieFileGroup> getAllFileGroups(String partitionPath);
}
