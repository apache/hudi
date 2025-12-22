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

package org.apache.hudi.common.table.view;

import org.apache.hudi.common.model.CompactionOperation;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Interface for viewing the table file system.
 *
 * @since 0.3.0
 */
public interface TableFileSystemView {

  /**
   * Methods to only access latest version of file for the instant(s) passed.
   */
  interface BaseFileOnlyViewWithLatestSlice {

    /**
     * Stream all the latest data files in the given partition.
     */
    Stream<HoodieBaseFile> getLatestBaseFiles(String partitionPath);

    /**
     * Get Latest data file for a partition and file-Id.
     */
    Option<HoodieBaseFile> getLatestBaseFile(String partitionPath, String fileId);

    /**
     * Stream all the latest data files, in the file system view.
     */
    Stream<HoodieBaseFile> getLatestBaseFiles();

    /**
     * Stream all the latest version data files in the given partition with precondition that commitTime(file) before
     * maxCommitTime.
     */
    Stream<HoodieBaseFile> getLatestBaseFilesBeforeOrOn(String partitionPath, String maxCommitTime);

    /**
     * Streams the latest version base files in all partitions with precondition that
     * commitTime(file) before maxCommitTime.
     *
     * @param maxCommitTime The max commit time to consider.
     * @return A {@link Map} of partition path to the latest version base files before or on the
     * commit time
     */
    Map<String, Stream<HoodieBaseFile>> getAllLatestBaseFilesBeforeOrOn(String maxCommitTime);

    /**
     * Stream all the latest data files pass.
     */
    Stream<HoodieBaseFile> getLatestBaseFilesInRange(List<String> commitsToReturn);
  }

  /**
   * Methods to provide a view of base files only.
   */
  interface BaseFileOnlyView extends BaseFileOnlyViewWithLatestSlice {
    /**
     * Stream all the data file versions grouped by FileId for a given partition.
     */
    Stream<HoodieBaseFile> getAllBaseFiles(String partitionPath);

    /**
     * Get the version of data file matching the instant time in the given partition.
     */
    Option<HoodieBaseFile> getBaseFileOn(String partitionPath, String instantTime, String fileId);

  }

  /**
   * Methods to only access latest version of file-slice for the instant(s) passed.
   */
  interface SliceViewWithLatestSlice {

    /**
     * Stream all the latest file slices in the given partition.
     */
    Stream<FileSlice> getLatestFileSlices(String partitionPath);

    /**
     * Stream all the latest file slices in the given partition
     * without caching the file group mappings.
     *
     * <p>This is useful for some table services such as compaction and clustering, these services may search around the files to clean
     * within some ancient data partitions, if there triggers a full table service for enormous number of partitions, the cache could
     * cause a huge memory pressure to the timeline server which induces an OOM exception.
     *
     * <p>The caching of these file groups does not benefit to writers most often because the writers
     * write to recent data partitions usually.
     */
    Stream<FileSlice> getLatestFileSlicesStateless(String partitionPath);

    /**
     * Get Latest File Slice for a given fileId in a given partition.
     */
    Option<FileSlice> getLatestFileSlice(String partitionPath, String fileId);

    /**
     * Stream all the latest uncompacted file slices in the given partition.
     */
    Stream<FileSlice> getLatestUnCompactedFileSlices(String partitionPath);

    /**
     * Stream all latest file slices in given partition with precondition that commitTime(file) before maxCommitTime.
     *
     * @param partitionPath Partition path
     * @param maxCommitTime Max Instant Time
     * @param includeFileSlicesInPendingCompaction include file-slices that are in pending compaction
     */
    Stream<FileSlice> getLatestFileSlicesBeforeOrOn(String partitionPath, String maxCommitTime,
        boolean includeFileSlicesInPendingCompaction);

    /**
     * Stream all latest file slices with precondition that commitTime(file) before maxCommitTime.
     *
     * @param maxCommitTime Max Instant Time
     * @return A {@link Map} of partition path to the latest file slices before maxCommitTime.
     */
    Map<String, Stream<FileSlice>> getAllLatestFileSlicesBeforeOrOn(String maxCommitTime);

    /**
     * Stream all "merged" file-slices before on an instant time If a file-group has a pending compaction request, the
     * file-slice before and after compaction request instant is merged and returned.
     * 
     * @param partitionPath Partition Path
     * @param maxInstantTime Max Instant Time
     * @return
     */
    Stream<FileSlice> getLatestMergedFileSlicesBeforeOrOn(String partitionPath, String maxInstantTime);

    /**
     * Stream all the latest file slices, in the given range.
     */
    Stream<FileSlice> getLatestFileSliceInRange(List<String> commitsToReturn);
  }

  /**
   * Methods to access a combination of base files + log file slices.
   */
  interface SliceView extends SliceViewWithLatestSlice {

    /**
     * Stream all the file slices for a given partition, latest or not.
     */
    Stream<FileSlice> getAllFileSlices(String partitionPath);

  }

  /**
   * Stream all the file groups for a given partition.
   */
  Stream<HoodieFileGroup> getAllFileGroups(String partitionPath);

  /**
   * Stream all the file groups for a given partition without caching the file group mappings.
   *
   * <p>This is useful for some table services such as cleaning, the cleaning service may search around the files to clean
   * within some ancient data partitions, if there triggers a full table cleaning for enormous number of partitions, the cache could
   * cause a huge memory pressure to the timeline server which induces an OOM exception.
   *
   * <p>The caching of these file groups does not benefit to writers most often because the writers
   * write to recent data partitions usually.
   */
  Stream<HoodieFileGroup> getAllFileGroupsStateless(String partitionPath);

  /**
   * Return Pending Compaction Operations.
   *
   * @return Stream<Pair<InstantTime,CompactionOperation>>
   */
  Stream<Pair<String, CompactionOperation>> getPendingCompactionOperations();

  /**
   * Return Pending Compaction Operations.
   *
   * @return Stream<Pair<InstantTime,CompactionOperation>>
   */
  Stream<Pair<String, CompactionOperation>> getPendingLogCompactionOperations();

  /**
   * Last Known Instant on which the view is built.
   */
  Option<HoodieInstant> getLastInstant();

  /**
   * Timeline corresponding to the view.
   */
  HoodieTimeline getTimeline();

  /**
   * Stream all the replaced file groups before or on maxCommitTime for given partition.
   */
  Stream<HoodieFileGroup> getReplacedFileGroupsBeforeOrOn(String maxCommitTime, String partitionPath);

  /**
   * Stream all the replaced file groups before maxCommitTime for given partition.
   */
  Stream<HoodieFileGroup> getReplacedFileGroupsBefore(String maxCommitTime, String partitionPath);

  /**
   * Stream all the replaced file groups after or on minCommitTime.
   */
  Stream<HoodieFileGroup> getReplacedFileGroupsAfterOrOn(String minCommitTime, String partitionPath);

  /**
   * Stream all the replaced file groups for given partition.
   */
  Stream<HoodieFileGroup> getAllReplacedFileGroups(String partitionPath);

  /**
   * Filegroups that are in pending clustering.
   */
  Stream<Pair<HoodieFileGroupId, HoodieInstant>> getFileGroupsInPendingClustering();


  /**
   * Load all partition and file slices into view
   */
  void loadAllPartitions();

  /**
   * Load all partition and file slices into view for the provided partition paths
   * @param partitionPaths List of partition paths to load
   */
  void loadPartitions(List<String> partitionPaths);
}
