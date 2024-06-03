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

import org.apache.hudi.common.function.SerializableSupplier;
import org.apache.hudi.common.model.CompactionOperation;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

class LazyFileSystemView implements SyncableFileSystemView, Serializable {
  private final SerializableSupplier<SyncableFileSystemView> viewSupplier;
  private transient SyncableFileSystemView view;

  LazyFileSystemView(SerializableSupplier<SyncableFileSystemView> viewSupplier) {
    this.viewSupplier = viewSupplier;
  }

  private synchronized SyncableFileSystemView getView() {
    if (view == null) {
      view = viewSupplier.get();
    }
    return view;
  }

  @Override
  public void close() {
    if (view != null) {
      getView().close();
    }
  }

  @Override
  public void reset() {
    getView().reset();
  }

  @Override
  public void sync() {
    getView().sync();
  }

  @Override
  public Stream<HoodieBaseFile> getLatestBaseFiles(String partitionPath) {
    return getView().getLatestBaseFiles(partitionPath);
  }

  @Override
  public Option<HoodieBaseFile> getLatestBaseFile(String partitionPath, String fileId) {
    return getView().getLatestBaseFile(partitionPath, fileId);
  }

  @Override
  public Stream<HoodieBaseFile> getLatestBaseFiles() {
    return getView().getLatestBaseFiles();
  }

  @Override
  public Stream<HoodieBaseFile> getLatestBaseFilesBeforeOrOn(String partitionPath, String maxCommitTime) {
    return getView().getLatestBaseFilesBeforeOrOn(partitionPath, maxCommitTime);
  }

  @Override
  public Map<String, Stream<HoodieBaseFile>> getAllLatestBaseFilesBeforeOrOn(String maxCommitTime) {
    return getView().getAllLatestBaseFilesBeforeOrOn(maxCommitTime);
  }

  @Override
  public Stream<HoodieBaseFile> getLatestBaseFilesInRange(List<String> commitsToReturn) {
    return getView().getLatestBaseFilesInRange(commitsToReturn);
  }

  @Override
  public Stream<HoodieBaseFile> getAllBaseFiles(String partitionPath) {
    return getView().getAllBaseFiles(partitionPath);
  }

  @Override
  public Option<HoodieBaseFile> getBaseFileOn(String partitionPath, String instantTime, String fileId) {
    return getView().getBaseFileOn(partitionPath, instantTime, fileId);
  }

  @Override
  public Stream<FileSlice> getLatestFileSlices(String partitionPath) {
    return getView().getLatestFileSlices(partitionPath);
  }

  @Override
  public Stream<FileSlice> getLatestFileSlicesIncludingInflight(String partitionPath) {
    return getView().getLatestFileSlicesIncludingInflight(partitionPath);
  }

  @Override
  public Stream<FileSlice> getLatestFileSlicesStateless(String partitionPath) {
    return getView().getLatestFileSlicesStateless(partitionPath);
  }

  @Override
  public Option<FileSlice> getLatestFileSlice(String partitionPath, String fileId) {
    return getView().getLatestFileSlice(partitionPath, fileId);
  }

  @Override
  public Stream<FileSlice> getLatestUnCompactedFileSlices(String partitionPath) {
    return getView().getLatestUnCompactedFileSlices(partitionPath);
  }

  @Override
  public Stream<FileSlice> getLatestFileSlicesBeforeOrOn(String partitionPath, String maxCommitTime, boolean includeFileSlicesInPendingCompaction) {
    return getView().getLatestFileSlicesBeforeOrOn(partitionPath, maxCommitTime, includeFileSlicesInPendingCompaction);
  }

  @Override
  public Map<String, Stream<FileSlice>> getAllLatestFileSlicesBeforeOrOn(String maxCommitTime) {
    return getView().getAllLatestFileSlicesBeforeOrOn(maxCommitTime);
  }

  @Override
  public Stream<FileSlice> getLatestMergedFileSlicesBeforeOrOn(String partitionPath, String maxInstantTime) {
    return getView().getLatestMergedFileSlicesBeforeOrOn(partitionPath, maxInstantTime);
  }

  @Override
  public Stream<FileSlice> getLatestFileSliceInRange(List<String> commitsToReturn) {
    return getView().getLatestFileSliceInRange(commitsToReturn);
  }

  @Override
  public Stream<FileSlice> getAllFileSlices(String partitionPath) {
    return getView().getAllFileSlices(partitionPath);
  }

  @Override
  public Stream<HoodieFileGroup> getAllFileGroups(String partitionPath) {
    return getView().getAllFileGroups(partitionPath);
  }

  @Override
  public Stream<HoodieFileGroup> getAllFileGroupsStateless(String partitionPath) {
    return getView().getAllFileGroupsStateless(partitionPath);
  }

  @Override
  public Stream<Pair<String, CompactionOperation>> getPendingCompactionOperations() {
    return getView().getPendingCompactionOperations();
  }

  @Override
  public Stream<Pair<String, CompactionOperation>> getPendingLogCompactionOperations() {
    return getView().getPendingLogCompactionOperations();
  }

  @Override
  public Option<HoodieInstant> getLastInstant() {
    return getView().getLastInstant();
  }

  @Override
  public HoodieTimeline getTimeline() {
    return getView().getTimeline();
  }

  @Override
  public Stream<HoodieFileGroup> getReplacedFileGroupsBeforeOrOn(String maxCommitTime, String partitionPath) {
    return getView().getReplacedFileGroupsBeforeOrOn(maxCommitTime, partitionPath);
  }

  @Override
  public Stream<HoodieFileGroup> getReplacedFileGroupsBefore(String maxCommitTime, String partitionPath) {
    return getView().getReplacedFileGroupsBefore(maxCommitTime, partitionPath);
  }

  @Override
  public Stream<HoodieFileGroup> getReplacedFileGroupsAfterOrOn(String minCommitTime, String partitionPath) {
    return getView().getReplacedFileGroupsAfterOrOn(minCommitTime, partitionPath);
  }

  @Override
  public Stream<HoodieFileGroup> getAllReplacedFileGroups(String partitionPath) {
    return getView().getAllReplacedFileGroups(partitionPath);
  }

  @Override
  public Stream<Pair<HoodieFileGroupId, HoodieInstant>> getFileGroupsInPendingClustering() {
    return getView().getFileGroupsInPendingClustering();
  }

  @Override
  public void loadAllPartitions() {
    getView().loadAllPartitions();
  }

  @Override
  public void loadPartitions(List<String> partitionPaths) {
    getView().loadPartitions(partitionPaths);
  }
}
