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
import org.apache.hudi.common.util.Functions.Function0;
import org.apache.hudi.common.util.Functions.Function1;
import org.apache.hudi.common.util.Functions.Function2;
import org.apache.hudi.common.util.Functions.Function3;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.http.HttpStatus;
import org.apache.http.client.HttpResponseException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Stream;

/**
 * A file system view which proxies request to a preferred File System View implementation. In case of error, flip all
 * subsequent calls to a backup file-system view implementation.
 */
public class PriorityBasedFileSystemView implements SyncableFileSystemView, Serializable {

  private static final Logger LOG = LogManager.getLogger(PriorityBasedFileSystemView.class);

  private final SyncableFileSystemView preferredView;
  private final SyncableFileSystemView secondaryView;
  private boolean errorOnPreferredView;

  public PriorityBasedFileSystemView(SyncableFileSystemView preferredView, SyncableFileSystemView secondaryView) {
    this.preferredView = preferredView;
    this.secondaryView = secondaryView;
    this.errorOnPreferredView = false;
  }

  private <R> R execute(Function0<R> preferredFunction, Function0<R> secondaryFunction) {
    if (errorOnPreferredView) {
      LOG.warn("Routing request to secondary file-system view");
      return secondaryFunction.apply();
    } else {
      try {
        return preferredFunction.apply();
      } catch (RuntimeException re) {
        handleRuntimeException(re);
        errorOnPreferredView = true;
        return secondaryFunction.apply();
      }
    }
  }

  private <T1, R> R execute(T1 val, Function1<T1, R> preferredFunction, Function1<T1, R> secondaryFunction) {
    if (errorOnPreferredView) {
      LOG.warn("Routing request to secondary file-system view");
      return secondaryFunction.apply(val);
    } else {
      try {
        return preferredFunction.apply(val);
      } catch (RuntimeException re) {
        handleRuntimeException(re);
        errorOnPreferredView = true;
        return secondaryFunction.apply(val);
      }
    }
  }

  private <T1, T2, R> R execute(T1 val, T2 val2, Function2<T1, T2, R> preferredFunction,
      Function2<T1, T2, R> secondaryFunction) {
    if (errorOnPreferredView) {
      LOG.warn("Routing request to secondary file-system view");
      return secondaryFunction.apply(val, val2);
    } else {
      try {
        return preferredFunction.apply(val, val2);
      } catch (RuntimeException re) {
        handleRuntimeException(re);
        errorOnPreferredView = true;
        return secondaryFunction.apply(val, val2);
      }
    }
  }

  private <T1, T2, T3, R> R execute(T1 val, T2 val2, T3 val3, Function3<T1, T2, T3, R> preferredFunction,
      Function3<T1, T2, T3, R> secondaryFunction) {
    if (errorOnPreferredView) {
      LOG.warn("Routing request to secondary file-system view");
      return secondaryFunction.apply(val, val2, val3);
    } else {
      try {
        return preferredFunction.apply(val, val2, val3);
      } catch (RuntimeException re) {
        handleRuntimeException(re);
        errorOnPreferredView = true;
        return secondaryFunction.apply(val, val2, val3);
      }
    }
  }

  private void handleRuntimeException(RuntimeException re) {
    if (re.getCause() instanceof HttpResponseException && ((HttpResponseException)re.getCause()).getStatusCode() == HttpStatus.SC_BAD_REQUEST) {
      LOG.warn("Got error running preferred function. Likely due to another concurrent writer in progress. Trying secondary");
    } else {
      LOG.error("Got error running preferred function. Trying secondary", re);
    }
  }

  @Override
  public Stream<HoodieBaseFile> getLatestBaseFiles(String partitionPath) {
    return execute(partitionPath, preferredView::getLatestBaseFiles, secondaryView::getLatestBaseFiles);
  }

  @Override
  public Stream<HoodieBaseFile> getLatestBaseFiles() {
    return execute(preferredView::getLatestBaseFiles, secondaryView::getLatestBaseFiles);
  }

  @Override
  public Stream<HoodieBaseFile> getLatestBaseFilesBeforeOrOn(String partitionPath, String maxCommitTime) {
    return execute(partitionPath, maxCommitTime, preferredView::getLatestBaseFilesBeforeOrOn,
        secondaryView::getLatestBaseFilesBeforeOrOn);
  }

  @Override
  public Option<HoodieBaseFile> getLatestBaseFile(String partitionPath, String fileId) {
    return execute(partitionPath, fileId, preferredView::getLatestBaseFile, secondaryView::getLatestBaseFile);
  }

  @Override
  public Option<HoodieBaseFile> getBaseFileOn(String partitionPath, String instantTime, String fileId) {
    return execute(partitionPath, instantTime, fileId, preferredView::getBaseFileOn, secondaryView::getBaseFileOn);
  }

  @Override
  public Stream<HoodieBaseFile> getLatestBaseFilesInRange(List<String> commitsToReturn) {
    return execute(commitsToReturn, preferredView::getLatestBaseFilesInRange, secondaryView::getLatestBaseFilesInRange);
  }

  @Override
  public Stream<HoodieBaseFile> getAllBaseFiles(String partitionPath) {
    return execute(partitionPath, preferredView::getAllBaseFiles, secondaryView::getAllBaseFiles);
  }

  @Override
  public Stream<FileSlice> getLatestFileSlices(String partitionPath) {
    return execute(partitionPath, preferredView::getLatestFileSlices, secondaryView::getLatestFileSlices);
  }

  @Override
  public Stream<FileSlice> getLatestUnCompactedFileSlices(String partitionPath) {
    return execute(partitionPath, preferredView::getLatestUnCompactedFileSlices,
        secondaryView::getLatestUnCompactedFileSlices);
  }

  @Override
  public Stream<FileSlice> getLatestFileSlicesBeforeOrOn(String partitionPath, String maxCommitTime,
      boolean includeFileSlicesInPendingCompaction) {
    return execute(partitionPath, maxCommitTime, includeFileSlicesInPendingCompaction,
        preferredView::getLatestFileSlicesBeforeOrOn, secondaryView::getLatestFileSlicesBeforeOrOn);
  }

  @Override
  public Stream<FileSlice> getLatestMergedFileSlicesBeforeOrOn(String partitionPath, String maxInstantTime) {
    return execute(partitionPath, maxInstantTime, preferredView::getLatestMergedFileSlicesBeforeOrOn,
        secondaryView::getLatestMergedFileSlicesBeforeOrOn);
  }

  @Override
  public Stream<FileSlice> getLatestFileSliceInRange(List<String> commitsToReturn) {
    return execute(commitsToReturn, preferredView::getLatestFileSliceInRange, secondaryView::getLatestFileSliceInRange);
  }

  @Override
  public Stream<FileSlice> getAllFileSlices(String partitionPath) {
    return execute(partitionPath, preferredView::getAllFileSlices, secondaryView::getAllFileSlices);
  }

  @Override
  public Stream<HoodieFileGroup> getAllFileGroups(String partitionPath) {
    return execute(partitionPath, preferredView::getAllFileGroups, secondaryView::getAllFileGroups);
  }

  @Override
  public Stream<HoodieFileGroup> getReplacedFileGroupsBeforeOrOn(String maxCommitTime, String partitionPath) {
    return execute(maxCommitTime, partitionPath, preferredView::getReplacedFileGroupsBeforeOrOn, secondaryView::getReplacedFileGroupsBeforeOrOn);
  }

  @Override
  public Stream<HoodieFileGroup> getReplacedFileGroupsBefore(String maxCommitTime, String partitionPath) {
    return execute(maxCommitTime, partitionPath, preferredView::getReplacedFileGroupsBefore, secondaryView::getReplacedFileGroupsBefore);
  }

  @Override
  public Stream<HoodieFileGroup> getAllReplacedFileGroups(String partitionPath) {
    return execute(partitionPath, preferredView::getAllReplacedFileGroups, secondaryView::getAllReplacedFileGroups);
  }

  @Override
  public Stream<Pair<String, CompactionOperation>> getPendingCompactionOperations() {
    return execute(preferredView::getPendingCompactionOperations, secondaryView::getPendingCompactionOperations);
  }

  @Override
  public Stream<Pair<String, CompactionOperation>> getPendingLogCompactionOperations() {
    return execute(preferredView::getPendingLogCompactionOperations, secondaryView::getPendingLogCompactionOperations);
  }

  @Override
  public Stream<Pair<HoodieFileGroupId, HoodieInstant>> getFileGroupsInPendingClustering() {
    return execute(preferredView::getFileGroupsInPendingClustering, secondaryView::getFileGroupsInPendingClustering);
  }

  @Override
  public void close() {
    preferredView.close();
    secondaryView.close();
  }

  @Override
  public void reset() {
    preferredView.reset();
    secondaryView.reset();
    errorOnPreferredView = false;
  }

  @Override
  public Option<HoodieInstant> getLastInstant() {
    return execute(preferredView::getLastInstant, secondaryView::getLastInstant);
  }

  @Override
  public HoodieTimeline getTimeline() {
    return execute(preferredView::getTimeline, secondaryView::getTimeline);
  }

  @Override
  public void sync() {
    preferredView.sync();
    secondaryView.sync();
    errorOnPreferredView = false;
  }

  @Override
  public Option<FileSlice> getLatestFileSlice(String partitionPath, String fileId) {
    return execute(partitionPath, fileId, preferredView::getLatestFileSlice, secondaryView::getLatestFileSlice);
  }

  public SyncableFileSystemView getPreferredView() {
    return preferredView;
  }

  public SyncableFileSystemView getSecondaryView() {
    return secondaryView;
  }
}
