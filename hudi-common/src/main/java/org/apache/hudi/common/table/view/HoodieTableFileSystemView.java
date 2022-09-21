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

import org.apache.hudi.common.model.BootstrapBaseFileMapping;
import org.apache.hudi.common.model.CompactionOperation;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.secondary.index.HoodieSecondaryIndex;

import org.apache.hadoop.fs.FileStatus;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * TableFileSystemView Implementations based on in-memory storage.
 * 
 * @see TableFileSystemView
 * @since 0.3.0
 */
public class HoodieTableFileSystemView extends IncrementalTimelineSyncFileSystemView {

  private static final Logger LOG = LogManager.getLogger(HoodieTableFileSystemView.class);

  // mapping from partition paths to file groups contained within them
  protected Map<String, List<HoodieFileGroup>> partitionToFileGroupsMap;

  /**
   * PartitionPath + File-Id to pending compaction instant time.
   */
  protected Map<HoodieFileGroupId, Pair<String, CompactionOperation>> fgIdToPendingCompaction;

  /**
   * PartitionPath + File-Id to bootstrap base File (Index Only bootstrapped).
   */
  protected Map<HoodieFileGroupId, BootstrapBaseFileMapping> fgIdToBootstrapBaseFile;

  /**
   * Track replace time for replaced file groups.
   */
  protected Map<HoodieFileGroupId, HoodieInstant> fgIdToReplaceInstants;

  /**
   * Track file groups in pending clustering.
   */
  protected Map<HoodieFileGroupId, HoodieInstant> fgIdToPendingClustering;

  /**
   * Track base files in secondary index.
   */
  protected Map<HoodieSecondaryIndex, Map<String, HoodieInstant>> secondaryIndexToBaseFiles;

  /**
   * Track base files in pending secondary index.
   */
  protected Map<HoodieSecondaryIndex, Map<String, HoodieInstant>> pendingSecondaryIndexToBaseFiles;

  /**
   * Flag to determine if closed.
   */
  private boolean closed = false;

  HoodieTableFileSystemView(boolean enableIncrementalTimelineSync) {
    super(enableIncrementalTimelineSync);
  }

  /**
   * Create a file system view, as of the given timeline.
   */
  public HoodieTableFileSystemView(HoodieTableMetaClient metaClient, HoodieTimeline visibleActiveTimeline) {
    this(metaClient, visibleActiveTimeline, false);
  }

  /**
   * Create a file system view, as of the given timeline.
   */
  public HoodieTableFileSystemView(HoodieTableMetaClient metaClient, HoodieTimeline visibleActiveTimeline,
      boolean enableIncrementalTimelineSync) {
    super(enableIncrementalTimelineSync);
    init(metaClient, visibleActiveTimeline);
  }

  @Override
  public void init(HoodieTableMetaClient metaClient, HoodieTimeline visibleActiveTimeline) {
    this.partitionToFileGroupsMap = createPartitionToFileGroups();
    super.init(metaClient, visibleActiveTimeline);
  }

  public void init(HoodieTableMetaClient metaClient, HoodieTimeline visibleActiveTimeline,
      FileStatus[] fileStatuses) {
    init(metaClient, visibleActiveTimeline);
    addFilesToView(fileStatuses);
  }

  @Override
  protected void resetViewState() {
    // do not nullify the members to avoid NPE.

    // there are two cases that #resetViewState is called:
    // 1. when #sync is invoked, the view clear the state through calling #resetViewState,
    // then re-initialize the view;
    // 2. when #close is invoked.
    // (see AbstractTableFileSystemView for details.)

    // for the 1st case, we better do not nullify the members when #resetViewState
    // because there is possibility that this in-memory view is a backend view under TimelineServer,
    // and many methods in the RequestHandler is not thread safe, when performRefreshCheck flag in ViewHandler
    // is set as false, the view does not perform refresh check, if #sync is called just before and the members
    // are nullified, the methods that use these members would throw NPE.

    // actually there is no need to nullify the members here for 1st case, the members are assigned with new values
    // when calling #init, for 2nd case, the #close method already nullify the members.
  }

  protected Map<String, List<HoodieFileGroup>> createPartitionToFileGroups() {
    return new ConcurrentHashMap<>();
  }

  protected Map<HoodieFileGroupId, Pair<String, CompactionOperation>> createFileIdToPendingCompactionMap(
      Map<HoodieFileGroupId, Pair<String, CompactionOperation>> fileIdToPendingCompaction) {
    return fileIdToPendingCompaction;
  }

  protected Map<HoodieFileGroupId, BootstrapBaseFileMapping> createFileIdToBootstrapBaseFileMap(
      Map<HoodieFileGroupId, BootstrapBaseFileMapping> fileGroupIdBootstrapBaseFileMap) {
    return fileGroupIdBootstrapBaseFileMap;
  }

  protected Map<HoodieFileGroupId, HoodieInstant> createFileIdToReplaceInstantMap(final Map<HoodieFileGroupId, HoodieInstant> replacedFileGroups) {
    Map<HoodieFileGroupId, HoodieInstant> replacedFileGroupsMap = new ConcurrentHashMap<>(replacedFileGroups);
    return replacedFileGroupsMap;
  }

  protected Map<HoodieFileGroupId, HoodieInstant> createFileIdToPendingClusteringMap(final Map<HoodieFileGroupId, HoodieInstant> fileGroupsInClustering) {
    Map<HoodieFileGroupId, HoodieInstant> fgInpendingClustering = new ConcurrentHashMap<>(fileGroupsInClustering);
    return fgInpendingClustering;
  }

  protected Map<HoodieSecondaryIndex, Map<String, HoodieInstant>> createSecondaryIndexToBaseFilesMap(
      final Map<HoodieSecondaryIndex, Map<String, HoodieInstant>> secondaryIndexToBaseFiles) {
    return new ConcurrentHashMap<>(secondaryIndexToBaseFiles);
  }

  protected Map<HoodieSecondaryIndex, Map<String, HoodieInstant>> createPendingSecondaryIndexToBaseFilesMap(
      final Map<HoodieSecondaryIndex, Map<String, HoodieInstant>> pendingSecondaryIndexToBaseFiles) {
    return new ConcurrentHashMap<>(pendingSecondaryIndexToBaseFiles);
  }

  /**
   * Create a file system view, as of the given timeline, with the provided file statuses.
   */
  public HoodieTableFileSystemView(HoodieTableMetaClient metaClient, HoodieTimeline visibleActiveTimeline,
                                   FileStatus[] fileStatuses) {
    this(metaClient, visibleActiveTimeline);
    addFilesToView(fileStatuses);
  }

  /**
   * This method is only used when this object is deserialized in a spark executor.
   *
   * @deprecated
   */
  private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
  }

  private void writeObject(java.io.ObjectOutputStream out) throws IOException {
    out.defaultWriteObject();
  }

  @Override
  protected boolean isPendingCompactionScheduledForFileId(HoodieFileGroupId fgId) {
    return fgIdToPendingCompaction.containsKey(fgId);
  }

  @Override
  protected void resetPendingCompactionOperations(Stream<Pair<String, CompactionOperation>> operations) {
    // Build fileId to Pending Compaction Instants
    this.fgIdToPendingCompaction = createFileIdToPendingCompactionMap(operations.map(entry ->
      Pair.of(entry.getValue().getFileGroupId(), Pair.of(entry.getKey(), entry.getValue()))).collect(Collectors.toMap(Pair::getKey, Pair::getValue)));
  }

  @Override
  protected void addPendingCompactionOperations(Stream<Pair<String, CompactionOperation>> operations) {
    operations.forEach(opInstantPair -> {
      ValidationUtils.checkArgument(!fgIdToPendingCompaction.containsKey(opInstantPair.getValue().getFileGroupId()),
          "Duplicate FileGroupId found in pending compaction operations. FgId :"
              + opInstantPair.getValue().getFileGroupId());
      fgIdToPendingCompaction.put(opInstantPair.getValue().getFileGroupId(),
          Pair.of(opInstantPair.getKey(), opInstantPair.getValue()));
    });
  }

  @Override
  protected void removePendingCompactionOperations(Stream<Pair<String, CompactionOperation>> operations) {
    operations.forEach(opInstantPair -> {
      ValidationUtils.checkArgument(fgIdToPendingCompaction.containsKey(opInstantPair.getValue().getFileGroupId()),
          "Trying to remove a FileGroupId which is not found in pending compaction operations. FgId :"
              + opInstantPair.getValue().getFileGroupId());
      fgIdToPendingCompaction.remove(opInstantPair.getValue().getFileGroupId());
    });
  }

  @Override
  protected boolean isPendingClusteringScheduledForFileId(HoodieFileGroupId fgId) {
    return fgIdToPendingClustering.containsKey(fgId);
  }

  @Override
  protected Option<HoodieInstant> getPendingClusteringInstant(HoodieFileGroupId fgId) {
    return Option.ofNullable(fgIdToPendingClustering.get(fgId));
  }

  @Override
  protected Stream<Pair<HoodieFileGroupId, HoodieInstant>> fetchFileGroupsInPendingClustering() {
    return fgIdToPendingClustering.entrySet().stream().map(entry -> Pair.of(entry.getKey(), entry.getValue()));
  }

  @Override
  void resetFileGroupsInPendingClustering(Map<HoodieFileGroupId, HoodieInstant> fgIdToInstantMap) {
    fgIdToPendingClustering = createFileIdToPendingClusteringMap(fgIdToInstantMap);
  }

  @Override
  void addFileGroupsInPendingClustering(Stream<Pair<HoodieFileGroupId, HoodieInstant>> fileGroups) {
    fileGroups.forEach(fileGroupInstantPair -> {
      ValidationUtils.checkArgument(fgIdToPendingClustering.containsKey(fileGroupInstantPair.getLeft()),
          "Trying to add a FileGroupId which is already in pending clustering operation. FgId :"
              + fileGroupInstantPair.getLeft() + ", new instant: " + fileGroupInstantPair.getRight() + ", existing instant "
              + fgIdToPendingClustering.get(fileGroupInstantPair.getLeft()));

      fgIdToPendingClustering.put(fileGroupInstantPair.getLeft(), fileGroupInstantPair.getRight());
    });
  }

  @Override
  void removeFileGroupsInPendingClustering(Stream<Pair<HoodieFileGroupId, HoodieInstant>> fileGroups) {
    fileGroups.forEach(fileGroupInstantPair -> {
      ValidationUtils.checkArgument(fgIdToPendingClustering.containsKey(fileGroupInstantPair.getLeft()),
          "Trying to remove a FileGroupId which is not found in pending clustering operation. FgId :"
              + fileGroupInstantPair.getLeft() + ", new instant: " + fileGroupInstantPair.getRight());

      fgIdToPendingClustering.remove(fileGroupInstantPair.getLeft());
    });
  }

  /**
   * Given a partition path, obtain all filegroups within that. All methods, that work at the partition level go through
   * this.
   */
  @Override
  Stream<HoodieFileGroup> fetchAllStoredFileGroups(String partition) {
    final List<HoodieFileGroup> fileGroups = new ArrayList<>(partitionToFileGroupsMap.get(partition));
    return fileGroups.stream();
  }

  public Stream<HoodieFileGroup> getAllFileGroups() {
    return fetchAllStoredFileGroups();
  }

  @Override
  Stream<Pair<String, CompactionOperation>> fetchPendingCompactionOperations() {
    return fgIdToPendingCompaction.values().stream();

  }

  @Override
  protected boolean isBootstrapBaseFilePresentForFileId(HoodieFileGroupId fgId) {
    return fgIdToBootstrapBaseFile.containsKey(fgId);
  }

  @Override
  void resetBootstrapBaseFileMapping(Stream<BootstrapBaseFileMapping> bootstrapBaseFileStream) {
    // Build fileId to bootstrap Data File
    this.fgIdToBootstrapBaseFile = createFileIdToBootstrapBaseFileMap(bootstrapBaseFileStream
        .collect(Collectors.toMap(BootstrapBaseFileMapping::getFileGroupId, x -> x)));
  }

  @Override
  void addBootstrapBaseFileMapping(Stream<BootstrapBaseFileMapping> bootstrapBaseFileStream) {
    bootstrapBaseFileStream.forEach(bootstrapBaseFile -> {
      ValidationUtils.checkArgument(!fgIdToBootstrapBaseFile.containsKey(bootstrapBaseFile.getFileGroupId()),
          "Duplicate FileGroupId found in bootstrap base file mapping. FgId :"
              + bootstrapBaseFile.getFileGroupId());
      fgIdToBootstrapBaseFile.put(bootstrapBaseFile.getFileGroupId(), bootstrapBaseFile);
    });
  }

  @Override
  void removeBootstrapBaseFileMapping(Stream<BootstrapBaseFileMapping> bootstrapBaseFileStream) {
    bootstrapBaseFileStream.forEach(bootstrapBaseFile -> {
      ValidationUtils.checkArgument(fgIdToBootstrapBaseFile.containsKey(bootstrapBaseFile.getFileGroupId()),
          "Trying to remove a FileGroupId which is not found in bootstrap base file mapping. FgId :"
              + bootstrapBaseFile.getFileGroupId());
      fgIdToBootstrapBaseFile.remove(bootstrapBaseFile.getFileGroupId());
    });
  }

  @Override
  protected Option<BootstrapBaseFileMapping> getBootstrapBaseFile(HoodieFileGroupId fileGroupId) {
    return Option.ofNullable(fgIdToBootstrapBaseFile.get(fileGroupId));
  }

  @Override
  Stream<BootstrapBaseFileMapping> fetchBootstrapBaseFiles() {
    return fgIdToBootstrapBaseFile.values().stream();
  }

  @Override
  protected Option<Pair<String, CompactionOperation>> getPendingCompactionOperationWithInstant(HoodieFileGroupId fgId) {
    return Option.ofNullable(fgIdToPendingCompaction.get(fgId));
  }

  @Override
  protected boolean isPartitionAvailableInStore(String partitionPath) {
    return partitionToFileGroupsMap.containsKey(partitionPath);
  }

  @Override
  protected void storePartitionView(String partitionPath, List<HoodieFileGroup> fileGroups) {
    LOG.debug("Adding file-groups for partition :" + partitionPath + ", #FileGroups=" + fileGroups.size());
    List<HoodieFileGroup> newList = new ArrayList<>(fileGroups);
    partitionToFileGroupsMap.put(partitionPath, newList);
  }

  @Override
  public Stream<HoodieFileGroup> fetchAllStoredFileGroups() {
    return partitionToFileGroupsMap.values().stream().flatMap(Collection::stream);
  }

  @Override
  protected void resetReplacedFileGroups(final Map<HoodieFileGroupId, HoodieInstant> replacedFileGroups) {
    fgIdToReplaceInstants = createFileIdToReplaceInstantMap(replacedFileGroups);
  }

  @Override
  protected void addReplacedFileGroups(final Map<HoodieFileGroupId, HoodieInstant> replacedFileGroups) {
    fgIdToReplaceInstants.putAll(replacedFileGroups);
  }

  @Override
  protected void removeReplacedFileIdsAtInstants(Set<String> instants) {
    fgIdToReplaceInstants.entrySet().removeIf(entry -> instants.contains(entry.getValue().getTimestamp()));
  }

  @Override
  protected Option<HoodieInstant> getReplaceInstant(final HoodieFileGroupId fileGroupId) {
    return Option.ofNullable(fgIdToReplaceInstants.get(fileGroupId));
  }

  @Override
  protected void resetSecondaryIndexBaseFiles(Map<HoodieSecondaryIndex, Map<String, HoodieInstant>> secondaryIndexBaseFiles) {
    this.secondaryIndexToBaseFiles = createSecondaryIndexToBaseFilesMap(secondaryIndexBaseFiles);
  }

  @Override
  protected void resetPendingSecondaryIndexBaseFiles(Map<HoodieSecondaryIndex, Map<String, HoodieInstant>> pendingSecondaryIndexBaseFiles) {
    this.pendingSecondaryIndexToBaseFiles = createPendingSecondaryIndexToBaseFilesMap(pendingSecondaryIndexBaseFiles);
  }

  @Override
  protected Stream<Pair<HoodieSecondaryIndex, Map<String, HoodieInstant>>> fetchSecondaryIndexBaseFiles() {
    return secondaryIndexToBaseFiles.entrySet().stream().map(x -> Pair.of(x.getKey(), x.getValue()));
  }

  @Override
  protected Stream<Pair<HoodieSecondaryIndex, Map<String, HoodieInstant>>> fetchPendingSecondaryIndexBaseFiles() {
    return pendingSecondaryIndexToBaseFiles.entrySet().stream().map(x -> Pair.of(x.getKey(), x.getValue()));
  }

  /**
   * Get the latest file slices for a given partition including the inflight ones.
   *
   * @param partitionPath
   * @return Stream of latest {@link FileSlice} in the partition path.
   */
  public Stream<FileSlice> fetchLatestFileSlicesIncludingInflight(String partitionPath) {
    return fetchAllStoredFileGroups(partitionPath)
        .map(HoodieFileGroup::getLatestFileSlicesIncludingInflight)
        .filter(Option::isPresent)
        .map(Option::get);
  }

  @Override
  public void close() {
    super.close();
    this.fgIdToPendingCompaction = null;
    this.partitionToFileGroupsMap = null;
    this.fgIdToBootstrapBaseFile = null;
    this.fgIdToReplaceInstants = null;
    this.fgIdToPendingClustering = null;
    this.secondaryIndexToBaseFiles = null;
    this.pendingSecondaryIndexToBaseFiles = null;
    closed = true;
  }

  @Override
  public boolean isClosed() {
    return closed;
  }
}
