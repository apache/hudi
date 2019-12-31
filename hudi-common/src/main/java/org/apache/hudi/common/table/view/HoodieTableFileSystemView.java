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
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTimeline;
import org.apache.hudi.common.table.TableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;

import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.FileStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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

  private static final Logger LOG = LoggerFactory.getLogger(HoodieTableFileSystemView.class);

  // mapping from partition paths to file groups contained within them
  protected Map<String, List<HoodieFileGroup>> partitionToFileGroupsMap;

  /**
   * PartitionPath + File-Id to pending compaction instant time.
   */
  protected Map<HoodieFileGroupId, Pair<String, CompactionOperation>> fgIdToPendingCompaction;

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

  @Override
  protected void resetViewState() {
    this.fgIdToPendingCompaction = null;
    this.partitionToFileGroupsMap = null;
  }

  protected Map<String, List<HoodieFileGroup>> createPartitionToFileGroups() {
    return new ConcurrentHashMap<>();
  }

  protected Map<HoodieFileGroupId, Pair<String, CompactionOperation>> createFileIdToPendingCompactionMap(
      Map<HoodieFileGroupId, Pair<String, CompactionOperation>> fileIdToPendingCompaction) {
    return fileIdToPendingCompaction;
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
    this.fgIdToPendingCompaction = createFileIdToPendingCompactionMap(operations.map(entry -> {
      return Pair.of(entry.getValue().getFileGroupId(), Pair.of(entry.getKey(), entry.getValue()));
    }).collect(Collectors.toMap(Pair::getKey, Pair::getValue)));
  }

  @Override
  protected void addPendingCompactionOperations(Stream<Pair<String, CompactionOperation>> operations) {
    operations.forEach(opInstantPair -> {
      Preconditions.checkArgument(!fgIdToPendingCompaction.containsKey(opInstantPair.getValue().getFileGroupId()),
          "Duplicate FileGroupId found in pending compaction operations. FgId :"
              + opInstantPair.getValue().getFileGroupId());
      fgIdToPendingCompaction.put(opInstantPair.getValue().getFileGroupId(),
          Pair.of(opInstantPair.getKey(), opInstantPair.getValue()));
    });
  }

  @Override
  protected void removePendingCompactionOperations(Stream<Pair<String, CompactionOperation>> operations) {
    operations.forEach(opInstantPair -> {
      Preconditions.checkArgument(fgIdToPendingCompaction.containsKey(opInstantPair.getValue().getFileGroupId()),
          "Trying to remove a FileGroupId which is not found in pending compaction operations. FgId :"
              + opInstantPair.getValue().getFileGroupId());
      fgIdToPendingCompaction.remove(opInstantPair.getValue().getFileGroupId());
    });
  }

  /**
   * Given a partition path, obtain all filegroups within that. All methods, that work at the partition level go through
   * this.
   */
  @Override
  Stream<HoodieFileGroup> fetchAllStoredFileGroups(String partition) {
    final List<HoodieFileGroup> fileGroups = new ArrayList<>();
    fileGroups.addAll(partitionToFileGroupsMap.get(partition));
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
  protected Option<Pair<String, CompactionOperation>> getPendingCompactionOperationWithInstant(HoodieFileGroupId fgId) {
    return Option.ofNullable(fgIdToPendingCompaction.get(fgId));
  }

  @Override
  protected boolean isPartitionAvailableInStore(String partitionPath) {
    return partitionToFileGroupsMap.containsKey(partitionPath);
  }

  @Override
  protected void storePartitionView(String partitionPath, List<HoodieFileGroup> fileGroups) {
    LOG.info("Adding file-groups for partition :{}, #FileGroups={}", partitionPath, fileGroups.size());
    List<HoodieFileGroup> newList = new ArrayList<>(fileGroups);
    partitionToFileGroupsMap.put(partitionPath, newList);
  }

  @Override
  public Stream<HoodieFileGroup> fetchAllStoredFileGroups() {
    return partitionToFileGroupsMap.values().stream().flatMap(fg -> {
      return fg.stream();
    });
  }

  @Override
  public void close() {
    closed = true;
    super.reset();
    partitionToFileGroupsMap = null;
    fgIdToPendingCompaction = null;
  }

  @Override
  public boolean isClosed() {
    return closed;
  }
}
