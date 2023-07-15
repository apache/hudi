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

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.avro.model.HoodieRestoreMetadata;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.CompactionOperation;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineDiffHelper;
import org.apache.hudi.common.table.timeline.TimelineDiffHelper.TimelineDiffResult;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.util.CleanerUtils;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Adds the capability to incrementally sync the changes to file-system view as and when new instants gets completed.
 */
public abstract class IncrementalTimelineSyncFileSystemView extends AbstractTableFileSystemView {

  private static final Logger LOG = LoggerFactory.getLogger(IncrementalTimelineSyncFileSystemView.class);

  // Allows incremental Timeline syncing
  private final boolean incrementalTimelineSyncEnabled;

  // This is the visible active timeline used only for incremental view syncing
  private HoodieTimeline visibleActiveTimeline;

  // Tracks the status of the last incremental file sync
  private boolean isLastIncrementalSyncSuccessful;

  protected IncrementalTimelineSyncFileSystemView(boolean enableIncrementalTimelineSync) {
    this.incrementalTimelineSyncEnabled = enableIncrementalTimelineSync;
  }

  @Override
  protected void refreshTimeline(HoodieTimeline visibleActiveTimeline) {
    this.visibleActiveTimeline = visibleActiveTimeline;
    super.refreshTimeline(visibleActiveTimeline);
  }

  @Override
  public void sync() {
    try {
      writeLock.lock();
      maySyncIncrementally();
    } finally {
      writeLock.unlock();
    }
  }

  protected void maySyncIncrementally() {
    HoodieTimeline oldTimeline = getTimeline();
    HoodieTimeline newTimeline = metaClient.reloadActiveTimeline();
    try {
      if (incrementalTimelineSyncEnabled) {
        TimelineDiffResult diffResult = TimelineDiffHelper
            .getNewInstantsForIncrementalSync(metaClient, oldTimeline, newTimeline);
        if (diffResult.canSyncIncrementally()) {
          LOG.info("Doing incremental sync");
          runIncrementalSync(newTimeline, diffResult);
          LOG.info("Finished incremental sync");
          // Reset timeline to latest
          refreshTimeline(newTimeline);
          this.isLastIncrementalSyncSuccessful = true;
          return;
        }
      }
    } catch (Exception ioe) {
      LOG.error("Got exception trying to perform incremental sync. Reverting to complete sync", ioe);
      this.isLastIncrementalSyncSuccessful = false;
    }
    clear();
    // Initialize with new Hoodie timeline.
    init(metaClient, newTimeline);
  }

  /**
   * Run incremental sync based on the diff result produced.
   *
   * @param newActiveTimeline   New active Timeline
   * @param diffResult Timeline Diff Result
   */
  private void runIncrementalSync(HoodieTimeline newActiveTimeline, TimelineDiffResult diffResult) {

    LOG.info("Timeline Diff Result is :" + diffResult);

    // First remove pending compaction instants which were completed
    diffResult.getFinishedCompactionInstants().stream().forEach(instant -> {
      try {
        removePendingCompactionInstant(instant);
      } catch (IOException e) {
        throw new HoodieException(e);
      }
    });

    // Now remove pending log compaction instants which were completed or removed
    diffResult.getFinishedOrRemovedLogCompactionInstants().stream().forEach(instantPair -> {
      try {
        removePendingLogCompactionInstant(instantPair.getKey(), instantPair.getValue());
      } catch (IOException e) {
        throw new HoodieException(e);
      }
    });

    // Now remove pending clustering instants which were completed or removed
    diffResult.getFinishedOrRemovedClusteringInstants().stream().forEach(instantPair -> {
      try {
        removePendingFileGroupsInPendingClustering(instantPair.getKey(), instantPair.getValue());
      } catch (IOException e) {
        throw new HoodieException(e);
      }
    });

    HoodieTimeline newCompletedWriteAndCompactionTimeline =
        newActiveTimeline.filterCompletedWriteAndCompactionInstants();
    // Add new completed instants found in the latest timeline,
    // this also contains inflight instants from rewrite timeline.
    diffResult.getNewlySeenCompletedAndRewriteInstants()
        .forEach(instant -> {
          try {
            if (instant.getAction().equals(HoodieTimeline.COMMIT_ACTION)
                || instant.getAction().equals(HoodieTimeline.DELTA_COMMIT_ACTION)) {
              addCommitInstant(newCompletedWriteAndCompactionTimeline, instant);
            } else if (instant.getAction().equals(HoodieTimeline.RESTORE_ACTION)) {
              addRestoreInstant(newActiveTimeline, instant);
            } else if (instant.getAction().equals(HoodieTimeline.CLEAN_ACTION)) {
              addCleanInstant(newActiveTimeline, instant);
            } else if (instant.getAction().equals(HoodieTimeline.COMPACTION_ACTION)) {
              addPendingCompactionInstant(newCompletedWriteAndCompactionTimeline, instant);
            } else if (instant.getAction().equals(HoodieTimeline.LOG_COMPACTION_ACTION)) {
              addPendingLogCompactionInstant(instant);
            } else if (instant.getAction().equals(HoodieTimeline.ROLLBACK_ACTION)) {
              addRollbackInstant(newActiveTimeline, instant);
            } else if (instant.getAction().equals(HoodieTimeline.REPLACE_COMMIT_ACTION)) {
              boolean isClusteringCommit = ClusteringUtils.isClusteringCommit(metaClient, instant);
              if (isClusteringCommit && !instant.isCompleted()) {
                addPendingClusteringInstant(instant);
              } else {
                addReplaceInstant(newCompletedWriteAndCompactionTimeline, instant);
              }
            }
          } catch (IOException ioe) {
            throw new HoodieException(ioe);
          }
        });
  }

  /**
   * Remove Pending compaction instant.
   *
   * @param instant Compaction Instant to be removed
   */
  private void removePendingCompactionInstant(HoodieInstant instant) throws IOException {
    LOG.info("Removing completed compaction instant (" + instant + ")");
    HoodieCompactionPlan plan = CompactionUtils.getCompactionPlan(metaClient, instant.getTimestamp());
    removePendingCompactionOperations(CompactionUtils.getPendingCompactionOperations(instant, plan)
        .map(instantPair -> Pair.of(instantPair.getValue().getKey(),
            CompactionOperation.convertFromAvroRecordInstance(instantPair.getValue().getValue()))));
  }

  /**
   * Remove pending log compaction instant. This is called when logcompaction is converted to delta commit or the log compaction
   * operation is failed, so it is no longer need to be tracked as pending.
   *
   * @param instant Log Compaction Instant to be removed
   * @param isSuccessful true if log compaction operation is successful, false if the operation is failed and rollbacked.
   */
  private void removePendingLogCompactionInstant(HoodieInstant instant, boolean isSuccessful) throws IOException {
    if (isSuccessful) {
      // If the log compaction is successful use the log compaction plan to remove
      // the file groups under pending clustering.
      LOG.info("Removing completed log compaction instant (" + instant + ")");
      HoodieCompactionPlan plan = CompactionUtils.getLogCompactionPlan(metaClient, instant.getTimestamp());
      removePendingLogCompactionOperations(CompactionUtils.getPendingCompactionOperations(instant, plan)
          .map(instantPair -> Pair.of(instantPair.getValue().getKey(),
              CompactionOperation.convertFromAvroRecordInstance(instantPair.getValue().getValue()))));
    } else {
      // If the log compaction is removed then there is no commit in the timeline
      // in that case iterate over all the file groups under pending log compaction and remove them.
      LOG.info("Removing failed log compaction instant (" + instant + ")");
      removePendingLogCompactionOperations(instant.getTimestamp());
    }
    LOG.info("Done Syncing log compaction transition for instant (" + instant + ")");
  }

  /**
   * Add newly found compaction instant.
   *
   * @param completedWriteAndCompactionTimeline Hoodie completedWriteAndCompactionTimeline
   * @param instant Compaction Instant
   */
  private void addPendingCompactionInstant(HoodieTimeline completedWriteAndCompactionTimeline, HoodieInstant instant) throws IOException {
    LOG.info("Syncing pending compaction instant (" + instant + ")");
    HoodieCompactionPlan compactionPlan = CompactionUtils.getCompactionPlan(metaClient, instant.getTimestamp());
    List<Pair<String, CompactionOperation>> pendingOps =
        CompactionUtils.getPendingCompactionOperations(instant, compactionPlan)
            .map(p -> Pair.of(p.getValue().getKey(),
                CompactionOperation.convertFromAvroRecordInstance(p.getValue().getValue())))
            .collect(Collectors.toList());
    // First, update Pending compaction instants
    addPendingCompactionOperations(pendingOps.stream());

    Map<String, List<Pair<String, HoodieFileGroup>>> partitionToFileGroups = pendingOps.stream().map(opPair -> {
      String compactionInstantTime = opPair.getKey();
      HoodieFileGroup fileGroup = new HoodieFileGroup(opPair.getValue().getFileGroupId(), completedWriteAndCompactionTimeline);
      fileGroup.addNewFileSliceAtInstant(compactionInstantTime);
      return Pair.of(compactionInstantTime, fileGroup);
    }).collect(Collectors.groupingBy(x -> x.getValue().getPartitionPath()));
    partitionToFileGroups.entrySet().forEach(entry -> {
      if (isPartitionAvailableInStore(entry.getKey())) {
        applyDeltaFileSlicesToPartitionView(entry.getKey(),
            entry.getValue().stream().map(Pair::getValue).collect(Collectors.toList()), DeltaApplyMode.ADD);
      }
    });
  }

  /**
   * Add newly found compaction instant.
   *
   * @param instant Compaction Instant
   */
  private void addPendingLogCompactionInstant(HoodieInstant instant) throws IOException {
    LOG.info("Syncing pending log compaction instant (" + instant + ")");
    HoodieCompactionPlan compactionPlan = CompactionUtils.getLogCompactionPlan(metaClient, instant.getTimestamp());
    List<Pair<String, CompactionOperation>> pendingOps =
        CompactionUtils.getPendingCompactionOperations(instant, compactionPlan)
            .map(p -> Pair.of(p.getValue().getKey(),
                CompactionOperation.convertFromAvroRecordInstance(p.getValue().getValue())))
            .collect(Collectors.toList());
    // Update Pending log compaction instants.
    // Since logcompaction works similar to a deltacommit. Updating the partition view is not required.
    addPendingLogCompactionOperations(pendingOps.stream());
  }

  /**
   * Add newly found commit/delta-commit instant.
   *
   * @param completedWriteAndCompactionTimeline Hoodie Timeline that is used for constructing file groups.
   * @param instant Instant
   */
  private void addCommitInstant(HoodieTimeline completedWriteAndCompactionTimeline, HoodieInstant instant) throws IOException {
    LOG.info("Syncing committed instant (" + instant + ")");
    HoodieCommitMetadata commitMetadata =
        HoodieCommitMetadata.fromBytes(completedWriteAndCompactionTimeline.getInstantDetails(instant).get(), HoodieCommitMetadata.class);
    updatePartitionWriteFileGroups(commitMetadata.getPartitionToWriteStats(), completedWriteAndCompactionTimeline, instant);
    LOG.info("Done Syncing committed instant (" + instant + ")");
  }

  private void updatePartitionWriteFileGroups(Map<String, List<HoodieWriteStat>> partitionToWriteStats,
                                              HoodieTimeline completedWriteAndCompactionTimeline,
                                              HoodieInstant instant) {
    partitionToWriteStats.entrySet().stream().forEach(entry -> {
      String partition = entry.getKey();
      if (isPartitionAvailableInStore(partition)) {
        LOG.info("Syncing partition (" + partition + ") of instant (" + instant + ")");
        FileStatus[] statuses = entry.getValue().stream().map(p -> {
          FileStatus status = new FileStatus(p.getFileSizeInBytes(), false, 0, 0, 0, 0, null, null, null,
              new Path(String.format("%s/%s", metaClient.getBasePath(), p.getPath())));
          return status;
        }).toArray(FileStatus[]::new);
        List<HoodieFileGroup> fileGroups =
            buildFileGroups(statuses, completedWriteAndCompactionTimeline, false);
        applyDeltaFileSlicesToPartitionView(partition, fileGroups, DeltaApplyMode.ADD);
      } else {
        LOG.warn("Skipping partition (" + partition + ") when syncing instant (" + instant + ") as it is not loaded");
      }
    });
    LOG.info("Done Syncing committed instant (" + instant + ")");
  }

  /**
   * Add newly found restore instant.
   * @param activeTimeline active timeline
   * @param instant restore instant
   * @throws IOException
   */
  private void addRestoreInstant(HoodieTimeline activeTimeline, HoodieInstant instant) throws IOException {
    LOG.info("Syncing restore instant (" + instant + ")");
    HoodieRestoreMetadata metadata =
        TimelineMetadataUtils.deserializeAvroMetadata(activeTimeline.getInstantDetails(instant).get(), HoodieRestoreMetadata.class);

    Map<String, List<Pair<String, String>>> partitionFiles =
        metadata.getHoodieRestoreMetadata().entrySet().stream().flatMap(entry -> {
          return entry.getValue().stream().flatMap(e -> e.getPartitionMetadata().entrySet().stream().flatMap(e2 -> {
            return e2.getValue().getSuccessDeleteFiles().stream().map(x -> Pair.of(e2.getKey(), x));
          }));
        }).collect(Collectors.groupingBy(Pair::getKey));
    partitionFiles.entrySet().stream().forEach(e -> {
      removeFileSlicesForPartition(activeTimeline, instant, e.getKey(),
          e.getValue().stream().map(x -> x.getValue()).collect(Collectors.toList()));
    });

    if (metadata.getRestoreInstantInfo() != null) {
      Set<String> rolledbackInstants = metadata.getRestoreInstantInfo().stream()
          .filter(instantInfo -> HoodieTimeline.REPLACE_COMMIT_ACTION.equals(instantInfo.getAction()))
          .map(instantInfo -> instantInfo.getCommitTime()).collect(Collectors.toSet());
      removeReplacedFileIdsAtInstants(rolledbackInstants);
    }
    LOG.info("Done Syncing restore instant (" + instant + ")");
  }

  /**
   * Add newly found rollback instant.
   *
   * @param timeline Hoodie Timeline
   * @param instant Rollback Instant
   */
  private void addRollbackInstant(HoodieTimeline timeline, HoodieInstant instant) throws IOException {
    LOG.info("Syncing rollback instant (" + instant + ")");
    HoodieRollbackMetadata metadata =
        TimelineMetadataUtils.deserializeAvroMetadata(timeline.getInstantDetails(instant).get(), HoodieRollbackMetadata.class);

    metadata.getPartitionMetadata().entrySet().stream().forEach(e -> {
      removeFileSlicesForPartition(timeline, instant, e.getKey(), e.getValue().getSuccessDeleteFiles());
    });
    LOG.info("Done Syncing rollback instant (" + instant + ")");
  }

  /**
   * Add pending replace file groups
   * @param pendingClusteringInstant pending instant
   * @throws IOException
   */
  private void addPendingClusteringInstant(HoodieInstant pendingClusteringInstant) throws IOException {
    LOG.info("Syncing pending clustering instant (" + pendingClusteringInstant + ")");
    Stream<Pair<HoodieFileGroupId, HoodieInstant>> fileGroupStreamToInstant =
        ClusteringUtils.getFileGroupEntriesFromClusteringInstant(pendingClusteringInstant, metaClient);
    addFileGroupsInPendingClustering(fileGroupStreamToInstant);
  }

  /**
   * Remove Pending clustering instant. This is called when clustering operation is completed or
   * if the operation has failed, so it is no longer need to be tracked as pending.
   *
   * @param clusteringInstant Clustering Instant to be removed
   * @param isSuccessful true if clustering operation is successful, false if the operation has failed and rollbacked.
   */
  private void removePendingFileGroupsInPendingClustering(HoodieInstant clusteringInstant, boolean isSuccessful) throws IOException {
    if (isSuccessful) {
      // If the clustering commit is successful use the clustering plan to remove
      // the file groups under pending clustering.
      LOG.info("Removing completed clustering instant (" + clusteringInstant + ")");
      Stream<Pair<HoodieFileGroupId, HoodieInstant>> fileGroupStreamToInstant =
          ClusteringUtils.getFileGroupEntriesFromClusteringInstant(clusteringInstant, metaClient);
      removeFileGroupsInPendingClustering(fileGroupStreamToInstant);
    } else {
      // If the clustering's replace-commit is removed then there is no commit in the timeline
      // in that case iterate over all the file groups under pending clustering and remove them.
      LOG.info("Removing failed clustering instant (" + clusteringInstant + ")");
      removeFileGroupsInPendingClustering(clusteringInstant.getTimestamp());
    }
    LOG.info("Done Syncing clustering transition for instant (" + clusteringInstant + ")");
  }

  /**
   * Add newly found REPLACE instant.
   *
   * @param completedWriteAndCompactionTimeline Hoodie Timeline used for constructing file groups.
   * @param instant REPLACE Instant is a completed instant
   */
  private void addReplaceInstant(HoodieTimeline completedWriteAndCompactionTimeline, HoodieInstant instant) throws IOException {
    LOG.info("Syncing replace instant (" + instant + ")");
    HoodieReplaceCommitMetadata replaceMetadata =
        HoodieReplaceCommitMetadata.fromBytes(completedWriteAndCompactionTimeline.getInstantDetails(instant).get(),
            HoodieReplaceCommitMetadata.class);
    updatePartitionWriteFileGroups(replaceMetadata.getPartitionToWriteStats(), completedWriteAndCompactionTimeline, instant);
    replaceMetadata.getPartitionToReplaceFileIds().entrySet().stream().forEach(entry -> {
      String partition = entry.getKey();
      Map<HoodieFileGroupId, HoodieInstant> replacedFileIds = entry.getValue().stream()
          .collect(Collectors.toMap(replaceStat -> new HoodieFileGroupId(partition, replaceStat), replaceStat -> instant));

      LOG.info("For partition (" + partition + ") of instant (" + instant + "), excluding " + replacedFileIds.size() + " file groups");
      addReplacedFileGroups(replacedFileIds);
    });
    LOG.info("Done Syncing REPLACE instant (" + instant + ")");
  }

  /**
   * Add newly found clean instant. Note that cleaner metadata (.clean.completed)
   * contains only relative paths unlike clean plans (.clean.requested) which contains absolute paths.
   *
   * @param timeline Timeline
   * @param instant Clean instant
   */
  private void addCleanInstant(HoodieTimeline timeline, HoodieInstant instant) throws IOException {
    LOG.info("Syncing cleaner instant (" + instant + ")");
    HoodieCleanMetadata cleanMetadata = CleanerUtils.getCleanerMetadata(metaClient, instant);
    cleanMetadata.getPartitionMetadata().entrySet().stream().forEach(entry -> {
      final String basePath = metaClient.getBasePath();
      final String partitionPath = entry.getValue().getPartitionPath();
      List<String> fullPathList = entry.getValue().getSuccessDeleteFiles()
          .stream().map(fileName -> new Path(FSUtils
              .getPartitionPath(basePath, partitionPath), fileName).toString())
          .collect(Collectors.toList());
      removeFileSlicesForPartition(timeline, instant, entry.getKey(), fullPathList);
    });
    LOG.info("Done Syncing cleaner instant (" + instant + ")");
  }

  private void removeFileSlicesForPartition(HoodieTimeline activeTimeline, HoodieInstant instant, String partition,
      List<String> paths) {
    if (isPartitionAvailableInStore(partition)) {
      LOG.info("Removing file slices for partition (" + partition + ") for instant (" + instant + ")");
      FileStatus[] statuses = paths.stream().map(p -> {
        FileStatus status = new FileStatus();
        status.setPath(new Path(p));
        return status;
      }).toArray(FileStatus[]::new);
      List<HoodieFileGroup> fileGroups =
          buildFileGroups(statuses, activeTimeline.filterCompletedWriteAndCompactionInstants(), false);
      applyDeltaFileSlicesToPartitionView(partition, fileGroups, DeltaApplyMode.REMOVE);
    } else {
      LOG.warn("Skipping partition (" + partition + ") when syncing instant (" + instant + ") as it is not loaded");
    }
  }

  /**
   * Apply mode whether to add or remove the delta view.
   */
  enum DeltaApplyMode {
    ADD, REMOVE
  }

  /**
   * Apply changes to partition file-system view. Base Implementation overwrites the entire partitions view assuming
   * some sort of map (in-mem/disk-based) is used. For View implementation which supports fine-granular updates (e:g
   * RocksDB), override this method.
   *
   * @param partition PartitionPath
   * @param deltaFileGroups Changed file-slices aggregated as file-groups
   * @param mode Delta Apply mode
   */
  protected void applyDeltaFileSlicesToPartitionView(String partition, List<HoodieFileGroup> deltaFileGroups,
      DeltaApplyMode mode) {
    if (deltaFileGroups.isEmpty()) {
      LOG.info("No delta file groups for partition :" + partition);
      return;
    }

    List<HoodieFileGroup> fileGroups = fetchAllStoredFileGroups(partition).collect(Collectors.toList());
    /**
     * Note that while finding the new data/log files added/removed, the path stored in metadata will be missing the
     * base-path,scheme and authority. Ensure the matching process takes care of this discrepancy.
     */
    Map<String, HoodieBaseFile> viewDataFiles = fileGroups.stream().flatMap(HoodieFileGroup::getAllRawFileSlices)
        .map(FileSlice::getBaseFile).filter(Option::isPresent).map(Option::get)
        .map(df -> Pair.of(Path.getPathWithoutSchemeAndAuthority(new Path(df.getPath())).toString(), df))
        .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
    // Note: Delta Log Files and Data Files can be empty when adding/removing pending compactions
    Map<String, HoodieBaseFile> deltaDataFiles = deltaFileGroups.stream().flatMap(HoodieFileGroup::getAllRawFileSlices)
        .map(FileSlice::getBaseFile).filter(Option::isPresent).map(Option::get)
        .map(df -> Pair.of(Path.getPathWithoutSchemeAndAuthority(new Path(df.getPath())).toString(), df))
        .collect(Collectors.toMap(Pair::getKey, Pair::getValue));

    Map<String, HoodieLogFile> viewLogFiles =
        fileGroups.stream().flatMap(HoodieFileGroup::getAllRawFileSlices).flatMap(FileSlice::getLogFiles)
            .map(lf -> Pair.of(Path.getPathWithoutSchemeAndAuthority(lf.getPath()).toString(), lf))
            .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
    Map<String, HoodieLogFile> deltaLogFiles =
        deltaFileGroups.stream().flatMap(HoodieFileGroup::getAllRawFileSlices).flatMap(FileSlice::getLogFiles)
            .map(lf -> Pair.of(Path.getPathWithoutSchemeAndAuthority(lf.getPath()).toString(), lf))
            .collect(Collectors.toMap(Pair::getKey, Pair::getValue));

    switch (mode) {
      case ADD:
        viewDataFiles.putAll(deltaDataFiles);
        viewLogFiles.putAll(deltaLogFiles);
        break;
      case REMOVE:
        deltaDataFiles.keySet().stream().forEach(p -> viewDataFiles.remove(p));
        deltaLogFiles.keySet().stream().forEach(p -> viewLogFiles.remove(p));
        break;
      default:
        throw new IllegalStateException("Unknown diff apply mode=" + mode);
    }

    // Here timeline is fetched from the existing file groups so it is automatically completedWriteAndCompactionTimeline
    HoodieTimeline completedWriteAndCompactionTimeline = deltaFileGroups.stream().map(df -> df.getTimeline()).findAny().get();
    List<HoodieFileGroup> fgs =
        buildFileGroups(viewDataFiles.values().stream(), viewLogFiles.values().stream(), completedWriteAndCompactionTimeline, true);
    storePartitionView(partition, fgs);
  }

  @Override
  public HoodieTimeline getTimeline() {
    return visibleActiveTimeline;
  }

  public boolean isLastIncrementalSyncSuccessful() {
    return isLastIncrementalSyncSuccessful;
  }
}
