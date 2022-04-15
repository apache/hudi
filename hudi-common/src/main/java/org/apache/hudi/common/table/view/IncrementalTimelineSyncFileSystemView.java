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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
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
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Adds the capability to incrementally sync the changes to file-system view as and when new instants gets completed.
 */
public abstract class IncrementalTimelineSyncFileSystemView extends AbstractTableFileSystemView {

  private static final Logger LOG = LogManager.getLogger(IncrementalTimelineSyncFileSystemView.class);

  // Allows incremental Timeline syncing
  private final boolean incrementalTimelineSyncEnabled;

  // This is the visible active timeline used only for incremental view syncing
  private HoodieTimeline visibleActiveTimeline;

  protected IncrementalTimelineSyncFileSystemView(boolean enableIncrementalTimelineSync) {
    this.incrementalTimelineSyncEnabled = enableIncrementalTimelineSync;
  }

  @Override
  protected void refreshTimeline(HoodieTimeline visibleActiveTimeline) {
    this.visibleActiveTimeline = visibleActiveTimeline;
    super.refreshTimeline(visibleActiveTimeline);
  }

  @Override
  protected void runSync(HoodieTimeline oldTimeline, HoodieTimeline newTimeline) {
    try {
      if (incrementalTimelineSyncEnabled) {
        TimelineDiffResult diffResult = TimelineDiffHelper.getNewInstantsForIncrementalSync(oldTimeline, newTimeline);
        if (diffResult.canSyncIncrementally()) {
          LOG.info("Doing incremental sync");
          runIncrementalSync(newTimeline, diffResult);
          LOG.info("Finished incremental sync");
          // Reset timeline to latest
          refreshTimeline(newTimeline);
          return;
        }
      }
    } catch (Exception ioe) {
      LOG.error("Got exception trying to perform incremental sync. Reverting to complete sync", ioe);
    }

    super.runSync(oldTimeline, newTimeline);
  }

  /**
   * Run incremental sync based on the diff result produced.
   *
   * @param timeline New Timeline
   * @param diffResult Timeline Diff Result
   */
  private void runIncrementalSync(HoodieTimeline timeline, TimelineDiffResult diffResult) {

    LOG.info("Timeline Diff Result is :" + diffResult);

    // First remove pending compaction instants which were completed
    diffResult.getFinishedCompactionInstants().stream().forEach(instant -> {
      try {
        removePendingCompactionInstant(timeline, instant);
      } catch (IOException e) {
        throw new HoodieException(e);
      }
    });

    // Now remove pending log compaction instants which were completed or removed
    diffResult.getFinishedOrRemovedLogCompactionInstants().stream().forEach(instant -> {
      try {
        removePendingLogCompactionInstant(timeline, instant);
      } catch (IOException e) {
        throw new HoodieException(e);
      }
    });

    // Add new completed instants found in the latest timeline, this also contains inflight instants.
    diffResult.getNewlySeenInstants().stream()
        .filter(instant -> instant.isCompleted()
            || instant.getAction().equals(HoodieTimeline.COMPACTION_ACTION)
            || instant.getAction().equals(HoodieTimeline.LOG_COMPACTION_ACTION))
        .forEach(instant -> {
          try {
            if (instant.getAction().equals(HoodieTimeline.COMMIT_ACTION)
                || instant.getAction().equals(HoodieTimeline.DELTA_COMMIT_ACTION)) {
              addCommitInstant(timeline, instant);
            } else if (instant.getAction().equals(HoodieTimeline.RESTORE_ACTION)) {
              addRestoreInstant(timeline, instant);
            } else if (instant.getAction().equals(HoodieTimeline.CLEAN_ACTION)) {
              addCleanInstant(timeline, instant);
            } else if (instant.getAction().equals(HoodieTimeline.COMPACTION_ACTION)) {
              addPendingCompactionInstant(timeline, instant);
            } else if (instant.getAction().equals(HoodieTimeline.LOG_COMPACTION_ACTION)) {
              addPendingLogCompactionInstant(instant);
            } else if (instant.getAction().equals(HoodieTimeline.ROLLBACK_ACTION)) {
              addRollbackInstant(timeline, instant);
            } else if (instant.getAction().equals(HoodieTimeline.REPLACE_COMMIT_ACTION)) {
              addReplaceInstant(timeline, instant);
            }
          } catch (IOException ioe) {
            throw new HoodieException(ioe);
          }
        });
  }

  /**
   * Remove Pending compaction instant.
   *
   * @param timeline New Hoodie Timeline
   * @param instant Compaction Instant to be removed
   */
  private void removePendingCompactionInstant(HoodieTimeline timeline, HoodieInstant instant) throws IOException {
    LOG.info("Removing completed compaction instant (" + instant + ")");
    HoodieCompactionPlan plan = CompactionUtils.getCompactionPlan(metaClient, instant.getTimestamp());
    removePendingCompactionOperations(CompactionUtils.getPendingCompactionOperations(instant, plan)
        .map(instantPair -> Pair.of(instantPair.getValue().getKey(),
            CompactionOperation.convertFromAvroRecordInstance(instantPair.getValue().getValue()))));
  }

  /**
   * Remove Pending compaction instant. This is called when logcompaction is converted to delta commit,
   * so you no longer need to track them as pending.
   *
   * @param timeline New Hoodie Timeline
   * @param instant Log Compaction Instant to be removed
   */
  private void removePendingLogCompactionInstant(HoodieTimeline timeline, HoodieInstant instant) throws IOException {
    LOG.info("Removing completed log compaction instant (" + instant + ")");
    HoodieCompactionPlan plan = CompactionUtils.getLogCompactionPlan(metaClient, instant.getTimestamp());
    removePendingLogCompactionOperations(CompactionUtils.getPendingCompactionOperations(instant, plan)
        .map(instantPair -> Pair.of(instantPair.getValue().getKey(),
            CompactionOperation.convertFromAvroRecordInstance(instantPair.getValue().getValue()))));
  }

  /**
   * Add newly found compaction instant.
   *
   * @param timeline Hoodie Timeline
   * @param instant Compaction Instant
   */
  private void addPendingCompactionInstant(HoodieTimeline timeline, HoodieInstant instant) throws IOException {
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
      HoodieFileGroup fileGroup = new HoodieFileGroup(opPair.getValue().getFileGroupId(), timeline);
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
   * @param timeline Hoodie Timeline
   * @param instant Instant
   */
  private void addCommitInstant(HoodieTimeline timeline, HoodieInstant instant) throws IOException {
    LOG.info("Syncing committed instant (" + instant + ")");
    HoodieCommitMetadata commitMetadata =
        HoodieCommitMetadata.fromBytes(timeline.getInstantDetails(instant).get(), HoodieCommitMetadata.class);
    updatePartitionWriteFileGroups(commitMetadata.getPartitionToWriteStats(), timeline, instant);
    LOG.info("Done Syncing committed instant (" + instant + ")");
  }

  private void updatePartitionWriteFileGroups(Map<String, List<HoodieWriteStat>> partitionToWriteStats,
                                              HoodieTimeline timeline,
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
            buildFileGroups(statuses, timeline.filterCompletedAndCompactionInstants(), false);
        applyDeltaFileSlicesToPartitionView(partition, fileGroups, DeltaApplyMode.ADD);
      } else {
        LOG.warn("Skipping partition (" + partition + ") when syncing instant (" + instant + ") as it is not loaded");
      }
    });
    LOG.info("Done Syncing committed instant (" + instant + ")");
  }

  /**
   * Add newly found restore instant.
   *
   * @param timeline Hoodie Timeline
   * @param instant Restore Instant
   */
  private void addRestoreInstant(HoodieTimeline timeline, HoodieInstant instant) throws IOException {
    LOG.info("Syncing restore instant (" + instant + ")");
    HoodieRestoreMetadata metadata =
        TimelineMetadataUtils.deserializeAvroMetadata(timeline.getInstantDetails(instant).get(), HoodieRestoreMetadata.class);

    Map<String, List<Pair<String, String>>> partitionFiles =
        metadata.getHoodieRestoreMetadata().entrySet().stream().flatMap(entry -> {
          return entry.getValue().stream().flatMap(e -> e.getPartitionMetadata().entrySet().stream().flatMap(e2 -> {
            return e2.getValue().getSuccessDeleteFiles().stream().map(x -> Pair.of(e2.getKey(), x));
          }));
        }).collect(Collectors.groupingBy(Pair::getKey));
    partitionFiles.entrySet().stream().forEach(e -> {
      removeFileSlicesForPartition(timeline, instant, e.getKey(),
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
   * Add newly found REPLACE instant.
   *
   * @param timeline Hoodie Timeline
   * @param instant REPLACE Instant
   */
  private void addReplaceInstant(HoodieTimeline timeline, HoodieInstant instant) throws IOException {
    LOG.info("Syncing replace instant (" + instant + ")");
    HoodieReplaceCommitMetadata replaceMetadata =
        HoodieReplaceCommitMetadata.fromBytes(timeline.getInstantDetails(instant).get(), HoodieReplaceCommitMetadata.class);
    updatePartitionWriteFileGroups(replaceMetadata.getPartitionToWriteStats(), timeline, instant);
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

  private void removeFileSlicesForPartition(HoodieTimeline timeline, HoodieInstant instant, String partition,
      List<String> paths) {
    if (isPartitionAvailableInStore(partition)) {
      LOG.info("Removing file slices for partition (" + partition + ") for instant (" + instant + ")");
      FileStatus[] statuses = paths.stream().map(p -> {
        FileStatus status = new FileStatus();
        status.setPath(new Path(p));
        return status;
      }).toArray(FileStatus[]::new);
      List<HoodieFileGroup> fileGroups =
          buildFileGroups(statuses, timeline.filterCompletedAndCompactionInstants(), false);
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
    // Note: Delta Log Files and Data FIles can be empty when adding/removing pending compactions
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

    HoodieTimeline timeline = deltaFileGroups.stream().map(df -> df.getTimeline()).findAny().get();
    List<HoodieFileGroup> fgs =
        buildFileGroups(viewDataFiles.values().stream(), viewLogFiles.values().stream(), timeline, true);
    storePartitionView(partition, fgs);
  }

  @Override
  public HoodieTimeline getTimeline() {
    return visibleActiveTimeline;
  }
}
