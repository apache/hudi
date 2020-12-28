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

package org.apache.hudi.table.action.clean;

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieSavepointMetadata;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.CleanFileInfo;
import org.apache.hudi.common.model.CompactionOperation;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.table.timeline.versioning.clean.CleanPlanV1MigrationHandler;
import org.apache.hudi.common.table.timeline.versioning.clean.CleanPlanV2MigrationHandler;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieSavepointException;
import org.apache.hudi.table.HoodieTable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Cleaner is responsible for garbage collecting older files in a given partition path. Such that
 * <p>
 * 1) It provides sufficient time for existing queries running on older versions, to close
 * <p>
 * 2) It bounds the growth of the files in the file system
 */
public class CleanPlanner<T extends HoodieRecordPayload, I, K, O> implements Serializable {

  private static final Logger LOG = LogManager.getLogger(CleanPlanner.class);

  public static final Integer CLEAN_PLAN_VERSION_1 = CleanPlanV1MigrationHandler.VERSION;
  public static final Integer CLEAN_PLAN_VERSION_2 = CleanPlanV2MigrationHandler.VERSION;
  public static final Integer LATEST_CLEAN_PLAN_VERSION = CLEAN_PLAN_VERSION_2;

  private final SyncableFileSystemView fileSystemView;
  private final HoodieTimeline commitTimeline;
  private final Map<HoodieFileGroupId, CompactionOperation> fgIdToPendingCompactionOperations;
  private HoodieTable<T, I, K, O> hoodieTable;
  private HoodieWriteConfig config;

  public CleanPlanner(HoodieTable<T, I, K, O> hoodieTable, HoodieWriteConfig config) {
    this.hoodieTable = hoodieTable;
    this.fileSystemView = hoodieTable.getHoodieView();
    this.commitTimeline = hoodieTable.getCompletedCommitsTimeline();
    this.config = config;
    this.fgIdToPendingCompactionOperations =
        ((SyncableFileSystemView) hoodieTable.getSliceView()).getPendingCompactionOperations()
            .map(entry -> Pair.of(
                new HoodieFileGroupId(entry.getValue().getPartitionPath(), entry.getValue().getFileId()),
                entry.getValue()))
            .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
  }

  /**
   * Get the list of data file names savepointed.
   */
  public Stream<String> getSavepointedDataFiles(String savepointTime) {
    if (!hoodieTable.getSavepoints().contains(savepointTime)) {
      throw new HoodieSavepointException(
          "Could not get data files for savepoint " + savepointTime + ". No such savepoint.");
    }
    HoodieInstant instant = new HoodieInstant(false, HoodieTimeline.SAVEPOINT_ACTION, savepointTime);
    HoodieSavepointMetadata metadata;
    try {
      metadata = TimelineMetadataUtils.deserializeHoodieSavepointMetadata(
          hoodieTable.getActiveTimeline().getInstantDetails(instant).get());
    } catch (IOException e) {
      throw new HoodieSavepointException("Could not get savepointed data files for savepoint " + savepointTime, e);
    }
    return metadata.getPartitionMetadata().values().stream().flatMap(s -> s.getSavepointDataFile().stream());
  }

  /**
   * Returns list of partitions where clean operations needs to be performed.
   *
   * @param newInstantToRetain New instant to be retained after this cleanup operation
   * @return list of partitions to scan for cleaning
   * @throws IOException when underlying file-system throws this exception
   */
  public List<String> getPartitionPathsToClean(Option<HoodieInstant> newInstantToRetain) throws IOException {
    switch (config.getCleanerPolicy()) {
      case KEEP_LATEST_COMMITS:
        return getPartitionPathsForCleanByCommits(newInstantToRetain);
      case KEEP_LATEST_FILE_VERSIONS:
        return getPartitionPathsForFullCleaning();
      default:
        throw new IllegalStateException("Unknown Cleaner Policy");
    }
  }

  /**
   * Return partition paths for cleaning by commits mode.
   * @param instantToRetain Earliest Instant to retain
   * @return list of partitions
   * @throws IOException
   */
  private List<String> getPartitionPathsForCleanByCommits(Option<HoodieInstant> instantToRetain) throws IOException {
    if (!instantToRetain.isPresent()) {
      LOG.info("No earliest commit to retain. No need to scan partitions !!");
      return Collections.emptyList();
    }

    if (config.incrementalCleanerModeEnabled()) {
      Option<HoodieInstant> lastClean = hoodieTable.getCleanTimeline().filterCompletedInstants().lastInstant();
      if (lastClean.isPresent()) {
        HoodieCleanMetadata cleanMetadata = TimelineMetadataUtils
            .deserializeHoodieCleanMetadata(hoodieTable.getActiveTimeline().getInstantDetails(lastClean.get()).get());
        if ((cleanMetadata.getEarliestCommitToRetain() != null)
            && (cleanMetadata.getEarliestCommitToRetain().length() > 0)) {
          return getPartitionPathsForIncrementalCleaning(cleanMetadata, instantToRetain);
        }
      }
    }
    return getPartitionPathsForFullCleaning();
  }

  /**
   * Use Incremental Mode for finding partition paths.
   * @param cleanMetadata
   * @param newInstantToRetain
   * @return
   */
  private List<String> getPartitionPathsForIncrementalCleaning(HoodieCleanMetadata cleanMetadata,
      Option<HoodieInstant> newInstantToRetain) {
    LOG.warn("Incremental Cleaning mode is enabled. Looking up partition-paths that have since changed "
        + "since last cleaned at " + cleanMetadata.getEarliestCommitToRetain()
        + ". New Instant to retain : " + newInstantToRetain);
    return hoodieTable.getCompletedCommitsTimeline().getInstants().filter(
        instant -> HoodieTimeline.compareTimestamps(instant.getTimestamp(), HoodieTimeline.GREATER_THAN_OR_EQUALS,
            cleanMetadata.getEarliestCommitToRetain()) && HoodieTimeline.compareTimestamps(instant.getTimestamp(),
            HoodieTimeline.LESSER_THAN, newInstantToRetain.get().getTimestamp())).flatMap(instant -> {
              try {
                HoodieCommitMetadata commitMetadata = HoodieCommitMetadata
                    .fromBytes(hoodieTable.getActiveTimeline().getInstantDetails(instant).get(),
                        HoodieCommitMetadata.class);
                return commitMetadata.getPartitionToWriteStats().keySet().stream();
              } catch (IOException e) {
                throw new HoodieIOException(e.getMessage(), e);
              }
            }).distinct().collect(Collectors.toList());
  }

  /**
   * Scan and list all paritions for cleaning.
   * @return all partitions paths for the dataset.
   * @throws IOException
   */
  private List<String> getPartitionPathsForFullCleaning() throws IOException {
    // Go to brute force mode of scanning all partitions
    return FSUtils.getAllPartitionPaths(hoodieTable.getMetaClient().getFs(), hoodieTable.getMetaClient().getBasePath(),
        config.shouldAssumeDatePartitioning());
  }

  /**
   * Selects the older versions of files for cleaning, such that it bounds the number of versions of each file. This
   * policy is useful, if you are simply interested in querying the table, and you don't want too many versions for a
   * single file (i.e run it with versionsRetained = 1)
   */
  private List<CleanFileInfo> getFilesToCleanKeepingLatestVersions(String partitionPath) {
    LOG.info("Cleaning " + partitionPath + ", retaining latest " + config.getCleanerFileVersionsRetained()
        + " file versions. ");
    List<HoodieFileGroup> fileGroups = fileSystemView.getAllFileGroups(partitionPath).collect(Collectors.toList());
    List<CleanFileInfo> deletePaths = new ArrayList<>();
    // Collect all the datafiles savepointed by all the savepoints
    List<String> savepointedFiles = hoodieTable.getSavepoints().stream()
        .flatMap(this::getSavepointedDataFiles)
        .collect(Collectors.toList());

    for (HoodieFileGroup fileGroup : fileGroups) {
      int keepVersions = config.getCleanerFileVersionsRetained();
      // do not cleanup slice required for pending compaction
      Iterator<FileSlice> fileSliceIterator =
          fileGroup.getAllFileSlices().filter(fs -> !isFileSliceNeededForPendingCompaction(fs)).iterator();
      if (isFileGroupInPendingCompaction(fileGroup)) {
        // We have already saved the last version of file-groups for pending compaction Id
        keepVersions--;
      }

      while (fileSliceIterator.hasNext() && keepVersions > 0) {
        // Skip this most recent version
        FileSlice nextSlice = fileSliceIterator.next();
        Option<HoodieBaseFile> dataFile = nextSlice.getBaseFile();
        if (dataFile.isPresent() && savepointedFiles.contains(dataFile.get().getFileName())) {
          // do not clean up a savepoint data file
          continue;
        }
        keepVersions--;
      }
      // Delete the remaining files
      while (fileSliceIterator.hasNext()) {
        FileSlice nextSlice = fileSliceIterator.next();
        if (nextSlice.getBaseFile().isPresent()) {
          HoodieBaseFile dataFile = nextSlice.getBaseFile().get();
          deletePaths.add(new CleanFileInfo(dataFile.getPath(), false));
          if (dataFile.getBootstrapBaseFile().isPresent() && config.shouldCleanBootstrapBaseFile()) {
            deletePaths.add(new CleanFileInfo(dataFile.getBootstrapBaseFile().get().getPath(), true));
          }
        }
        if (hoodieTable.getMetaClient().getTableType() == HoodieTableType.MERGE_ON_READ) {
          // If merge on read, then clean the log files for the commits as well
          deletePaths.addAll(nextSlice.getLogFiles().map(lf -> new CleanFileInfo(lf.getPath().toString(), false))
              .collect(Collectors.toList()));
        }
      }
    }
    return deletePaths;
  }

  /**
   * Selects the versions for file for cleaning, such that it
   * <p>
   * - Leaves the latest version of the file untouched - For older versions, - It leaves all the commits untouched which
   * has occurred in last <code>config.getCleanerCommitsRetained()</code> commits - It leaves ONE commit before this
   * window. We assume that the max(query execution time) == commit_batch_time * config.getCleanerCommitsRetained().
   * This is 5 hours by default (assuming ingestion is running every 30 minutes). This is essential to leave the file
   * used by the query that is running for the max time.
   * <p>
   * This provides the effect of having lookback into all changes that happened in the last X commits. (eg: if you
   * retain 10 commits, and commit batch time is 30 mins, then you have 5 hrs of lookback)
   * <p>
   * This policy is the default.
   */
  private List<CleanFileInfo> getFilesToCleanKeepingLatestCommits(String partitionPath) {
    int commitsRetained = config.getCleanerCommitsRetained();
    LOG.info("Cleaning " + partitionPath + ", retaining latest " + commitsRetained + " commits. ");
    List<CleanFileInfo> deletePaths = new ArrayList<>();

    // Collect all the datafiles savepointed by all the savepoints
    List<String> savepointedFiles = hoodieTable.getSavepoints().stream()
        .flatMap(this::getSavepointedDataFiles)
        .collect(Collectors.toList());

    // determine if we have enough commits, to start cleaning.
    if (commitTimeline.countInstants() > commitsRetained) {
      HoodieInstant earliestCommitToRetain = getEarliestCommitToRetain().get();
      List<HoodieFileGroup> fileGroups = fileSystemView.getAllFileGroups(partitionPath).collect(Collectors.toList());
      for (HoodieFileGroup fileGroup : fileGroups) {
        List<FileSlice> fileSliceList = fileGroup.getAllFileSlices().collect(Collectors.toList());

        if (fileSliceList.isEmpty()) {
          continue;
        }

        String lastVersion = fileSliceList.get(0).getBaseInstantTime();
        String lastVersionBeforeEarliestCommitToRetain =
            getLatestVersionBeforeCommit(fileSliceList, earliestCommitToRetain);

        // Ensure there are more than 1 version of the file (we only clean old files from updates)
        // i.e always spare the last commit.
        for (FileSlice aSlice : fileSliceList) {
          Option<HoodieBaseFile> aFile = aSlice.getBaseFile();
          String fileCommitTime = aSlice.getBaseInstantTime();
          if (aFile.isPresent() && savepointedFiles.contains(aFile.get().getFileName())) {
            // do not clean up a savepoint data file
            continue;
          }
          // Dont delete the latest commit and also the last commit before the earliest commit we
          // are retaining
          // The window of commit retain == max query run time. So a query could be running which
          // still
          // uses this file.
          if (fileCommitTime.equals(lastVersion) || (fileCommitTime.equals(lastVersionBeforeEarliestCommitToRetain))) {
            // move on to the next file
            continue;
          }

          // Always keep the last commit
          if (!isFileSliceNeededForPendingCompaction(aSlice) && HoodieTimeline
              .compareTimestamps(earliestCommitToRetain.getTimestamp(), HoodieTimeline.GREATER_THAN, fileCommitTime)) {
            // this is a commit, that should be cleaned.
            aFile.ifPresent(hoodieDataFile -> {
              deletePaths.add(new CleanFileInfo(hoodieDataFile.getPath(), false));
              if (hoodieDataFile.getBootstrapBaseFile().isPresent() && config.shouldCleanBootstrapBaseFile()) {
                deletePaths.add(new CleanFileInfo(hoodieDataFile.getBootstrapBaseFile().get().getPath(), true));
              }
            });
            if (hoodieTable.getMetaClient().getTableType() == HoodieTableType.MERGE_ON_READ) {
              // If merge on read, then clean the log files for the commits as well
              deletePaths.addAll(aSlice.getLogFiles().map(lf -> new CleanFileInfo(lf.getPath().toString(), false))
                  .collect(Collectors.toList()));
            }
          }
        }
      }
    }
    return deletePaths;
  }

  /**
   * Gets the latest version < instantTime. This version file could still be used by queries.
   */
  private String getLatestVersionBeforeCommit(List<FileSlice> fileSliceList, HoodieInstant instantTime) {
    for (FileSlice file : fileSliceList) {
      String fileCommitTime = file.getBaseInstantTime();
      if (HoodieTimeline.compareTimestamps(instantTime.getTimestamp(), HoodieTimeline.GREATER_THAN, fileCommitTime)) {
        // fileList is sorted on the reverse, so the first commit we find <= instantTime is the
        // one we want
        return fileCommitTime;
      }
    }
    // There is no version of this file which is <= instantTime
    return null;
  }

  /**
   * Returns files to be cleaned for the given partitionPath based on cleaning policy.
   */
  public List<CleanFileInfo> getDeletePaths(String partitionPath) {
    HoodieCleaningPolicy policy = config.getCleanerPolicy();
    List<CleanFileInfo> deletePaths;
    if (policy == HoodieCleaningPolicy.KEEP_LATEST_COMMITS) {
      deletePaths = getFilesToCleanKeepingLatestCommits(partitionPath);
    } else if (policy == HoodieCleaningPolicy.KEEP_LATEST_FILE_VERSIONS) {
      deletePaths = getFilesToCleanKeepingLatestVersions(partitionPath);
    } else {
      throw new IllegalArgumentException("Unknown cleaning policy : " + policy.name());
    }
    LOG.info(deletePaths.size() + " patterns used to delete in partition path:" + partitionPath);

    return deletePaths;
  }

  /**
   * Returns earliest commit to retain based on cleaning policy.
   */
  public Option<HoodieInstant> getEarliestCommitToRetain() {
    Option<HoodieInstant> earliestCommitToRetain = Option.empty();
    int commitsRetained = config.getCleanerCommitsRetained();
    if (config.getCleanerPolicy() == HoodieCleaningPolicy.KEEP_LATEST_COMMITS
        && commitTimeline.countInstants() > commitsRetained) {
      earliestCommitToRetain = commitTimeline.nthInstant(commitTimeline.countInstants() - commitsRetained);
    }
    return earliestCommitToRetain;
  }

  /**
   * Determine if file slice needed to be preserved for pending compaction.
   * 
   * @param fileSlice File Slice
   * @return true if file slice needs to be preserved, false otherwise.
   */
  private boolean isFileSliceNeededForPendingCompaction(FileSlice fileSlice) {
    CompactionOperation op = fgIdToPendingCompactionOperations.get(fileSlice.getFileGroupId());
    if (null != op) {
      // If file slice's instant time is newer or same as that of operation, do not clean
      return HoodieTimeline.compareTimestamps(fileSlice.getBaseInstantTime(), HoodieTimeline.GREATER_THAN_OR_EQUALS, op.getBaseInstantTime()
      );
    }
    return false;
  }

  private boolean isFileGroupInPendingCompaction(HoodieFileGroup fg) {
    return fgIdToPendingCompactionOperations.containsKey(fg.getFileGroupId());
  }
}
