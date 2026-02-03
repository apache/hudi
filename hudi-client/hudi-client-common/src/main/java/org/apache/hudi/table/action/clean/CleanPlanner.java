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
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.CleanFileInfo;
import org.apache.hudi.common.model.CompactionOperation;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.versioning.clean.CleanPlanV1MigrationHandler;
import org.apache.hudi.common.table.timeline.versioning.clean.CleanPlanV2MigrationHandler;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.util.CleanerUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieSavepointException;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.Serializable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.table.timeline.InstantComparison.GREATER_THAN;
import static org.apache.hudi.common.table.timeline.InstantComparison.GREATER_THAN_OR_EQUALS;
import static org.apache.hudi.common.table.timeline.InstantComparison.LESSER_THAN;
import static org.apache.hudi.common.table.timeline.InstantComparison.compareTimestamps;

/**
 * Cleaner is responsible for garbage collecting older files in a given partition path. Such that
 * <p>
 * 1) It provides sufficient time for existing queries running on older versions, to close
 * <p>
 * 2) It bounds the growth of the files in the file system
 */
@Slf4j
public class CleanPlanner<T, I, K, O> implements Serializable {

  public static final Integer CLEAN_PLAN_VERSION_1 = CleanPlanV1MigrationHandler.VERSION;
  public static final Integer CLEAN_PLAN_VERSION_2 = CleanPlanV2MigrationHandler.VERSION;
  public static final Integer LATEST_CLEAN_PLAN_VERSION = CLEAN_PLAN_VERSION_2;

  private transient HoodieTimeline commitTimeline;
  private final Map<HoodieFileGroupId, CompactionOperation> fgIdToPendingCompactionOperations;
  private final Map<HoodieFileGroupId, CompactionOperation> fgIdToPendingLogCompactionOperations;
  private final HoodieTable<T, I, K, O> hoodieTable;
  private final HoodieWriteConfig config;
  private final transient HoodieEngineContext context;
  @Getter(AccessLevel.PACKAGE)
  private final List<String> savepointedTimestamps;
  private Option<HoodieInstant> earliestCommitToRetain = Option.empty();

  public CleanPlanner(HoodieEngineContext context, HoodieTable<T, I, K, O> hoodieTable, HoodieWriteConfig config) {
    this.context = context;
    this.hoodieTable = hoodieTable;
    this.config = config;
    SyncableFileSystemView fileSystemView = (SyncableFileSystemView) hoodieTable.getSliceView();
    this.fgIdToPendingCompactionOperations = fileSystemView
        .getPendingCompactionOperations()
            .map(entry -> Pair.of(
                new HoodieFileGroupId(entry.getValue().getPartitionPath(), entry.getValue().getFileId()),
                entry.getValue()))
            .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
    this.fgIdToPendingLogCompactionOperations = fileSystemView.getPendingLogCompactionOperations()
        .map(entry -> Pair.of(new HoodieFileGroupId(entry.getValue().getPartitionPath(), entry.getValue().getFileId()), entry.getValue()))
        .collect(Collectors.toMap(Pair::getKey, Pair::getValue));

    // collect savepointed timestamps to assist with incremental cleaning. For non-partitioned and metadata table, we may not need this.
    this.savepointedTimestamps = hoodieTable.isMetadataTable() ? Collections.emptyList() : (hoodieTable.isPartitioned() ? new ArrayList<>(hoodieTable.getSavepointTimestamps())
        : Collections.emptyList());
  }

  private HoodieTimeline getCommitTimeline() {
    if (commitTimeline == null) {
      commitTimeline = hoodieTable.getCompletedCommitsTimeline();
    }
    return commitTimeline;
  }

  /**
   * Get the list of data file names savepointed.
   */
  public Stream<String> getSavepointedDataFiles(String savepointTime) {
    HoodieSavepointMetadata metadata = getSavepointMetadata(savepointTime);
    return metadata.getPartitionMetadata().values().stream().flatMap(s -> s.getSavepointDataFile().stream());
  }

  private HoodieSavepointMetadata getSavepointMetadata(String savepointTimestamp) {
    if (!hoodieTable.getSavepointTimestamps().contains(savepointTimestamp)) {
      throw new HoodieSavepointException(
          "Could not get data files for savepoint " + savepointTimestamp + ". No such savepoint.");
    }
    HoodieInstant instant = hoodieTable.getMetaClient().createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.SAVEPOINT_ACTION, savepointTimestamp);
    try {
      return hoodieTable.getActiveTimeline().readSavepointMetadata(instant);
    } catch (IOException e) {
      throw new HoodieSavepointException("Could not get savepointed data files for savepoint " + savepointTimestamp, e);
    }
  }

  /**
   * Returns list of partitions where clean operations needs to be performed.
   *
   * @param earliestRetainedInstant New instant to be retained after this cleanup operation
   * @return list of partitions to scan for cleaning
   * @throws IOException when underlying file-system throws this exception
   */
  public List<String> getPartitionPathsToClean(Option<HoodieInstant> earliestRetainedInstant) throws IOException {
    switch (config.getCleanerPolicy()) {
      case KEEP_LATEST_COMMITS:
      case KEEP_LATEST_BY_HOURS:
        return getPartitionPathsForCleanByCommits(earliestRetainedInstant);
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
      log.info("No earliest commit to retain. No need to scan partitions !!");
      return Collections.emptyList();
    }

    if (config.incrementalCleanerModeEnabled()) {
      Option<HoodieInstant> lastClean = hoodieTable.getCleanTimeline().filterCompletedInstants().lastInstant();
      if (lastClean.isPresent()) {
        HoodieCleanMetadata cleanMetadata = hoodieTable.getActiveTimeline().readCleanMetadata(lastClean.get());
        if ((cleanMetadata.getEarliestCommitToRetain() != null)
                && !cleanMetadata.getEarliestCommitToRetain().trim().isEmpty()
                && !hoodieTable.getActiveTimeline().getCommitsTimeline().isBeforeTimelineStarts(cleanMetadata.getEarliestCommitToRetain())) {
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

    boolean isAnySavepointDeleted = isAnySavepointDeleted(cleanMetadata);
    if (isAnySavepointDeleted) {
      log.info("Since savepoints have been removed compared to previous clean, triggering clean planning for all partitions");
      return getPartitionPathsForFullCleaning();
    } else {
      log.info(
          "Incremental Cleaning mode is enabled. Looking up partition-paths that have changed "
              + "since last clean at {}. New Instant to retain {}.",
          cleanMetadata.getEarliestCommitToRetain(),
          newInstantToRetain);

      return hoodieTable.getCompletedCommitsTimeline().getInstantsAsStream()
          .filter(instant -> compareTimestamps(instant.requestedTime(), GREATER_THAN_OR_EQUALS,
              cleanMetadata.getEarliestCommitToRetain()) && compareTimestamps(instant.requestedTime(),
              LESSER_THAN, newInstantToRetain.get().requestedTime()))
          .flatMap(this::getPartitionsForInstants).distinct().collect(Collectors.toList());
    }
  }

  private boolean isAnySavepointDeleted(HoodieCleanMetadata cleanMetadata) {
    List<String> savepointedTimestampsFromLastClean = cleanMetadata.getExtraMetadata() == null ? Collections.emptyList()
        : Arrays.stream(cleanMetadata.getExtraMetadata().getOrDefault(CleanerUtils.SAVEPOINTED_TIMESTAMPS, StringUtils.EMPTY_STRING).split(","))
        .filter(partition -> !StringUtils.isNullOrEmpty(partition)).collect(Collectors.toList());
    if (savepointedTimestampsFromLastClean.isEmpty()) {
      return false;
    }
    // check for any savepointed removed in latest compared to previous saved list
    List<String> removedSavepointedTimestamps = new ArrayList<>(savepointedTimestampsFromLastClean);
    removedSavepointedTimestamps.removeAll(savepointedTimestamps);
    return !removedSavepointedTimestamps.isEmpty();
  }

  /**
   * Fetch partitions updated as part of a HoodieInstant.
   * @param instant {@link HoodieInstant} of interest.
   * @return partitions that were part of {@link HoodieInstant} given.
   */
  private Stream<String> getPartitionsForInstants(HoodieInstant instant) {
    try {
      if (HoodieTimeline.REPLACE_COMMIT_ACTION.equals(instant.getAction())) {
        HoodieReplaceCommitMetadata replaceCommitMetadata =
            hoodieTable.getActiveTimeline().readReplaceCommitMetadata(instant);
        return Stream.concat(replaceCommitMetadata.getPartitionToReplaceFileIds().keySet().stream(), replaceCommitMetadata.getPartitionToWriteStats().keySet().stream());
      } else {
        HoodieCommitMetadata commitMetadata =
            hoodieTable.getActiveTimeline().readCommitMetadata(instant);
        return commitMetadata.getPartitionToWriteStats().keySet().stream();
      }
    } catch (IOException e) {
      throw new HoodieIOException(e.getMessage(), e);
    }
  }

  /**
   * Scan and list all partitions for cleaning.
   * @return all partitions paths for the dataset.
   */
  private List<String> getPartitionPathsForFullCleaning() {
    // Go to brute force mode of scanning all partitions
    try {
      return hoodieTable.getTableMetadata().getAllPartitionPaths();
    } catch (IOException ioe) {
      throw new HoodieIOException("Fetching all partitions failed ", ioe);
    }
  }

  /**
   *  Verify whether file slice exists in savepointedFiles, check both base file and log files
   */
  private boolean isFileSliceExistInSavepointedFiles(FileSlice fs, List<String> savepointedFiles) {
    if (fs.getBaseFile().isPresent() && savepointedFiles.contains(fs.getBaseFile().get().getFileName())) {
      return true;
    }
    for (HoodieLogFile hoodieLogFile : fs.getLogFiles().collect(Collectors.toList())) {
      if (savepointedFiles.contains(hoodieLogFile.getFileName())) {
        return true;
      }
    }
    return false;
  }

  /**
   * Selects the older versions of files for cleaning, such that it bounds the number of versions of each file. This
   * policy is useful, if you are simply interested in querying the table, and you don't want too many versions for a
   * single file (i.e., run it with versionsRetained = 1)
   */
  private Pair<Boolean, List<CleanFileInfo>> getFilesToCleanKeepingLatestVersions(String partitionPath) {
    log.info(
        "Cleaning {}, retaining latest {} file versions.",
        partitionPath,
        config.getCleanerFileVersionsRetained());

    List<CleanFileInfo> deletePaths = new ArrayList<>();
    // Collect all the datafiles savepointed by all the savepoints
    List<String> savepointedFiles = hoodieTable.getSavepointTimestamps().stream()
        .flatMap(this::getSavepointedDataFiles)
        .collect(Collectors.toList());

    // In this scenario, we will assume that once replaced a file group automatically becomes eligible for cleaning completely
    // In other words, the file versions only apply to the active file groups.
    deletePaths.addAll(getReplacedFilesEligibleToClean(savepointedFiles, partitionPath, Option.empty()));
    boolean toDeletePartition = false;
    List<HoodieFileGroup> fileGroups = hoodieTable.getHoodieView().getAllFileGroupsStateless(partitionPath).collect(Collectors.toList());
    for (HoodieFileGroup fileGroup : fileGroups) {
      int keepVersions = config.getCleanerFileVersionsRetained();
      // do not cleanup slice required for pending compaction
      Iterator<FileSlice> fileSliceIterator =
          fileGroup.getAllFileSlices()
              .filter(fs -> !isFileSliceNeededForPendingMajorOrMinorCompaction(fs))
              .iterator();
      if (isFileGroupInPendingMajorOrMinorCompaction(fileGroup)) {
        // We have already saved the last version of file-groups for pending compaction Id
        keepVersions--;
      }

      while (fileSliceIterator.hasNext() && keepVersions > 0) {
        // Skip this most recent version
        fileSliceIterator.next();
        keepVersions--;
      }
      // Delete the remaining files
      while (fileSliceIterator.hasNext()) {
        FileSlice nextSlice = fileSliceIterator.next();
        if (isFileSliceExistInSavepointedFiles(nextSlice, savepointedFiles)) {
          // do not clean up a savepoint data file
          continue;
        }
        deletePaths.addAll(getCleanFileInfoForSlice(nextSlice));
      }
    }
    // if there are no valid file groups
    // and no pending data files under the partition [IMPORTANT],
    // mark it to be deleted
    if (fileGroups.isEmpty() && !hasPendingFiles(partitionPath)) {
      toDeletePartition = true;
    }
    return Pair.of(toDeletePartition, deletePaths);
  }

  private Pair<Boolean, List<CleanFileInfo>> getFilesToCleanKeepingLatestCommits(String partitionPath, Option<HoodieInstant> earliestCommitToRetain) {
    return getFilesToCleanKeepingLatestCommits(partitionPath, config.getCleanerCommitsRetained(), earliestCommitToRetain, HoodieCleaningPolicy.KEEP_LATEST_COMMITS);
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
   *
   * @return A {@link Pair} whose left is boolean indicating whether partition itself needs to be deleted,
   *         and right is a list of {@link CleanFileInfo} about the files in the partition that needs to be deleted.
   */
  private Pair<Boolean, List<CleanFileInfo>> getFilesToCleanKeepingLatestCommits(String partitionPath,
      int commitsRetained, Option<HoodieInstant> earliestCommitToRetain, HoodieCleaningPolicy policy) {
    if (policy != HoodieCleaningPolicy.KEEP_LATEST_COMMITS && policy != HoodieCleaningPolicy.KEEP_LATEST_BY_HOURS) {
      throw new IllegalArgumentException("getFilesToCleanKeepingLatestCommits can only be used for KEEP_LATEST_COMMITS or KEEP_LATEST_BY_HOURS");
    }
    log.info("Cleaning {}, retaining latest {} commits.", partitionPath, commitsRetained);
    List<CleanFileInfo> deletePaths = new ArrayList<>();

    // Collect all the datafiles savepointed by all the savepoints
    List<String> savepointedFiles = hoodieTable.getSavepointTimestamps().stream()
        .flatMap(this::getSavepointedDataFiles)
        .collect(Collectors.toList());

    // determine if we have enough commits, to start cleaning.
    boolean toDeletePartition = false;
    if (getCommitTimeline().countInstants() > commitsRetained) {
      HoodieInstant earliestInstant = earliestCommitToRetain.get();
      // all replaced file groups before earliestCommitToRetain are eligible to clean
      deletePaths.addAll(getReplacedFilesEligibleToClean(savepointedFiles, partitionPath, earliestCommitToRetain));
      // add active files
      List<HoodieFileGroup> fileGroups = hoodieTable.getHoodieView().getAllFileGroupsStateless(partitionPath).collect(Collectors.toList());
      for (HoodieFileGroup fileGroup : fileGroups) {
        List<FileSlice> fileSliceList = fileGroup.getAllFileSlices().collect(Collectors.toList());

        if (fileSliceList.isEmpty()) {
          continue;
        }

        String lastVersion = fileSliceList.get(0).getBaseInstantTime();
        String lastVersionBeforeEarliestCommitToRetain =
            getLatestVersionBeforeCommit(fileSliceList, earliestInstant);

        // Ensure there are more than 1 version of the file (we only clean old files from updates)
        // i.e., always spare the last commit.
        for (FileSlice aSlice : fileSliceList) {
          Option<HoodieBaseFile> aFile = aSlice.getBaseFile();
          String fileCommitTime = aSlice.getBaseInstantTime();
          if (isFileSliceExistInSavepointedFiles(aSlice, savepointedFiles)) {
            // do not clean up a savepoint data file
            continue;
          }

          // Do not delete the latest commit and also the last commit before the earliest commit we
          // are retaining
          // The window of commit retain == max query run time. So a query could be running which
          // still uses this file.
          if (fileCommitTime.equals(lastVersion) || fileCommitTime.equals(lastVersionBeforeEarliestCommitToRetain)) {
            // move on to the next file
            continue;
          }

          // Always keep the last commit
          if (!isFileSliceNeededForPendingMajorOrMinorCompaction(aSlice)
              && compareTimestamps(earliestInstant.requestedTime(), GREATER_THAN, fileCommitTime)) {
            // this is a commit, that should be cleaned.
            aFile.ifPresent(hoodieDataFile -> {
              deletePaths.add(new CleanFileInfo(hoodieDataFile.getPath(), false));
              if (hoodieDataFile.getBootstrapBaseFile().isPresent() && config.shouldCleanBootstrapBaseFile()) {
                deletePaths.add(new CleanFileInfo(hoodieDataFile.getBootstrapBaseFile().get().getPath(), true));
              }
            });
            // clean the log files for the commits, which contain cdc log files in cdc scenario
            // and normal log files for mor tables.
            deletePaths.addAll(aSlice.getLogFiles().map(lf -> new CleanFileInfo(lf.getPath().toString(), false))
                .collect(Collectors.toList()));
          }
        }
      }
      // if there are no valid file groups
      // and no pending data files under the partition [IMPORTANT],
      // and no subsequent replace commit after the earliest retained commit
      // mark it to be deleted
      if (fileGroups.isEmpty()
          && !hasPendingFiles(partitionPath)
          && noSubsequentReplaceCommit(earliestInstant.requestedTime(), partitionPath)) {
        toDeletePartition = true;
      }
    }
    return Pair.of(toDeletePartition, deletePaths);
  }

  /**
   * Returns whether there are uncommitted data files under the given partition,
   * the pending files are generated by the inflight instants and maybe ready to commit,
   * the partition can not be deleted as a whole if any pending file exists.
   *
   * <p>IMPORTANT: {@code fsView.getAllFileGroups} does not return pending file groups for metadata table,
   * file listing must be used instead.
   */
  private boolean hasPendingFiles(String partitionPath) {
    try {
      HoodieTableFileSystemView fsView = HoodieTableFileSystemView.fileListingBasedFileSystemView(context, hoodieTable.getMetaClient(), hoodieTable.getActiveTimeline());
      StoragePath fullPartitionPath = new StoragePath(hoodieTable.getMetaClient().getBasePath(), partitionPath);
      fsView.addFilesToView(partitionPath, FSUtils.getAllDataFilesInPartition(
          hoodieTable.getStorage(), fullPartitionPath));
      // use #getAllFileGroups(partitionPath) instead of #getAllFileGroups() to exclude the replaced file groups.
      return fsView.getAllFileGroups(partitionPath).findAny().isPresent();
    } catch (Exception ex) {
      // if any exception throws, assume there are existing pending files
      log.warn("Error while checking the pending files under partition: {}, we will assume that pending files exist", partitionPath, ex);
      return true;
    }
  }

  /**
   * This method finds the files to be cleaned based on the number of hours. If {@code config.getCleanerHoursRetained()} is set to 5,
   * all the files with commit time earlier than 5 hours will be removed. Also the latest file for any file group is retained.
   * This policy gives much more flexibility to users for retaining data for running incremental queries as compared to
   * KEEP_LATEST_COMMITS cleaning policy. The default number of hours is 5.
   * @param partitionPath partition path to check
   * @param earliestCommitToRetain earliest commit to retain
   * @return list of files to clean
   */
  private Pair<Boolean, List<CleanFileInfo>> getFilesToCleanKeepingLatestHours(String partitionPath, Option<HoodieInstant> earliestCommitToRetain) {
    return getFilesToCleanKeepingLatestCommits(partitionPath, 0, earliestCommitToRetain, HoodieCleaningPolicy.KEEP_LATEST_BY_HOURS);
  }

  private List<CleanFileInfo> getReplacedFilesEligibleToClean(List<String> savepointedFiles, String partitionPath, Option<HoodieInstant> earliestCommitToRetain) {
    final Stream<HoodieFileGroup> replacedGroups;
    if (earliestCommitToRetain.isPresent()) {
      replacedGroups = hoodieTable.getHoodieView().getReplacedFileGroupsBefore(earliestCommitToRetain.get().requestedTime(), partitionPath);
    } else {
      replacedGroups = hoodieTable.getHoodieView().getAllReplacedFileGroups(partitionPath);
    }
    return replacedGroups.flatMap(HoodieFileGroup::getAllFileSlices)
        // do not delete savepointed files  (archival will make sure corresponding replacecommit file is not deleted)
        .filter(slice -> !isFileSliceExistInSavepointedFiles(slice, savepointedFiles))
        .flatMap(slice -> getCleanFileInfoForSlice(slice).stream())
        .collect(Collectors.toList());
  }

  /**
   * Gets the latest version < instantTime. This version file could still be used by queries.
   */
  private String getLatestVersionBeforeCommit(List<FileSlice> fileSliceList, HoodieInstant instantTime) {
    for (FileSlice file : fileSliceList) {
      String fileCommitTime = file.getBaseInstantTime();
      if (compareTimestamps(instantTime.requestedTime(), GREATER_THAN, fileCommitTime)) {
        // fileList is sorted on the reverse, so the first commit we find <= instantTime is the
        // one we want
        return fileCommitTime;
      }
    }
    // There is no version of this file which is <= instantTime
    return null;
  }

  private List<CleanFileInfo> getCleanFileInfoForSlice(FileSlice nextSlice) {
    List<CleanFileInfo> cleanPaths = new ArrayList<>();
    if (nextSlice.getBaseFile().isPresent()) {
      HoodieBaseFile dataFile = nextSlice.getBaseFile().get();
      cleanPaths.add(new CleanFileInfo(dataFile.getPath(), false));
      if (dataFile.getBootstrapBaseFile().isPresent() && config.shouldCleanBootstrapBaseFile()) {
        cleanPaths.add(new CleanFileInfo(dataFile.getBootstrapBaseFile().get().getPath(), true));
      }
    }

    // clean the log files for the commits, which contain cdc log files in cdc scenario
    // and normal log files for mor tables.
    cleanPaths.addAll(
        nextSlice.getLogFiles().map(lf -> new CleanFileInfo(lf.getPath().toString(), false))
            .collect(Collectors.toList()));
    return cleanPaths;
  }

  /**
   * Returns files to be cleaned for the given partitionPath based on cleaning policy.
   */
  public Pair<Boolean, List<CleanFileInfo>> getDeletePaths(String partitionPath, Option<HoodieInstant> earliestCommitToRetain) {
    HoodieCleaningPolicy policy = config.getCleanerPolicy();
    Pair<Boolean, List<CleanFileInfo>> deletePaths;
    if (policy == HoodieCleaningPolicy.KEEP_LATEST_COMMITS) {
      deletePaths = getFilesToCleanKeepingLatestCommits(partitionPath, earliestCommitToRetain);
    } else if (policy == HoodieCleaningPolicy.KEEP_LATEST_FILE_VERSIONS) {
      deletePaths = getFilesToCleanKeepingLatestVersions(partitionPath);
    } else if (policy == HoodieCleaningPolicy.KEEP_LATEST_BY_HOURS) {
      deletePaths = getFilesToCleanKeepingLatestHours(partitionPath, earliestCommitToRetain);
    } else {
      throw new IllegalArgumentException("Unknown cleaning policy : " + policy.name());
    }
    log.info("{} patterns used to delete in partition path:{}", deletePaths.getValue().size(), partitionPath);
    if (deletePaths.getKey()) {
      log.info("Partition {} to be deleted", partitionPath);
    }
    return deletePaths;
  }

  /**
   * Returns the earliest commit to retain based on cleaning policy.
   */
  public Option<HoodieInstant> getEarliestCommitToRetain() {
    if (!earliestCommitToRetain.isPresent()) {
      earliestCommitToRetain = CleanerUtils.getEarliestCommitToRetain(
          hoodieTable.getMetaClient().getActiveTimeline().getCommitsAndCompactionTimeline(),
          config.getCleanerPolicy(),
          config.getCleanerCommitsRetained(),
          Instant.now(),
          config.getCleanerHoursRetained(),
          hoodieTable.getMetaClient().getTableConfig().getTimelineTimezone());
    }
    return earliestCommitToRetain;
  }

  /**
   * Returns the last completed commit timestamp before clean.
   */
  public String getLastCompletedCommitTimestamp() {
    return getCommitTimeline().lastInstant().map(HoodieInstant::requestedTime).orElse("");
  }

  /*
   * Determine if file slice needed to be preserved for pending compaction or log compaction.
   * @param fileSlice File slice
   * @return true if file slice needs to be preserved, false otherwise.
   */
  private boolean isFileSliceNeededForPendingMajorOrMinorCompaction(FileSlice fileSlice) {
    return isFileSliceNeededForPendingCompaction(fileSlice) || isFileSliceNeededForPendingLogCompaction(fileSlice);
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
      return compareTimestamps(fileSlice.getBaseInstantTime(), GREATER_THAN_OR_EQUALS, op.getBaseInstantTime()
      );
    }
    return false;
  }

  /**
   * Determine if file slice needed to be preserved for pending logcompaction.
   *
   * @param fileSlice File Slice
   * @return true if file slice needs to be preserved, false otherwise.
   */
  private boolean isFileSliceNeededForPendingLogCompaction(FileSlice fileSlice) {
    CompactionOperation op = fgIdToPendingLogCompactionOperations.get(fileSlice.getFileGroupId());
    if (null != op) {
      // If file slice's instant time is newer or same as that of operation, do not clean
      return compareTimestamps(fileSlice.getBaseInstantTime(), GREATER_THAN_OR_EQUALS, op.getBaseInstantTime()
      );
    }
    return false;
  }

  private boolean isFileGroupInPendingMajorOrMinorCompaction(HoodieFileGroup fg) {
    return fgIdToPendingCompactionOperations.containsKey(fg.getFileGroupId())
        || fgIdToPendingLogCompactionOperations.containsKey(fg.getFileGroupId());
  }

  private boolean noSubsequentReplaceCommit(String earliestCommitToRetain, String partitionPath) {
    return !hoodieTable.getHoodieView().getReplacedFileGroupsAfterOrOn(earliestCommitToRetain, partitionPath).findAny().isPresent();
  }
}
