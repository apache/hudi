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

package org.apache.hudi.io;

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.common.model.CompactionOperation;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieDataFile;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTimeline;
import org.apache.hudi.common.table.SyncableFileSystemView;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.AvroUtils;
import org.apache.hudi.common.util.FSUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.table.HoodieTable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Cleaner is responsible for garbage collecting older files in a given partition path. Such that
 * <p>
 * 1) It provides sufficient time for existing queries running on older versions, to close
 * <p>
 * 2) It bounds the growth of the files in the file system
 * <p>
 * TODO: Should all cleaning be done based on {@link HoodieCommitMetadata}
 */
public class HoodieCleanHelper<T extends HoodieRecordPayload<T>> implements Serializable {

  private static Logger logger = LogManager.getLogger(HoodieCleanHelper.class);

  private final SyncableFileSystemView fileSystemView;
  private final HoodieTimeline commitTimeline;
  private final Map<HoodieFileGroupId, CompactionOperation> fgIdToPendingCompactionOperations;
  private HoodieTable<T> hoodieTable;
  private HoodieWriteConfig config;

  public HoodieCleanHelper(HoodieTable<T> hoodieTable, HoodieWriteConfig config) {
    this.hoodieTable = hoodieTable;
    this.fileSystemView = hoodieTable.getHoodieView();
    this.commitTimeline = hoodieTable.getCompletedCommitTimeline();
    this.config = config;
    this.fgIdToPendingCompactionOperations =
        ((SyncableFileSystemView) hoodieTable.getRTFileSystemView()).getPendingCompactionOperations()
            .map(entry -> Pair.of(
                new HoodieFileGroupId(entry.getValue().getPartitionPath(), entry.getValue().getFileId()),
                entry.getValue()))
            .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
  }

  /**
   * Returns list of partitions where clean operations needs to be performed.
   *
   * @param newInstantToRetain New instant to be retained after this cleanup operation
   * @return list of partitions to scan for cleaning
   * @throws IOException when underlying file-system throws this exception
   */
  public List<String> getPartitionPathsToClean(Option<HoodieInstant> newInstantToRetain) throws IOException {
    if (config.incrementalCleanerModeEnabled() && newInstantToRetain.isPresent()
        && (HoodieCleaningPolicy.KEEP_LATEST_COMMITS == config.getCleanerPolicy())) {
      Option<HoodieInstant> lastClean =
          hoodieTable.getCleanTimeline().filterCompletedInstants().lastInstant();
      if (lastClean.isPresent()) {
        HoodieCleanMetadata cleanMetadata = AvroUtils
            .deserializeHoodieCleanMetadata(hoodieTable.getActiveTimeline().getInstantDetails(lastClean.get()).get());
        if ((cleanMetadata.getEarliestCommitToRetain() != null)
            && (cleanMetadata.getEarliestCommitToRetain().length() > 0)) {
          logger.warn("Incremental Cleaning mode is enabled. Looking up partition-paths that have since changed "
              + "since last cleaned at " + cleanMetadata.getEarliestCommitToRetain()
              + ". New Instant to retain : " + newInstantToRetain);
          return hoodieTable.getCompletedCommitsTimeline().getInstants().filter(instant -> {
            return HoodieTimeline.compareTimestamps(instant.getTimestamp(), cleanMetadata.getEarliestCommitToRetain(),
                HoodieTimeline.GREATER_OR_EQUAL) && HoodieTimeline.compareTimestamps(instant.getTimestamp(),
                newInstantToRetain.get().getTimestamp(), HoodieTimeline.LESSER);
          }).flatMap(instant -> {
            try {
              HoodieCommitMetadata commitMetadata = HoodieCommitMetadata.fromBytes(
                  hoodieTable.getActiveTimeline().getInstantDetails(instant).get(), HoodieCommitMetadata.class);
              return commitMetadata.getPartitionToWriteStats().keySet().stream();
            } catch (IOException e) {
              throw new HoodieIOException(e.getMessage(), e);
            }
          }).distinct().collect(Collectors.toList());
        }
      }
    }
    // Otherwise go to brute force mode of scanning all partitions
    return FSUtils.getAllPartitionPaths(hoodieTable.getMetaClient().getFs(),
        hoodieTable.getMetaClient().getBasePath(), config.shouldAssumeDatePartitioning());
  }

  /**
   * Selects the older versions of files for cleaning, such that it bounds the number of versions of each file. This
   * policy is useful, if you are simply interested in querying the table, and you don't want too many versions for a
   * single file (i.e run it with versionsRetained = 1)
   */
  private List<String> getFilesToCleanKeepingLatestVersions(String partitionPath) throws IOException {
    logger.info("Cleaning " + partitionPath + ", retaining latest " + config.getCleanerFileVersionsRetained()
        + " file versions. ");
    List<HoodieFileGroup> fileGroups = fileSystemView.getAllFileGroups(partitionPath).collect(Collectors.toList());
    List<String> deletePaths = new ArrayList<>();
    // Collect all the datafiles savepointed by all the savepoints
    List<String> savepointedFiles = hoodieTable.getSavepoints().stream()
        .flatMap(s -> hoodieTable.getSavepointedDataFiles(s)).collect(Collectors.toList());

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
        Option<HoodieDataFile> dataFile = nextSlice.getDataFile();
        if (dataFile.isPresent() && savepointedFiles.contains(dataFile.get().getFileName())) {
          // do not clean up a savepoint data file
          continue;
        }
        keepVersions--;
      }
      // Delete the remaining files
      while (fileSliceIterator.hasNext()) {
        FileSlice nextSlice = fileSliceIterator.next();
        if (nextSlice.getDataFile().isPresent()) {
          HoodieDataFile dataFile = nextSlice.getDataFile().get();
          deletePaths.add(dataFile.getFileName());
        }
        if (hoodieTable.getMetaClient().getTableType() == HoodieTableType.MERGE_ON_READ) {
          // If merge on read, then clean the log files for the commits as well
          deletePaths.addAll(nextSlice.getLogFiles().map(file -> file.getFileName()).collect(Collectors.toList()));
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
  private List<String> getFilesToCleanKeepingLatestCommits(String partitionPath) throws IOException {
    int commitsRetained = config.getCleanerCommitsRetained();
    logger.info("Cleaning " + partitionPath + ", retaining latest " + commitsRetained + " commits. ");
    List<String> deletePaths = new ArrayList<>();

    // Collect all the datafiles savepointed by all the savepoints
    List<String> savepointedFiles = hoodieTable.getSavepoints().stream()
        .flatMap(s -> hoodieTable.getSavepointedDataFiles(s)).collect(Collectors.toList());

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
          Option<HoodieDataFile> aFile = aSlice.getDataFile();
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
              .compareTimestamps(earliestCommitToRetain.getTimestamp(), fileCommitTime, HoodieTimeline.GREATER)) {
            // this is a commit, that should be cleaned.
            aFile.ifPresent(hoodieDataFile -> deletePaths.add(hoodieDataFile.getFileName()));
            if (hoodieTable.getMetaClient().getTableType() == HoodieTableType.MERGE_ON_READ) {
              // If merge on read, then clean the log files for the commits as well
              deletePaths.addAll(aSlice.getLogFiles().map(file -> file.getFileName()).collect(Collectors.toList()));
            }
          }
        }
      }
    }

    return deletePaths;
  }

  /**
   * Gets the latest version < commitTime. This version file could still be used by queries.
   */
  private String getLatestVersionBeforeCommit(List<FileSlice> fileSliceList, HoodieInstant commitTime) {
    for (FileSlice file : fileSliceList) {
      String fileCommitTime = file.getBaseInstantTime();
      if (HoodieTimeline.compareTimestamps(commitTime.getTimestamp(), fileCommitTime, HoodieTimeline.GREATER)) {
        // fileList is sorted on the reverse, so the first commit we find <= commitTime is the
        // one we want
        return fileCommitTime;
      }
    }
    // There is no version of this file which is <= commitTime
    return null;
  }

  /**
   * Returns files to be cleaned for the given partitionPath based on cleaning policy.
   */
  public List<String> getDeletePaths(String partitionPath) throws IOException {
    HoodieCleaningPolicy policy = config.getCleanerPolicy();
    List<String> deletePaths;
    if (policy == HoodieCleaningPolicy.KEEP_LATEST_COMMITS) {
      deletePaths = getFilesToCleanKeepingLatestCommits(partitionPath);
    } else if (policy == HoodieCleaningPolicy.KEEP_LATEST_FILE_VERSIONS) {
      deletePaths = getFilesToCleanKeepingLatestVersions(partitionPath);
    } else {
      throw new IllegalArgumentException("Unknown cleaning policy : " + policy.name());
    }
    logger.info(deletePaths.size() + " patterns used to delete in partition path:" + partitionPath);

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
      return HoodieTimeline.compareTimestamps(fileSlice.getBaseInstantTime(), op.getBaseInstantTime(),
          HoodieTimeline.GREATER_OR_EQUAL);
    }
    return false;
  }

  private boolean isFileGroupInPendingCompaction(HoodieFileGroup fg) {
    return fgIdToPendingCompactionOperations.containsKey(fg.getFileGroupId());
  }
}
