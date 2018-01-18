/*
 * Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.io;

import com.uber.hoodie.common.model.FileSlice;
import com.uber.hoodie.common.model.HoodieCleaningPolicy;
import com.uber.hoodie.common.model.HoodieDataFile;
import com.uber.hoodie.common.model.HoodieFileGroup;
import com.uber.hoodie.common.model.HoodieRecordPayload;
import com.uber.hoodie.common.model.HoodieTableType;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.table.TableFileSystemView;
import com.uber.hoodie.common.table.timeline.HoodieInstant;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.table.HoodieTable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * Cleaner is responsible for garbage collecting older files in a given partition path, such that
 * <p> 1) It provides sufficient time for existing queries running on older versions, to finish <p>
 * 2) It bounds the growth of the files in the file system <p> TODO: Should all cleaning be done
 * based on {@link com.uber.hoodie.common.model.HoodieCommitMetadata}
 */
public class HoodieCleanHelper<T extends HoodieRecordPayload<T>> {

  private static Logger logger = LogManager.getLogger(HoodieCleanHelper.class);

  private final TableFileSystemView fileSystemView;
  private final HoodieTimeline commitTimeline;
  private HoodieTable<T> hoodieTable;
  private HoodieWriteConfig config;

  public HoodieCleanHelper(HoodieTable<T> hoodieTable, HoodieWriteConfig config) {
    this.hoodieTable = hoodieTable;
    this.fileSystemView = hoodieTable.getCompletedFileSystemView();
    this.commitTimeline = hoodieTable.getCompletedCommitTimeline();
    this.config = config;
  }


  /**
   * Selects the older versions of files for cleaning, such that it bounds the number of versions of
   * each file. This policy is useful, if you are simply interested in querying the table, and you
   * don't want too many versions for a single file (i.e run it with versionsRetained = 1)
   */
  private List<String> getFilesToCleanKeepingLatestVersions(String partitionPath)
      throws IOException {
    logger.info("Cleaning " + partitionPath + ", retaining latest " + config
        .getCleanerFileVersionsRetained() + " file versions. ");
    List<HoodieFileGroup> fileGroups =
        fileSystemView.getAllFileGroups(partitionPath)
            .collect(Collectors.toList());
    List<String> deletePaths = new ArrayList<>();
    // Collect all the datafiles savepointed by all the savepoints
    List<String> savepointedFiles = hoodieTable.getSavepoints().stream()
        .flatMap(s -> hoodieTable.getSavepointedDataFiles(s)).collect(Collectors.toList());

    for (HoodieFileGroup fileGroup : fileGroups) {
      int keepVersions = config.getCleanerFileVersionsRetained();
      Iterator<FileSlice> fileSliceIterator = fileGroup.getAllFileSlices().iterator();
      while (fileSliceIterator.hasNext() && keepVersions > 0) {
        // Skip this most recent version
        FileSlice nextSlice = fileSliceIterator.next();
        HoodieDataFile dataFile = nextSlice.getDataFile().get();
        if (savepointedFiles.contains(dataFile.getFileName())) {
          // do not clean up a savepoint data file
          continue;
        }
        keepVersions--;
      }
      // Delete the remaining files
      while (fileSliceIterator.hasNext()) {
        FileSlice nextSlice = fileSliceIterator.next();
        HoodieDataFile dataFile = nextSlice.getDataFile().get();
        deletePaths.add(dataFile.getFileStatus().getPath().toString());
        if (hoodieTable.getMetaClient().getTableType()
            == HoodieTableType.MERGE_ON_READ) {
          // If merge on read, then clean the log files for the commits as well
          deletePaths.addAll(nextSlice.getLogFiles()
              .map(file -> file.getPath().toString())
              .collect(Collectors.toList()));
        }
      }
    }
    return deletePaths;
  }


  /**
   * Selects the versions for file for cleaning, such that it <p> - Leaves the latest version of the
   * file untouched - For older versions, - It leaves all the commits untouched which has occured in
   * last <code>config.getCleanerCommitsRetained()</code> commits - It leaves ONE commit before this
   * window. We assume that the max(query execution time) == commit_batch_time *
   * config.getCleanerCommitsRetained(). This is 12 hours by default. This is essential to leave the
   * file used by the query thats running for the max time. <p> This provides the effect of having
   * lookback into all changes that happened in the last X commits. (eg: if you retain 24 commits,
   * and commit batch time is 30 mins, then you have 12 hrs of lookback) <p> This policy is the
   * default.
   */
  private List<String> getFilesToCleanKeepingLatestCommits(String partitionPath)
      throws IOException {
    int commitsRetained = config.getCleanerCommitsRetained();
    logger.info(
        "Cleaning " + partitionPath + ", retaining latest " + commitsRetained + " commits. ");
    List<String> deletePaths = new ArrayList<>();

    // Collect all the datafiles savepointed by all the savepoints
    List<String> savepointedFiles = hoodieTable.getSavepoints().stream()
        .flatMap(s -> hoodieTable.getSavepointedDataFiles(s)).collect(Collectors.toList());

    // determine if we have enough commits, to start cleaning.
    if (commitTimeline.countInstants() > commitsRetained) {
      HoodieInstant earliestCommitToRetain = getEarliestCommitToRetain().get();
      List<HoodieFileGroup> fileGroups =
          fileSystemView.getAllFileGroups(partitionPath)
              .collect(Collectors.toList());
      for (HoodieFileGroup fileGroup : fileGroups) {
        List<FileSlice> fileSliceList = fileGroup.getAllFileSlices().collect(Collectors.toList());
        HoodieDataFile dataFile = fileSliceList.get(0).getDataFile().get();
        String lastVersion = dataFile.getCommitTime();
        String lastVersionBeforeEarliestCommitToRetain =
            getLatestVersionBeforeCommit(fileSliceList, earliestCommitToRetain);

        // Ensure there are more than 1 version of the file (we only clean old files from updates)
        // i.e always spare the last commit.
        for (FileSlice aSlice : fileSliceList) {
          HoodieDataFile aFile = aSlice.getDataFile().get();
          String fileCommitTime = aFile.getCommitTime();
          if (savepointedFiles.contains(aFile.getFileName())) {
            // do not clean up a savepoint data file
            continue;
          }
          // Dont delete the latest commit and also the last commit before the earliest commit we are retaining
          // The window of commit retain == max query run time. So a query could be running which still
          // uses this file.
          if (fileCommitTime.equals(lastVersion) || (
              lastVersionBeforeEarliestCommitToRetain != null && fileCommitTime
                  .equals(lastVersionBeforeEarliestCommitToRetain))) {
            // move on to the next file
            continue;
          }

          // Always keep the last commit
          if (HoodieTimeline.compareTimestamps(
              earliestCommitToRetain.getTimestamp(),
              fileCommitTime,
              HoodieTimeline.GREATER)) {
            // this is a commit, that should be cleaned.
            deletePaths.add(aFile.getFileStatus().getPath().toString());
            if (hoodieTable.getMetaClient().getTableType()
                == HoodieTableType.MERGE_ON_READ) {
              // If merge on read, then clean the log files for the commits as well
              deletePaths.addAll(aSlice.getLogFiles()
                  .map(file -> file.getPath().toString())
                  .collect(Collectors.toList()));
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
  private String getLatestVersionBeforeCommit(List<FileSlice> fileSliceList,
      HoodieInstant commitTime) {
    for (FileSlice file : fileSliceList) {
      String fileCommitTime = file.getDataFile().get().getCommitTime();
      if (HoodieTimeline.compareTimestamps(commitTime.getTimestamp(), fileCommitTime,
          HoodieTimeline.GREATER)) {
        // fileList is sorted on the reverse, so the first commit we find <= commitTime is the one we want
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
    logger.info(
        deletePaths.size() + " patterns used to delete in partition path:" + partitionPath);

    return deletePaths;
  }

  /**
   * Returns earliest commit to retain based on cleaning policy.
   */
  public Optional<HoodieInstant> getEarliestCommitToRetain() {
    Optional<HoodieInstant> earliestCommitToRetain = Optional.empty();
    int commitsRetained = config.getCleanerCommitsRetained();
    if (config.getCleanerPolicy() == HoodieCleaningPolicy.KEEP_LATEST_COMMITS
        && commitTimeline.countInstants() > commitsRetained) {
      earliestCommitToRetain =
          commitTimeline.nthInstant(commitTimeline.countInstants() - commitsRetained);
    }
    return earliestCommitToRetain;
  }
}
