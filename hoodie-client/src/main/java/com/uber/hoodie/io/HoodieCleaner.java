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

import com.clearspring.analytics.util.Lists;
import com.google.common.collect.Maps;
import com.uber.hoodie.common.HoodieCleanStat;
import com.uber.hoodie.common.model.HoodieCleaningPolicy;
import com.uber.hoodie.common.model.HoodieDataFile;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.table.TableFileSystemView;
import com.uber.hoodie.common.table.timeline.HoodieInstant;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.table.HoodieTable;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Cleaner is responsible for garbage collecting older files in a given partition path, such that
 * <p>
 * 1) It provides sufficient time for existing queries running on older versions, to finish
 * <p>
 * 2) It bounds the growth of the files in the file system
 * <p>
 * TODO: Should all cleaning be done based on {@link com.uber.hoodie.common.model.HoodieCommitMetadata}
 */
public class HoodieCleaner {
    private static Logger logger = LogManager.getLogger(HoodieCleaner.class);

    private final TableFileSystemView fileSystemView;
    private final HoodieTimeline commitTimeline;
    private HoodieTable hoodieTable;
    private HoodieWriteConfig config;
    private FileSystem fs;

    public HoodieCleaner(HoodieTable hoodieTable, HoodieWriteConfig config) {
        this.hoodieTable = hoodieTable;
        this.fileSystemView = hoodieTable.getCompactedFileSystemView();
        this.commitTimeline = hoodieTable.getCompletedCommitTimeline();
        this.config = config;
        this.fs = hoodieTable.getFs();
    }


    /**
     * Selects the older versions of files for cleaning, such that it bounds the number of versions of each file.
     * This policy is useful, if you are simply interested in querying the table, and you don't want too many
     * versions for a single file (i.e run it with versionsRetained = 1)
     *
     * @param partitionPath
     * @return
     * @throws IOException
     */
    private List<String> getFilesToCleanKeepingLatestVersions(String partitionPath)
        throws IOException {
        logger.info("Cleaning " + partitionPath + ", retaining latest " + config
            .getCleanerFileVersionsRetained() + " file versions. ");
        List<List<HoodieDataFile>> fileVersions =
            fileSystemView.getEveryVersionInPartition(partitionPath)
                .collect(Collectors.toList());
        List<String> deletePaths = new ArrayList<>();
        List<String> savepoints = hoodieTable.getSavepoints();

        for (List<HoodieDataFile> versionsForFileId : fileVersions) {
            int keepVersions = config.getCleanerFileVersionsRetained();
            Iterator<HoodieDataFile> commitItr = versionsForFileId.iterator();
            while (commitItr.hasNext() && keepVersions > 0) {
                // Skip this most recent version
                HoodieDataFile next = commitItr.next();
                if(savepoints.contains(next.getCommitTime())) {
                    // do not clean datafiles that are savepointed
                    continue;
                }
                keepVersions--;
            }
            // Delete the remaining files
            while (commitItr.hasNext()) {
                deletePaths.add(String.format("%s/%s/%s", config.getBasePath(), partitionPath,
                    commitItr.next().getFileName()));
            }
        }
        return deletePaths;
    }


    /**
     * Selects the versions for file for cleaning, such that it
     * <p>
     * - Leaves the latest version of the file untouched
     * - For older versions,
     * - It leaves all the commits untouched which has occured in last <code>config.getCleanerCommitsRetained()</code> commits
     * - It leaves ONE commit before this window. We assume that the max(query execution time) == commit_batch_time *  config.getCleanerCommitsRetained(). This is 12 hours by default.
     * This is essential to leave the file used by the query thats running for the max time.
     * <p>
     * This provides the effect of having lookback into all changes that happened in the last X
     * commits. (eg: if you retain 24 commits, and commit batch time is 30 mins, then you have 12 hrs of lookback)
     * <p>
     * This policy is the default.
     *
     * @param partitionPath
     * @return
     * @throws IOException
     */
    private List<String> getFilesToCleanKeepingLatestCommits(String partitionPath)
        throws IOException {
        int commitsRetained = config.getCleanerCommitsRetained();
        logger.info(
            "Cleaning " + partitionPath + ", retaining latest " + commitsRetained + " commits. ");
        List<String> deletePaths = new ArrayList<>();

        List<String> savepoints = hoodieTable.getSavepoints();

        // determine if we have enough commits, to start cleaning.
        if (commitTimeline.countInstants() > commitsRetained) {
            HoodieInstant earliestCommitToRetain =
                commitTimeline.nthInstant(commitTimeline.countInstants() - commitsRetained).get();
            List<List<HoodieDataFile>> fileVersions =
                fileSystemView.getEveryVersionInPartition(partitionPath)
                    .collect(Collectors.toList());
            for (List<HoodieDataFile> fileList : fileVersions) {
                String lastVersion = FSUtils.getCommitTime(fileList.get(0).getFileName());
                String lastVersionBeforeEarliestCommitToRetain =
                    getLatestVersionBeforeCommit(fileList, earliestCommitToRetain);

                // Ensure there are more than 1 version of the file (we only clean old files from updates)
                // i.e always spare the last commit.
                for (HoodieDataFile afile : fileList) {
                    String fileCommitTime = afile.getCommitTime();
                    if(savepoints.contains(fileCommitTime)) {
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
                    if (commitTimeline
                        .compareTimestamps(earliestCommitToRetain.getTimestamp(), fileCommitTime,
                            HoodieTimeline.GREATER)) {
                        // this is a commit, that should be cleaned.
                        deletePaths.add(String
                            .format("%s/%s/%s", config.getBasePath(), partitionPath, FSUtils
                                .maskWithoutTaskPartitionId(fileCommitTime, afile.getFileId())));
                    }
                }
            }
        }

        return deletePaths;
    }

    /**
     * Gets the latest version < commitTime. This version file could still be used by queries.
     */
    private String getLatestVersionBeforeCommit(List<HoodieDataFile> fileList,
        HoodieInstant commitTime) {
        for (HoodieDataFile file : fileList) {
            String fileCommitTime = FSUtils.getCommitTime(file.getFileName());
            if (commitTimeline.compareTimestamps(commitTime.getTimestamp(), fileCommitTime,
                HoodieTimeline.GREATER)) {
                // fileList is sorted on the reverse, so the first commit we find <= commitTime is the one we want
                return fileCommitTime;
            }
        }
        // There is no version of this file which is <= commitTime
        return null;
    }


    /**
     * Performs cleaning of the partition path according to cleaning policy and returns the number
     * of files cleaned.
     *
     * @throws IllegalArgumentException if unknown cleaning policy is provided
     */
    public HoodieCleanStat clean(String partitionPath) throws IOException {
        HoodieCleaningPolicy policy = config.getCleanerPolicy();
        List<String> deletePaths;
        Optional<HoodieInstant> earliestCommitToRetain = Optional.empty();
        if (policy == HoodieCleaningPolicy.KEEP_LATEST_COMMITS) {
            deletePaths = getFilesToCleanKeepingLatestCommits(partitionPath);
            int commitsRetained = config.getCleanerCommitsRetained();
            if (commitTimeline.countInstants() > commitsRetained) {
                earliestCommitToRetain =
                    commitTimeline.nthInstant(commitTimeline.countInstants() - commitsRetained);
            }
        } else if (policy == HoodieCleaningPolicy.KEEP_LATEST_FILE_VERSIONS) {
            deletePaths = getFilesToCleanKeepingLatestVersions(partitionPath);
        } else {
            throw new IllegalArgumentException("Unknown cleaning policy : " + policy.name());
        }

        // perform the actual deletes
        Map<FileStatus, Boolean> deletedFiles = Maps.newHashMap();
        for (String deletePath : deletePaths) {
            logger.info("Working on delete path :" + deletePath);
            FileStatus[] deleteVersions = fs.globStatus(new Path(deletePath));
            if (deleteVersions != null) {
                for (FileStatus deleteVersion : deleteVersions) {
                    boolean deleteResult = fs.delete(deleteVersion.getPath(), false);
                    deletedFiles.put(deleteVersion, deleteResult);
                    if (deleteResult) {
                        logger.info("Cleaned file at path :" + deleteVersion.getPath());
                    }
                }
            }
        }

        logger.info(deletePaths.size() + " patterns used to delete in partition path:" + partitionPath);
        return HoodieCleanStat.newBuilder().withPolicy(policy).withDeletePathPattern(deletePaths)
            .withPartitionPath(partitionPath).withEarliestCommitRetained(earliestCommitToRetain)
            .withDeletedFileResults(deletedFiles).build();
    }
}
