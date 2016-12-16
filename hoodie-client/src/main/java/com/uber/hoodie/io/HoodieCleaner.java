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

import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.common.model.HoodieCommits;
import com.uber.hoodie.common.model.HoodieTableMetadata;
import com.uber.hoodie.common.util.FSUtils;

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

/**
 * Cleaner is responsible for garbage collecting older files in a given partition path, such that
 *
 * 1) It provides sufficient time for existing queries running on older versions, to finish
 *
 * 2) It bounds the growth of the files in the file system
 *
 * TODO: Should all cleaning be done based on {@link com.uber.hoodie.common.model.HoodieCommitMetadata}
 *
 *
 */
public class HoodieCleaner {

    public enum CleaningPolicy {
        KEEP_LATEST_FILE_VERSIONS,
        KEEP_LATEST_COMMITS
    }


    private static Logger logger = LogManager.getLogger(HoodieCleaner.class);


    private HoodieTableMetadata metadata;

    private HoodieWriteConfig config;

    private FileSystem fs;

    public HoodieCleaner(HoodieTableMetadata metadata,
                         HoodieWriteConfig config,
                         FileSystem fs) {
        this.metadata = metadata;
        this.config = config;
        this.fs = fs;
    }


    /**
     *
     * Selects the older versions of files for cleaning, such that it bounds the number of versions of each file.
     * This policy is useful, if you are simply interested in querying the table, and you don't want too many
     * versions for a single file (i.e run it with versionsRetained = 1)
     *
     *
     * @param partitionPath
     * @return
     * @throws IOException
     */
    private List<String> getFilesToCleanKeepingLatestVersions(String partitionPath) throws IOException {
        logger.info("Cleaning "+ partitionPath+", retaining latest "+ config.getCleanerFileVersionsRetained()+" file versions. ");
        Map<String, List<FileStatus>> fileVersions = metadata.getAllVersionsInPartition(fs, partitionPath);
        List<String> deletePaths = new ArrayList<>();

        for (String file : fileVersions.keySet()) {
            List<FileStatus> commitList = fileVersions.get(file);
            int keepVersions = config.getCleanerFileVersionsRetained();
            Iterator<FileStatus> commitItr = commitList.iterator();
            while (commitItr.hasNext() && keepVersions > 0) {
                // Skip this most recent version
                commitItr.next();
                keepVersions--;
            }
            // Delete the remaining files
            while (commitItr.hasNext()) {
                deletePaths.add(String.format("%s/%s/%s",
                        config.getBasePath(),
                        partitionPath,
                        commitItr.next().getPath().getName()));
            }
        }
        return deletePaths;
    }


    /**
     * Selects the versions for file for cleaning, such that it
     *
     *  - Leaves the latest version of the file untouched
     *  - For older versions,
     *      - It leaves all the commits untouched which has occured in last <code>config.getCleanerCommitsRetained()</code> commits
     *      - It leaves ONE commit before this window. We assume that the max(query execution time) == commit_batch_time *  config.getCleanerCommitsRetained(). This is 12 hours by default.
     *        This is essential to leave the file used by the query thats running for the max time.
     *
     *  This provides the effect of having lookback into all changes that happened in the last X
     *  commits. (eg: if you retain 24 commits, and commit batch time is 30 mins, then you have 12 hrs of lookback)
     *
     *  This policy is the default.
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

        // determine if we have enough commits, to start cleaning.
        HoodieCommits commits = metadata.getAllCommits();
        if (commits.getNumCommits() > commitsRetained) {
            String earliestCommitToRetain =
                commits.nthCommit(commits.getNumCommits() - commitsRetained);
            Map<String, List<FileStatus>> fileVersions =
                metadata.getAllVersionsInPartition(fs, partitionPath);
            for (String file : fileVersions.keySet()) {
                List<FileStatus> fileList = fileVersions.get(file);
                String lastVersion = FSUtils.getCommitTime(fileList.get(0).getPath().getName());
                String lastVersionBeforeEarliestCommitToRetain =
                    getLatestVersionBeforeCommit(fileList, earliestCommitToRetain);

                // Ensure there are more than 1 version of the file (we only clean old files from updates)
                // i.e always spare the last commit.
                for (FileStatus afile : fileList) {
                    String fileCommitTime = FSUtils.getCommitTime(afile.getPath().getName());
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
                    if (HoodieCommits.isCommit1After(earliestCommitToRetain, fileCommitTime)) {
                        // this is a commit, that should be cleaned.
                        deletePaths.add(String
                            .format("%s/%s/%s", config.getBasePath(), partitionPath,
                                FSUtils.maskWithoutTaskPartitionId(fileCommitTime, file)));
                    }
                }
            }
        }

        return deletePaths;
    }

    /**
     * Gets the latest version < commitTime. This version file could still be used by queries.
     */
    private String getLatestVersionBeforeCommit(List<FileStatus> fileList, String commitTime) {
        for (FileStatus file : fileList) {
            String fileCommitTime = FSUtils.getCommitTime(file.getPath().getName());
            if (HoodieCommits.isCommit1After(commitTime, fileCommitTime)) {
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
    public int clean(String partitionPath) throws IOException {
        CleaningPolicy policy = config.getCleanerPolicy();
        List<String> deletePaths;
        if (policy == CleaningPolicy.KEEP_LATEST_COMMITS) {
            deletePaths = getFilesToCleanKeepingLatestCommits(partitionPath);
        } else if (policy == CleaningPolicy.KEEP_LATEST_FILE_VERSIONS) {
            deletePaths = getFilesToCleanKeepingLatestVersions(partitionPath);
        } else {
            throw new IllegalArgumentException("Unknown cleaning policy : " + policy.name());
        }

        // perform the actual deletes
        for (String deletePath : deletePaths) {
            logger.info("Working on delete path :" + deletePath);
            FileStatus[] deleteVersions = fs.globStatus(new Path(deletePath));
            if (deleteVersions != null) {
                for (FileStatus deleteVersion : deleteVersions) {
                    if (fs.delete(deleteVersion.getPath(), false)) {
                        logger.info("Cleaning file at path :" + deleteVersion.getPath());
                    }
                }
            }
        }
        logger.info(deletePaths.size() + " files deleted for partition path:" + partitionPath);
        return deletePaths.size();
    }
}
