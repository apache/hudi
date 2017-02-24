/*
 *  Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.uber.hoodie.common.table;

import com.uber.hoodie.common.model.HoodieDataFile;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.table.log.HoodieLogFile;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Interface for viewing the table file system.
 * Dependening on the Hoodie Table Type - The view of the filesystem changes.
 * <p>
 * ReadOptimizedView - Lets queries run only on organized columnar data files at the expense of latency
 * WriteOptimizedView - Lets queries run on columnar data as well as delta files (sequential) at the expense of query execution time
 *
 * @since 0.3.0
 */
public interface TableFileSystemView {
    /**
     * Stream all the data files for a specific FileId.
     * This usually has a single RO file and multiple WO files if present.
     *
     * @param partitionPath
     * @param fileId
     * @return
     */
    Stream<HoodieDataFile> getLatestDataFilesForFileId(final String partitionPath,
        final String fileId);

    /**
     * Stream all the latest version data files in the given partition
     * with precondition that commitTime(file) before maxCommitTime
     *
     * @param partitionPathStr
     * @param maxCommitTime
     * @return
     */
    Stream<HoodieDataFile> getLatestVersionInPartition(String partitionPathStr,
        String maxCommitTime);

    /**
     * Stream all the data file versions grouped by FileId for a given partition
     *
     * @param partitionPath
     * @return
     */
    Stream<List<HoodieDataFile>> getEveryVersionInPartition(String partitionPath);

    /**
     * Stream all the versions from the passed in fileStatus[] with commit times containing in commitsToReturn.
     *
     * @param fileStatuses
     * @param commitsToReturn
     * @return
     */
    Stream<HoodieDataFile> getLatestVersionInRange(FileStatus[] fileStatuses,
        List<String> commitsToReturn);

    /**
     * Stream the latest version from the passed in FileStatus[] with commit times less than maxCommitToReturn
     *
     * @param fileStatuses
     * @param maxCommitToReturn
     * @return
     */
    Stream<HoodieDataFile> getLatestVersionsBeforeOrOn(FileStatus[] fileStatuses,
        String maxCommitToReturn);

    /**
     * Stream latest versions from the passed in FileStatus[].
     * Similar to calling getLatestVersionsBeforeOrOn(fileStatuses, currentTimeAsCommitTime)
     *
     * @param fileStatuses
     * @return
     */
    Stream<HoodieDataFile> getLatestVersions(FileStatus[] fileStatuses);

    /**
     * Group data files with corresponding delta files
     * @param fs
     * @param partitionPath
     * @return
     * @throws IOException
     */
    Map<HoodieDataFile, List<HoodieLogFile>> groupLatestDataFileWithLogFiles(String partitionPath) throws IOException;

}
