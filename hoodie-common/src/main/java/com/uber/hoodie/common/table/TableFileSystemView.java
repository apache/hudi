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

import com.uber.hoodie.common.model.FileSlice;
import com.uber.hoodie.common.model.HoodieDataFile;
import com.uber.hoodie.common.model.HoodieFileGroup;
import com.uber.hoodie.common.model.HoodieLogFile;
import org.apache.hadoop.fs.FileStatus;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
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
     * Stream all the latest data files in the given partition
     *
     * @param partitionPath
     * @return
     */
    Stream<HoodieDataFile> getLatestDataFiles(String partitionPath);

    /**
     * Stream all the latest data files, in the file system view
     *
     * @return
     */
    Stream<HoodieDataFile> getLatestDataFiles();

    /**
     * Stream all the latest version data files in the given partition
     * with precondition that commitTime(file) before maxCommitTime
     *
     * @param partitionPath
     * @param maxCommitTime
     * @return
     */
    Stream<HoodieDataFile> getLatestDataFilesBeforeOrOn(String partitionPath,
                                                        String maxCommitTime);

    /**
     * Stream all the latest data files pass
     *
     * @param commitsToReturn
     * @return
     */
    Stream<HoodieDataFile> getLatestDataFilesInRange(List<String> commitsToReturn);

    /**
     * Stream all the data file versions grouped by FileId for a given partition
     *
     * @param partitionPath
     * @return
     */
    Stream<HoodieDataFile> getAllDataFiles(String partitionPath);

    /**
     * Stream all the latest file slices in the given partition
     *
     * @param partitionPath
     * @return
     */
    Stream<FileSlice> getLatestFileSlices(String partitionPath);

    /**
     * Stream all the latest file slices in the given partition
     * with precondition that commitTime(file) before maxCommitTime
     *
     * @param partitionPath
     * @param maxCommitTime
     * @return
     */
    Stream<FileSlice> getLatestFileSlicesBeforeOrOn(String partitionPath,
                                                    String maxCommitTime);

    /**
     * Stream all the latest file slices, in the given range
     *
     * @param commitsToReturn
     * @return
     */
    Stream<FileSlice> getLatestFileSliceInRange(List<String> commitsToReturn);

    /**
     * Stream all the file slices for a given partition, latest or not.
     *
     * @param partitionPath
     * @return
     */
    Stream<FileSlice> getAllFileSlices(String partitionPath);

    /**
     * Stream all the file groups for a given partition
     *
     * @param partitionPath
     * @return
     */
    Stream<HoodieFileGroup> getAllFileGroups(String partitionPath);

    /**
     * Get the file Status for the path specified
     *
     * @param path
     * @return
     */
    FileStatus getFileStatus(String path);
}
