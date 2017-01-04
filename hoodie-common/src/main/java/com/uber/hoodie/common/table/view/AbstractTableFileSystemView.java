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

package com.uber.hoodie.common.table.view;

import com.uber.hoodie.common.model.HoodieDataFile;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.TableFileSystemView;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.exception.HoodieIOException;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Common abstract implementation for multiple TableFileSystemView Implementations.
 * 2 possible implementations are ReadOptimizedView and RealtimeView
 *
 * Concrete implementations extending this abstract class, should only implement
 * listDataFilesInPartition which includes files to be included in the view
 *
 * @see TableFileSystemView
 * @see ReadOptimizedTableView
 * @since 0.3.0
 */
public abstract class AbstractTableFileSystemView implements TableFileSystemView {
    protected final HoodieTableMetaClient metaClient;
    protected final transient FileSystem fs;
    protected final HoodieTimeline activeCommitTimeline;

    public AbstractTableFileSystemView(FileSystem fs, HoodieTableMetaClient metaClient) {
        this.metaClient = metaClient;
        this.fs = fs;
        this.activeCommitTimeline = metaClient.getActiveCommitTimeline();
    }

    public Stream<HoodieDataFile> getLatestDataFilesForFileId(final String partitionPath,
        String fileId) {
        Optional<String> lastInstant = activeCommitTimeline.lastInstant();
        if (lastInstant.isPresent()) {
            return streamLatestVersionInPartition(partitionPath, lastInstant.get())
                .filter(hoodieDataFile -> hoodieDataFile.getFileId().equals(fileId));
        }
        return Stream.empty();
    }

    @Override
    public Stream<HoodieDataFile> streamLatestVersionInPartition(String partitionPathStr,
        String maxCommitTime) {
        try {
            return streamLatestVersionsBeforeOrOn(listDataFilesInPartition(partitionPathStr),
                maxCommitTime);
        } catch (IOException e) {
            throw new HoodieIOException(
                "Could not get latest versions in Partition " + partitionPathStr, e);
        }
    }


    @Override
    public Stream<List<HoodieDataFile>> streamEveryVersionInPartition(String partitionPath) {
        try {
            if(activeCommitTimeline.lastInstant().isPresent()) {
                return streamFilesByFileId(listDataFilesInPartition(partitionPath),
                    activeCommitTimeline.lastInstant().get());
            }
            return Stream.empty();
        } catch (IOException e) {
            throw new HoodieIOException(
                "Could not load all file versions in partition " + partitionPath, e);
        }
    }

    protected abstract FileStatus[] listDataFilesInPartition(String partitionPathStr)
        throws IOException;

    @Override
    public Stream<HoodieDataFile> streamLatestVersionInRange(FileStatus[] fileStatuses,
        List<String> commitsToReturn) {
        if (!activeCommitTimeline.hasInstants() || commitsToReturn.isEmpty()) {
            return Stream.empty();
        }
        try {
            return streamFilesByFileId(fileStatuses, activeCommitTimeline.lastInstant().get())
                .map((Function<List<HoodieDataFile>, Optional<HoodieDataFile>>) fss -> {
                    for (HoodieDataFile fs : fss) {
                        if (commitsToReturn.contains(fs.getCommitTime())) {
                            return Optional.of(fs);
                        }
                    }
                    return Optional.empty();
                }).filter(Optional::isPresent).map(Optional::get);
        } catch (IOException e) {
            throw new HoodieIOException("Could not filter files from commits " + commitsToReturn,
                e);
        }
    }

    @Override
    public Stream<HoodieDataFile> streamLatestVersionsBeforeOrOn(FileStatus[] fileStatuses,
        String maxCommitToReturn) {
        try {
            if (!activeCommitTimeline.hasInstants()) {
                return Stream.empty();
            }
            return streamFilesByFileId(fileStatuses, activeCommitTimeline.lastInstant().get())
                .map((Function<List<HoodieDataFile>, Optional<HoodieDataFile>>) fss -> {
                    for (HoodieDataFile fs1 : fss) {
                        if (activeCommitTimeline
                            .compareInstants(fs1.getCommitTime(), maxCommitToReturn,
                                HoodieTimeline.LESSER_OR_EQUAL)) {
                            return Optional.of(fs1);
                        }
                    }
                    return Optional.empty();
                }).filter(Optional::isPresent).map(Optional::get);
        } catch (IOException e) {
            throw new HoodieIOException("Could not filter files for latest version ", e);
        }
    }

    @Override
    public Stream<HoodieDataFile> streamLatestVersions(FileStatus[] fileStatuses) {
        try {
            if (!activeCommitTimeline.hasInstants()) {
                return Stream.empty();
            }
            return streamFilesByFileId(fileStatuses, activeCommitTimeline.lastInstant().get())
                .map(statuses -> statuses.get(0));
        } catch (IOException e) {
            throw new HoodieIOException("Could not filter files for latest version ", e);
        }
    }

    protected Stream<List<HoodieDataFile>> streamFilesByFileId(FileStatus[] files,
        String maxCommitTime) throws IOException {
        return groupFilesByFileId(files, maxCommitTime).values().stream();
    }

    /**
     * Filters the list of FileStatus to exclude non-committed data files and group by FileID
     * and sort the actial files by commit time (newer commit first)
     *
     * @param files         Files to filter and group from
     * @param maxCommitTime maximum permissible commit time
     * @return Grouped map by fileId
     */
    private Map<String, List<HoodieDataFile>> groupFilesByFileId(FileStatus[] files,
        String maxCommitTime) throws IOException {
        return Arrays.stream(files).flatMap(fileStatus -> {
            HoodieDataFile dataFile = new HoodieDataFile(fileStatus);
            if (activeCommitTimeline.containsOrBeforeTimelineStarts(dataFile.getCommitTime())
                && activeCommitTimeline.compareInstants(dataFile.getCommitTime(), maxCommitTime,
                HoodieTimeline.LESSER_OR_EQUAL)) {
                return Stream.of(Pair.of(dataFile.getFileId(), dataFile));
            }
            return Stream.empty();
        }).collect(Collectors
            .groupingBy(Pair::getKey, Collectors.mapping(Pair::getValue, toSortedFileStatus())));
    }

    private Collector<HoodieDataFile, ?, List<HoodieDataFile>> toSortedFileStatus() {
        return Collectors.collectingAndThen(Collectors.toList(),
            l -> l.stream().sorted(HoodieDataFile.getCommitTimeComparator())
                .collect(Collectors.toList()));
    }


}
