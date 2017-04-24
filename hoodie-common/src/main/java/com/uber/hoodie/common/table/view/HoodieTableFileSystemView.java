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

import static java.util.stream.Collectors.toList;

import com.google.common.collect.Maps;
import com.uber.hoodie.common.model.HoodieCompactionMetadata;
import com.uber.hoodie.common.model.HoodieDataFile;
import com.uber.hoodie.common.model.HoodieTableType;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.TableFileSystemView;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.table.log.HoodieLogFile;
import com.uber.hoodie.common.table.timeline.HoodieInstant;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.exception.HoodieException;
import com.uber.hoodie.exception.HoodieIOException;
import java.util.function.BinaryOperator;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.Serializable;
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
 * <p>
 * Concrete implementations extending this abstract class, should only implement
 * listDataFilesInPartition which includes files to be included in the view
 *
 * @see TableFileSystemView
 * @since 0.3.0
 */
public class HoodieTableFileSystemView implements TableFileSystemView, Serializable {
    protected HoodieTableMetaClient metaClient;
    protected transient FileSystem fs;
    // This is the commits that will be visible for all views extending this view
    protected HoodieTimeline visibleActiveCommitTimeline;

    public HoodieTableFileSystemView(HoodieTableMetaClient metaClient,
        HoodieTimeline visibleActiveCommitTimeline) {
        this.metaClient = metaClient;
        this.fs = metaClient.getFs();
        this.visibleActiveCommitTimeline = visibleActiveCommitTimeline;
    }

    /**
     * This method is only used when this object is deserialized in a spark executor.
     *
     * @deprecated
     */
    private void readObject(java.io.ObjectInputStream in)
        throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        this.fs = FSUtils.getFs();
    }

    private void writeObject(java.io.ObjectOutputStream out)
        throws IOException {
        out.defaultWriteObject();
    }

    public Stream<HoodieDataFile> getLatestDataFilesForFileId(final String partitionPath,
        String fileId) {
        Optional<HoodieInstant> lastInstant = visibleActiveCommitTimeline.lastInstant();
        if (lastInstant.isPresent()) {
            return getLatestVersionInPartition(partitionPath, lastInstant.get().getTimestamp())
                .filter(hoodieDataFile -> hoodieDataFile.getFileId().equals(fileId));
        }
        return Stream.empty();
    }

    @Override
    public Stream<HoodieDataFile> getLatestVersionInPartition(String partitionPathStr,
        String maxCommitTime) {
        return getLatestVersionsBeforeOrOn(listDataFilesInPartition(partitionPathStr),
            maxCommitTime);
    }


    @Override
    public Stream<List<HoodieDataFile>> getEveryVersionInPartition(String partitionPath) {
        try {
            if (visibleActiveCommitTimeline.lastInstant().isPresent()) {
                return getFilesByFileId(listDataFilesInPartition(partitionPath),
                    visibleActiveCommitTimeline.lastInstant().get().getTimestamp());
            }
            return Stream.empty();
        } catch (IOException e) {
            throw new HoodieIOException(
                "Could not load all file versions in partition " + partitionPath, e);
        }
    }

    protected FileStatus[] listDataFilesInPartition(String partitionPathStr) {
        Path partitionPath = new Path(metaClient.getBasePath(), partitionPathStr);
        try {
            // Create the path if it does not exist already
            FSUtils.createPathIfNotExists(fs, partitionPath);
            return fs.listStatus(partitionPath, path -> path.getName()
                .contains(metaClient.getTableConfig().getROFileFormat().getFileExtension()));
        } catch (IOException e) {
            throw new HoodieIOException(
                "Failed to list data files in partition " + partitionPathStr, e);
        }
    }

    @Override
    public Stream<HoodieDataFile> getLatestVersionInRange(FileStatus[] fileStatuses,
        List<String> commitsToReturn) {
        if (visibleActiveCommitTimeline.empty() || commitsToReturn.isEmpty()) {
            return Stream.empty();
        }
        try {
            return getFilesByFileId(fileStatuses,
                visibleActiveCommitTimeline.lastInstant().get().getTimestamp())
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
    public Stream<HoodieDataFile> getLatestVersionsBeforeOrOn(FileStatus[] fileStatuses,
        String maxCommitToReturn) {
        try {
            if (visibleActiveCommitTimeline.empty()) {
                return Stream.empty();
            }
            return getFilesByFileId(fileStatuses,
                visibleActiveCommitTimeline.lastInstant().get().getTimestamp())
                .map((Function<List<HoodieDataFile>, Optional<HoodieDataFile>>) fss -> {
                    for (HoodieDataFile fs1 : fss) {
                        if (visibleActiveCommitTimeline
                            .compareTimestamps(fs1.getCommitTime(), maxCommitToReturn,
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
    public Stream<HoodieDataFile> getLatestVersions(FileStatus[] fileStatuses) {
        try {
            if (visibleActiveCommitTimeline.empty()) {
                return Stream.empty();
            }
            return getFilesByFileId(fileStatuses,
                visibleActiveCommitTimeline.lastInstant().get().getTimestamp())
                .map(statuses -> statuses.get(0));
        } catch (IOException e) {
            throw new HoodieIOException("Could not filter files for latest version ", e);
        }
    }

    public Map<HoodieDataFile, List<HoodieLogFile>> groupLatestDataFileWithLogFiles(
        String partitionPath) throws IOException {
        if (metaClient.getTableType() != HoodieTableType.MERGE_ON_READ) {
            throw new HoodieException("Unsupported table type :" + metaClient.getTableType());
        }

        // All the files in the partition
        FileStatus[] files = fs.listStatus(new Path(metaClient.getBasePath(), partitionPath));
        // All the log files filtered from the above list, sorted by version numbers
        List<HoodieLogFile> allLogFiles = Arrays.stream(files).filter(s -> s.getPath().getName()
            .contains(metaClient.getTableConfig().getRTFileFormat().getFileExtension()))
            .map(HoodieLogFile::new).collect(Collectors.collectingAndThen(toList(),
                l -> l.stream().sorted(HoodieLogFile.getLogVersionComparator())
                    .collect(toList())));

        // Filter the delta files by the commit time of the latest base file and collect as a list
        Optional<HoodieInstant> lastTimestamp = metaClient.getActiveTimeline().lastInstant();
        return lastTimestamp.map(hoodieInstant -> getLatestVersionInPartition(partitionPath,
            hoodieInstant.getTimestamp()).map(
            hoodieDataFile -> Pair.of(hoodieDataFile, allLogFiles.stream().filter(
                s -> s.getFileId().equals(hoodieDataFile.getFileId()) && s.getBaseCommitTime()
                    .equals(hoodieDataFile.getCommitTime())).collect(Collectors.toList()))).collect(
            Collectors.toMap(Pair::getKey, Pair::getRight))).orElseGet(Maps::newHashMap);
    }


    protected Stream<List<HoodieDataFile>> getFilesByFileId(FileStatus[] files,
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
        return Arrays.stream(files)
                // filter out files starting with "."
                .filter(file -> !file.getPath().getName().startsWith("."))
                .flatMap(fileStatus -> {
            HoodieDataFile dataFile = new HoodieDataFile(fileStatus);
            if (visibleActiveCommitTimeline.containsOrBeforeTimelineStarts(dataFile.getCommitTime())
                && visibleActiveCommitTimeline
                .compareTimestamps(dataFile.getCommitTime(), maxCommitTime,
                HoodieTimeline.LESSER_OR_EQUAL)) {
                return Stream.of(Pair.of(dataFile.getFileId(), dataFile));
            }
            return Stream.empty();
        }).collect(Collectors
            .groupingBy(Pair::getKey, Collectors.mapping(Pair::getValue, toSortedFileStatus())));
    }

    private Collector<HoodieDataFile, ?, List<HoodieDataFile>> toSortedFileStatus() {
        return Collectors.collectingAndThen(toList(),
            l -> l.stream().sorted(HoodieDataFile.getCommitTimeComparator())
                .collect(toList()));
    }


}
