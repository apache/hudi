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

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.uber.hoodie.common.model.HoodieDataFile;
import com.uber.hoodie.common.model.HoodieTableType;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.log.HoodieLogFile;
import com.uber.hoodie.common.table.timeline.HoodieActiveTimeline;
import com.uber.hoodie.common.table.timeline.HoodieInstant;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Realtime Table View which includes both ROStorageformat files and RTStorageFormat files
 */
public class RealtimeTableView extends AbstractTableFileSystemView {
    public RealtimeTableView(FileSystem fs, HoodieTableMetaClient metaClient) {
        // For realtime table view, visibleActiveCommitTimeline is a merged timeline of all commits and compactions
        super(fs, metaClient, metaClient.getActiveTimeline().getTimelineOfActions(
            Sets.newHashSet(HoodieActiveTimeline.COMMIT_ACTION,
                HoodieActiveTimeline.COMPACTION_ACTION)).filterCompletedInstants());
        Preconditions.checkArgument(metaClient.getTableType() == HoodieTableType.MERGE_ON_READ,
            "Realtime view can only be constructed on Hoodie tables with MERGE_ON_READ storage type");
    }

    public Map<HoodieDataFile, List<HoodieLogFile>> groupLatestDataFileWithLogFiles(FileSystem fs,
        String partitionPath) throws IOException {
        // All the files in the partition
        FileStatus[] files = fs.listStatus(new Path(metaClient.getBasePath(), partitionPath));
        // All the log files filtered from the above list, sorted by version numbers
        List<HoodieLogFile> allLogFiles = Arrays.stream(files).filter(s -> s.getPath().getName()
            .contains(metaClient.getTableConfig().getRTFileFormat().getFileExtension()))
            .map(HoodieLogFile::new).collect(Collectors.collectingAndThen(Collectors.toList(),
                l -> l.stream().sorted(HoodieLogFile.getLogVersionComparator())
                    .collect(Collectors.toList())));

        // Filter the delta files by the commit time of the latest base fine and collect as a list
        Optional<HoodieInstant> lastTimestamp = metaClient.getActiveTimeline().lastInstant();
        if(!lastTimestamp.isPresent()) {
            return Maps.newHashMap();
        }

        return getLatestVersionInPartition(partitionPath, lastTimestamp.get().getTimestamp()).map(
            hoodieDataFile -> Pair.of(hoodieDataFile, allLogFiles.stream().filter(
                s -> s.getFileId().equals(hoodieDataFile.getFileId()) && s.getBaseCommitTime()
                    .equals(hoodieDataFile.getCommitTime())).collect(Collectors.toList()))).collect(
            Collectors.toMap(
                (Function<Pair<HoodieDataFile, List<HoodieLogFile>>, HoodieDataFile>) Pair::getKey,
                (Function<Pair<HoodieDataFile, List<HoodieLogFile>>, List<HoodieLogFile>>) Pair::getRight));
    }

}
