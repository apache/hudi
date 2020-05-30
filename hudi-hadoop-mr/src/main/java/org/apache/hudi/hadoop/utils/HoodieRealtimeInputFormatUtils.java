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

package org.apache.hudi.hadoop.utils;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.hadoop.realtime.HoodieRealtimeFileSplit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class HoodieRealtimeInputFormatUtils extends HoodieInputFormatUtils {

  private static final Logger LOG = LogManager.getLogger(HoodieRealtimeInputFormatUtils.class);

  public static InputSplit[] getRealtimeSplits(Configuration conf, Stream<FileSplit> fileSplits) throws IOException {
    Map<Path, List<FileSplit>> partitionsToParquetSplits =
        fileSplits.collect(Collectors.groupingBy(split -> split.getPath().getParent()));
    // TODO(vc): Should we handle also non-hoodie splits here?
    Map<Path, HoodieTableMetaClient> partitionsToMetaClient = getMetaClientPerPartition(conf, partitionsToParquetSplits.keySet());

    // for all unique split parents, obtain all delta files based on delta commit timeline,
    // grouped on file id
    List<HoodieRealtimeFileSplit> rtSplits = new ArrayList<>();
    partitionsToParquetSplits.keySet().forEach(partitionPath -> {
      // for each partition path obtain the data & log file groupings, then map back to inputsplits
      HoodieTableMetaClient metaClient = partitionsToMetaClient.get(partitionPath);
      HoodieTableFileSystemView fsView = new HoodieTableFileSystemView(metaClient, metaClient.getActiveTimeline());
      String relPartitionPath = FSUtils.getRelativePartitionPath(new Path(metaClient.getBasePath()), partitionPath);

      try {
        // Both commit and delta-commits are included - pick the latest completed one
        Option<HoodieInstant> latestCompletedInstant =
            metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants().lastInstant();

        Stream<FileSlice> latestFileSlices = latestCompletedInstant
            .map(instant -> fsView.getLatestMergedFileSlicesBeforeOrOn(relPartitionPath, instant.getTimestamp()))
            .orElse(Stream.empty());

        // subgroup splits again by file id & match with log files.
        Map<String, List<FileSplit>> groupedInputSplits = partitionsToParquetSplits.get(partitionPath).stream()
            .collect(Collectors.groupingBy(split -> FSUtils.getFileId(split.getPath().getName())));
        latestFileSlices.forEach(fileSlice -> {
          List<FileSplit> dataFileSplits = groupedInputSplits.get(fileSlice.getFileId());
          dataFileSplits.forEach(split -> {
            try {
              List<String> logFilePaths = fileSlice.getLogFiles().sorted(HoodieLogFile.getLogFileComparator())
                  .map(logFile -> logFile.getPath().toString()).collect(Collectors.toList());
              // Get the maxCommit from the last delta or compaction or commit - when
              // bootstrapped from COW table
              String maxCommitTime = metaClient
                  .getActiveTimeline().getTimelineOfActions(CollectionUtils.createSet(HoodieTimeline.COMMIT_ACTION,
                      HoodieTimeline.ROLLBACK_ACTION, HoodieTimeline.DELTA_COMMIT_ACTION))
                  .filterCompletedInstants().lastInstant().get().getTimestamp();
              rtSplits.add(new HoodieRealtimeFileSplit(split, metaClient.getBasePath(), logFilePaths, maxCommitTime));
            } catch (IOException e) {
              throw new HoodieIOException("Error creating hoodie real time split ", e);
            }
          });
        });
      } catch (Exception e) {
        throw new HoodieException("Error obtaining data file/log file grouping: " + partitionPath, e);
      }
    });
    LOG.info("Returning a total splits of " + rtSplits.size());
    return rtSplits.toArray(new InputSplit[0]);
  }

  // Return parquet file with a list of log files in the same file group.
  public static Map<String, List<String>> getRealtimeFileGroup(Configuration conf, Stream<FileStatus> fileStatuses) {
    Map<Path, List<FileStatus>> partitionsToParquetSplits =
        fileStatuses.collect(Collectors.groupingBy(file -> file.getPath().getParent()));
    // TODO(vc): Should we handle also non-hoodie splits here?
    Map<Path, HoodieTableMetaClient> partitionsToMetaClient = getMetaClientPerPartition(conf, partitionsToParquetSplits.keySet());

    // for all unique split parents, obtain all delta files based on delta commit timeline,
    // grouped on file id
    Map<String, List<String>> resultMap = new HashMap<>();
    partitionsToParquetSplits.keySet().forEach(partitionPath -> {
      // for each partition path obtain the data & log file groupings, then map back to inputsplits
      HoodieTableMetaClient metaClient = partitionsToMetaClient.get(partitionPath);
      HoodieTableFileSystemView fsView = new HoodieTableFileSystemView(metaClient, metaClient.getActiveTimeline());
      String relPartitionPath = FSUtils.getRelativePartitionPath(new Path(metaClient.getBasePath()), partitionPath);

      try {
        // Both commit and delta-commits are included - pick the latest completed one
        Option<HoodieInstant> latestCompletedInstant =
            metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants().lastInstant();

        Stream<FileSlice> latestFileSlices = latestCompletedInstant
            .map(instant -> fsView.getLatestMergedFileSlicesBeforeOrOn(relPartitionPath, instant.getTimestamp()))
            .orElse(Stream.empty());

        // subgroup splits again by file id & match with log files.
        Map<String, List<FileStatus>> groupedInputSplits = partitionsToParquetSplits.get(partitionPath).stream()
            .collect(Collectors.groupingBy(file -> FSUtils.getFileId(file.getPath().getName())));
        latestFileSlices.forEach(fileSlice -> {
          List<FileStatus> dataFileSplits = groupedInputSplits.get(fileSlice.getFileId());
          dataFileSplits.forEach(split -> {
            try {
              List<String> logFilePaths = fileSlice.getLogFiles().sorted(HoodieLogFile.getLogFileComparator())
                  .map(logFile -> logFile.getPath().toString()).collect(Collectors.toList());
              resultMap.put(split.getPath().toString(), logFilePaths);
            } catch (Exception e) {
              throw new HoodieException("Error creating hoodie real time split ", e);
            }
          });
        });
      } catch (Exception e) {
        throw new HoodieException("Error obtaining data file/log file grouping: " + partitionPath, e);
      }
    });
    return resultMap;
  }

}
