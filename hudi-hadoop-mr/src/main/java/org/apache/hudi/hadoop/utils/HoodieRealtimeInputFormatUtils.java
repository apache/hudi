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

import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.hadoop.BootstrapBaseFileSplit;
import org.apache.hudi.hadoop.realtime.HoodieRealtimeFileSplit;
import org.apache.hudi.hadoop.realtime.RealtimeBootstrapBaseFileSplit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.SplitLocationInfo;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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
    Map<Path, HoodieTableMetaClient> partitionsToMetaClient = getTableMetaClientByBasePath(conf, partitionsToParquetSplits.keySet());

    // Create file system cache so metadata table is only instantiated once. Also can benefit normal file listing if
    // partition path is listed twice so file groups will already be loaded in file system
    Map<HoodieTableMetaClient, HoodieTableFileSystemView> fsCache = new HashMap<>();
    // for all unique split parents, obtain all delta files based on delta commit timeline,
    // grouped on file id
    List<InputSplit> rtSplits = new ArrayList<>();
    try {
      partitionsToParquetSplits.keySet().forEach(partitionPath -> {
        // for each partition path obtain the data & log file groupings, then map back to inputsplits
        HoodieTableMetaClient metaClient = partitionsToMetaClient.get(partitionPath);
        if (!fsCache.containsKey(metaClient)) {
          HoodieLocalEngineContext engineContext = new HoodieLocalEngineContext(conf);
          HoodieTableFileSystemView fsView = FileSystemViewManager.createInMemoryFileSystemView(engineContext,
              metaClient, HoodieInputFormatUtils.buildMetadataConfig(conf));
          fsCache.put(metaClient, fsView);
        }
        HoodieTableFileSystemView fsView = fsCache.get(metaClient);

        String relPartitionPath = FSUtils.getRelativePartitionPath(new Path(metaClient.getBasePath()), partitionPath);
        // Both commit and delta-commits are included - pick the latest completed one
        Option<HoodieInstant> latestCompletedInstant =
            metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants().lastInstant();

        Stream<FileSlice> latestFileSlices = latestCompletedInstant
            .map(instant -> fsView.getLatestMergedFileSlicesBeforeOrOn(relPartitionPath, instant.getTimestamp()))
            .orElse(Stream.empty());

        // subgroup splits again by file id & match with log files.
        Map<String, List<FileSplit>> groupedInputSplits = partitionsToParquetSplits.get(partitionPath).stream()
            .collect(Collectors.groupingBy(split -> FSUtils.getFileId(split.getPath().getName())));
        // Get the maxCommit from the last delta or compaction or commit - when bootstrapped from COW table
        String maxCommitTime = metaClient.getActiveTimeline().getTimelineOfActions(CollectionUtils.createSet(HoodieTimeline.COMMIT_ACTION,
            HoodieTimeline.ROLLBACK_ACTION, HoodieTimeline.DELTA_COMMIT_ACTION, HoodieTimeline.REPLACE_COMMIT_ACTION))
            .filterCompletedInstants().lastInstant().get().getTimestamp();
        latestFileSlices.forEach(fileSlice -> {
          List<FileSplit> dataFileSplits = groupedInputSplits.get(fileSlice.getFileId());
          dataFileSplits.forEach(split -> {
            try {
              List<String> logFilePaths = fileSlice.getLogFiles().sorted(HoodieLogFile.getLogFileComparator())
                  .map(logFile -> logFile.getPath().toString()).collect(Collectors.toList());
              if (split instanceof BootstrapBaseFileSplit) {
                BootstrapBaseFileSplit eSplit = (BootstrapBaseFileSplit) split;
                String[] hosts = split.getLocationInfo() != null ? Arrays.stream(split.getLocationInfo())
                    .filter(x -> !x.isInMemory()).toArray(String[]::new) : new String[0];
                String[] inMemoryHosts = split.getLocationInfo() != null ? Arrays.stream(split.getLocationInfo())
                    .filter(SplitLocationInfo::isInMemory).toArray(String[]::new) : new String[0];
                FileSplit baseSplit = new FileSplit(eSplit.getPath(), eSplit.getStart(), eSplit.getLength(),
                    hosts, inMemoryHosts);
                rtSplits.add(new RealtimeBootstrapBaseFileSplit(baseSplit, metaClient.getBasePath(),
                    logFilePaths, maxCommitTime, eSplit.getBootstrapFileSplit()));
              } else {
                rtSplits.add(new HoodieRealtimeFileSplit(split, metaClient.getBasePath(), logFilePaths, maxCommitTime));
              }
            } catch (IOException e) {
              throw new HoodieIOException("Error creating hoodie real time split ", e);
            }
          });
        });
      });
    } catch (Exception e) {
      throw new HoodieException("Error obtaining data file/log file grouping ", e);
    } finally {
      // close all the open fs views.
      fsCache.forEach((k, view) -> view.close());
    }
    LOG.info("Returning a total splits of " + rtSplits.size());
    return rtSplits.toArray(new InputSplit[0]);
  }

  // Return parquet file with a list of log files in the same file group.
  public static Map<HoodieBaseFile, List<String>> groupLogsByBaseFile(Configuration conf, List<HoodieBaseFile> fileStatuses) {
    Map<Path, List<HoodieBaseFile>> partitionsToParquetSplits =
        fileStatuses.stream().collect(Collectors.groupingBy(file -> file.getFileStatus().getPath().getParent()));
    // TODO(vc): Should we handle also non-hoodie splits here?
    Map<Path, HoodieTableMetaClient> partitionsToMetaClient = getTableMetaClientByBasePath(conf, partitionsToParquetSplits.keySet());

    // for all unique split parents, obtain all delta files based on delta commit timeline,
    // grouped on file id
    Map<HoodieBaseFile, List<String>> resultMap = new HashMap<>();
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
        Map<String, List<HoodieBaseFile>> groupedInputSplits = partitionsToParquetSplits.get(partitionPath).stream()
            .collect(Collectors.groupingBy(file -> FSUtils.getFileId(file.getFileStatus().getPath().getName())));
        latestFileSlices.forEach(fileSlice -> {
          List<HoodieBaseFile> dataFileSplits = groupedInputSplits.get(fileSlice.getFileId());
          dataFileSplits.forEach(split -> {
            try {
              List<String> logFilePaths = fileSlice.getLogFiles().sorted(HoodieLogFile.getLogFileComparator())
                  .map(logFile -> logFile.getPath().toString()).collect(Collectors.toList());
              resultMap.put(split, logFilePaths);
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


  /**
   * Add a field to the existing fields projected.
   */
  private static Configuration addProjectionField(Configuration conf, String fieldName, int fieldIndex) {
    String readColNames = conf.get(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, "");
    String readColIds = conf.get(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, "");

    String readColNamesPrefix = readColNames + ",";
    if (readColNames == null || readColNames.isEmpty()) {
      readColNamesPrefix = "";
    }
    String readColIdsPrefix = readColIds + ",";
    if (readColIds == null || readColIds.isEmpty()) {
      readColIdsPrefix = "";
    }

    if (!readColNames.contains(fieldName)) {
      // If not already in the list - then add it
      conf.set(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, readColNamesPrefix + fieldName);
      conf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, readColIdsPrefix + fieldIndex);
      if (LOG.isDebugEnabled()) {
        LOG.debug(String.format("Adding extra column " + fieldName + ", to enable log merging cols (%s) ids (%s) ",
            conf.get(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR),
            conf.get(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR)));
      }
    }
    return conf;
  }

  public static void addRequiredProjectionFields(Configuration configuration) {
    // Need this to do merge records in HoodieRealtimeRecordReader
    addProjectionField(configuration, HoodieRecord.RECORD_KEY_METADATA_FIELD, HoodieInputFormatUtils.HOODIE_RECORD_KEY_COL_POS);
    addProjectionField(configuration, HoodieRecord.COMMIT_TIME_METADATA_FIELD, HoodieInputFormatUtils.HOODIE_COMMIT_TIME_COL_POS);
    addProjectionField(configuration, HoodieRecord.PARTITION_PATH_METADATA_FIELD, HoodieInputFormatUtils.HOODIE_PARTITION_PATH_COL_POS);
  }

  /**
   * Hive will append read columns' ids to old columns' ids during getRecordReader. In some cases, e.g. SELECT COUNT(*),
   * the read columns' id is an empty string and Hive will combine it with Hoodie required projection ids and becomes
   * e.g. ",2,0,3" and will cause an error. Actually this method is a temporary solution because the real bug is from
   * Hive. Hive has fixed this bug after 3.0.0, but the version before that would still face this problem. (HIVE-22438)
   */
  public static void cleanProjectionColumnIds(Configuration conf) {
    String columnIds = conf.get(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR);
    if (!columnIds.isEmpty() && columnIds.charAt(0) == ',') {
      conf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, columnIds.substring(1));
      if (LOG.isDebugEnabled()) {
        LOG.debug("The projection Ids: {" + columnIds + "} start with ','. First comma is removed");
      }
    }
  }
}
