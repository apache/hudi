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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.hadoop.realtime.HoodieRealtimeBootstrapBaseFileSplit;
import org.apache.hudi.hadoop.realtime.HoodieRealtimeFileSplit;
import org.apache.hudi.hadoop.realtime.HoodieVirtualKeyInfo;
import org.apache.hudi.hadoop.realtime.RealtimeSplit;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.TypeUtils.unsafeCast;

public class HoodieRealtimeInputFormatUtils extends HoodieInputFormatUtils {

  private static final Logger LOG = LogManager.getLogger(HoodieRealtimeInputFormatUtils.class);

  public static boolean doesBelongToIncrementalQuery(FileSplit s) {
    if (s instanceof HoodieRealtimeFileSplit) {
      HoodieRealtimeFileSplit bs = unsafeCast(s);
      return bs.getBelongsToIncrementalQuery();
    } else if (s instanceof HoodieRealtimeBootstrapBaseFileSplit) {
      HoodieRealtimeBootstrapBaseFileSplit bs = unsafeCast(s);
      return bs.getBelongsToIncrementalQuery();
    }

    return false;
  }

  // Return parquet file with a list of log files in the same file group.
  public static List<Pair<Option<HoodieBaseFile>, List<String>>> groupLogsByBaseFile(Configuration conf, List<Path> partitionPaths) {
    Set<Path> partitionSet = new HashSet<>(partitionPaths);
    // TODO(vc): Should we handle also non-hoodie splits here?
    Map<Path, HoodieTableMetaClient> partitionsToMetaClient = getTableMetaClientByPartitionPath(conf, partitionSet);

    // Get all the base file and it's log files pairs in required partition paths.
    List<Pair<Option<HoodieBaseFile>, List<String>>> baseAndLogsList = new ArrayList<>();
    partitionSet.forEach(partitionPath -> {
      // for each partition path obtain the data & log file groupings, then map back to inputsplits
      HoodieTableMetaClient metaClient = partitionsToMetaClient.get(partitionPath);
      HoodieTableFileSystemView fsView = new HoodieTableFileSystemView(metaClient, metaClient.getActiveTimeline());
      String relPartitionPath = FSUtils.getRelativePartitionPath(new Path(metaClient.getBasePath()), partitionPath);

      try {
        // Both commit and delta-commits are included - pick the latest completed one
        Option<HoodieInstant> latestCompletedInstant =
            metaClient.getCommitsAndCompactionTimeline().filterCompletedAndCompactionInstants().lastInstant();

        Stream<FileSlice> latestFileSlices = latestCompletedInstant
            .map(instant -> fsView.getLatestMergedFileSlicesBeforeOrOn(relPartitionPath, instant.getTimestamp()))
            .orElse(Stream.empty());

        latestFileSlices.forEach(fileSlice -> {
          List<String> logFilePaths = fileSlice.getLogFiles().sorted(HoodieLogFile.getLogFileComparator())
                  .map(logFile -> logFile.getPath().toString()).collect(Collectors.toList());
          baseAndLogsList.add(Pair.of(fileSlice.getBaseFile(), logFilePaths));
        });
      } catch (Exception e) {
        throw new HoodieException("Error obtaining data file/log file grouping: " + partitionPath, e);
      }
    });
    return baseAndLogsList;
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

  public static void addRequiredProjectionFields(Configuration configuration, Option<HoodieVirtualKeyInfo> hoodieVirtualKeyInfo) {
    // Need this to do merge records in HoodieRealtimeRecordReader
    if (!hoodieVirtualKeyInfo.isPresent()) {
      addProjectionField(configuration, HoodieRecord.RECORD_KEY_METADATA_FIELD, HoodieInputFormatUtils.HOODIE_RECORD_KEY_COL_POS);
      addProjectionField(configuration, HoodieRecord.COMMIT_TIME_METADATA_FIELD, HoodieInputFormatUtils.HOODIE_COMMIT_TIME_COL_POS);
      addProjectionField(configuration, HoodieRecord.PARTITION_PATH_METADATA_FIELD, HoodieInputFormatUtils.HOODIE_PARTITION_PATH_COL_POS);
    } else {
      HoodieVirtualKeyInfo hoodieVirtualKey = hoodieVirtualKeyInfo.get();
      addProjectionField(configuration, hoodieVirtualKey.getRecordKeyField(), hoodieVirtualKey.getRecordKeyFieldIndex());
      addProjectionField(configuration, hoodieVirtualKey.getPartitionPathField(), hoodieVirtualKey.getPartitionPathFieldIndex());
    }
  }

  public static boolean requiredProjectionFieldsExistInConf(Configuration configuration, Option<HoodieVirtualKeyInfo> hoodieVirtualKeyInfo) {
    String readColNames = configuration.get(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, "");
    if (!hoodieVirtualKeyInfo.isPresent()) {
      return readColNames.contains(HoodieRecord.RECORD_KEY_METADATA_FIELD)
          && readColNames.contains(HoodieRecord.COMMIT_TIME_METADATA_FIELD)
          && readColNames.contains(HoodieRecord.PARTITION_PATH_METADATA_FIELD);
    } else {
      return readColNames.contains(hoodieVirtualKeyInfo.get().getRecordKeyField())
          && readColNames.contains(hoodieVirtualKeyInfo.get().getPartitionPathField());
    }
  }

  public static boolean canAddProjectionToJobConf(final RealtimeSplit realtimeSplit, final JobConf jobConf) {
    return jobConf.get(HoodieInputFormatUtils.HOODIE_READ_COLUMNS_PROP) == null
            || (!realtimeSplit.getDeltaLogPaths().isEmpty() && !HoodieRealtimeInputFormatUtils.requiredProjectionFieldsExistInConf(jobConf, realtimeSplit.getVirtualKeyInfo()));
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
