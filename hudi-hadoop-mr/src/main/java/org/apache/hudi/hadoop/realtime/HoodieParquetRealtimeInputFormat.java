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

package org.apache.hudi.hadoop.realtime;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieDefaultTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.hadoop.HoodieParquetInputFormat;
import org.apache.hudi.hadoop.InputPathHandler;
import org.apache.hudi.hadoop.UseFileSplitsFromInputFormat;
import org.apache.hudi.hadoop.utils.HoodieHiveUtils;
import org.apache.hudi.hadoop.utils.HoodieInputFormatUtils;
import org.apache.hudi.hadoop.utils.HoodieRealtimeInputFormatUtils;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hudi.hadoop.UseRecordReaderFromInputFormat;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.mortbay.log.Log;

import static org.apache.hudi.hadoop.utils.HoodieHiveUtils.getIncrementalTableNames;

/**
 * Input Format, that provides a real-time view of data in a Hoodie table.
 */
@UseRecordReaderFromInputFormat
@UseFileSplitsFromInputFormat
public class HoodieParquetRealtimeInputFormat extends HoodieParquetInputFormat implements Configurable {

  private static final Logger LOG = LogManager.getLogger(HoodieParquetRealtimeInputFormat.class);

  // To make Hive on Spark queries work with RT tables. Our theory is that due to
  // {@link org.apache.hadoop.hive.ql.io.parquet.ProjectionPusher}
  // not handling empty list correctly, the ParquetRecordReaderWrapper ends up adding the same column ids multiple
  // times which ultimately breaks the query.

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {

    // is this an incremental query
    List<String> incrementalTables = getIncrementalTableNames(Job.getInstance(job));
    if (!incrementalTables.isEmpty()) {
      //TODO For now assuming the query can be either incremental or snapshot and NOT both.
      return getSplitsForIncrementalQueries(job, incrementalTables);
    }

    Stream<FileSplit> fileSplits = Arrays.stream(super.getSplits(job, numSplits)).map(is -> (FileSplit) is);

    return HoodieRealtimeInputFormatUtils.getRealtimeSplits(job, fileSplits);
  }

  protected InputSplit[] getSplitsForIncrementalQueries(JobConf job, List<String> incrementalTables) throws IOException {
    InputPathHandler inputPathHandler = new InputPathHandler(conf, getInputPaths(job), incrementalTables);
    Map<String, HoodieTableMetaClient> tableMetaClientMap = inputPathHandler.getTableMetaClientMap();
    List<InputSplit> splits = new ArrayList<>();

    for (String table : incrementalTables) {
      HoodieTableMetaClient metaClient = tableMetaClientMap.get(table);
      if (metaClient == null) {
        /* This can happen when the INCREMENTAL mode is set for a table but there were no InputPaths
         * in the jobConf
         */
        continue;
      }
      String tableName = metaClient.getTableConfig().getTableName();
      Path basePath = new Path(metaClient.getBasePath());
      HoodieTimeline timeline = metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants();
      String lastIncrementalTs = HoodieHiveUtils.readStartCommitTime(Job.getInstance(job), tableName);
      // Total number of commits to return in this batch. Set this to -1 to get all the commits.
      Integer maxCommits = HoodieHiveUtils.readMaxCommits(Job.getInstance(job), tableName);
      LOG.info("Last Incremental timestamp for table: " + table + ", was set as " + lastIncrementalTs);
      List<HoodieInstant> commitsToCheck = timeline.findInstantsAfter(lastIncrementalTs, maxCommits)
          .getInstants().collect(Collectors.toList());

      Map<String, List<FileStatus>> partitionToFileStatusesMap = listStatusForAffectedPartitions(basePath, commitsToCheck, timeline);

      List<FileStatus> fileStatuses = new ArrayList<>();
      for (List<FileStatus> statuses: partitionToFileStatusesMap.values()) {
        fileStatuses.addAll(statuses);
      }
      LOG.info("Stats after applying Hudi incremental filter: total_commits_to_check: " + commitsToCheck.size()
          + ", total_partitions_touched: " + partitionToFileStatusesMap.size() + ", total_files_processed: "
          + fileStatuses.size());
      FileStatus[] statuses = fileStatuses.toArray(new FileStatus[0]);
      List<String> commitsList = commitsToCheck.stream().map(HoodieInstant::getTimestamp).collect(Collectors.toList());
      HoodieTableFileSystemView fsView = new HoodieTableFileSystemView(metaClient, timeline, statuses);

      // Iterate partitions to create splits
      partitionToFileStatusesMap.keySet().forEach(path -> {
        // create an Incremental Split for each file group.
        fsView.getAllFileGroups(path)
            .forEach(
                fileGroup -> splits.add(
                    new HoodieMORIncrementalFileSplit(fileGroup, basePath.toString(), commitsList.get(commitsList.size() - 1))
            ));
      });
    }
    Log.info("Total splits generated: " + splits.size());
    return splits.toArray(new InputSplit[0]);
  }

  private Map<String, List<FileStatus>> listStatusForAffectedPartitions(
      Path basePath, List<HoodieInstant> commitsToCheck, HoodieTimeline timeline) throws IOException {
    // Extract files touched by these commits.
    // TODO This might need to be done in parallel like listStatus parallelism ?
    HashMap<String, List<FileStatus>> partitionToFileStatusesMap = new HashMap<>();
    for (HoodieInstant commit: commitsToCheck) {
      HoodieCommitMetadata commitMetadata = HoodieCommitMetadata.fromBytes(timeline.getInstantDetails(commit).get(),
          HoodieCommitMetadata.class);
      for (Map.Entry<String, List<HoodieWriteStat>> entry: commitMetadata.getPartitionToWriteStats().entrySet()) {
        if (!partitionToFileStatusesMap.containsKey(entry.getKey())) {
          partitionToFileStatusesMap.put(entry.getKey(), new ArrayList<>());
        }
        for (HoodieWriteStat stat : entry.getValue()) {
          String relativeFilePath = stat.getPath();
          Path fullPath = relativeFilePath != null ? FSUtils.getPartitionPath(basePath, relativeFilePath) : null;
          if (fullPath != null) {
            //TODO Should the length of file be totalWriteBytes or fileSizeInBytes?
            FileStatus fs = new FileStatus(stat.getTotalWriteBytes(), false, 0, 0,
                0, fullPath);
            partitionToFileStatusesMap.get(entry.getKey()).add(fs);
          }
        }
      }
    }
    return partitionToFileStatusesMap;
  }

  @Override
  protected HoodieDefaultTimeline filterInstantsTimeline(HoodieDefaultTimeline timeline) {
    // no specific filtering for Realtime format
    return timeline;
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

  private static void addRequiredProjectionFields(Configuration configuration) {
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
  private static void cleanProjectionColumnIds(Configuration conf) {
    String columnIds = conf.get(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR);
    if (!columnIds.isEmpty() && columnIds.charAt(0) == ',') {
      conf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, columnIds.substring(1));
      if (LOG.isDebugEnabled()) {
        LOG.debug("The projection Ids: {" + columnIds + "} start with ','. First comma is removed");
      }
    }
  }

  @Override
  public RecordReader<NullWritable, ArrayWritable> getRecordReader(final InputSplit split, final JobConf jobConf,
      final Reporter reporter) throws IOException {
    // Hive on Spark invokes multiple getRecordReaders from different threads in the same spark task (and hence the
    // same JVM) unlike Hive on MR. Due to this, accesses to JobConf, which is shared across all threads, is at the
    // risk of experiencing race conditions. Hence, we synchronize on the JobConf object here. There is negligible
    // latency incurred here due to the synchronization since get record reader is called once per spilt before the
    // actual heavy lifting of reading the parquet files happen.
    if (jobConf.get(HoodieInputFormatUtils.HOODIE_READ_COLUMNS_PROP) == null) {
      synchronized (jobConf) {
        LOG.info(
            "Before adding Hoodie columns, Projections :" + jobConf.get(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR)
                + ", Ids :" + jobConf.get(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR));
        if (jobConf.get(HoodieInputFormatUtils.HOODIE_READ_COLUMNS_PROP) == null) {
          // Hive (across all versions) fails for queries like select count(`_hoodie_commit_time`) from table;
          // In this case, the projection fields gets removed. Looking at HiveInputFormat implementation, in some cases
          // hoodie additional projection columns are reset after calling setConf and only natural projections
          // (one found in select queries) are set. things would break because of this.
          // For e:g _hoodie_record_key would be missing and merge step would throw exceptions.
          // TO fix this, hoodie columns are appended late at the time record-reader gets built instead of construction
          // time.
          cleanProjectionColumnIds(jobConf);
          addRequiredProjectionFields(jobConf);

          this.conf = jobConf;
          this.conf.set(HoodieInputFormatUtils.HOODIE_READ_COLUMNS_PROP, "true");
        }
      }
    }

    LOG.info("Creating record reader with readCols :" + jobConf.get(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR)
        + ", Ids :" + jobConf.get(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR));
    // sanity check
    ValidationUtils.checkArgument(split instanceof HoodieRealtimeFileSplit || split instanceof HoodieMORIncrementalFileSplit,
        "HoodieRealtimeRecordReader can only work on HoodieRealtimeFileSplit and not with " + split);

    if (split instanceof HoodieRealtimeFileSplit) {
      return new HoodieRealtimeRecordReader((HoodieRealtimeFileSplit) split, jobConf,
          super.getRecordReader(split, jobConf, reporter));
    }

    return new HoodieMORIncrementalRecordReader((HoodieMORIncrementalFileSplit) split, jobConf, reporter);
  }

  @Override
  public Configuration getConf() {
    return conf;
  }
}