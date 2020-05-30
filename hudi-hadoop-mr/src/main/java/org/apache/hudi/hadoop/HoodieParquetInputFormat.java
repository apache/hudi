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

package org.apache.hudi.hadoop;

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieDefaultTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.hadoop.utils.HoodieHiveUtils;
import org.apache.hudi.hadoop.utils.HoodieInputFormatUtils;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * HoodieInputFormat which understands the Hoodie File Structure and filters files based on the Hoodie Mode. If paths
 * that does not correspond to a hoodie table then they are passed in as is (as what FileInputFormat.listStatus()
 * would do). The JobConf could have paths from multipe Hoodie/Non-Hoodie tables
 */
@UseFileSplitsFromInputFormat
public class HoodieParquetInputFormat extends MapredParquetInputFormat implements Configurable {

  private static final Logger LOG = LogManager.getLogger(HoodieParquetInputFormat.class);

  protected Configuration conf;

  protected HoodieDefaultTimeline filterInstantsTimeline(HoodieDefaultTimeline timeline) {
    return HoodieInputFormatUtils.filterInstantsTimeline(timeline);
  }

  @Override
  public FileStatus[] listStatus(JobConf job) throws IOException {
    // Segregate inputPaths[] to incremental, snapshot and non hoodie paths
    List<String> incrementalTables = HoodieHiveUtils.getIncrementalTableNames(Job.getInstance(job));
    InputPathHandler inputPathHandler = new InputPathHandler(conf, getInputPaths(job), incrementalTables);
    List<FileStatus> returns = new ArrayList<>();

    Map<String, HoodieTableMetaClient> tableMetaClientMap = inputPathHandler.getTableMetaClientMap();
    // process incremental pulls first
    for (String table : incrementalTables) {
      HoodieTableMetaClient metaClient = tableMetaClientMap.get(table);
      if (metaClient == null) {
        /* This can happen when the INCREMENTAL mode is set for a table but there were no InputPaths
         * in the jobConf
         */
        continue;
      }
      List<Path> inputPaths = inputPathHandler.getGroupedIncrementalPaths().get(metaClient);
      List<FileStatus> result = listStatusForIncrementalMode(job, metaClient, inputPaths);
      if (result != null) {
        returns.addAll(result);
      }
    }

    // process non hoodie Paths next.
    List<Path> nonHoodiePaths = inputPathHandler.getNonHoodieInputPaths();
    if (nonHoodiePaths.size() > 0) {
      setInputPaths(job, nonHoodiePaths.toArray(new Path[nonHoodiePaths.size()]));
      FileStatus[] fileStatuses = super.listStatus(job);
      returns.addAll(Arrays.asList(fileStatuses));
    }

    // process snapshot queries next.
    List<Path> snapshotPaths = inputPathHandler.getSnapshotPaths();
    if (snapshotPaths.size() > 0) {
      setInputPaths(job, snapshotPaths.toArray(new Path[snapshotPaths.size()]));
      FileStatus[] fileStatuses = super.listStatus(job);
      Map<HoodieTableMetaClient, List<FileStatus>> groupedFileStatus =
          HoodieInputFormatUtils.groupFileStatusForSnapshotPaths(fileStatuses, tableMetaClientMap.values());
      LOG.info("Found a total of " + groupedFileStatus.size() + " groups");
      for (Map.Entry<HoodieTableMetaClient, List<FileStatus>> entry : groupedFileStatus.entrySet()) {
        List<FileStatus> result = HoodieInputFormatUtils.filterFileStatusForSnapshotMode(job, entry.getKey(), entry.getValue());
        if (result != null) {
          returns.addAll(result);
        }
      }
    }
    return returns.toArray(new FileStatus[returns.size()]);
  }

  /**
   * Achieves listStatus functionality for an incrementally queried table. Instead of listing all
   * partitions and then filtering based on the commits of interest, this logic first extracts the
   * partitions touched by the desired commits and then lists only those partitions.
   */
  private List<FileStatus> listStatusForIncrementalMode(
      JobConf job, HoodieTableMetaClient tableMetaClient, List<Path> inputPaths) throws IOException {
    String tableName = tableMetaClient.getTableConfig().getTableName();
    Job jobContext = Job.getInstance(job);
    Option<HoodieTimeline> timeline = HoodieInputFormatUtils.getTimeline(jobContext, tableMetaClient);
    if (!timeline.isPresent()) {
      return null;
    }
    Option<List<HoodieInstant>> commitsToCheck = HoodieInputFormatUtils.getCommitsToCheck(jobContext, tableName, timeline.get());
    if (!commitsToCheck.isPresent()) {
      return null;
    }
    Option<String> incrementalInputPaths = HoodieInputFormatUtils.getAffectedPartitions(commitsToCheck.get(), tableMetaClient, timeline.get(), inputPaths);
    // Mutate the JobConf to set the input paths to only partitions touched by incremental pull.
    if (!incrementalInputPaths.isPresent()) {
      return null;
    }
    setInputPaths(job, incrementalInputPaths.get());
    FileStatus[] fileStatuses = super.listStatus(job);
    return HoodieInputFormatUtils.filterIncrementalFileStatus(jobContext, tableMetaClient, timeline.get(), fileStatuses, commitsToCheck.get());
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public RecordReader<NullWritable, ArrayWritable> getRecordReader(final InputSplit split, final JobConf job,
      final Reporter reporter) throws IOException {
    // TODO enable automatic predicate pushdown after fixing issues
    // FileSplit fileSplit = (FileSplit) split;
    // HoodieTableMetadata metadata = getTableMetadata(fileSplit.getPath().getParent());
    // String tableName = metadata.getTableName();
    // String mode = HoodieHiveUtil.readMode(job, tableName);

    // if (HoodieHiveUtil.INCREMENTAL_SCAN_MODE.equals(mode)) {
    // FilterPredicate predicate = constructHoodiePredicate(job, tableName, split);
    // LOG.info("Setting parquet predicate push down as " + predicate);
    // ParquetInputFormat.setFilterPredicate(job, predicate);
    // clearOutExistingPredicate(job);
    // }
    return super.getRecordReader(split, job, reporter);
  }

}
