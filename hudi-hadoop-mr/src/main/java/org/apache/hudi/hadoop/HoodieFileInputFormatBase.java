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

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.hadoop.utils.HoodieHiveUtils;
import org.apache.hudi.hadoop.utils.HoodieInputFormatUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Base implementation of the Hive's {@link FileInputFormat} allowing for reading of Hudi's
 * Copy-on-Write (COW) tables in various configurations:
 *
 * <ul>
 *   <li>Snapshot mode: reading table's state as of particular timestamp (or instant, in Hudi's terms)</li>
 *   <li>Incremental mode: reading table's state as of particular timestamp (or instant, in Hudi's terms)</li>
 *   <li>External mode: reading non-Hudi partitions</li>
 * </ul>
 */
public abstract class HoodieFileInputFormatBase extends FileInputFormat<NullWritable, ArrayWritable>
    implements Configurable {

  protected Configuration conf;

  protected abstract boolean includeLogFilesForSnapShotView();

  @Override
  public final Configuration getConf() {
    return conf;
  }

  @Override
  public final void setConf(Configuration conf) {
    this.conf = conf;
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
      FileStatus[] fileStatuses = doListStatus(job);
      returns.addAll(Arrays.asList(fileStatuses));
    }

    // process snapshot queries next.
    List<Path> snapshotPaths = inputPathHandler.getSnapshotPaths();
    if (snapshotPaths.size() > 0) {
      returns.addAll(HoodieInputFormatUtils.filterFileStatusForSnapshotMode(job, tableMetaClientMap, snapshotPaths, includeLogFilesForSnapShotView()));
    }
    return returns.toArray(new FileStatus[0]);
  }

  /**
   * Achieves listStatus functionality for an incrementally queried table. Instead of listing all
   * partitions and then filtering based on the commits of interest, this logic first extracts the
   * partitions touched by the desired commits and then lists only those partitions.
   */
  protected List<FileStatus> listStatusForIncrementalMode(
      JobConf job, HoodieTableMetaClient tableMetaClient, List<Path> inputPaths) throws IOException {
    String tableName = tableMetaClient.getTableConfig().getTableName();
    Job jobContext = Job.getInstance(job);
    Option<HoodieTimeline> timeline = HoodieInputFormatUtils.getFilteredCommitsTimeline(jobContext, tableMetaClient);
    if (!timeline.isPresent()) {
      return null;
    }
    Option<List<HoodieInstant>> commitsToCheck = HoodieInputFormatUtils.getCommitsForIncrementalQuery(jobContext, tableName, timeline.get());
    if (!commitsToCheck.isPresent()) {
      return null;
    }
    Option<String> incrementalInputPaths = HoodieInputFormatUtils.getAffectedPartitions(commitsToCheck.get(), tableMetaClient, timeline.get(), inputPaths);
    // Mutate the JobConf to set the input paths to only partitions touched by incremental pull.
    if (!incrementalInputPaths.isPresent()) {
      return null;
    }
    setInputPaths(job, incrementalInputPaths.get());
    FileStatus[] fileStatuses = doListStatus(job);
    return HoodieInputFormatUtils.filterIncrementalFileStatus(jobContext, tableMetaClient, timeline.get(), fileStatuses, commitsToCheck.get());
  }

  /**
   * Abstracts and exposes {@link FileInputFormat#listStatus(JobConf)} operation to subclasses that
   * lists files (returning an array of {@link FileStatus}) corresponding to the input paths specified
   * as part of provided {@link JobConf}
   */
  protected final FileStatus[] doListStatus(JobConf job) throws IOException {
    return super.listStatus(job);
  }
}
