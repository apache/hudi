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

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieDefaultTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.hadoop.utils.HoodieHiveUtils;
import org.apache.hudi.hadoop.utils.HoodieInputFormatUtils;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat;
import org.apache.hadoop.hive.ql.io.sarg.ConvertAstToSearchArg;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileSplit;
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
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * HoodieInputFormat which understands the Hoodie File Structure and filters files based on the Hoodie Mode. If paths
 * that does not correspond to a hoodie table then they are passed in as is (as what FileInputFormat.listStatus()
 * would do). The JobConf could have paths from multiple Hoodie/Non-Hoodie tables
 */
@UseRecordReaderFromInputFormat
@UseFileSplitsFromInputFormat
public class HoodieParquetInputFormat extends MapredParquetInputFormat implements Configurable {

  private static final Logger LOG = LogManager.getLogger(HoodieParquetInputFormat.class);

  protected Configuration conf;

  protected HoodieDefaultTimeline filterInstantsTimeline(HoodieDefaultTimeline timeline) {
    return HoodieInputFormatUtils.filterInstantsTimeline(timeline);
  }

  protected FileStatus[] getStatus(JobConf job) throws IOException {
    return super.listStatus(job);
  }

  protected boolean includeLogFilesForSnapShotView() {
    return false;
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
    if (split instanceof BootstrapBaseFileSplit) {
      BootstrapBaseFileSplit eSplit = (BootstrapBaseFileSplit) split;
      String[] rawColNames = HoodieColumnProjectionUtils.getReadColumnNames(job);
      List<Integer> rawColIds = HoodieColumnProjectionUtils.getReadColumnIDs(job);
      List<Pair<Integer, String>> projectedColsWithIndex =
          IntStream.range(0, rawColIds.size()).mapToObj(idx -> Pair.of(rawColIds.get(idx), rawColNames[idx]))
              .collect(Collectors.toList());

      List<Pair<Integer, String>> hoodieColsProjected = projectedColsWithIndex.stream()
          .filter(idxWithName -> HoodieRecord.HOODIE_META_COLUMNS.contains(idxWithName.getValue()))
          .collect(Collectors.toList());
      List<Pair<Integer, String>> externalColsProjected = projectedColsWithIndex.stream()
          .filter(idxWithName -> !HoodieRecord.HOODIE_META_COLUMNS.contains(idxWithName.getValue())
              && !HoodieHiveUtils.VIRTUAL_COLUMN_NAMES.contains(idxWithName.getValue()))
          .collect(Collectors.toList());

      // This always matches hive table description
      List<Pair<String, String>> colNameWithTypes = HoodieColumnProjectionUtils.getIOColumnNameAndTypes(job);
      List<Pair<String, String>> colNamesWithTypesForExternal = colNameWithTypes.stream()
          .filter(p -> !HoodieRecord.HOODIE_META_COLUMNS.contains(p.getKey())).collect(Collectors.toList());
      LOG.info("colNameWithTypes =" + colNameWithTypes + ", Num Entries =" + colNameWithTypes.size());
      if (hoodieColsProjected.isEmpty()) {
        return super.getRecordReader(eSplit.getBootstrapFileSplit(), job, reporter);
      } else if (externalColsProjected.isEmpty()) {
        return super.getRecordReader(split, job, reporter);
      } else {
        FileSplit rightSplit = eSplit.getBootstrapFileSplit();
        // Hive PPD works at row-group level and only enabled when hive.optimize.index.filter=true;
        // The above config is disabled by default. But when enabled, would cause misalignment between
        // skeleton and bootstrap file. We will disable them specifically when query needs bootstrap and skeleton
        // file to be stitched.
        // This disables row-group filtering
        JobConf jobConfCopy = new JobConf(job);
        jobConfCopy.unset(TableScanDesc.FILTER_EXPR_CONF_STR);
        jobConfCopy.unset(ConvertAstToSearchArg.SARG_PUSHDOWN);

        LOG.info("Generating column stitching reader for " + eSplit.getPath() + " and " + rightSplit.getPath());
        return new BootstrapColumnStichingRecordReader(super.getRecordReader(eSplit, jobConfCopy, reporter),
            HoodieRecord.HOODIE_META_COLUMNS.size(),
            super.getRecordReader(rightSplit, jobConfCopy, reporter),
            colNamesWithTypesForExternal.size(),
            true);
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("EMPLOYING DEFAULT RECORD READER - " + split);
    }
    return super.getRecordReader(split, job, reporter);
  }

  @Override
  protected boolean isSplitable(FileSystem fs, Path filename) {
    return !(filename instanceof PathWithBootstrapFileStatus);
  }

  @Override
  protected FileSplit makeSplit(Path file, long start, long length,
                                String[] hosts) {
    FileSplit split = new FileSplit(file, start, length, hosts);

    if (file instanceof PathWithBootstrapFileStatus) {
      return makeExternalFileSplit((PathWithBootstrapFileStatus)file, split);
    }
    return split;
  }

  @Override
  protected FileSplit makeSplit(Path file, long start, long length,
                                String[] hosts, String[] inMemoryHosts) {
    FileSplit split = new FileSplit(file, start, length, hosts, inMemoryHosts);
    if (file instanceof PathWithBootstrapFileStatus) {
      return makeExternalFileSplit((PathWithBootstrapFileStatus)file, split);
    }
    return split;
  }

  private BootstrapBaseFileSplit makeExternalFileSplit(PathWithBootstrapFileStatus file, FileSplit split) {
    try {
      LOG.info("Making external data split for " + file);
      FileStatus externalFileStatus = file.getBootstrapFileStatus();
      FileSplit externalFileSplit = makeSplit(externalFileStatus.getPath(), 0, externalFileStatus.getLen(),
          new String[0], new String[0]);
      return new BootstrapBaseFileSplit(split, externalFileSplit);
    } catch (IOException e) {
      throw new HoodieIOException(e.getMessage(), e);
    }
  }
}