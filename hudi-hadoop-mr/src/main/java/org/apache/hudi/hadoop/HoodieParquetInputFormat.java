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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieDefaultTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.table.view.TableFileSystemView.BaseFileOnlyView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * HoodieInputFormat which understands the Hoodie File Structure and filters files based on the Hoodie Mode. If paths
 * that does not correspond to a hoodie table then they are passed in as is (as what FileInputFormat.listStatus()
 * would do). The JobConf could have paths from multipe Hoodie/Non-Hoodie tables
 */
@UseFileSplitsFromInputFormat
public class HoodieParquetInputFormat extends MapredParquetInputFormat implements Configurable {

  private static final Logger LOG = LogManager.getLogger(HoodieParquetInputFormat.class);

  protected Configuration conf;

  @Override
  public FileStatus[] listStatus(JobConf job) throws IOException {
    // Segregate inputPaths[] to incremental, snapshot and non hoodie paths
    List<String> incrementalTables = HoodieHiveUtil.getIncrementalTableNames(Job.getInstance(job));
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
          groupFileStatusForSnapshotPaths(fileStatuses, tableMetaClientMap.values());
      LOG.info("Found a total of " + groupedFileStatus.size() + " groups");
      for (Map.Entry<HoodieTableMetaClient, List<FileStatus>> entry : groupedFileStatus.entrySet()) {
        List<FileStatus> result = filterFileStatusForSnapshotMode(entry.getKey(), entry.getValue());
        if (result != null) {
          returns.addAll(result);
        }
      }
    }
    return returns.toArray(new FileStatus[returns.size()]);
  }

  /**
   * Filter any specific instants that we do not want to process.
   * example timeline:
   *
   * t0 -> create bucket1.parquet
   * t1 -> create and append updates bucket1.log
   * t2 -> request compaction
   * t3 -> create bucket2.parquet
   *
   * if compaction at t2 takes a long time, incremental readers on RO tables can move to t3 and would skip updates in t1
   *
   * To workaround this problem, we want to stop returning data belonging to commits > t2.
   * After compaction is complete, incremental reader would see updates in t2, t3, so on.
   */
  protected HoodieDefaultTimeline filterInstantsTimeline(HoodieDefaultTimeline timeline) {
    HoodieDefaultTimeline commitsAndCompactionTimeline = timeline.getCommitsAndCompactionTimeline();
    Option<HoodieInstant> pendingCompactionInstant = commitsAndCompactionTimeline.filterPendingCompactionTimeline().firstInstant();
    if (pendingCompactionInstant.isPresent()) {
      HoodieDefaultTimeline instantsTimeline = commitsAndCompactionTimeline.findInstantsBefore(pendingCompactionInstant.get().getTimestamp());
      int numCommitsFilteredByCompaction = commitsAndCompactionTimeline.getCommitsTimeline().countInstants()
          - instantsTimeline.getCommitsTimeline().countInstants();
      LOG.info("Earliest pending compaction instant is: " + pendingCompactionInstant.get().getTimestamp()
              + " skipping " + numCommitsFilteredByCompaction + " commits");

      return instantsTimeline;
    } else {
      return timeline;
    }
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
    HoodieDefaultTimeline baseTimeline;
    if (HoodieHiveUtil.stopAtCompaction(jobContext, tableName)) {
      baseTimeline = filterInstantsTimeline(tableMetaClient.getActiveTimeline());
    } else {
      baseTimeline = tableMetaClient.getActiveTimeline();
    }

    HoodieTimeline timeline = baseTimeline.getCommitsTimeline().filterCompletedInstants();
    String lastIncrementalTs = HoodieHiveUtil.readStartCommitTime(jobContext, tableName);
    // Total number of commits to return in this batch. Set this to -1 to get all the commits.
    Integer maxCommits = HoodieHiveUtil.readMaxCommits(jobContext, tableName);
    LOG.info("Last Incremental timestamp was set as " + lastIncrementalTs);
    List<HoodieInstant> commitsToCheck = timeline.findInstantsAfter(lastIncrementalTs, maxCommits)
        .getInstants().collect(Collectors.toList());
    // Extract partitions touched by the commitsToCheck
    Set<String> partitionsToList = new HashSet<>();
    for (HoodieInstant commit : commitsToCheck) {
      HoodieCommitMetadata commitMetadata = HoodieCommitMetadata.fromBytes(timeline.getInstantDetails(commit).get(),
          HoodieCommitMetadata.class);
      partitionsToList.addAll(commitMetadata.getPartitionToWriteStats().keySet());
    }
    if (partitionsToList.isEmpty()) {
      return null;
    }
    String incrementalInputPaths = partitionsToList.stream()
        .map(s -> tableMetaClient.getBasePath() + Path.SEPARATOR + s)
        .filter(s -> {
          /*
           * Ensure to return only results from the original input path that has incremental changes
           * This check is needed for the following corner case -  When the caller invokes
           * HoodieInputFormat.listStatus multiple times (with small batches of Hive partitions each
           * time. Ex. Hive fetch task calls listStatus for every partition once) we do not want to
           * accidentally return all incremental changes for the entire table in every listStatus()
           * call. This will create redundant splits. Instead we only want to return the incremental
           * changes (if so any) in that batch of input paths.
           *
           * NOTE on Hive queries that are executed using Fetch task:
           * Since Fetch tasks invoke InputFormat.listStatus() per partition, Hoodie metadata can be
           * listed in every such listStatus() call. In order to avoid this, it might be useful to
           * disable fetch tasks using the hive session property for incremental queries:
           * `set hive.fetch.task.conversion=none;`
           * This would ensure Map Reduce execution is chosen for a Hive query, which combines
           * partitions (comma separated) and calls InputFormat.listStatus() only once with all
           * those partitions.
           */
          for (Path path : inputPaths) {
            if (path.toString().contains(s)) {
              return true;
            }
          }
          return false;
        })
        .collect(Collectors.joining(","));
    if (StringUtils.isNullOrEmpty(incrementalInputPaths)) {
      return null;
    }
    // Mutate the JobConf to set the input paths to only partitions touched by incremental pull.
    setInputPaths(job, incrementalInputPaths);
    FileStatus[] fileStatuses = super.listStatus(job);
    BaseFileOnlyView roView = new HoodieTableFileSystemView(tableMetaClient, timeline, fileStatuses);
    List<String> commitsList = commitsToCheck.stream().map(HoodieInstant::getTimestamp).collect(Collectors.toList());
    List<HoodieBaseFile> filteredFiles = roView.getLatestBaseFilesInRange(commitsList).collect(Collectors.toList());
    List<FileStatus> returns = new ArrayList<>();
    for (HoodieBaseFile filteredFile : filteredFiles) {
      LOG.debug("Processing incremental hoodie file - " + filteredFile.getPath());
      filteredFile = checkFileStatus(filteredFile);
      returns.add(filteredFile.getFileStatus());
    }
    LOG.info("Total paths to process after hoodie incremental filter " + filteredFiles.size());
    return returns;
  }

  /**
   * Takes in a list of filesStatus and a list of table metadatas. Groups the files status list
   * based on given table metadata.
   * @param fileStatuses
   * @param metaClientList
   * @return
   * @throws IOException
   */
  private Map<HoodieTableMetaClient, List<FileStatus>> groupFileStatusForSnapshotPaths(
      FileStatus[] fileStatuses, Collection<HoodieTableMetaClient> metaClientList) {
    // This assumes the paths for different tables are grouped together
    Map<HoodieTableMetaClient, List<FileStatus>> grouped = new HashMap<>();
    HoodieTableMetaClient metadata = null;
    for (FileStatus status : fileStatuses) {
      Path inputPath = status.getPath();
      if (!inputPath.getName().endsWith(".parquet")) {
        //FIXME(vc): skip non parquet files for now. This wont be needed once log file name start
        // with "."
        continue;
      }
      if ((metadata == null) || (!inputPath.toString().contains(metadata.getBasePath()))) {
        for (HoodieTableMetaClient metaClient : metaClientList) {
          if (inputPath.toString().contains(metaClient.getBasePath())) {
            metadata = metaClient;
            if (!grouped.containsKey(metadata)) {
              grouped.put(metadata, new ArrayList<>());
            }
            break;
          }
        }
      }
      grouped.get(metadata).add(status);
    }
    return grouped;
  }

  /**
   * Filters data files for a snapshot queried table.
   */
  private List<FileStatus> filterFileStatusForSnapshotMode(
      HoodieTableMetaClient metadata, List<FileStatus> fileStatuses) {
    FileStatus[] statuses = fileStatuses.toArray(new FileStatus[0]);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Hoodie Metadata initialized with completed commit Ts as :" + metadata);
    }
    // Get all commits, delta commits, compactions, as all of them produce a base parquet file today
    HoodieTimeline timeline = metadata.getActiveTimeline().getCommitsTimeline().filterCompletedInstants();
    BaseFileOnlyView roView = new HoodieTableFileSystemView(metadata, timeline, statuses);
    // filter files on the latest commit found
    List<HoodieBaseFile> filteredFiles = roView.getLatestBaseFiles().collect(Collectors.toList());
    LOG.info("Total paths to process after hoodie filter " + filteredFiles.size());
    List<FileStatus> returns = new ArrayList<>();
    for (HoodieBaseFile filteredFile : filteredFiles) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Processing latest hoodie file - " + filteredFile.getPath());
      }
      filteredFile = checkFileStatus(filteredFile);
      returns.add(filteredFile.getFileStatus());
    }
    return returns;
  }

  /**
   * Checks the file status for a race condition which can set the file size to 0. 1. HiveInputFormat does
   * super.listStatus() and gets back a FileStatus[] 2. Then it creates the HoodieTableMetaClient for the paths listed.
   * 3. Generation of splits looks at FileStatus size to create splits, which skips this file
   */
  private HoodieBaseFile checkFileStatus(HoodieBaseFile dataFile) {
    Path dataPath = dataFile.getFileStatus().getPath();
    try {
      if (dataFile.getFileSize() == 0) {
        FileSystem fs = dataPath.getFileSystem(conf);
        LOG.info("Refreshing file status " + dataFile.getPath());
        return new HoodieBaseFile(fs.getFileStatus(dataPath));
      }
      return dataFile;
    } catch (IOException e) {
      throw new HoodieIOException("Could not get FileStatus on path " + dataPath);
    }
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

  /**
   * Read the table metadata from a data path. This assumes certain hierarchy of files which should be changed once a
   * better way is figured out to pass in the hoodie meta directory
   */
  protected static HoodieTableMetaClient getTableMetaClient(FileSystem fs, Path dataPath) throws IOException {
    int levels = HoodieHiveUtil.DEFAULT_LEVELS_TO_BASEPATH;
    if (HoodiePartitionMetadata.hasPartitionMetadata(fs, dataPath)) {
      HoodiePartitionMetadata metadata = new HoodiePartitionMetadata(fs, dataPath);
      metadata.readFromFS();
      levels = metadata.getPartitionDepth();
    }
    Path baseDir = HoodieHiveUtil.getNthParent(dataPath, levels);
    LOG.info("Reading hoodie metadata from path " + baseDir.toString());
    return new HoodieTableMetaClient(fs.getConf(), baseDir.toString());
  }
}
