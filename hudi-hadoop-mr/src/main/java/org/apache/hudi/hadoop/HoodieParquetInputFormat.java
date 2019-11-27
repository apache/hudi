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

import org.apache.hudi.common.model.HoodieDataFile;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTimeline;
import org.apache.hudi.common.table.TableFileSystemView.ReadOptimizedView;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.exception.DatasetNotFoundException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.InvalidDatasetException;

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
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * HoodieInputFormat which understands the Hoodie File Structure and filters files based on the Hoodie Mode. If paths
 * that does not correspond to a hoodie dataset then they are passed in as is (as what FileInputFormat.listStatus()
 * would do). The JobConf could have paths from multipe Hoodie/Non-Hoodie datasets
 */
@UseFileSplitsFromInputFormat
public class HoodieParquetInputFormat extends MapredParquetInputFormat implements Configurable {

  private static final transient Logger LOG = LogManager.getLogger(HoodieParquetInputFormat.class);

  protected Configuration conf;

  @Override
  public FileStatus[] listStatus(JobConf job) throws IOException {
    // Get all the file status from FileInputFormat and then do the filter
    FileStatus[] fileStatuses = super.listStatus(job);
    Map<HoodieTableMetaClient, List<FileStatus>> groupedFileStatus = groupFileStatus(fileStatuses);
    LOG.info("Found a total of " + groupedFileStatus.size() + " groups");
    List<FileStatus> returns = new ArrayList<>();
    for (Map.Entry<HoodieTableMetaClient, List<FileStatus>> entry : groupedFileStatus.entrySet()) {
      HoodieTableMetaClient metadata = entry.getKey();
      if (metadata == null) {
        // Add all the paths which are not hoodie specific
        returns.addAll(entry.getValue());
        continue;
      }

      FileStatus[] statuses = entry.getValue().toArray(new FileStatus[entry.getValue().size()]);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Hoodie Metadata initialized with completed commit Ts as :" + metadata);
      }
      String tableName = metadata.getTableConfig().getTableName();
      String mode = HoodieHiveUtil.readMode(Job.getInstance(job), tableName);
      // Get all commits, delta commits, compactions, as all of them produce a base parquet file
      // today
      HoodieTimeline timeline = metadata.getActiveTimeline().getCommitsTimeline().filterCompletedInstants();
      ReadOptimizedView roView = new HoodieTableFileSystemView(metadata, timeline, statuses);

      if (HoodieHiveUtil.INCREMENTAL_SCAN_MODE.equals(mode)) {
        // this is of the form commitTs_partition_sequenceNumber
        String lastIncrementalTs = HoodieHiveUtil.readStartCommitTime(Job.getInstance(job), tableName);
        // Total number of commits to return in this batch. Set this to -1 to get all the commits.
        Integer maxCommits = HoodieHiveUtil.readMaxCommits(Job.getInstance(job), tableName);
        LOG.info("Last Incremental timestamp was set as " + lastIncrementalTs);
        List<String> commitsToReturn = timeline.findInstantsAfter(lastIncrementalTs, maxCommits).getInstants()
            .map(HoodieInstant::getTimestamp).collect(Collectors.toList());
        List<HoodieDataFile> filteredFiles =
            roView.getLatestDataFilesInRange(commitsToReturn).collect(Collectors.toList());
        for (HoodieDataFile filteredFile : filteredFiles) {
          LOG.info("Processing incremental hoodie file - " + filteredFile.getPath());
          filteredFile = checkFileStatus(filteredFile);
          returns.add(filteredFile.getFileStatus());
        }
        LOG.info("Total paths to process after hoodie incremental filter " + filteredFiles.size());
      } else {
        // filter files on the latest commit found
        List<HoodieDataFile> filteredFiles = roView.getLatestDataFiles().collect(Collectors.toList());
        LOG.info("Total paths to process after hoodie filter " + filteredFiles.size());
        for (HoodieDataFile filteredFile : filteredFiles) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Processing latest hoodie file - " + filteredFile.getPath());
          }
          filteredFile = checkFileStatus(filteredFile);
          returns.add(filteredFile.getFileStatus());
        }
      }
    }
    return returns.toArray(new FileStatus[returns.size()]);

  }

  /**
   * Checks the file status for a race condition which can set the file size to 0. 1. HiveInputFormat does
   * super.listStatus() and gets back a FileStatus[] 2. Then it creates the HoodieTableMetaClient for the paths listed.
   * 3. Generation of splits looks at FileStatus size to create splits, which skips this file
   */
  private HoodieDataFile checkFileStatus(HoodieDataFile dataFile) throws IOException {
    Path dataPath = dataFile.getFileStatus().getPath();
    try {
      if (dataFile.getFileSize() == 0) {
        FileSystem fs = dataPath.getFileSystem(conf);
        LOG.info("Refreshing file status " + dataFile.getPath());
        return new HoodieDataFile(fs.getFileStatus(dataPath));
      }
      return dataFile;
    } catch (IOException e) {
      throw new HoodieIOException("Could not get FileStatus on path " + dataPath);
    }
  }

  private Map<HoodieTableMetaClient, List<FileStatus>> groupFileStatus(FileStatus[] fileStatuses) throws IOException {
    // This assumes the paths for different tables are grouped together
    Map<HoodieTableMetaClient, List<FileStatus>> grouped = new HashMap<>();
    HoodieTableMetaClient metadata = null;
    String nonHoodieBasePath = null;
    for (FileStatus status : fileStatuses) {
      if (!status.getPath().getName().endsWith(".parquet")) {
        // FIXME(vc): skip non parquet files for now. This wont be needed once log file name start
        // with "."
        continue;
      }
      if ((metadata == null && nonHoodieBasePath == null)
          || (metadata == null && !status.getPath().toString().contains(nonHoodieBasePath))
          || (metadata != null && !status.getPath().toString().contains(metadata.getBasePath()))) {
        try {
          metadata = getTableMetaClient(status.getPath().getFileSystem(conf), status.getPath().getParent());
          nonHoodieBasePath = null;
        } catch (DatasetNotFoundException | InvalidDatasetException e) {
          LOG.info("Handling a non-hoodie path " + status.getPath());
          metadata = null;
          nonHoodieBasePath = status.getPath().getParent().toString();
        }
        if (!grouped.containsKey(metadata)) {
          grouped.put(metadata, new ArrayList<>());
        }
      }
      grouped.get(metadata).add(status);
    }
    return grouped;
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
  }

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
