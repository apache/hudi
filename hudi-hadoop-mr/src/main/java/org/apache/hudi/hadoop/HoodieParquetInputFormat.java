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
import org.apache.hudi.common.util.HoodieAvroUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.DatasetNotFoundException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.InvalidDatasetException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat;
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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.hudi.hadoop.HoodieColumnProjectionUtils.READ_NESTED_COLUMN_PATH_CONF_STR;


/**
 * HoodieInputFormat which understands the Hoodie File Structure and filters files based on the Hoodie Mode. If paths
 * that does not correspond to a hoodie dataset then they are passed in as is (as what FileInputFormat.listStatus()
 * would do). The JobConf could have paths from multipe Hoodie/Non-Hoodie datasets
 */
@UseFileSplitsFromInputFormat
public class HoodieParquetInputFormat extends MapredParquetInputFormat implements Configurable {

  private static final Logger LOG = LogManager.getLogger(HoodieParquetInputFormat.class);

  protected Configuration conf;
  public boolean firstTime = true;

  @Override
  public FileStatus[] listStatus(JobConf job) throws IOException {
    System.out.println("HoodieParquetInputFormat listing Status");
    // Get all the file status from FileInputFormat and then do the filter
    FileStatus[] fileStatuses = super.listStatus(job);
    Map<HoodieTableMetaClient, List<FileStatus>> groupedFileStatus = groupFileStatus(fileStatuses);
    LOG.info("Found a total of " + groupedFileStatus.size() + " groups");
    System.out.println("Found a total of " + groupedFileStatus.size() + " groups");
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
          returns.add(getFileStatus(filteredFile));
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
          returns.add(getFileStatus(filteredFile));
        }
      }
    }
    return returns.toArray(new FileStatus[returns.size()]);
  }

  private static FileStatus getFileStatus(HoodieDataFile dataFile) throws IOException {
    if (dataFile.getExternalDataFile().isPresent()) {
      if (dataFile.getFileStatus() instanceof LocatedFileStatus) {
        return new LocatedFileStatusWithExternalDataFile((LocatedFileStatus)dataFile.getFileStatus(),
            dataFile.getExternalDataFile().get());
      } else {
        return new FileStatusWithExternalDataFile(dataFile.getFileStatus(), dataFile.getExternalDataFile().get());
      }
    }
    return dataFile.getFileStatus();
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
        return new HoodieDataFile(fs.getFileStatus(dataPath), dataFile.getExternalDataFile().orElse(null));
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
    if (split instanceof ExternalDataFileSplit) {
      ExternalDataFileSplit eSplit = (ExternalDataFileSplit)split;
      System.out.println("ExternalDataFileSplit is " + eSplit);
      String[] rawColNames = HoodieColumnProjectionUtils.getReadColumnNames(job);
      List<Integer> rawColIds = HoodieColumnProjectionUtils.getReadColumnIDs(job);
      List<Pair<Integer, String>> colsWithIndex =
          IntStream.range(0, rawColIds.size()).mapToObj(idx -> Pair.of(rawColIds.get(idx), rawColNames[idx]))
          .collect(Collectors.toList());

      List<Pair<Integer, String>> hoodieColsProjected = colsWithIndex.stream()
          .filter(idxWithName -> idxWithName.getKey() < HoodieAvroUtils.NUM_HUDI_METADATA_COLS)
          .collect(Collectors.toList());
      // This always matches hive table description
      List<Pair<String, String>> colNameWithTypes = HoodieColumnProjectionUtils.getIOColumnNameAndTypes(job);
      List<Pair<String, String>> hoodieColNamesWithTypes = colNameWithTypes.subList(0, 5);
      List<Pair<String, String>> otherColNamesWithTypes =
          colNameWithTypes.subList(5, colNameWithTypes.size());
      JobConf jobConf1 = new JobConf(job);
      JobConf jobConf2 = new JobConf(job);
      HoodieColumnProjectionUtils.setIOColumnNameAndTypes(jobConf1, hoodieColNamesWithTypes);
      HoodieColumnProjectionUtils.setIOColumnNameAndTypes(jobConf2, otherColNamesWithTypes);
      if (hoodieColsProjected.isEmpty()) {
        // Adjust adjustedColsProjected
        List<Integer> adjustednonHoodieColsProjected = colsWithIndex.stream()
            .filter(idxWithName -> idxWithName.getKey() >= HoodieAvroUtils.NUM_HUDI_METADATA_COLS)
            .map(idxWithName -> idxWithName.getKey() - HoodieAvroUtils.NUM_HUDI_METADATA_COLS)
            .collect(Collectors.toList());
        List<String> adjustednonHoodieColNamesProjected = colsWithIndex.stream()
            .filter(idxWithName -> idxWithName.getKey() >= HoodieAvroUtils.NUM_HUDI_METADATA_COLS)
            .map(idxWithName -> idxWithName.getValue())
            .collect(Collectors.toList());
        HoodieColumnProjectionUtils.setReadColumns(jobConf1, adjustednonHoodieColsProjected,
            adjustednonHoodieColNamesProjected);
        return super.getRecordReader(split, jobConf1, reporter);
      } else {
        HoodieColumnProjectionUtils.setReadColumns(jobConf1, new ArrayList<>(), new ArrayList<>());
        HoodieColumnProjectionUtils.setReadColumns(jobConf2, new ArrayList<>(), new ArrayList<>());
        List<String> hoodieColNames = colsWithIndex.stream()
            .filter(idxWithName -> idxWithName.getKey() < HoodieAvroUtils.NUM_HUDI_METADATA_COLS)
            .map(idxWithName -> idxWithName.getValue()).collect(Collectors.toList());
        List<Integer> hoodieColIds = colsWithIndex.stream()
            .filter(idxWithName -> idxWithName.getKey() < HoodieAvroUtils.NUM_HUDI_METADATA_COLS)
            .map(idxWithName -> idxWithName.getKey()).collect(Collectors.toList());
        List<String> nonHoodieColNames = colsWithIndex.stream()
            .filter(idxWithName -> idxWithName.getKey() >= HoodieAvroUtils.NUM_HUDI_METADATA_COLS)
            .map(idxWithName -> idxWithName.getValue()).collect(Collectors.toList());
        List<Integer> nonHoodieColIdsAdjusted = colsWithIndex.stream()
            .filter(idxWithName -> idxWithName.getKey() >= HoodieAvroUtils.NUM_HUDI_METADATA_COLS)
            .map(idxWithName -> idxWithName.getKey() - HoodieAvroUtils.NUM_HUDI_METADATA_COLS)
            .collect(Collectors.toList());
        List<String> groupCols = Arrays.asList(job.get(READ_NESTED_COLUMN_PATH_CONF_STR, "").split(","));
        HoodieColumnProjectionUtils.appendReadColumns(jobConf1, hoodieColIds, hoodieColNames, new ArrayList<>());
        HoodieColumnProjectionUtils.appendReadColumns(jobConf2, nonHoodieColIdsAdjusted, nonHoodieColNames, groupCols);
        System.out.println("hoodieColNames=" + hoodieColNames + ", hoodieColIds=" + hoodieColIds);
        System.out.println("SIZES : hoodieColNames=" + hoodieColNames.size()
            + ", hoodieColIds=" + hoodieColIds.size());
        System.out.println("nonHoodieColNames=" + nonHoodieColNames + ", nonHoodieColIdsAdjusted="
            + nonHoodieColIdsAdjusted);
        System.out.println("SIZES : nonHoodieColNames=" + nonHoodieColNames.size() + ", nonHoodieColIdsAdjusted="
            + nonHoodieColIdsAdjusted.size());
        FileSystem fs = FileSystem.get(job);
        Path externalFile = new Path(eSplit.getSourceFileFullPath());
        FileStatus externalFileStatus = fs.getFileStatus(externalFile);
        FileSplit rightSplit =
            makeSplit(externalFile, 0, externalFileStatus.getLen(), new String[0], new String[0]);
        System.out.println("Generating HoodieColumnStichingRecordReader for " + eSplit.getSourceFileFullPath()
            + " and " + externalFileStatus);
        if (firstTime) {
          System.out.println("JobConf1=");
          Iterator<Entry<String, String>> e = jobConf1.iterator();
          while (e.hasNext()) {
            Entry<String, String> kv = e.next();
            System.out.println("\tKey=" + kv.getKey() + ", Value=" + kv.getValue());
          }
          System.out.println("\n\n\nJobConf2=");
          e = jobConf2.iterator();
          while (e.hasNext()) {
            Entry<String, String> kv = e.next();
            System.out.println("\tKey=" + kv.getKey() + ", Value=" + kv.getValue());
          }
          firstTime = false;
        }
        return new HoodieColumnStichingRecordReader(super.getRecordReader(eSplit, jobConf1, reporter),
            super.getRecordReader(rightSplit, jobConf2, reporter));
      }
    }
    System.out.println("EMPLOYING DEFAULT RECORD READER - " + split);
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

  @Override
  protected boolean isSplitable(FileSystem fs, Path filename) {
    return !(filename instanceof PathWithExternalDataFile);
  }

  @Override
  protected FileSplit makeSplit(Path file, long start, long length,
      String[] hosts) {
    FileSplit split = new FileSplit(file, start, length, hosts);

    if (file instanceof PathWithExternalDataFile) {
      try {
        System.out.println("Making external data split for " + file);
        return new ExternalDataFileSplit(split, ((PathWithExternalDataFile)file).getExternalPath());
      } catch (IOException e) {
        throw new HoodieIOException(e.getMessage(), e);
      }
    }
    return split;
  }

  @Override
  protected FileSplit makeSplit(Path file, long start, long length,
      String[] hosts, String[] inMemoryHosts) {
    FileSplit split = new FileSplit(file, start, length, hosts, inMemoryHosts);
    if (file instanceof PathWithExternalDataFile) {
      try {
        System.out.println("Making external data split for " + file);
        return new ExternalDataFileSplit(split, ((PathWithExternalDataFile)file).getExternalPath());
      } catch (IOException e) {
        throw new HoodieIOException(e.getMessage(), e);
      }
    }
    return split;
  }
}
