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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieDefaultTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.hadoop.BootstrapBaseFileSplit;
import org.apache.hudi.hadoop.FileStatusWithBootstrapBaseFile;
import org.apache.hudi.hadoop.LocatedFileStatusWithBootstrapBaseFile;
import org.apache.hudi.hadoop.RealtimeFileStatus;
import org.apache.hudi.hadoop.PathWithLogFilePath;
import org.apache.hudi.hadoop.HoodieParquetInputFormat;
import org.apache.hudi.hadoop.UseFileSplitsFromInputFormat;
import org.apache.hudi.hadoop.UseRecordReaderFromInputFormat;
import org.apache.hudi.hadoop.utils.HoodieInputFormatUtils;
import org.apache.hudi.hadoop.utils.HoodieRealtimeInputFormatUtils;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.stream.Collectors;

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

    List<FileSplit> fileSplits = Arrays.stream(super.getSplits(job, numSplits)).map(is -> (FileSplit) is).collect(Collectors.toList());

    boolean isIncrementalSplits = HoodieRealtimeInputFormatUtils.isIncrementalQuerySplits(fileSplits);

    return isIncrementalSplits ? HoodieRealtimeInputFormatUtils.getIncrementalRealtimeSplits(job, fileSplits.stream()) : HoodieRealtimeInputFormatUtils.getRealtimeSplits(job, fileSplits.stream());
  }

  /**
   * Keep the logic of mor_incr_view as same as spark datasource.
   * Step1: Get list of commits to be fetched based on start commit and max commits(for snapshot max commits is -1).
   * Step2: Get list of affected files status for these affected file status.
   * Step3: Construct HoodieTableFileSystemView based on those affected file status.
   *        a. Filter affected partitions based on inputPaths.
   *        b. Get list of fileGroups based on affected partitions by fsView.getAllFileGroups.
   * Step4: Set input paths based on filtered affected partition paths. changes that amony original input paths passed to
   *        this method. some partitions did not have commits as part of the trimmed down list of commits and hence we need this step.
   * Step5: Find candidate fileStatus, since when we get baseFileStatus from HoodieTableFileSystemView,
   *        the BaseFileStatus will missing file size information.
   *        We should use candidate fileStatus to update the size information for BaseFileStatus.
   * Step6: For every file group from step3(b)
   *        Get 1st available base file from all file slices. then we use candidate file status to update the baseFileStatus,
   *        and construct RealTimeFileStatus and add it to result along with log files.
   *        If file group just has log files, construct RealTimeFileStatus and add it to result.
   * TODO: unify the incremental view code between hive/spark-sql and spark datasource
   */
  @Override
  protected List<FileStatus> listStatusForIncrementalMode(
      JobConf job, HoodieTableMetaClient tableMetaClient, List<Path> inputPaths) throws IOException {
    List<FileStatus> result = new ArrayList<>();
    String tableName = tableMetaClient.getTableConfig().getTableName();
    Job jobContext = Job.getInstance(job);

    // step1
    Option<HoodieTimeline> timeline = HoodieInputFormatUtils.getFilteredCommitsTimeline(jobContext, tableMetaClient);
    if (!timeline.isPresent()) {
      return result;
    }
    HoodieTimeline commitsTimelineToReturn = HoodieInputFormatUtils.getHoodieTimelineForIncrementalQuery(jobContext, tableName, timeline.get());
    Option<List<HoodieInstant>> commitsToCheck = Option.of(commitsTimelineToReturn.getInstants().collect(Collectors.toList()));
    if (!commitsToCheck.isPresent()) {
      return result;
    }
    // step2
    commitsToCheck.get().sort(HoodieInstant::compareTo);
    List<HoodieCommitMetadata> metadataList = commitsToCheck
        .get().stream().map(instant -> {
          try {
            return HoodieInputFormatUtils.getCommitMetadata(instant, commitsTimelineToReturn);
          } catch (IOException e) {
            throw new HoodieException(String.format("cannot get metadata for instant: %s", instant));
          }
        }).collect(Collectors.toList());

    // build fileGroup from fsView
    List<FileStatus> affectedFileStatus = Arrays.asList(HoodieInputFormatUtils
        .listAffectedFilesForCommits(new Path(tableMetaClient.getBasePath()), metadataList));
    // step3
    HoodieTableFileSystemView fsView = new HoodieTableFileSystemView(tableMetaClient, commitsTimelineToReturn, affectedFileStatus.toArray(new FileStatus[0]));
    // build fileGroup from fsView
    Path basePath = new Path(tableMetaClient.getBasePath());
    // filter affectedPartition by inputPaths
    List<String> affectedPartition = HoodieInputFormatUtils.getWritePartitionPaths(metadataList).stream()
        .filter(k -> k.isEmpty() ? inputPaths.contains(basePath) : inputPaths.contains(new Path(basePath, k))).collect(Collectors.toList());
    if (affectedPartition.isEmpty()) {
      return result;
    }
    List<HoodieFileGroup> fileGroups = affectedPartition.stream()
        .flatMap(partitionPath -> fsView.getAllFileGroups(partitionPath)).collect(Collectors.toList());
    // step4
    setInputPaths(job, affectedPartition.stream()
        .map(p -> p.isEmpty() ? basePath.toString() : new Path(basePath, p).toString()).collect(Collectors.joining(",")));

    // step5
    // find all file status in partitionPaths.
    FileStatus[] fileStatuses = getStatus(job);
    Map<String, FileStatus> candidateFileStatus = new HashMap<>();
    for (int i = 0; i < fileStatuses.length; i++) {
      String key = fileStatuses[i].getPath().toString();
      candidateFileStatus.put(key, fileStatuses[i]);
    }

    String maxCommitTime = fsView.getLastInstant().get().getTimestamp();
    // step6
    result.addAll(collectAllIncrementalFiles(fileGroups, maxCommitTime, basePath.toString(), candidateFileStatus));
    return result;
  }

  private List<FileStatus> collectAllIncrementalFiles(List<HoodieFileGroup> fileGroups, String maxCommitTime, String basePath, Map<String, FileStatus> candidateFileStatus) {
    List<FileStatus> result = new ArrayList<>();
    fileGroups.stream().forEach(f -> {
      try {
        List<FileSlice> baseFiles = f.getAllFileSlices().filter(slice -> slice.getBaseFile().isPresent()).collect(Collectors.toList());
        if (!baseFiles.isEmpty()) {
          FileStatus baseFileStatus = HoodieInputFormatUtils.getFileStatus(baseFiles.get(0).getBaseFile().get());
          String baseFilePath = baseFileStatus.getPath().toUri().toString();
          if (!candidateFileStatus.containsKey(baseFilePath)) {
            throw new HoodieException("Error obtaining fileStatus for file: " + baseFilePath);
          }
          // We cannot use baseFileStatus.getPath() here, since baseFileStatus.getPath() missing file size information.
          // So we use candidateFileStatus.get(baseFileStatus.getPath()) to get a correct path.
          RealtimeFileStatus fileStatus = new RealtimeFileStatus(candidateFileStatus.get(baseFilePath));
          fileStatus.setMaxCommitTime(maxCommitTime);
          fileStatus.setBelongToIncrementalFileStatus(true);
          fileStatus.setBasePath(basePath);
          fileStatus.setBaseFilePath(baseFilePath);
          fileStatus.setDeltaLogFiles(f.getLatestFileSlice().get().getLogFiles().collect(Collectors.toList()));
          // try to set bootstrapfileStatus
          if (baseFileStatus instanceof LocatedFileStatusWithBootstrapBaseFile || baseFileStatus instanceof FileStatusWithBootstrapBaseFile) {
            fileStatus.setBootStrapFileStatus(baseFileStatus);
          }
          result.add(fileStatus);
        }
        // add file group which has only logs.
        if (f.getLatestFileSlice().isPresent() && baseFiles.isEmpty()) {
          List<FileStatus> logFileStatus = f.getLatestFileSlice().get().getLogFiles().map(logFile -> logFile.getFileStatus()).collect(Collectors.toList());
          if (logFileStatus.size() > 0) {
            RealtimeFileStatus fileStatus = new RealtimeFileStatus(logFileStatus.get(0));
            fileStatus.setBelongToIncrementalFileStatus(true);
            fileStatus.setDeltaLogFiles(logFileStatus.stream().map(l -> new HoodieLogFile(l.getPath(), l.getLen())).collect(Collectors.toList()));
            fileStatus.setMaxCommitTime(maxCommitTime);
            fileStatus.setBasePath(basePath);
            result.add(fileStatus);
          }
        }
      } catch (IOException e) {
        throw new HoodieException("Error obtaining data file/log file grouping ", e);
      }
    });
    return result;
  }

  @Override
  protected boolean includeLogFilesForSnapShotView() {
    return true;
  }

  @Override
  protected boolean isSplitable(FileSystem fs, Path filename) {
    if (filename instanceof PathWithLogFilePath) {
      return ((PathWithLogFilePath)filename).splitable();
    }
    return super.isSplitable(fs, filename);
  }

  // make split for path.
  // When query the incremental view, the read files may be bootstrap files, we wrap those bootstrap files into
  // PathWithLogFilePath, so those bootstrap files should be processed int this function.
  @Override
  protected FileSplit makeSplit(Path file, long start, long length, String[] hosts) {
    if (file instanceof PathWithLogFilePath) {
      return doMakeSplitForPathWithLogFilePath((PathWithLogFilePath) file, start, length, hosts, null);
    }
    return super.makeSplit(file, start, length, hosts);
  }

  @Override
  protected FileSplit makeSplit(Path file, long start, long length, String[] hosts, String[] inMemoryHosts) {
    if (file instanceof PathWithLogFilePath) {
      return doMakeSplitForPathWithLogFilePath((PathWithLogFilePath) file, start, length, hosts, inMemoryHosts);
    }
    return super.makeSplit(file, start, length, hosts, inMemoryHosts);
  }

  private FileSplit doMakeSplitForPathWithLogFilePath(PathWithLogFilePath path, long start, long length, String[] hosts, String[] inMemoryHosts) {
    if (!path.includeBootstrapFilePath()) {
      return path.buildSplit(path, start, length, hosts);
    } else {
      FileSplit bf =
          inMemoryHosts == null
              ? super.makeSplit(path.getPathWithBootstrapFileStatus(), start, length, hosts)
              : super.makeSplit(path.getPathWithBootstrapFileStatus(), start, length, hosts, inMemoryHosts);
      return HoodieRealtimeInputFormatUtils
          .createRealtimeBoostrapBaseFileSplit((BootstrapBaseFileSplit) bf, path.getBasePath(), path.getDeltaLogFiles(), path.getMaxCommitTime());
    }
  }

  @Override
  public FileStatus[] listStatus(JobConf job) throws IOException {
    // Call the HoodieInputFormat::listStatus to obtain all latest parquet files, based on commit
    // timeline.
    return super.listStatus(job);
  }

  @Override
  protected HoodieDefaultTimeline filterInstantsTimeline(HoodieDefaultTimeline timeline) {
    // no specific filtering for Realtime format
    return timeline;
  }

  void addProjectionToJobConf(final RealtimeSplit realtimeSplit, final JobConf jobConf) {
    // Hive on Spark invokes multiple getRecordReaders from different threads in the same spark task (and hence the
    // same JVM) unlike Hive on MR. Due to this, accesses to JobConf, which is shared across all threads, is at the
    // risk of experiencing race conditions. Hence, we synchronize on the JobConf object here. There is negligible
    // latency incurred here due to the synchronization since get record reader is called once per spilt before the
    // actual heavy lifting of reading the parquet files happen.
    if (HoodieRealtimeInputFormatUtils.canAddProjectionToJobConf(realtimeSplit, jobConf)) {
      synchronized (jobConf) {
        LOG.info(
            "Before adding Hoodie columns, Projections :" + jobConf.get(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR)
                + ", Ids :" + jobConf.get(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR));
        if (HoodieRealtimeInputFormatUtils.canAddProjectionToJobConf(realtimeSplit, jobConf)) {
          // Hive (across all versions) fails for queries like select count(`_hoodie_commit_time`) from table;
          // In this case, the projection fields gets removed. Looking at HiveInputFormat implementation, in some cases
          // hoodie additional projection columns are reset after calling setConf and only natural projections
          // (one found in select queries) are set. things would break because of this.
          // For e:g _hoodie_record_key would be missing and merge step would throw exceptions.
          // TO fix this, hoodie columns are appended late at the time record-reader gets built instead of construction
          // time.
          if (!realtimeSplit.getDeltaLogPaths().isEmpty()) {
            HoodieRealtimeInputFormatUtils.addRequiredProjectionFields(jobConf, realtimeSplit.getHoodieVirtualKeyInfo());
          }
          this.conf = jobConf;
          this.conf.set(HoodieInputFormatUtils.HOODIE_READ_COLUMNS_PROP, "true");
        }
      }
    }
    HoodieRealtimeInputFormatUtils.cleanProjectionColumnIds(jobConf);
  }

  @Override
  public RecordReader<NullWritable, ArrayWritable> getRecordReader(final InputSplit split, final JobConf jobConf,
                                                                   final Reporter reporter) throws IOException {
    // sanity check
    ValidationUtils.checkArgument(split instanceof RealtimeSplit,
        "HoodieRealtimeRecordReader can only work on RealtimeSplit and not with " + split);
    RealtimeSplit realtimeSplit = (RealtimeSplit) split;
    addProjectionToJobConf(realtimeSplit, jobConf);
    LOG.info("Creating record reader with readCols :" + jobConf.get(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR)
        + ", Ids :" + jobConf.get(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR));

    // for log only split, set the parquet reader as empty.
    if (FSUtils.isLogFile(realtimeSplit.getPath())) {
      return new HoodieRealtimeRecordReader(realtimeSplit, jobConf, new HoodieEmptyRecordReader(realtimeSplit, jobConf));
    }
    return new HoodieRealtimeRecordReader(realtimeSplit, jobConf,
        super.getRecordReader(split, jobConf, reporter));
  }

  @Override
  public Configuration getConf() {
    return conf;
  }
}
