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

import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineUtils;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.hadoop.BootstrapBaseFileSplit;
import org.apache.hudi.hadoop.FileStatusWithBootstrapBaseFile;
import org.apache.hudi.hadoop.HoodieCopyOnWriteTableInputFormat;
import org.apache.hudi.hadoop.LocatedFileStatusWithBootstrapBaseFile;
import org.apache.hudi.hadoop.RealtimeFileStatus;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.hadoop.utils.HoodieInputFormatUtils;
import org.apache.hudi.hadoop.utils.HoodieRealtimeInputFormatUtils;
import org.apache.hudi.internal.schema.HoodieSchemaException;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.storage.StoragePathInfo;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SplitLocationInfo;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.util.ValidationUtils.checkState;
import static org.apache.hudi.hadoop.utils.HoodieInputFormatUtils.createRealtimeFileSplit;

/**
 * Base implementation of the Hive's {@link FileInputFormat} allowing for reading of Hudi's
 * Merge-on-Read (COW) tables in various configurations:
 *
 * <ul>
 *   <li>Snapshot mode: reading table's state as of particular timestamp (or instant, in Hudi's terms)</li>
 *   <li>Incremental mode: reading table's state as of particular timestamp (or instant, in Hudi's terms)</li>
 *   <li>External mode: reading non-Hudi partitions</li>
 * </ul>
 * <p>
 * NOTE: This class is invariant of the underlying file-format of the files being read
 */
public class HoodieMergeOnReadTableInputFormat extends HoodieCopyOnWriteTableInputFormat implements Configurable {

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    List<FileSplit> fileSplits = Arrays.stream(super.getSplits(job, numSplits))
        .map(is -> (FileSplit) is)
        .collect(Collectors.toList());

    return (containsIncrementalQuerySplits(fileSplits) ? filterIncrementalQueryFileSplits(fileSplits) : fileSplits)
        .toArray(new FileSplit[0]);
  }

  @Override
  protected FileStatus createFileStatusUnchecked(FileSlice fileSlice, Option<HoodieInstant> latestCompletedInstantOpt,
                                                 String tableBasePath, HoodieTableMetaClient metaClient) {
    Option<HoodieBaseFile> baseFileOpt = fileSlice.getBaseFile();
    Option<HoodieLogFile> latestLogFileOpt = fileSlice.getLatestLogFile();
    Stream<HoodieLogFile> logFiles = fileSlice.getLogFiles();

    // Check if we're reading a MOR table
    if (baseFileOpt.isPresent()) {
      return createRealtimeFileStatusUnchecked(baseFileOpt.get(), logFiles, tableBasePath, latestCompletedInstantOpt, getHoodieVirtualKeyInfo(metaClient));
    } else if (latestLogFileOpt.isPresent()) {
      return createRealtimeFileStatusUnchecked(latestLogFileOpt.get(), logFiles, tableBasePath, latestCompletedInstantOpt, getHoodieVirtualKeyInfo(metaClient));
    } else {
      throw new IllegalStateException("Invalid state: either base-file or log-file has to be present");
    }
  }

  /**
   * return non hoodie paths
   * @param job
   * @return
   * @throws IOException
   */
  @Override
  public FileStatus[] listStatusForNonHoodiePaths(JobConf job) throws IOException {
    FileStatus[] fileStatuses = doListStatus(job);
    List<FileStatus> result = new ArrayList<>();
    for (FileStatus fileStatus : fileStatuses) {
      String baseFilePath = fileStatus.getPath().toUri().toString();
      RealtimeFileStatus realtimeFileStatus = new RealtimeFileStatus(fileStatus, baseFilePath, new ArrayList<>(), false, Option.empty());
      result.add(realtimeFileStatus);
    }
    return result.toArray(new FileStatus[0]);
  }

  @Override
  protected boolean checkIfValidFileSlice(FileSlice fileSlice) {
    Option<HoodieBaseFile> baseFileOpt = fileSlice.getBaseFile();
    Option<HoodieLogFile> latestLogFileOpt = fileSlice.getLatestLogFile();

    if (baseFileOpt.isPresent() || latestLogFileOpt.isPresent()) {
      return true;
    } else {
      throw new IllegalStateException("Invalid state: either base-file or log-file has to be present for " + fileSlice.getFileId());
    }
  }

  /**
   * Keep the logic of mor_incr_view as same as spark datasource.
   * Step1: Get list of commits to be fetched based on start commit and max commits(for snapshot max commits is -1).
   * Step2: Get list of affected files status for these affected file status.
   * Step3: Construct HoodieTableFileSystemView based on those affected file status.
   * a. Filter affected partitions based on inputPaths.
   * b. Get list of fileGroups based on affected partitions by fsView.getAllFileGroups.
   * Step4: Set input paths based on filtered affected partition paths. changes that among original input paths passed to
   * this method. some partitions did not have commits as part of the trimmed down list of commits and hence we need this step.
   * Step5: Find candidate fileStatus, since when we get baseFileStatus from HoodieTableFileSystemView,
   * the BaseFileStatus will missing file size information.
   * We should use candidate fileStatus to update the size information for BaseFileStatus.
   * Step6: For every file group from step3(b)
   * Get 1st available base file from all file slices. then we use candidate file status to update the baseFileStatus,
   * and construct RealTimeFileStatus and add it to result along with log files.
   * If file group just has log files, construct RealTimeFileStatus and add it to result.
   * TODO: unify the incremental view code between hive/spark-sql and spark datasource
   */
  @Override
  protected List<FileStatus> listStatusForIncrementalMode(JobConf job,
                                                          HoodieTableMetaClient tableMetaClient,
                                                          List<Path> inputPaths,
                                                          String incrementalTableName) throws IOException {
    List<FileStatus> result = new ArrayList<>();
    Job jobContext = Job.getInstance(job);

    // step1
    Option<HoodieTimeline> timeline = HoodieInputFormatUtils.getFilteredCommitsTimeline(jobContext, tableMetaClient);
    if (!timeline.isPresent()) {
      return result;
    }
    HoodieTimeline commitsTimelineToReturn = HoodieInputFormatUtils.getHoodieTimelineForIncrementalQuery(jobContext, incrementalTableName, timeline.get());
    Option<List<HoodieInstant>> commitsToCheck = Option.of(commitsTimelineToReturn.getInstants());
    if (!commitsToCheck.isPresent()) {
      return result;
    }
    // step2
    commitsToCheck.get().sort(HoodieInstant::compareTo);
    List<HoodieCommitMetadata> metadataList = commitsToCheck
        .get().stream().map(instant -> {
          try {
            return TimelineUtils.getCommitMetadata(instant, commitsTimelineToReturn);
          } catch (IOException e) {
            throw new HoodieException(
                String.format("cannot get metadata for instant: %s", instant));
          }
        }).collect(Collectors.toList());

    // build fileGroup from fsView
    List<StoragePathInfo> affectedPathInfoList = HoodieInputFormatUtils
        .listAffectedFilesForCommits(job, tableMetaClient.getBasePath(),
            metadataList);
    // step3
    HoodieTableFileSystemView fsView = new HoodieTableFileSystemView(
        tableMetaClient, commitsTimelineToReturn, affectedPathInfoList);
    // build fileGroup from fsView
    Path basePath = new Path(tableMetaClient.getBasePath().toString());
    // filter affectedPartition by inputPaths
    List<String> affectedPartition =
        HoodieTableMetadataUtil.getWritePartitionPaths(metadataList).stream()
            .filter(k -> k.isEmpty() ? inputPaths.contains(basePath) :
                inputPaths.contains(new Path(basePath, k))).collect(Collectors.toList());
    if (affectedPartition.isEmpty()) {
      return result;
    }
    List<HoodieFileGroup> fileGroups = affectedPartition.stream()
        .flatMap(partitionPath -> fsView.getAllFileGroups(partitionPath))
        .collect(Collectors.toList());
    // step4
    setInputPaths(job, affectedPartition.stream()
        .map(p -> p.isEmpty() ? basePath.toString() : new Path(basePath, p).toString())
        .collect(Collectors.joining(",")));

    // step5
    // find all file status in partitionPaths.
    FileStatus[] fileStatuses = doListStatus(job);
    Map<String, FileStatus> candidateFileStatus = new HashMap<>();
    for (int i = 0; i < fileStatuses.length; i++) {
      String key = fileStatuses[i].getPath().toString();
      candidateFileStatus.put(key, fileStatuses[i]);
    }

    Option<HoodieVirtualKeyInfo> virtualKeyInfoOpt = getHoodieVirtualKeyInfo(tableMetaClient);
    String maxCommitTime = fsView.getLastInstant().get().requestedTime();
    // step6
    result.addAll(collectAllIncrementalFiles(fileGroups, maxCommitTime, basePath.toString(), candidateFileStatus, virtualKeyInfoOpt));
    return result;
  }

  @Override
  protected boolean isSplitable(FileSystem fs, Path filename) {
    if (filename instanceof HoodieRealtimePath) {
      return ((HoodieRealtimePath) filename).isSplitable();
    }

    return super.isSplitable(fs, filename);
  }

  // make split for path.
  // When query the incremental view, the read files may be bootstrap files, we wrap those bootstrap files into
  // PathWithLogFilePath, so those bootstrap files should be processed int this function.
  @Override
  protected FileSplit makeSplit(Path file, long start, long length, String[] hosts) {
    if (file instanceof HoodieRealtimePath) {
      return doMakeSplitForRealtimePath((HoodieRealtimePath) file, start, length, hosts, null);
    }
    return super.makeSplit(file, start, length, hosts);
  }

  @Override
  protected FileSplit makeSplit(Path file, long start, long length, String[] hosts, String[] inMemoryHosts) {
    if (file instanceof HoodieRealtimePath) {
      return doMakeSplitForRealtimePath((HoodieRealtimePath) file, start, length, hosts, inMemoryHosts);
    }
    return super.makeSplit(file, start, length, hosts, inMemoryHosts);
  }

  private static List<FileStatus> collectAllIncrementalFiles(List<HoodieFileGroup> fileGroups,
                                                             String maxCommitTime,
                                                             String basePath,
                                                             Map<String, FileStatus> candidateFileStatus,
                                                             Option<HoodieVirtualKeyInfo> virtualKeyInfoOpt) {

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
          List<HoodieLogFile> deltaLogFiles = f.getLatestFileSlice().get().getLogFiles().collect(Collectors.toList());
          // We cannot use baseFileStatus.getPath() here, since baseFileStatus.getPath() missing file size information.
          // So we use candidateFileStatus.get(baseFileStatus.getPath()) to get a correct path.
          RealtimeFileStatus fileStatus = new RealtimeFileStatus(candidateFileStatus.get(baseFilePath),
              basePath, deltaLogFiles, true, virtualKeyInfoOpt);
          fileStatus.setMaxCommitTime(maxCommitTime);
          if (baseFileStatus instanceof LocatedFileStatusWithBootstrapBaseFile || baseFileStatus instanceof FileStatusWithBootstrapBaseFile) {
            fileStatus.setBootStrapFileStatus(baseFileStatus);
          }
          result.add(fileStatus);
        }
        // add file group which has only logs.
        if (f.getLatestFileSlice().isPresent() && baseFiles.isEmpty()) {
          List<StoragePathInfo> logPathInfoList = f.getLatestFileSlice().get().getLogFiles()
              .map(logFile -> logFile.getPathInfo()).collect(Collectors.toList());
          if (logPathInfoList.size() > 0) {
            List<HoodieLogFile> deltaLogFiles = logPathInfoList.stream()
                .map(l -> new HoodieLogFile(l.getPath(), l.getLength())).collect(Collectors.toList());
            RealtimeFileStatus fileStatus = new RealtimeFileStatus(
                HadoopFSUtils.convertToHadoopFileStatus(logPathInfoList.get(0)), basePath,
                deltaLogFiles, true, virtualKeyInfoOpt);
            fileStatus.setMaxCommitTime(maxCommitTime);
            result.add(fileStatus);
          }
        }
      } catch (IOException e) {
        throw new HoodieException("Error obtaining data file/log file grouping ", e);
      }
    });
    return result;
  }

  private FileSplit doMakeSplitForRealtimePath(HoodieRealtimePath path, long start, long length, String[] hosts, String[] inMemoryHosts) {
    if (path.includeBootstrapFilePath()) {
      FileSplit bf =
          inMemoryHosts == null
              ? super.makeSplit(path.getPathWithBootstrapFileStatus(), start, length, hosts)
              : super.makeSplit(path.getPathWithBootstrapFileStatus(), start, length, hosts, inMemoryHosts);
      return createRealtimeBootstrapBaseFileSplit(
          (BootstrapBaseFileSplit) bf,
          path.getBasePath(),
          path.getDeltaLogFiles(),
          path.getMaxCommitTime(),
          path.getBelongsToIncrementalQuery(),
          path.getVirtualKeyInfo()
      );
    }

    return createRealtimeFileSplit(path, start, length, hosts);
  }

  private static boolean containsIncrementalQuerySplits(List<FileSplit> fileSplits) {
    return fileSplits.stream().anyMatch(HoodieRealtimeInputFormatUtils::doesBelongToIncrementalQuery);
  }

  private static List<FileSplit> filterIncrementalQueryFileSplits(List<FileSplit> fileSplits) {
    return fileSplits.stream().filter(HoodieRealtimeInputFormatUtils::doesBelongToIncrementalQuery)
        .collect(Collectors.toList());
  }

  private static HoodieRealtimeBootstrapBaseFileSplit createRealtimeBootstrapBaseFileSplit(BootstrapBaseFileSplit split,
                                                                                           String basePath,
                                                                                           List<HoodieLogFile> logFiles,
                                                                                           String maxInstantTime,
                                                                                           boolean belongsToIncrementalQuery,
                                                                                           Option<HoodieVirtualKeyInfo> virtualKeyInfoOpt) {
    try {
      String[] hosts = split.getLocationInfo() != null ? Arrays.stream(split.getLocationInfo())
          .filter(x -> !x.isInMemory()).toArray(String[]::new) : new String[0];
      String[] inMemoryHosts = split.getLocationInfo() != null ? Arrays.stream(split.getLocationInfo())
          .filter(SplitLocationInfo::isInMemory).toArray(String[]::new) : new String[0];
      FileSplit baseSplit = new FileSplit(split.getPath(), split.getStart(), split.getLength(),
          hosts, inMemoryHosts);
      return new HoodieRealtimeBootstrapBaseFileSplit(baseSplit, basePath, logFiles, maxInstantTime, split.getBootstrapFileSplit(),
          belongsToIncrementalQuery, virtualKeyInfoOpt);
    } catch (IOException e) {
      throw new HoodieIOException("Error creating hoodie real time split ", e);
    }
  }

  /**
   * Creates {@link RealtimeFileStatus} for the file-slice where base file is present
   */
  private static RealtimeFileStatus createRealtimeFileStatusUnchecked(HoodieBaseFile baseFile,
                                                                      Stream<HoodieLogFile> logFiles,
                                                                      String basePath,
                                                                      Option<HoodieInstant> latestCompletedInstantOpt,
                                                                      Option<HoodieVirtualKeyInfo> virtualKeyInfoOpt) {
    FileStatus baseFileStatus = getFileStatusUnchecked(baseFile);
    List<HoodieLogFile> sortedLogFiles = logFiles.sorted(HoodieLogFile.getLogFileComparator()).collect(Collectors.toList());

    try {
      RealtimeFileStatus rtFileStatus = new RealtimeFileStatus(baseFileStatus, basePath, sortedLogFiles,
          false, virtualKeyInfoOpt);

      if (latestCompletedInstantOpt.isPresent()) {
        HoodieInstant latestCompletedInstant = latestCompletedInstantOpt.get();
        checkState(latestCompletedInstant.isCompleted());

        rtFileStatus.setMaxCommitTime(latestCompletedInstant.requestedTime());
      }

      if (baseFileStatus instanceof LocatedFileStatusWithBootstrapBaseFile || baseFileStatus instanceof FileStatusWithBootstrapBaseFile) {
        rtFileStatus.setBootStrapFileStatus(baseFileStatus);
      }

      return rtFileStatus;
    } catch (IOException e) {
      throw new HoodieIOException(String.format("Failed to init %s", RealtimeFileStatus.class.getSimpleName()), e);
    }
  }

  /**
   * Creates {@link RealtimeFileStatus} for the file-slice where base file is NOT present
   */
  private static RealtimeFileStatus createRealtimeFileStatusUnchecked(HoodieLogFile latestLogFile,
                                                                      Stream<HoodieLogFile> logFiles,
                                                                      String basePath,
                                                                      Option<HoodieInstant> latestCompletedInstantOpt,
                                                                      Option<HoodieVirtualKeyInfo> virtualKeyInfoOpt) {
    List<HoodieLogFile> sortedLogFiles = logFiles.sorted(HoodieLogFile.getLogFileComparator()).collect(Collectors.toList());
    try {
      RealtimeFileStatus rtFileStatus = new RealtimeFileStatus(
          HadoopFSUtils.convertToHadoopFileStatus(latestLogFile.getPathInfo()), basePath,
          sortedLogFiles, false, virtualKeyInfoOpt);

      if (latestCompletedInstantOpt.isPresent()) {
        HoodieInstant latestCompletedInstant = latestCompletedInstantOpt.get();
        checkState(latestCompletedInstant.isCompleted());

        rtFileStatus.setMaxCommitTime(latestCompletedInstant.requestedTime());
      }

      return rtFileStatus;
    } catch (IOException e) {
      throw new HoodieIOException(String.format("Failed to init %s", RealtimeFileStatus.class.getSimpleName()), e);
    }
  }

  private static Option<HoodieVirtualKeyInfo> getHoodieVirtualKeyInfo(HoodieTableMetaClient metaClient) {
    HoodieTableConfig tableConfig = metaClient.getTableConfig();
    if (tableConfig.populateMetaFields()) {
      return Option.empty();
    }
    TableSchemaResolver tableSchemaResolver = new TableSchemaResolver(metaClient);
    try {
      HoodieSchema schema = tableSchemaResolver.getTableSchema();
      String partitionFieldProp = tableConfig.getPartitionFieldProp();
      boolean isNonPartitionedKeyGen = StringUtils.isNullOrEmpty(partitionFieldProp);
      return Option.of(
          new HoodieVirtualKeyInfo(
              tableConfig.getRecordKeyFieldProp(),
              isNonPartitionedKeyGen ? Option.empty() : Option.of(partitionFieldProp),
              schema.getField(tableConfig.getRecordKeyFieldProp())
                  .orElseThrow(() -> new HoodieSchemaException("Field: " + partitionFieldProp + " not found"))
                  .pos(),
              isNonPartitionedKeyGen ? Option.empty() : Option.of(schema.getField(partitionFieldProp)
                  .orElseThrow(() -> new HoodieSchemaException("Field: " + partitionFieldProp + " not found"))
                  .pos())));
    } catch (Exception exception) {
      throw new HoodieException("Fetching table schema failed with exception ", exception);
    }
  }
}
