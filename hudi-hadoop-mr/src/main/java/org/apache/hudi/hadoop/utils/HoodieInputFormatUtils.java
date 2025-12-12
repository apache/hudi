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

import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.HoodieReaderConfig;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineUtils.HollowCommitHandling;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.table.view.TableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.TablePathUtils;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.TableNotFoundException;
import org.apache.hudi.hadoop.BootstrapBaseFileSplit;
import org.apache.hudi.hadoop.FileStatusWithBootstrapBaseFile;
import org.apache.hudi.hadoop.HoodieHFileInputFormat;
import org.apache.hudi.hadoop.HoodieParquetInputFormat;
import org.apache.hudi.hadoop.LocatedFileStatusWithBootstrapBaseFile;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.hadoop.realtime.HoodieHFileRealtimeInputFormat;
import org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat;
import org.apache.hudi.hadoop.realtime.HoodieRealtimeFileSplit;
import org.apache.hudi.hadoop.realtime.HoodieRealtimePath;
import org.apache.hudi.hadoop.realtime.RealtimeSplit;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.HoodieStorageUtils;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat;
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.hudi.common.config.HoodieCommonConfig.INCREMENTAL_READ_HANDLE_HOLLOW_COMMIT;
import static org.apache.hudi.common.config.HoodieMetadataConfig.DEFAULT_METADATA_ENABLE_FOR_READERS;
import static org.apache.hudi.common.config.HoodieMetadataConfig.ENABLE;
import static org.apache.hudi.common.table.HoodieTableMetaClient.METAFOLDER_NAME;
import static org.apache.hudi.common.table.timeline.TimelineUtils.handleHollowCommitIfNeeded;
import static org.apache.hudi.hadoop.fs.HadoopFSUtils.convertToStoragePath;

public class HoodieInputFormatUtils {

  // These positions have to be deterministic across all tables
  public static final int HOODIE_COMMIT_TIME_COL_POS = 0;
  public static final int HOODIE_RECORD_KEY_COL_POS = 2;
  public static final int HOODIE_PARTITION_PATH_COL_POS = 3;
  public static final String HOODIE_READ_COLUMNS_PROP = "hoodie.read.columns.set";

  private static final Logger LOG = LoggerFactory.getLogger(HoodieInputFormatUtils.class);

  public static FileInputFormat getInputFormat(HoodieFileFormat baseFileFormat, boolean realtime, Configuration conf) {
    switch (baseFileFormat) {
      case PARQUET:
        if (realtime) {
          HoodieParquetRealtimeInputFormat inputFormat = new HoodieParquetRealtimeInputFormat();
          inputFormat.setConf(conf);
          return inputFormat;
        } else {
          HoodieParquetInputFormat inputFormat = new HoodieParquetInputFormat();
          inputFormat.setConf(conf);
          return inputFormat;
        }
      case HFILE:
        if (realtime) {
          HoodieHFileRealtimeInputFormat inputFormat = new HoodieHFileRealtimeInputFormat();
          inputFormat.setConf(conf);
          return inputFormat;
        } else {
          HoodieHFileInputFormat inputFormat = new HoodieHFileInputFormat();
          inputFormat.setConf(conf);
          return inputFormat;
        }
      default:
        throw new HoodieIOException("Hoodie InputFormat not implemented for base file format " + baseFileFormat);
    }
  }

  public static String getInputFormatClassName(HoodieFileFormat baseFileFormat, boolean realtime, boolean usePreApacheFormat) {
    if (baseFileFormat.equals(HoodieFileFormat.PARQUET) && usePreApacheFormat) {
      // Parquet input format had an InputFormat class visible under the old naming scheme.
      return realtime
          ? com.uber.hoodie.hadoop.realtime.HoodieRealtimeInputFormat.class.getName()
          : com.uber.hoodie.hadoop.HoodieInputFormat.class.getName();
    }
    return getInputFormatClassName(baseFileFormat, realtime);
  }

  public static String getInputFormatClassName(HoodieFileFormat baseFileFormat, boolean realtime) {
    switch (baseFileFormat) {
      case PARQUET:
        if (realtime) {
          return HoodieParquetRealtimeInputFormat.class.getName();
        } else {
          return HoodieParquetInputFormat.class.getName();
        }
      case HFILE:
        if (realtime) {
          return HoodieHFileRealtimeInputFormat.class.getName();
        } else {
          return HoodieHFileInputFormat.class.getName();
        }
      case ORC:
        return OrcInputFormat.class.getName();
      default:
        throw new HoodieIOException("Hoodie InputFormat not implemented for base file format " + baseFileFormat);
    }
  }

  public static String getOutputFormatClassName(HoodieFileFormat baseFileFormat) {
    switch (baseFileFormat) {
      case PARQUET:
      case HFILE:
        return MapredParquetOutputFormat.class.getName();
      case ORC:
        return OrcOutputFormat.class.getName();
      default:
        throw new HoodieIOException("No OutputFormat for base file format " + baseFileFormat);
    }
  }

  public static String getSerDeClassName(HoodieFileFormat baseFileFormat) {
    switch (baseFileFormat) {
      case PARQUET:
      case HFILE:
        return ParquetHiveSerDe.class.getName();
      case ORC:
        return OrcSerde.class.getName();
      default:
        throw new HoodieIOException("No SerDe for base file format " + baseFileFormat);
    }
  }

  public static FileInputFormat getInputFormat(String path, boolean realtime, Configuration conf) {
    final String extension = FSUtils.getFileExtension(path);
    if (extension.equals(HoodieFileFormat.PARQUET.getFileExtension())) {
      return getInputFormat(HoodieFileFormat.PARQUET, realtime, conf);
    }
    if (extension.equals(HoodieFileFormat.HFILE.getFileExtension())) {
      return getInputFormat(HoodieFileFormat.HFILE, realtime, conf);
    }
    // now we support read log file, try to find log file
    if (HadoopFSUtils.isLogFile(new Path(path)) && realtime) {
      return getInputFormat(HoodieFileFormat.PARQUET, realtime, conf);
    }
    throw new HoodieIOException("Hoodie InputFormat not implemented for base file of type " + extension);
  }

  /**
   * Filter any specific instants that we do not want to process.
   * example timeline:
   * <p>
   * t0 -> create bucket1.parquet
   * t1 -> create and append updates bucket1.log
   * t2 -> request compaction
   * t3 -> create bucket2.parquet
   * <p>
   * if compaction at t2 takes a long time, incremental readers on RO tables can move to t3 and would skip updates in t1
   * <p>
   * To workaround this problem, we want to stop returning data belonging to commits > t2.
   * After compaction is complete, incremental reader would see updates in t2, t3, so on.
   *
   * @param timeline
   * @return
   */
  public static HoodieTimeline filterInstantsTimeline(HoodieTimeline timeline) {
    HoodieTimeline commitsAndCompactionTimeline = timeline.getWriteTimeline();
    Option<HoodieInstant> pendingCompactionInstant = commitsAndCompactionTimeline
        .filterPendingCompactionTimeline().firstInstant();
    if (pendingCompactionInstant.isPresent()) {
      HoodieTimeline instantsTimeline = commitsAndCompactionTimeline
          .findInstantsBefore(pendingCompactionInstant.get().requestedTime());
      int numCommitsFilteredByCompaction = commitsAndCompactionTimeline.getCommitsTimeline().countInstants()
          - instantsTimeline.getCommitsTimeline().countInstants();
      LOG.info("Earliest pending compaction instant is: {} skipping {} commits",
          pendingCompactionInstant.get().requestedTime(), numCommitsFilteredByCompaction);

      return instantsTimeline;
    } else {
      return timeline;
    }
  }

  /**
   * Extract partitions touched by the commitsToCheck.
   *
   * @param commitsToCheck
   * @param tableMetaClient
   * @param timeline
   * @param inputPaths
   * @return
   * @throws IOException
   */
  public static Option<String> getAffectedPartitions(List<HoodieInstant> commitsToCheck,
                                                     HoodieTableMetaClient tableMetaClient,
                                                     HoodieTimeline timeline,
                                                     List<Path> inputPaths) throws IOException {
    Set<String> partitionsToList = new HashSet<>();
    for (HoodieInstant commit : commitsToCheck) {
      HoodieCommitMetadata commitMetadata = timeline.readCommitMetadata(commit);
      partitionsToList.addAll(commitMetadata.getPartitionToWriteStats().keySet());
    }
    if (partitionsToList.isEmpty()) {
      return Option.empty();
    }
    String incrementalInputPaths = partitionsToList.stream()
        .map(s -> StringUtils.isNullOrEmpty(s) ? tableMetaClient.getBasePath().toString() : tableMetaClient.getBasePath() + StoragePath.SEPARATOR + s)
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
            if (path.toString().endsWith(s)) {
              return true;
            }
          }
          return false;
        })
        .collect(Collectors.joining(","));
    return StringUtils.isNullOrEmpty(incrementalInputPaths) ? Option.empty() : Option.of(incrementalInputPaths);
  }

  /**
   * Extract HoodieTimeline based on HoodieTableMetaClient.
   *
   * @param job
   * @param tableMetaClient
   * @return
   */
  public static Option<HoodieTimeline> getFilteredCommitsTimeline(JobContext job, HoodieTableMetaClient tableMetaClient) {
    String tableName = tableMetaClient.getTableConfig().getTableName();
    HoodieTimeline baseTimeline;
    if (HoodieHiveUtils.stopAtCompaction(job, tableName)) {
      baseTimeline = filterInstantsTimeline(tableMetaClient.getActiveTimeline());
    } else {
      baseTimeline = tableMetaClient.getActiveTimeline();
    }
    HollowCommitHandling handlingMode = HollowCommitHandling.valueOf(job.getConfiguration()
        .get(INCREMENTAL_READ_HANDLE_HOLLOW_COMMIT.key(), INCREMENTAL_READ_HANDLE_HOLLOW_COMMIT.defaultValue()));
    HoodieTimeline filteredTimeline = handleHollowCommitIfNeeded(
        baseTimeline.getCommitsTimeline().filterCompletedInstants(),
        tableMetaClient,
        handlingMode);

    return Option.of(filteredTimeline);
  }

  /**
   * Get commits for incremental query from Hive map reduce configuration.
   *
   * @param job
   * @param tableName
   * @param timeline
   * @return
   */
  public static Option<List<HoodieInstant>> getCommitsForIncrementalQuery(Job job, String tableName, HoodieTimeline timeline) {
    return Option.of(getHoodieTimelineForIncrementalQuery(job, tableName, timeline).getInstants());
  }

  /**
   * Get HoodieTimeline for incremental query from Hive map reduce configuration.
   *
   * @param job
   * @param tableName
   * @param timeline
   * @return
   */
  public static HoodieTimeline getHoodieTimelineForIncrementalQuery(JobContext job, String tableName, HoodieTimeline timeline) {
    String lastIncrementalTs = HoodieHiveUtils.readStartCommitTime(job, tableName);
    // Total number of commits to return in this batch. Set this to -1 to get all the commits.
    Integer maxCommits = HoodieHiveUtils.readMaxCommits(job, tableName);
    LOG.info("Last Incremental timestamp was set as {}", lastIncrementalTs);
    return timeline.findInstantsAfter(lastIncrementalTs, maxCommits);
  }

  /**
   * Extract HoodieTableMetaClient by partition path.
   *
   * @param conf       The hadoop conf
   * @param partitions The partitions
   * @return partition path to table meta client mapping
   */
  public static Map<Path, HoodieTableMetaClient> getTableMetaClientByPartitionPath(Configuration conf, Set<Path> partitions) {
    Map<Path, HoodieTableMetaClient> metaClientMap = new HashMap<>();
    return partitions.stream().collect(Collectors.toMap(Function.identity(), p -> {
      try {
        HoodieTableMetaClient metaClient = getTableMetaClientForBasePathUnchecked(conf, p);
        metaClientMap.put(p, metaClient);
        return metaClient;
      } catch (IOException e) {
        throw new HoodieIOException("Error creating hoodie meta client against : " + p, e);
      }
    }));
  }

  /**
   * Extract HoodieTableMetaClient from a partition path (not base path)
   */
  public static HoodieTableMetaClient getTableMetaClientForBasePathUnchecked(Configuration conf, Path partitionPath) throws IOException {
    Path baseDir = partitionPath;
    HoodieStorage storage = HoodieStorageUtils.getStorage(
        partitionPath.toString(), HadoopFSUtils.getStorageConf(conf));
    StoragePath partitionStoragePath = convertToStoragePath(partitionPath);
    if (HoodiePartitionMetadata.hasPartitionMetadata(storage,  partitionStoragePath)) {
      HoodiePartitionMetadata metadata = new HoodiePartitionMetadata(storage, partitionStoragePath);
      metadata.readFromFS();
      int levels = metadata.getPartitionDepth();
      baseDir = HoodieHiveUtils.getNthParent(partitionPath, levels);
    } else {
      for (int i = 0; i < partitionPath.depth(); i++) {
        if (storage.exists(new StoragePath(convertToStoragePath(baseDir), METAFOLDER_NAME))) {
          break;
        } else if (i == partitionPath.depth() - 1) {
          throw new TableNotFoundException(partitionPath.toString());
        } else {
          baseDir = baseDir.getParent();
        }
      }
    }
    LOG.info("Reading hoodie metadata from path {}", baseDir);
    return HoodieTableMetaClient.builder()
        .setConf(storage.getConf().newInstance()).setBasePath(baseDir.toString()).build();
  }

  public static FileStatus getFileStatus(HoodieBaseFile baseFile) throws IOException {
    FileStatus fileStatus = HadoopFSUtils.convertToHadoopFileStatus(baseFile.getPathInfo());
    if (baseFile.getBootstrapBaseFile().isPresent()) {
      if (fileStatus instanceof LocatedFileStatus) {
        return new LocatedFileStatusWithBootstrapBaseFile((LocatedFileStatus) fileStatus,
            HadoopFSUtils.convertToHadoopFileStatus(
                baseFile.getBootstrapBaseFile().get().getPathInfo()));
      } else {
        return new FileStatusWithBootstrapBaseFile(fileStatus,
            HadoopFSUtils.convertToHadoopFileStatus(
                baseFile.getBootstrapBaseFile().get().getPathInfo()));
      }
    }
    return fileStatus;
  }

  /**
   * Filter a list of FileStatus based on commitsToCheck for incremental view.
   *
   * @param job
   * @param tableMetaClient
   * @param timeline
   * @param fileStatuses
   * @param commitsToCheck
   * @return
   */
  public static List<FileStatus> filterIncrementalFileStatus(Job job, HoodieTableMetaClient tableMetaClient,
                                                             HoodieTimeline timeline, FileStatus[] fileStatuses, List<HoodieInstant> commitsToCheck) throws IOException {
    TableFileSystemView.BaseFileOnlyView roView = new HoodieTableFileSystemView(tableMetaClient, timeline,
        Arrays.stream(fileStatuses)
            .map(HadoopFSUtils::convertToStoragePathInfo)
            .collect(Collectors.toList()));
    List<String> commitsList = commitsToCheck.stream().map(HoodieInstant::requestedTime).collect(Collectors.toList());
    List<HoodieBaseFile> filteredFiles = roView.getLatestBaseFilesInRange(commitsList).collect(Collectors.toList());
    List<FileStatus> returns = new ArrayList<>();
    for (HoodieBaseFile filteredFile : filteredFiles) {
      LOG.debug("Processing incremental hoodie file - {}", filteredFile.getPath());
      filteredFile = refreshFileStatus(job.getConfiguration(), filteredFile);
      returns.add(getFileStatus(filteredFile));
    }
    LOG.info("Total paths to process after hoodie incremental filter {}", filteredFiles.size());
    return returns;
  }

  public static Map<HoodieTableMetaClient, List<Path>> groupSnapshotPathsByMetaClient(
      Collection<HoodieTableMetaClient> metaClientList,
      List<Path> snapshotPaths
  ) {
    Map<HoodieTableMetaClient, List<Path>> grouped = new HashMap<>();
    metaClientList.forEach(metaClient -> grouped.put(metaClient, new ArrayList<>()));
    for (Path path : snapshotPaths) {
      String inputPathStr = path.toString();
      // Find meta client associated with the input path
      Option<HoodieTableMetaClient> matchedMetaClient = Option.fromJavaOptional(metaClientList.stream()
          .filter(metaClient -> {
            String basePathStr = metaClient.getBasePath().toString();
            return inputPathStr.equals(basePathStr) || inputPathStr.startsWith(basePathStr + "/"); })
          .findFirst());
      matchedMetaClient.ifPresent(metaClient -> grouped.get(metaClient).add(path));
    }
    return grouped;
  }

  public static HoodieMetadataConfig buildMetadataConfig(Configuration conf) {
    return HoodieMetadataConfig.newBuilder()
        .enable(conf.getBoolean(ENABLE.key(), DEFAULT_METADATA_ENABLE_FOR_READERS))
        .build();
  }

  /**
   * Checks the file status for a race condition which can set the file size to 0. 1. HiveInputFormat does
   * super.listStatus() and gets back a FileStatus[] 2. Then it creates the HoodieTableMetaClient for the paths listed.
   * 3. Generation of splits looks at FileStatus size to create splits, which skips this file
   *
   * @param conf
   * @param dataFile
   * @return
   */
  private static HoodieBaseFile refreshFileStatus(Configuration conf, HoodieBaseFile dataFile) {
    StoragePath dataPath = dataFile.getPathInfo().getPath();
    try {
      if (dataFile.getFileSize() == 0) {
        HoodieStorage storage = HoodieStorageUtils.getStorage(dataPath, HadoopFSUtils.getStorageConf(conf));
        LOG.info("Refreshing file status {}", dataFile.getPath());
        return new HoodieBaseFile(storage.getPathInfo(dataPath),
            dataFile.getBootstrapBaseFile().orElse(null));
      }
      return dataFile;
    } catch (IOException e) {
      throw new HoodieIOException("Could not get FileStatus on path " + dataPath);
    }
  }

  /**
   * Iterate through a list of commit metadata in natural order, and extract the file status of
   * all affected files from the commits metadata grouping by file full path. If the files has
   * been touched multiple times in the given commits, the return value will keep the one
   * from the latest commit.
   *
   * @param basePath     The table base path
   * @param metadataList The metadata list to read the data from
   * @return the affected file status array
   */
  public static List<StoragePathInfo> listAffectedFilesForCommits(Configuration hadoopConf,
                                                                  StoragePath basePath,
                                                                  List<HoodieCommitMetadata> metadataList) {
    // TODO: Use HoodieMetaTable to extract affected file directly.
    HashMap<String, StoragePathInfo> fullPathToInfoMap = new HashMap<>();
    HoodieStorage storage = HoodieStorageUtils.getStorage(basePath, HadoopFSUtils.getStorageConf(hadoopConf));
    // Iterate through the given commits.
    for (HoodieCommitMetadata metadata : metadataList) {
      fullPathToInfoMap.putAll(metadata.getFullPathToInfo(storage, basePath.toString()));
    }
    return new ArrayList<>(fullPathToInfoMap.values());
  }

  public static HoodieRealtimeFileSplit createRealtimeFileSplit(HoodieRealtimePath path, long start, long length, String[] hosts) {
    try {
      return new HoodieRealtimeFileSplit(new FileSplit(path, start, length, hosts), path);
    } catch (IOException e) {
      throw new HoodieIOException(String.format("Failed to create instance of %s", HoodieRealtimeFileSplit.class.getName()), e);
    }
  }

  public static List<String> getPartitionFieldNames(JobConf jobConf) {
    String partitionFields = jobConf.get(hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS, "");
    return partitionFields.isEmpty() ? new ArrayList<>() : Arrays.stream(partitionFields.split("/")).collect(Collectors.toList());
  }

  public static String getTableBasePath(InputSplit split, JobConf jobConf) throws IOException {
    if (split instanceof RealtimeSplit) {
      RealtimeSplit realtimeSplit = (RealtimeSplit) split;
      return realtimeSplit.getBasePath();
    } else {
      Path inputPath = ((FileSplit) split).getPath();
      HoodieStorage storage = HoodieStorageUtils.getStorage(
              convertToStoragePath(inputPath), HadoopFSUtils.getStorageConf(jobConf));
      Option<StoragePath> tablePath = TablePathUtils.getTablePath(storage, convertToStoragePath(inputPath));
      return tablePath.get().toString();
    }
  }

  /**
   * `schema.on.read` and skip merge not implemented
   */
  public static boolean shouldUseFilegroupReader(final JobConf jobConf, final InputSplit split) {
    return jobConf.getBoolean(HoodieReaderConfig.FILE_GROUP_READER_ENABLED.key(), HoodieReaderConfig.FILE_GROUP_READER_ENABLED.defaultValue())
        && !jobConf.getBoolean(HoodieCommonConfig.SCHEMA_EVOLUTION_ENABLE.key(), HoodieCommonConfig.SCHEMA_EVOLUTION_ENABLE.defaultValue())
        && !(split instanceof BootstrapBaseFileSplit);
  }
}
