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

package org.apache.hudi.table.action.rollback;

import org.apache.hudi.avro.model.HoodieRollbackRequest;
import org.apache.hudi.common.HoodieRollbackStat;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.function.SerializableFunction;
import org.apache.hudi.common.function.SerializablePairFunction;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.HoodieLogFileWriteCallback;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.block.HoodieCommandBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieRollbackException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.HoodieStorageUtils;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.marker.WriteMarkers;
import org.apache.hudi.table.marker.WriteMarkersFactory;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Contains common methods to be used across engines for rollback operation.
 */
public class BaseRollbackHelper implements Serializable {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(BaseRollbackHelper.class);
  protected static final String EMPTY_STRING = "";

  protected final HoodieTable table;
  protected final HoodieTableMetaClient metaClient;
  protected final HoodieWriteConfig config;

  public BaseRollbackHelper(HoodieTable table, HoodieWriteConfig config) {
    this.table = table;
    this.metaClient = table.getMetaClient();
    this.config = config;
  }

  /**
   * Performs all rollback actions that we have collected in parallel.
   */
  public List<HoodieRollbackStat> performRollback(HoodieEngineContext context, String instantTime, HoodieInstant instantToRollback,
                                                  List<HoodieRollbackRequest> rollbackRequests) {
    int parallelism = Math.max(Math.min(rollbackRequests.size(), config.getRollbackParallelism()), 1);
    context.setJobStatus(this.getClass().getSimpleName(), "Perform rollback actions: " + config.getTableName());
    // If not for conversion to HoodieRollbackInternalRequests, code fails. Using avro model (HoodieRollbackRequest) within spark.parallelize
    // is failing with com.esotericsoftware.kryo.KryoException
    // stack trace: https://gist.github.com/nsivabalan/b6359e7d5038484f8043506c8bc9e1c8
    // related stack overflow post: https://issues.apache.org/jira/browse/SPARK-3601. Avro deserializes list as GenericData.Array.
    List<SerializableHoodieRollbackRequest> serializableRequests = rollbackRequests.stream().map(SerializableHoodieRollbackRequest::new).collect(Collectors.toList());
    WriteMarkers markers = WriteMarkersFactory.get(config.getMarkersType(), table, instantTime);

    // Considering rollback may failed before, which generated some additional log files. We need to add these log files back.
    // rollback markers are added under rollback instant itself.
    Set<String> logPaths = new HashSet<>();
    try {
      logPaths = markers.getAppendedLogPaths(context, config.getFinalizeWriteParallelism());
    } catch (FileNotFoundException fnf) {
      LOG.warn("Rollback never failed and hence no marker dir was found. Safely moving on");
    } catch (IOException e) {
      throw new HoodieRollbackException("Failed to list log file markers for previous attempt of rollback ", e);
    }

    List<Pair<String, HoodieRollbackStat>> getRollbackStats = maybeDeleteAndCollectStats(context, instantTime, instantToRollback, serializableRequests, true, parallelism);
    List<HoodieRollbackStat> mergedRollbackStatByPartitionPath = context.reduceByKey(getRollbackStats, RollbackUtils::mergeRollbackStat, parallelism);
    return addLogFilesFromPreviousFailedRollbacksToStat(context, mergedRollbackStatByPartitionPath, logPaths);
  }

  /**
   * Collect all file info that needs to be rolled back.
   */
  public List<HoodieRollbackStat> collectRollbackStats(HoodieEngineContext context, String instantTime, HoodieInstant instantToRollback,
                                                       List<HoodieRollbackRequest> rollbackRequests) {
    int parallelism = Math.max(Math.min(rollbackRequests.size(), config.getRollbackParallelism()), 1);
    context.setJobStatus(this.getClass().getSimpleName(), "Collect rollback stats for upgrade/downgrade: " + config.getTableName());
    // If not for conversion to HoodieRollbackInternalRequests, code fails. Using avro model (HoodieRollbackRequest) within spark.parallelize
    // is failing with com.esotericsoftware.kryo.KryoException
    // stack trace: https://gist.github.com/nsivabalan/b6359e7d5038484f8043506c8bc9e1c8
    // related stack overflow post: https://issues.apache.org/jira/browse/SPARK-3601. Avro deserializes list as GenericData.Array.
    List<SerializableHoodieRollbackRequest> serializableRequests = rollbackRequests.stream().map(SerializableHoodieRollbackRequest::new).collect(Collectors.toList());
    return context.reduceByKey(maybeDeleteAndCollectStats(context, instantTime, instantToRollback, serializableRequests, false, parallelism),
        RollbackUtils::mergeRollbackStat, parallelism);
  }

  /**
   * May be delete interested files and collect stats or collect stats only.
   *
   * @param context           instance of {@link HoodieEngineContext} to use.
   * @param instantToRollback {@link HoodieInstant} of interest for which deletion or collect stats is requested.
   * @param rollbackRequests  List of {@link ListingBasedRollbackRequest} to be operated on.
   * @param doDelete          {@code true} if deletion has to be done. {@code false} if only stats are to be collected w/o performing any deletes.
   * @return stats collected with or w/o actual deletions.
   */
  List<Pair<String, HoodieRollbackStat>> maybeDeleteAndCollectStats(HoodieEngineContext context,
                                                                    String instantTime,
                                                                    HoodieInstant instantToRollback,
                                                                    List<SerializableHoodieRollbackRequest> rollbackRequests,
                                                                    boolean doDelete, int numPartitions) {
    return context.flatMap(rollbackRequests, (SerializableFunction<SerializableHoodieRollbackRequest, Stream<Pair<String, HoodieRollbackStat>>>) rollbackRequest -> {
      List<String> filesToBeDeleted = rollbackRequest.getFilesToBeDeleted();
      if (!filesToBeDeleted.isEmpty()) {
        List<HoodieRollbackStat> rollbackStats = deleteFiles(metaClient, filesToBeDeleted, doDelete);
        List<Pair<String, HoodieRollbackStat>> partitionToRollbackStats = new ArrayList<>();
        rollbackStats.forEach(entry -> partitionToRollbackStats.add(Pair.of(entry.getPartitionPath(), entry)));
        return partitionToRollbackStats.stream();
      } else if (!rollbackRequest.getLogBlocksToBeDeleted().isEmpty()) {
        HoodieLogFormat.Writer writer = null;
        final StoragePath filePath;
        try {
          String partitionPath = rollbackRequest.getPartitionPath();
          String fileId = rollbackRequest.getFileId();
          String latestBaseInstant = rollbackRequest.getLatestBaseInstant();

          // Let's emit markers for rollback as well. markers are emitted under rollback instant time.
          WriteMarkers writeMarkers = WriteMarkersFactory.get(config.getMarkersType(), table, instantTime);

          writer = HoodieLogFormat.newWriterBuilder()
              .onParentPath(FSUtils.constructAbsolutePath(metaClient.getBasePath(), partitionPath))
              .withFileId(fileId)
              .overBaseCommit(latestBaseInstant)
              .withStorage(metaClient.getStorage())
              .withLogWriteCallback(getRollbackLogMarkerCallback(writeMarkers, partitionPath, fileId))
              .withFileExtension(HoodieLogFile.DELTA_EXTENSION).build();

          // generate metadata
          if (doDelete) {
            Map<HoodieLogBlock.HeaderMetadataType, String> header = generateHeader(instantToRollback.getTimestamp());
            // if update belongs to an existing log file
            // use the log file path from AppendResult in case the file handle may roll over
            filePath = writer.appendBlock(new HoodieCommandBlock(header)).logFile().getPath();
          } else {
            filePath = writer.getLogFile().getPath();
          }
        } catch (IOException | InterruptedException io) {
          throw new HoodieRollbackException("Failed to rollback for instant " + instantToRollback, io);
        } finally {
          try {
            if (writer != null) {
              writer.close();
            }
          } catch (IOException io) {
            throw new HoodieIOException("Error appending rollback block", io);
          }
        }

        // This step is intentionally done after writer is closed. Guarantees that
        // getFileStatus would reflect correct stats and FileNotFoundException is not thrown in
        // cloud-storage : HUDI-168
        Map<StoragePathInfo, Long> filesToNumBlocksRollback = Collections.singletonMap(
            metaClient.getStorage().getPathInfo(Objects.requireNonNull(filePath)),
            1L
        );

        // With listing based rollback, sometimes we only get the fileID of interest(so that we can add rollback command block) w/o the actual file name.
        // So, we want to ignore such invalid files from this list before we add it to the rollback stats.
        String partitionFullPath = FSUtils.constructAbsolutePath(metaClient.getBasePath(), rollbackRequest.getPartitionPath()).toString();
        Map<String, Long> validLogBlocksToDelete = new HashMap<>();
        rollbackRequest.getLogBlocksToBeDeleted().entrySet().stream().forEach((kv) -> {
          String logFileFullPath = kv.getKey();
          String logFileName = logFileFullPath.replace(partitionFullPath, "");
          if (!StringUtils.isNullOrEmpty(logFileName)) {
            validLogBlocksToDelete.put(kv.getKey(), kv.getValue());
          }
        });

        return Collections.singletonList(
                Pair.of(rollbackRequest.getPartitionPath(),
                    HoodieRollbackStat.newBuilder()
                        .withPartitionPath(rollbackRequest.getPartitionPath())
                        .withRollbackBlockAppendResults(filesToNumBlocksRollback)
                        .withLogFilesFromFailedCommit(validLogBlocksToDelete)
                        .build()))
            .stream();
      } else {
        return Collections.singletonList(
                Pair.of(rollbackRequest.getPartitionPath(),
                    HoodieRollbackStat.newBuilder()
                        .withPartitionPath(rollbackRequest.getPartitionPath())
                        .build()))
            .stream();
      }
    }, numPartitions);
  }

  private HoodieLogFileWriteCallback getRollbackLogMarkerCallback(final WriteMarkers writeMarkers, String partitionPath, String fileId) {
    return new HoodieLogFileWriteCallback() {
      @Override
      public boolean preLogFileOpen(HoodieLogFile logFileToAppend) {
        // there may be existed marker file if fs support append. So always return true;
        createAppendMarker(logFileToAppend);
        return true;
      }

      @Override
      public boolean preLogFileCreate(HoodieLogFile logFileToCreate) {
        return createAppendMarker(logFileToCreate);
      }

      private boolean createAppendMarker(HoodieLogFile logFileToAppend) {
        return writeMarkers.createIfNotExists(partitionPath, logFileToAppend.getFileName(), IOType.APPEND,
            config, fileId, metaClient.getActiveTimeline()).isPresent();
      }
    };
  }

  /**
   * If there are log files created by previous rollback attempts, we want to add them to rollback stats so that MDT is able to track them.
   * @param context HoodieEngineContext
   * @param originalRollbackStats original rollback stats
   * @param logPaths log paths due to failed rollback attempts
   * @return
   */
  private List<HoodieRollbackStat> addLogFilesFromPreviousFailedRollbacksToStat(HoodieEngineContext context,
                                                                                List<HoodieRollbackStat> originalRollbackStats,
                                                                                Set<String> logPaths) {
    if (logPaths.isEmpty()) {
      // if rollback is not failed and re-attempted, we should not find any additional log files here.
      return originalRollbackStats;
    }

    final String basePathStr = metaClient.getBasePath().toString();
    List<String> logFiles = new ArrayList<>(logPaths);
    // populate partitionPath -> List<log file name>
    HoodiePairData<String, List<String>> partitionPathToLogFilesHoodieData = populatePartitionToLogFilesHoodieData(context, basePathStr, logFiles);

    // populate partitionPath -> HoodieRollbackStat
    HoodiePairData<String, HoodieRollbackStat> partitionPathToRollbackStatsHoodieData =
        context.parallelize(originalRollbackStats)
            .mapToPair((SerializablePairFunction<HoodieRollbackStat, String, HoodieRollbackStat>) t -> Pair.of(t.getPartitionPath(), t));

    StorageConfiguration<?> storageConf = context.getStorageConf();

    // lets do left outer join and append missing log files to HoodieRollbackStat for each partition path.
    List<HoodieRollbackStat> finalRollbackStats = addMissingLogFilesAndGetRollbackStats(partitionPathToRollbackStatsHoodieData,
        partitionPathToLogFilesHoodieData, basePathStr, storageConf);
    return finalRollbackStats;
  }

  private HoodiePairData<String, List<String>> populatePartitionToLogFilesHoodieData(HoodieEngineContext context, String basePathStr, List<String> logFiles) {
    return context.parallelize(logFiles)
        // lets map each log file to partition path and log file name
        .mapToPair((SerializablePairFunction<String, String, String>) t -> {
          Path logFilePath = new Path(basePathStr, t);
          String partitionPath = HadoopFSUtils.getRelativePartitionPath(new Path(basePathStr), logFilePath.getParent());
          return Pair.of(partitionPath, logFilePath.getName());
        })
        // lets group by partition path and collect it as log file list per partition path
        .groupByKey().mapToPair((SerializablePairFunction<Pair<String, Iterable<String>>, String, List<String>>) t -> {
          List<String> allFiles = new ArrayList<>();
          t.getRight().forEach(entry -> allFiles.add(entry));
          return Pair.of(t.getKey(), allFiles);
        });
  }

  /**
   * Add missing log files to HoodieRollbackStat for each partition path. Performs a left outer join on the partition
   * key between partitionPathToRollbackStatsHoodieData and partitionPathToLogFilesHoodieData to add the rollback
   * stats for missing log files.
   *
   * @param partitionPathToRollbackStatsHoodieData HoodieRollbackStat by partition path
   * @param partitionPathToLogFilesHoodieData list of missing log files by partition path
   * @param basePathStr base path
   * @param storageConf storage configuration
   * @return
   */
  private List<HoodieRollbackStat> addMissingLogFilesAndGetRollbackStats(HoodiePairData<String, HoodieRollbackStat> partitionPathToRollbackStatsHoodieData,
                                                                         HoodiePairData<String, List<String>> partitionPathToLogFilesHoodieData,
                                                                         String basePathStr, StorageConfiguration<?> storageConf) {
    return partitionPathToRollbackStatsHoodieData
        .leftOuterJoin(partitionPathToLogFilesHoodieData)
        .map((SerializableFunction<Pair<String, Pair<HoodieRollbackStat, Option<List<String>>>>, HoodieRollbackStat>) v1 -> {
          if (v1.getValue().getValue().isPresent()) {

            String partition = v1.getKey();
            HoodieRollbackStat rollbackStat = v1.getValue().getKey();
            List<String> missingLogFiles = v1.getValue().getRight().get();

            // fetch file sizes.
            StoragePath fullPartitionPath = StringUtils.isNullOrEmpty(partition) ? new StoragePath(basePathStr) : new StoragePath(basePathStr, partition);
            HoodieStorage storage = HoodieStorageUtils.getStorage(fullPartitionPath, storageConf);
            List<Option<StoragePathInfo>> pathInfoOptList = FSUtils.getPathInfoUnderPartition(storage,
                fullPartitionPath, new HashSet<>(missingLogFiles), true);
            List<StoragePathInfo> pathInfoList = pathInfoOptList.stream().filter(fileStatusOption -> fileStatusOption.isPresent())
                .map(fileStatusOption -> fileStatusOption.get()).collect(Collectors.toList());

            HashMap<StoragePathInfo, Long> commandBlocksCount = new HashMap<>(rollbackStat.getCommandBlocksCount());
            pathInfoList.forEach(pathInfo -> commandBlocksCount.put(pathInfo, pathInfo.getLength()));

            return new HoodieRollbackStat(
                rollbackStat.getPartitionPath(),
                rollbackStat.getSuccessDeleteFiles(),
                rollbackStat.getFailedDeleteFiles(),
                commandBlocksCount,
                rollbackStat.getLogFilesFromFailedCommit());
          } else {
            return v1.getValue().getKey();
          }
        }).collectAsList();
  }

  /**
   * Common method used for cleaning out files during rollback.
   */
  protected List<HoodieRollbackStat> deleteFiles(HoodieTableMetaClient metaClient, List<String> filesToBeDeleted, boolean doDelete) throws IOException {
    return filesToBeDeleted.stream().map(fileToDelete -> {
      String basePath = metaClient.getBasePath().toString();
      try {
        Path fullDeletePath = new Path(fileToDelete);
        String partitionPath = HadoopFSUtils.getRelativePartitionPath(new Path(basePath), fullDeletePath.getParent());
        boolean isDeleted = true;
        if (doDelete) {
          try {
            isDeleted = ((FileSystem) metaClient.getStorage().getFileSystem()).delete(fullDeletePath);
          } catch (FileNotFoundException e) {
            // if first rollback attempt failed and retried again, chances that some files are already deleted.
            isDeleted = true;
          }
        }
        return HoodieRollbackStat.newBuilder()
            .withPartitionPath(partitionPath)
            .withDeletedFileResult(fullDeletePath.toString(), isDeleted)
            .build();
      } catch (IOException e) {
        LOG.error("Fetching file status for ");
        throw new HoodieIOException("Fetching file status for " + fileToDelete + " failed ", e);
      }
    }).collect(Collectors.toList());
  }

  protected Map<HoodieLogBlock.HeaderMetadataType, String> generateHeader(String commit) {
    // generate metadata
    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>(3);
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, metaClient.getActiveTimeline().lastInstant().get().getTimestamp());
    header.put(HoodieLogBlock.HeaderMetadataType.TARGET_INSTANT_TIME, commit);
    header.put(HoodieLogBlock.HeaderMetadataType.COMMAND_BLOCK_TYPE,
        String.valueOf(HoodieCommandBlock.HoodieCommandBlockTypeEnum.ROLLBACK_BLOCK.ordinal()));
    return header;
  }
}
