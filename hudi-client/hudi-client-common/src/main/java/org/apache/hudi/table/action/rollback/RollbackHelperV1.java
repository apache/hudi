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
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.function.SerializableFunction;
import org.apache.hudi.common.function.SerializablePairFunction;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.LogFileCreationCallback;
import org.apache.hudi.common.table.log.block.HoodieCommandBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieRollbackException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.HoodieStorageUtils;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.storage.StorageSchemes;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.marker.AppendMarkerHandler;
import org.apache.hudi.table.marker.WriteMarkers;
import org.apache.hudi.table.marker.WriteMarkersFactory;
import org.apache.hudi.util.CommonClientUtils;

import lombok.extern.slf4j.Slf4j;

import java.io.FileNotFoundException;
import java.io.IOException;
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

import static org.apache.hudi.table.action.rollback.RollbackUtils.groupSerializableRollbackRequestsBasedOnFileGroup;

/**
 * Contains common methods to be used across engines for rollback operation.
 * This class is meant to be used only for table version 6. Any table version 8 and above will be using {@link RollbackHelper}.
 */
@Slf4j
public class RollbackHelperV1 extends RollbackHelper {

  public RollbackHelperV1(HoodieTable table, HoodieWriteConfig config) {
    super(table, config);
  }

  /**
   * Get all the files in the given partition path.
   *
   * @param storage Hoodie Storage
   * @param partitionPathIncludeBasePath The full partition path including the base path
   * @param filesNamesUnderThisPartition The names of the files under this partition for which file status is needed
   * @param ignoreMissingFiles If true, missing files will be ignored and empty Option will be added to the result list
   * @return List of file statuses for the files under this partition
   */
  public static List<Option<StoragePathInfo>> getPathInfoUnderPartition(HoodieStorage storage,
                                                                        StoragePath partitionPathIncludeBasePath,
                                                                        Set<String> filesNamesUnderThisPartition,
                                                                        boolean ignoreMissingFiles) {
    String storageScheme = storage.getScheme();
    boolean useListStatus = StorageSchemes.isListStatusFriendly(storageScheme);
    List<Option<StoragePathInfo>> result = new ArrayList<>(filesNamesUnderThisPartition.size());
    try {
      if (useListStatus) {
        List<StoragePathInfo> storagePathInfos = storage.listDirectEntries(partitionPathIncludeBasePath,
            path -> filesNamesUnderThisPartition.contains(path.getName()));
        Map<String, StoragePathInfo> filenameToPathInfoMap = storagePathInfos.stream()
            .collect(Collectors.toMap(
                storagePathInfo -> storagePathInfo.getPath().getName(),
                storagePathInfo -> storagePathInfo
            ));

        for (String fileName : filesNamesUnderThisPartition) {
          if (filenameToPathInfoMap.containsKey(fileName)) {
            result.add(Option.of(filenameToPathInfoMap.get(fileName)));
          } else {
            if (!ignoreMissingFiles) {
              throw new FileNotFoundException("File not found: " + new StoragePath(partitionPathIncludeBasePath.toString(), fileName));
            }
            result.add(Option.empty());
          }
        }
      } else {
        for (String fileName : filesNamesUnderThisPartition) {
          StoragePath fullPath = new StoragePath(partitionPathIncludeBasePath.toString(), fileName);
          try {
            StoragePathInfo storagePathInfo = storage.getPathInfo(fullPath);
            result.add(Option.of(storagePathInfo));
          } catch (FileNotFoundException fileNotFoundException) {
            if (ignoreMissingFiles) {
              result.add(Option.empty());
            } else {
              throw new FileNotFoundException("File not found: " + fullPath.toString());
            }
          }
        }
      }
    } catch (IOException e) {
      throw new HoodieIOException("List files under " + partitionPathIncludeBasePath + " failed", e);
    }
    return result;
  }

  /**
   * Performs all rollback actions that we have collected in parallel.
   */
  @Override
  public List<HoodieRollbackStat> performRollback(HoodieEngineContext context, String instantTime, HoodieInstant instantToRollback,
                                                  List<HoodieRollbackRequest> rollbackRequests) {
    int parallelism = Math.max(Math.min(rollbackRequests.size(), config.getRollbackParallelism()), 1);
    context.setJobStatus(this.getClass().getSimpleName(), "Perform rollback actions: " + config.getTableName());
    // If not for conversion to HoodieRollbackInternalRequests, code fails. Using avro model (HoodieRollbackRequest) within spark.parallelize
    // is failing with com.esotericsoftware.kryo.KryoException
    // stack trace: https://gist.github.com/nsivabalan/b6359e7d5038484f8043506c8bc9e1c8
    // related stack overflow post: https://issues.apache.org/jira/browse/SPARK-3601. Avro deserializes list as GenericData.Array.
    List<SerializableHoodieRollbackRequest> serializableRequests = rollbackRequests.stream().map(SerializableHoodieRollbackRequest::new).collect(Collectors.toList());
    AppendMarkerHandler markerHandler = WriteMarkersFactory.getAppendMarkerHandler(config.getMarkersType(), table, instantTime);

    // Considering rollback may failed before, which generated some additional log files. We need to add these log files back.
    // rollback markers are added under rollback instant itself.
    Set<String> logPaths = new HashSet<>();
    try {
      logPaths = markerHandler.getAppendedLogPaths(context, config.getFinalizeWriteParallelism());
    } catch (FileNotFoundException fnf) {
      log.info("Rollback never failed and hence no marker dir was found. Safely moving on");
    } catch (IOException e) {
      throw new HoodieRollbackException("Failed to list log file markers for previous attempt of rollback ", e);
    }

    List<Pair<String, HoodieRollbackStat>> getRollbackStats = maybeDeleteAndCollectStats(context, instantTime, instantToRollback, serializableRequests, true, parallelism);
    List<HoodieRollbackStat> mergedRollbackStatByPartitionPath = context.reduceByKey(getRollbackStats, RollbackUtils::mergeRollbackStat, parallelism);
    return addLogFilesFromPreviousFailedRollbacksToStat(context, mergedRollbackStatByPartitionPath, logPaths);
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
    // The rollback requests for append only exist in table version 6 and below which require groupBy
    List<SerializableHoodieRollbackRequest> processedRollbackRequests =
        metaClient.getTableConfig().getTableVersion().greaterThanOrEquals(HoodieTableVersion.EIGHT)
            ? rollbackRequests
            : groupSerializableRollbackRequestsBasedOnFileGroup(rollbackRequests);
    final TaskContextSupplier taskContextSupplier = context.getTaskContextSupplier();
    return context.flatMap(processedRollbackRequests, (SerializableFunction<SerializableHoodieRollbackRequest, Stream<Pair<String, HoodieRollbackStat>>>) rollbackRequest -> {
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
          HoodieTableVersion tableVersion = metaClient.getTableConfig().getTableVersion();

          // Let's emit markers for rollback as well. markers are emitted under rollback instant time.
          WriteMarkers writeMarkers = WriteMarkersFactory.get(config.getMarkersType(), table, instantTime);

          writer = HoodieLogFormat.newWriterBuilder()
              .onParentPath(FSUtils.constructAbsolutePath(metaClient.getBasePath(), partitionPath))
              .withFileId(fileId)
              .withLogWriteToken(CommonClientUtils.generateWriteToken(taskContextSupplier))
              .withInstantTime(tableVersion.greaterThanOrEquals(HoodieTableVersion.EIGHT)
                  ? instantToRollback.requestedTime() : rollbackRequest.getLatestBaseInstant()
              )
              .withStorage(metaClient.getStorage())
              .withFileCreationCallback(getRollbackLogMarkerCallback(writeMarkers, partitionPath, fileId))
              .withTableVersion(tableVersion)
              .withFileExtension(HoodieLogFile.DELTA_EXTENSION).build();

          // generate metadata
          if (doDelete) {
            Map<HoodieLogBlock.HeaderMetadataType, String> header = generateHeader(instantToRollback.requestedTime());
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
        String partitionFullPath = FSUtils.constructAbsolutePath(metaClient.getBasePath().toString(), rollbackRequest.getPartitionPath()).toString();
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

  private LogFileCreationCallback getRollbackLogMarkerCallback(final WriteMarkers writeMarkers, String partitionPath, String fileId) {
    return new LogFileCreationCallback() {
      @Override
      public boolean preFileCreation(HoodieLogFile logFileToCreate) {
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

    // lets do left outer join and append missing log files to HoodieRollbackStat for each partition path.
    List<HoodieRollbackStat> finalRollbackStats = addMissingLogFilesAndGetRollbackStats(partitionPathToRollbackStatsHoodieData,
        partitionPathToLogFilesHoodieData, basePathStr, context.getStorageConf());
    return finalRollbackStats;
  }

  private HoodiePairData<String, List<String>> populatePartitionToLogFilesHoodieData(HoodieEngineContext context, String basePathStr, List<String> logFiles) {
    return context.parallelize(logFiles)
        // lets map each log file to partition path and log file name
        .mapToPair((SerializablePairFunction<String, String, String>) t -> {
          StoragePath logFilePath = new StoragePath(basePathStr, t);
          String partitionPath = FSUtils.getRelativePartitionPath(new StoragePath(basePathStr), logFilePath.getParent());
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
   * @param storageConfiguration Storage configuration
   * @return
   */
  private List<HoodieRollbackStat> addMissingLogFilesAndGetRollbackStats(HoodiePairData<String, HoodieRollbackStat> partitionPathToRollbackStatsHoodieData,
                                                                         HoodiePairData<String, List<String>> partitionPathToLogFilesHoodieData,
                                                                         String basePathStr, StorageConfiguration storageConfiguration) {
    return partitionPathToRollbackStatsHoodieData
        .leftOuterJoin(partitionPathToLogFilesHoodieData)
        .map((SerializableFunction<Pair<String, Pair<HoodieRollbackStat, Option<List<String>>>>, HoodieRollbackStat>) v1 -> {
          if (v1.getValue().getValue().isPresent()) {

            String partition = v1.getKey();
            HoodieRollbackStat rollbackStat = v1.getValue().getKey();
            List<String> missingLogFiles = v1.getValue().getRight().get();

            // fetch file sizes.
            StoragePath fullPartitionPath = StringUtils.isNullOrEmpty(partition) ? new StoragePath(basePathStr) : new StoragePath(basePathStr, partition);
            HoodieStorage storage = HoodieStorageUtils.getStorage(storageConfiguration);
            List<Option<StoragePathInfo>> storagePathInfoOpts = getPathInfoUnderPartition(storage,
                fullPartitionPath, new HashSet<>(missingLogFiles), true);
            List<StoragePathInfo> storagePathInfos = storagePathInfoOpts.stream().filter(storagePathInfoOpt -> storagePathInfoOpt.isPresent())
                .map(Option::get).collect(Collectors.toList());

            HashMap<StoragePathInfo, Long> commandBlocksCount = new HashMap<>(rollbackStat.getCommandBlocksCount());
            storagePathInfos.forEach(storagePathInfo -> commandBlocksCount.put(storagePathInfo, storagePathInfo.getLength()));

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
}
