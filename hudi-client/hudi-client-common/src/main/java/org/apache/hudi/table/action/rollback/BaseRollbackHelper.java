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
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.function.SerializableFunction;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.HoodieLogFileWriteCallback;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.block.HoodieCommandBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieRollbackException;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.marker.WriteMarkers;
import org.apache.hudi.table.marker.WriteMarkersFactory;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
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
  public List<HoodieRollbackStat> performRollback(HoodieEngineContext context, HoodieInstant instantToRollback,
                                                  List<HoodieRollbackRequest> rollbackRequests) {
    int parallelism = Math.max(Math.min(rollbackRequests.size(), config.getRollbackParallelism()), 1);
    context.setJobStatus(this.getClass().getSimpleName(), "Perform rollback actions: " + config.getTableName());
    // If not for conversion to HoodieRollbackInternalRequests, code fails. Using avro model (HoodieRollbackRequest) within spark.parallelize
    // is failing with com.esotericsoftware.kryo.KryoException
    // stack trace: https://gist.github.com/nsivabalan/b6359e7d5038484f8043506c8bc9e1c8
    // related stack overflow post: https://issues.apache.org/jira/browse/SPARK-3601. Avro deserializes list as GenericData.Array.
    List<SerializableHoodieRollbackRequest> serializableRequests = rollbackRequests.stream().map(SerializableHoodieRollbackRequest::new).collect(Collectors.toList());
    List<Pair<String, HoodieRollbackStat>> getRollbackStats = maybeDeleteAndCollectStats(context, instantToRollback, serializableRequests, true, parallelism);
    List<HoodieRollbackStat> mergedRollbackStatByPartitionPath = context.reduceByKey(getRollbackStats, RollbackUtils::mergeRollbackStat, parallelism);

    // Considering rollback may failed before, which generated some additional log files. We need to add these log files back.
    boolean hasLogBlockToRollback = rollbackRequests.stream().anyMatch(rollbackRequest -> !rollbackRequest.getLogBlocksToBeDeleted().isEmpty());
    if (hasLogBlockToRollback) {
      mergedRollbackStatByPartitionPath = addAdditionalLogFile(context, instantToRollback, rollbackRequests,
          mergedRollbackStatByPartitionPath, parallelism);
    }
    return mergedRollbackStatByPartitionPath;
  }

  /**
   * Collect all file info that needs to be rolled back.
   */
  public List<HoodieRollbackStat> collectRollbackStats(HoodieEngineContext context, HoodieInstant instantToRollback,
                                                       List<HoodieRollbackRequest> rollbackRequests) {
    int parallelism = Math.max(Math.min(rollbackRequests.size(), config.getRollbackParallelism()), 1);
    context.setJobStatus(this.getClass().getSimpleName(), "Collect rollback stats for upgrade/downgrade: " + config.getTableName());
    // If not for conversion to HoodieRollbackInternalRequests, code fails. Using avro model (HoodieRollbackRequest) within spark.parallelize
    // is failing with com.esotericsoftware.kryo.KryoException
    // stack trace: https://gist.github.com/nsivabalan/b6359e7d5038484f8043506c8bc9e1c8
    // related stack overflow post: https://issues.apache.org/jira/browse/SPARK-3601. Avro deserializes list as GenericData.Array.
    List<SerializableHoodieRollbackRequest> serializableRequests = rollbackRequests.stream().map(SerializableHoodieRollbackRequest::new).collect(Collectors.toList());
    return context.reduceByKey(maybeDeleteAndCollectStats(context, instantToRollback, serializableRequests, false, parallelism),
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
        final Path filePath;
        try {
          String partitionPath = rollbackRequest.getPartitionPath();
          String fileId = rollbackRequest.getFileId();
          String latestBaseInstant = rollbackRequest.getLatestBaseInstant();

          // Pls attention that, we use instantToRollback rather than instant of current rollback for marker file.
          WriteMarkers writeMarkers = WriteMarkersFactory.get(config.getMarkersType(), table, instantToRollback.getTimestamp());

          writer = HoodieLogFormat.newWriterBuilder()
              .onParentPath(FSUtils.getPartitionPath(metaClient.getBasePath(), rollbackRequest.getPartitionPath()))
              .withFileId(fileId)
              .overBaseCommit(latestBaseInstant)
              .withFs(metaClient.getFs())
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
        Map<FileStatus, Long> filesToNumBlocksRollback = Collections.singletonMap(
            metaClient.getFs().getFileStatus(Objects.requireNonNull(filePath)),
            1L
        );

        return Collections.singletonList(
            Pair.of(rollbackRequest.getPartitionPath(),
                HoodieRollbackStat.newBuilder()
                    .withPartitionPath(rollbackRequest.getPartitionPath())
                    .withRollbackBlockAppendResults(filesToNumBlocksRollback)
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

  private List<HoodieRollbackStat> addAdditionalLogFile(HoodieEngineContext context,
                                                        final HoodieInstant instantToRollback,
                                                        List<HoodieRollbackRequest> rollbackRequests,
                                                        List<HoodieRollbackStat> originalRollbackStats,
                                                        int parallelism) {
    WriteMarkers markers = WriteMarkersFactory.get(config.getMarkersType(), table, instantToRollback.getTimestamp());
    Set<String> logPaths;
    try {
      logPaths = markers.getAppendedLogPaths(context, config.getFinalizeWriteParallelism());
    } catch (IOException e) {
      throw new HoodieRollbackException("Failed to list log file markers", e);
    }
    final Path basePath = new Path(config.getBasePath());

    // here we have all log files in this instant, including ones created by delta commit as well as rollback.
    Map<String, List<Path>> partitionsToLogFiles = logPaths
        .stream()
        .map(logFilePath -> new Path(config.getBasePath(), logFilePath))
        .collect(Collectors.groupingBy(fullFilePath -> FSUtils.getRelativePartitionPath(basePath, fullFilePath.getParent())));

    // one log file included in rollback request means they are created by delta commit.
    Map<String, Set<String>> partitionsToLogFileNameGeneratedByDeltaCommit = new HashMap<>();
    rollbackRequests.forEach(rbr -> {
      Set<String> logFileName = partitionsToLogFileNameGeneratedByDeltaCommit.getOrDefault(rbr.getPartitionPath(), new HashSet<>());
      for (String logFileFullPath : rbr.getLogBlocksToBeDeleted().keySet()) {
        logFileName.add(new Path(logFileFullPath).getName());
      }
      partitionsToLogFileNameGeneratedByDeltaCommit.putIfAbsent(rbr.getPartitionPath(), logFileName);
    });

    // get log files created by rollback by removing those created in delta commit from all.
    Map<String, Set<Path>> partitionsToLogFilesGeneratedByRollback = partitionsToLogFiles
        .entrySet()
        .stream()
        .map(partitionAndLogFiles -> {
          String partitionPath = partitionAndLogFiles.getKey();
          Set<String> logFilesGeneratedByDeltaCommit = Objects.requireNonNull(partitionsToLogFileNameGeneratedByDeltaCommit.get(partitionPath),
              "invalid partition: " + partitionPath);
          Set<Path> logFilesGeneratedByRollback = partitionAndLogFiles.getValue()
              .stream().filter(p -> !logFilesGeneratedByDeltaCommit.contains(p.getName()))
              .collect(Collectors.toSet());
          return Pair.of(partitionPath, logFilesGeneratedByRollback);
        })
        .collect(Collectors.toMap(Pair::getKey, Pair::getValue));

    List<Pair<HoodieRollbackStat, Set<Path>>> rollbackStatsAndLogFilesCreated = originalRollbackStats
        .stream()
        .map(rbs -> Pair.of(rbs, partitionsToLogFilesGeneratedByRollback.get(rbs.getPartitionPath())))
        .collect(Collectors.toList());
    context.setJobStatus(this.getClass().getName(), "add additional log files in rollback");
    return context.map(rollbackStatsAndLogFilesCreated, rollbackStatAndLogFiles -> {
      HoodieRollbackStat originalRollbackStat = rollbackStatAndLogFiles.getKey();
      // this partition doesn't have log blocks to rollback. So will not create rollback command block
      if (originalRollbackStat.getCommandBlocksCount().isEmpty()) {
        return originalRollbackStat;
      }
      Set<Path> allLogFilesCreatedInRollback = Objects.requireNonNull(rollbackStatAndLogFiles.getValue());

      Set<String> logFilesInRollbackStat = originalRollbackStat.getCommandBlocksCount()
          .keySet().stream()
          .map(f -> f.getPath().getName())
          .collect(Collectors.toSet());
      Set<String> additionalLogFileNames = allLogFilesCreatedInRollback
          .stream()
          .map(Path::getName)
          .filter(name -> !logFilesInRollbackStat.contains(name))
          .collect(Collectors.toSet());
      // No additional log files created
      if (additionalLogFileNames.size() == 0) {
        return originalRollbackStat;
      }

      // file additional log files' file statuses
      Path fullPartitionPath = new Path(config.getBasePath(), originalRollbackStat.getPartitionPath());
      FileStatus[] fileStatuses = FSUtils.getFileStatusesUnderPartition(table.getMetaClient().getFs(), fullPartitionPath, additionalLogFileNames);

      // build new rollbackStat
      HoodieRollbackStat.Builder builder = HoodieRollbackStat.newBuilder()
          .withPartitionPath(originalRollbackStat.getPartitionPath());
      for (String file : originalRollbackStat.getSuccessDeleteFiles()) {
        builder.withDeletedFileResult(file, true);
      }

      for (String file : originalRollbackStat.getFailedDeleteFiles()) {
        builder.withDeletedFileResult(file, false);
      }

      Map<FileStatus, Long> commandBlocksCount = new HashMap<>(originalRollbackStat.getCommandBlocksCount());
      Arrays.stream(fileStatuses).forEach(f -> commandBlocksCount.put(f, 1L));
      builder.withRollbackBlockAppendResults(commandBlocksCount);
      return builder.build();
    }, parallelism);
  }

  /**
   * Common method used for cleaning out files during rollback.
   */
  protected List<HoodieRollbackStat> deleteFiles(HoodieTableMetaClient metaClient, List<String> filesToBeDeleted, boolean doDelete) throws IOException {
    return filesToBeDeleted.stream().map(fileToDelete -> {
      String basePath = metaClient.getBasePath();
      try {
        Path fullDeletePath = new Path(fileToDelete);
        String partitionPath = FSUtils.getRelativePartitionPath(new Path(basePath), fullDeletePath.getParent());
        boolean isDeleted = true;
        if (doDelete) {
          try {
            isDeleted = metaClient.getFs().delete(fullDeletePath);
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
