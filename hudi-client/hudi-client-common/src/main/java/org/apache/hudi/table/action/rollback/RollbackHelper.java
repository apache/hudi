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
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.function.SerializableFunction;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.block.HoodieCommandBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieRollbackException;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.util.CommonClientUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.table.action.rollback.RollbackUtils.groupSerializableRollbackRequestsBasedOnFileGroup;

/**
 * Contains common methods to be used across engines for rollback operation.
 */
public class RollbackHelper implements Serializable {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(RollbackHelper.class);
  protected static final String EMPTY_STRING = "";

  protected final HoodieTable table;
  protected final HoodieTableMetaClient metaClient;
  protected final HoodieWriteConfig config;

  public RollbackHelper(HoodieTable table, HoodieWriteConfig config) {
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
    return context.reduceByKey(maybeDeleteAndCollectStats(context, instantToRollback, serializableRequests, true, parallelism),
        RollbackUtils::mergeRollbackStat, parallelism);
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
        return rollbackStats.stream().map(entry -> Pair.of(entry.getPartitionPath(), entry));
      } else if (!rollbackRequest.getLogBlocksToBeDeleted().isEmpty()) {
        HoodieLogFormat.Writer writer = null;
        final StoragePath filePath;
        try {
          String fileId = rollbackRequest.getFileId();
          HoodieTableVersion tableVersion = metaClient.getTableConfig().getTableVersion();

          writer = HoodieLogFormat.newWriterBuilder()
              .onParentPath(FSUtils.constructAbsolutePath(metaClient.getBasePath(), rollbackRequest.getPartitionPath()))
              .withFileId(fileId)
              .withLogWriteToken(CommonClientUtils.generateWriteToken(taskContextSupplier))
              .withInstantTime(tableVersion.greaterThanOrEquals(HoodieTableVersion.EIGHT)
                      ? instantToRollback.requestedTime() : rollbackRequest.getLatestBaseInstant()
                  )
              .withStorage(metaClient.getStorage())
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

        return Stream.of(
                Pair.of(rollbackRequest.getPartitionPath(),
                    HoodieRollbackStat.newBuilder()
                        .withPartitionPath(rollbackRequest.getPartitionPath())
                        .withRollbackBlockAppendResults(filesToNumBlocksRollback)
                        .build()));
      } else {
        // no action needed.
        return Stream.of(
            Pair.of(rollbackRequest.getPartitionPath(),
                HoodieRollbackStat.newBuilder()
                    .withPartitionPath(rollbackRequest.getPartitionPath())
                    .build()));
      }
    }, numPartitions);
  }

  /**
   * Common method used for cleaning out files during rollback.
   */
  protected List<HoodieRollbackStat> deleteFiles(HoodieTableMetaClient metaClient, List<String> filesToBeDeleted, boolean doDelete) throws IOException {
    return filesToBeDeleted.stream().map(fileToDelete -> {
      String basePath = metaClient.getBasePath().toString();
      try {
        StoragePath fullDeletePath = new StoragePath(fileToDelete);
        String partitionPath = FSUtils.getRelativePartitionPath(new StoragePath(basePath), fullDeletePath.getParent());
        boolean isDeleted = true;
        if (doDelete) {
          try {
            isDeleted = metaClient.getStorage().deleteFile(fullDeletePath);
          } catch (FileNotFoundException e) {
            // if first rollback attempt failed and retried again, chances that some files are already deleted.
            isDeleted = true;
          }
        }
        if (!isDeleted) {
          if (metaClient.getStorage().exists(fullDeletePath)) {
            throw new HoodieIOException("Failing to delete file during rollback execution failed : " + fullDeletePath);
          }
          isDeleted = true;
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
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, metaClient.getActiveTimeline().lastInstant().get().requestedTime());
    header.put(HoodieLogBlock.HeaderMetadataType.TARGET_INSTANT_TIME, commit);
    header.put(HoodieLogBlock.HeaderMetadataType.COMMAND_BLOCK_TYPE,
        String.valueOf(HoodieCommandBlock.HoodieCommandBlockTypeEnum.ROLLBACK_BLOCK.ordinal()));
    return header;
  }
}
