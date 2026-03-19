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
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.block.HoodieCommandBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.InvalidHoodiePathException;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.table.HoodieTable;

import lombok.extern.slf4j.Slf4j;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.util.ValidationUtils.checkArgument;

/**
 * Contains common methods to be used across engines for rollback operation.
 */
@Slf4j
public class RollbackHelper implements Serializable {

  private static final long serialVersionUID = 1L;
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
    return context.flatMap(rollbackRequests, (SerializableFunction<SerializableHoodieRollbackRequest, Stream<Pair<String, HoodieRollbackStat>>>) rollbackRequest -> {
      List<String> filesToBeDeleted = rollbackRequest.getFilesToBeDeleted();
      if (!filesToBeDeleted.isEmpty()) {
        List<HoodieRollbackStat> rollbackStats = deleteFiles(metaClient, filesToBeDeleted, doDelete);
        return rollbackStats.stream().map(entry -> Pair.of(entry.getPartitionPath(), entry));
      } else {
        checkArgument(rollbackRequest.getLogBlocksToBeDeleted().isEmpty(),
            "V8+ rollback should not have logBlocksToBeDeleted, but found for partition: "
                + rollbackRequest.getPartitionPath() + ", fileId: " + rollbackRequest.getFileId());
        return Stream.of(
            Pair.of(rollbackRequest.getPartitionPath(),
                HoodieRollbackStat.newBuilder()
                    .withPartitionPath(rollbackRequest.getPartitionPath())
                    .build()));
      }
    }, numPartitions);
  }

  /**
   * Builds the lookup key for pre-computed log versions. Used by {@link RollbackHelperV1} for V6 rollback.
   */
  protected static String logVersionLookupKey(String partitionPath, String fileId, String commitTime) {
    return partitionPath + "|" + fileId + "|" + commitTime;
  }

  /**
   * Pre-compute the latest log version for each (partition, fileId, deltaCommitTime) tuple
   * by listing each unique partition directory once. This replaces N per-request listing
   * calls (one per rollback request) with P per-partition listings (where P is much less than N).
   *
   * <p>Used by {@link RollbackHelperV1} for V6 rollback where log blocks are appended.
   */
  protected Map<String, Pair<Integer, String>> preComputeLogVersions(
      List<SerializableHoodieRollbackRequest> rollbackRequests) {
    Set<String> relativePartitionPaths = rollbackRequests.stream()
        .filter(req -> !req.getLogBlocksToBeDeleted().isEmpty())
        .map(SerializableHoodieRollbackRequest::getPartitionPath)
        .collect(Collectors.toSet());

    if (relativePartitionPaths.isEmpty()) {
      return Collections.emptyMap();
    }

    log.info("Pre-computing log versions for {} partition(s) to avoid per-request listStatus calls",
        relativePartitionPaths.size());

    Map<String, Pair<Integer, String>> logVersionMap = new HashMap<>();

    for (String relPartPath : relativePartitionPaths) {
      StoragePath absolutePartPath = FSUtils.constructAbsolutePath(metaClient.getBasePath(), relPartPath);
      try {
        List<StoragePathInfo> statuses = metaClient.getStorage().listDirectEntries(absolutePartPath,
            path -> path.getName().contains(HoodieLogFile.DELTA_EXTENSION));

        for (StoragePathInfo status : statuses) {
          try {
            HoodieLogFile logFile = new HoodieLogFile(status);
            String key = logVersionLookupKey(relPartPath, logFile.getFileId(), logFile.getDeltaCommitTime());
            Pair<Integer, String> existing = logVersionMap.get(key);
            if (existing == null || logFile.getLogVersion() > existing.getLeft()) {
              logVersionMap.put(key, Pair.of(logFile.getLogVersion(), logFile.getLogWriteToken()));
            }
          } catch (InvalidHoodiePathException e) {
            log.warn("Skipping non-standard log file during pre-compute: {}", status.getPath(), e);
          }
        }
      } catch (IOException e) {
        log.warn("Failed to pre-compute log versions for partition {}, will fall back to per-request listing",
            relPartPath, e);
      }
    }

    log.info("Pre-computed log versions for {} file groups across {} partition(s)",
        logVersionMap.size(), relativePartitionPaths.size());
    return logVersionMap;
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
        log.error("Fetching file status for ");
        throw new HoodieIOException("Fetching file status for " + fileToDelete + " failed ", e);
      }
    }).collect(Collectors.toList());
  }

  /**
   * Generates the header for a rollback command block. Used by {@link RollbackHelperV1} for V6 rollback.
   */
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
