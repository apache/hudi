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

package org.apache.hudi.common.util;

import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Helper class to generate commit metadata.
 */
public class CommitUtils {

  private static final Logger LOG = LoggerFactory.getLogger(CommitUtils.class);
  private static final String NULL_SCHEMA_STR = HoodieSchema.NULL_SCHEMA.toString();
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  /**
   * Gets the commit action type for given write operation and table type.
   * Use this API when commit action type can differ not only on the basis of table type but also write operation type.
   * For example, INSERT_OVERWRITE/INSERT_OVERWRITE_TABLE operations have REPLACE commit action type.
   */
  public static String getCommitActionType(WriteOperationType operation, HoodieTableType tableType) {
    if (operation == WriteOperationType.INSERT_OVERWRITE || operation == WriteOperationType.INSERT_OVERWRITE_TABLE
        || operation == WriteOperationType.DELETE_PARTITION || operation == WriteOperationType.BUCKET_RESCALE) {
      return HoodieTimeline.REPLACE_COMMIT_ACTION;
    } else {
      return getCommitActionType(tableType);
    }
  }

  /**
   * Gets the commit action type for given table type.
   * Note: Use this API only when the commit action type is not dependent on the write operation type.
   * See {@link CommitUtils#getCommitActionType(WriteOperationType, HoodieTableType)} for more details.
   */
  public static String getCommitActionType(HoodieTableType tableType) {
    switch (tableType) {
      case COPY_ON_WRITE:
        return HoodieActiveTimeline.COMMIT_ACTION;
      case MERGE_ON_READ:
        return HoodieActiveTimeline.DELTA_COMMIT_ACTION;
      default:
        throw new HoodieException("Could not commit on unknown table type " + tableType);
    }
  }

  public static HoodieCommitMetadata buildMetadata(List<HoodieWriteStat> writeStats,
                                                   Map<String, List<String>> partitionToReplaceFileIds,
                                                   Option<Map<String, String>> extraMetadata,
                                                   WriteOperationType operationType,
                                                   String schemaToStoreInCommit,
                                                   String commitActionType) {

    HoodieCommitMetadata commitMetadata = buildMetadataFromStats(writeStats, partitionToReplaceFileIds, commitActionType, operationType);

    // add in extra metadata
    if (extraMetadata.isPresent()) {
      extraMetadata.get().forEach(commitMetadata::addMetadata);
    }
    commitMetadata.addMetadata(HoodieCommitMetadata.SCHEMA_KEY, (schemaToStoreInCommit == null || schemaToStoreInCommit.equals(NULL_SCHEMA_STR))
        ? "" : schemaToStoreInCommit);
    commitMetadata.setOperationType(operationType);
    return commitMetadata;
  }

  private static HoodieCommitMetadata buildMetadataFromStats(List<HoodieWriteStat> writeStats,
                                                             Map<String, List<String>> partitionToReplaceFileIds,
                                                             String commitActionType,
                                                             WriteOperationType operationType) {
    final HoodieCommitMetadata commitMetadata;
    if (ClusteringUtils.isClusteringOrReplaceCommitAction(commitActionType)) {
      HoodieReplaceCommitMetadata replaceMetadata = new HoodieReplaceCommitMetadata();
      replaceMetadata.setPartitionToReplaceFileIds(partitionToReplaceFileIds);
      commitMetadata = replaceMetadata;
    } else {
      commitMetadata = new HoodieCommitMetadata();
    }

    for (HoodieWriteStat writeStat : writeStats) {
      String partition = writeStat.getPartitionPath();
      commitMetadata.addWriteStat(partition, writeStat);
    }

    LOG.info("Creating  metadata for " + operationType + " numWriteStats:" + writeStats.size()
        + " numReplaceFileIds:" + partitionToReplaceFileIds.values().stream().mapToInt(e -> e.size()).sum());
    return commitMetadata;
  }

  public static Option<HoodieCommitMetadata> buildMetadataFromInstant(HoodieTimeline timeline, HoodieInstant instant) {
    try {
      HoodieCommitMetadata commitMetadata = timeline.readCommitMetadata(instant);

      return Option.of(commitMetadata);
    } catch (IOException e) {
      LOG.info("Failed to parse HoodieCommitMetadata for " + instant.toString(), e);
    }

    return Option.empty();
  }

  public static Set<Pair<String, String>> getPartitionAndFileIdWithoutSuffixFromSpecificRecord(Map<String, List<org.apache.hudi.avro.model.HoodieWriteStat>>
                                                                                                     partitionToWriteStats) {
    Set<Pair<String, String>> partitionToFileId = new HashSet<>();
    // list all partitions paths
    for (Map.Entry<String, List<org.apache.hudi.avro.model.HoodieWriteStat>> entry : partitionToWriteStats.entrySet()) {
      for (org.apache.hudi.avro.model.HoodieWriteStat stat : entry.getValue()) {
        partitionToFileId.add(Pair.of(entry.getKey(), stat.getFileId()));
      }
    }
    return partitionToFileId;
  }

  public static Set<Pair<String, String>> getPartitionAndFileIdWithoutSuffix(Map<String, List<HoodieWriteStat>> partitionToWriteStats) {
    Set<Pair<String, String>> partitionTofileId = new HashSet<>();
    // list all partitions paths
    for (Map.Entry<String, List<HoodieWriteStat>> entry : partitionToWriteStats.entrySet()) {
      for (HoodieWriteStat stat : entry.getValue()) {
        partitionTofileId.add(Pair.of(entry.getKey(), stat.getFileId()));
      }
    }
    return partitionTofileId;
  }

  public static Set<Pair<String, String>> flattenPartitionToReplaceFileIds(Map<String, List<String>> partitionToReplaceFileIds) {
    return partitionToReplaceFileIds.entrySet().stream()
        .flatMap(partitionFileIds -> partitionFileIds.getValue().stream().map(replaceFileId -> Pair.of(partitionFileIds.getKey(), replaceFileId)))
        .collect(Collectors.toSet());
  }

  /**
   * Process previous commits metadata in the timeline to determine the checkpoint given a checkpoint key.
   * NOTE: This is very similar in intent to DeltaSync#getLatestCommitMetadataWithValidCheckpointInfo except that
   * different deployment models (deltastreamer or spark structured streaming) could have different checkpoint keys.
   *
   * @param timeline      completed commits in active timeline.
   * @param checkpointKey the checkpoint key in the extra metadata of the commit.
   * @param keyToLookup   key of interest for which checkpoint is looked up for.
   * @return An optional commit metadata with latest checkpoint.
   */
  public static Option<String> getValidCheckpointForCurrentWriter(HoodieTimeline timeline, String checkpointKey,
                                                                  String keyToLookup) {
    return (Option<String>) timeline.getWriteTimeline().filterCompletedInstants().getReverseOrderedInstants()
        .map(instant -> {
          try {
            HoodieCommitMetadata commitMetadata = timeline.readCommitMetadata(instant);
            // process commits only with checkpoint entries
            String checkpointValue = commitMetadata.getMetadata(checkpointKey);
            if (StringUtils.nonEmpty(checkpointValue)) {
              // return if checkpoint for "keyForLookup" exists.
              return readCheckpointValue(checkpointValue, keyToLookup);
            } else {
              return Option.empty();
            }
          } catch (IOException e) {
            throw new HoodieIOException("Failed to parse HoodieCommitMetadata for " + instant.toString(), e);
          }
        }).filter(Option::isPresent).findFirst().orElse(Option.empty());
  }

  public static Option<String> readCheckpointValue(String value, String id) {
    try {
      Map<String, String> checkpointMap = OBJECT_MAPPER.readValue(value, Map.class);
      if (!checkpointMap.containsKey(id)) {
        return Option.empty();
      }
      String checkpointVal = checkpointMap.get(id);
      return Option.of(checkpointVal);
    } catch (IOException e) {
      throw new HoodieIOException("Failed to parse checkpoint as map", e);
    }
  }

  public static String getCheckpointValueAsString(String identifier, String batchId) {
    try {
      Map<String, String> checkpointMap = new HashMap<>();
      checkpointMap.put(identifier, batchId);
      return OBJECT_MAPPER.writeValueAsString(checkpointMap);
    } catch (IOException e) {
      throw new HoodieIOException("Failed to parse checkpoint as map", e);
    }
  }
}
