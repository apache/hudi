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
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.exception.HoodieException;

import org.apache.avro.Schema;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Helper class to generate commit metadata.
 */
public class CommitUtils {

  private static final Logger LOG = LogManager.getLogger(CommitUtils.class);
  private static final String NULL_SCHEMA_STR = Schema.create(Schema.Type.NULL).toString();

  /**
   * Gets the commit action type for given write operation and table type.
   * Use this API when commit action type can differ not only on the basis of table type but also write operation type.
   * For example, INSERT_OVERWRITE/INSERT_OVERWRITE_TABLE operations have REPLACE commit action type.
   */
  public static String getCommitActionType(WriteOperationType operation, HoodieTableType tableType) {
    if (operation == WriteOperationType.INSERT_OVERWRITE || operation == WriteOperationType.INSERT_OVERWRITE_TABLE
        || operation == WriteOperationType.DELETE_PARTITION) {
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
    if (HoodieTimeline.REPLACE_COMMIT_ACTION.equals(commitActionType)) {
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

  public static HashMap<String, String> getFileIdWithoutSuffixAndRelativePathsFromSpecificRecord(Map<String, List<org.apache.hudi.avro.model.HoodieWriteStat>>
                                                                                       partitionToWriteStats) {
    HashMap<String, String> fileIdToPath = new HashMap<>();
    // list all partitions paths
    for (Map.Entry<String, List<org.apache.hudi.avro.model.HoodieWriteStat>> entry : partitionToWriteStats.entrySet()) {
      for (org.apache.hudi.avro.model.HoodieWriteStat stat : entry.getValue()) {
        fileIdToPath.put(stat.getFileId(), stat.getPath());
      }
    }
    return fileIdToPath;
  }

  public static HashMap<String, String> getFileIdWithoutSuffixAndRelativePaths(Map<String, List<HoodieWriteStat>>
      partitionToWriteStats) {
    HashMap<String, String> fileIdToPath = new HashMap<>();
    // list all partitions paths
    for (Map.Entry<String, List<HoodieWriteStat>> entry : partitionToWriteStats.entrySet()) {
      for (HoodieWriteStat stat : entry.getValue()) {
        fileIdToPath.put(stat.getFileId(), stat.getPath());
      }
    }
    return fileIdToPath;
  }
}
