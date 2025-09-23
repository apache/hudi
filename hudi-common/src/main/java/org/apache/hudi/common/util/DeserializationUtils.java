/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.common.util;

import org.apache.hudi.avro.model.HoodieActionInstant;
import org.apache.hudi.avro.model.HoodieBootstrapFilePartitionInfo;
import org.apache.hudi.avro.model.HoodieBootstrapIndexInfo;
import org.apache.hudi.avro.model.HoodieBootstrapPartitionMetadata;
import org.apache.hudi.avro.model.HoodieCleanFileInfo;
import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCleanPartitionMetadata;
import org.apache.hudi.avro.model.HoodieCleanerPlan;
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.avro.model.HoodieCompactionOperation;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.avro.model.HoodieCompactionStrategy;
import org.apache.hudi.avro.model.HoodieFSPermission;
import org.apache.hudi.avro.model.HoodieFileStatus;
import org.apache.hudi.avro.model.HoodieIndexPartitionInfo;
import org.apache.hudi.avro.model.HoodieIndexPlan;
import org.apache.hudi.avro.model.HoodieInstantInfo;
import org.apache.hudi.avro.model.HoodieMergeArchiveFilePlan;
import org.apache.hudi.avro.model.HoodiePath;
import org.apache.hudi.avro.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.avro.model.HoodieRequestedReplaceMetadata;
import org.apache.hudi.avro.model.HoodieRestoreMetadata;
import org.apache.hudi.avro.model.HoodieRestorePlan;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.avro.model.HoodieRollbackPartitionMetadata;
import org.apache.hudi.avro.model.HoodieRollbackPlan;
import org.apache.hudi.avro.model.HoodieRollbackRequest;
import org.apache.hudi.avro.model.HoodieSavepointMetadata;
import org.apache.hudi.avro.model.HoodieSavepointPartitionMetadata;
import org.apache.hudi.avro.model.HoodieWriteStat;

import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.HamletImpl;
import sun.net.www.content.text.Generic;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DeserializationUtils {

  private DeserializationUtils() {
  }

  public static final ThreadLocal<GenericDatumReader<IndexedRecord>> COMPACT_PLAN_DESERIALIZER =
      ThreadLocal.withInitial(() -> new GenericDatumReader<>(HoodieCompactionPlan.getClassSchema()));

  public static final ThreadLocal<GenericDatumReader<IndexedRecord>> BOOTSTRAP_INDEX_INFO_DESERIALIZER =
      ThreadLocal.withInitial(() -> new GenericDatumReader<>(HoodieBootstrapIndexInfo.getClassSchema()));

  public static final ThreadLocal<GenericDatumReader<IndexedRecord>> BOOTSTRAP_PARTITION_METADATA_DESERIALIZER =
      ThreadLocal.withInitial(() -> new GenericDatumReader<>(HoodieBootstrapPartitionMetadata.getClassSchema()));

  public static final ThreadLocal<GenericDatumReader<IndexedRecord>> BOOTSTRP_FILE_PARTITION_INFO_DESERIALIZER =
      ThreadLocal.withInitial(() -> new GenericDatumReader<>(HoodieBootstrapFilePartitionInfo.getClassSchema()));

  public static final ThreadLocal<GenericDatumReader<IndexedRecord>> COMMIT_METADATA_DESERIALIZER =
      ThreadLocal.withInitial(() -> new GenericDatumReader<>(org.apache.hudi.avro.model.HoodieCommitMetadata.getClassSchema()));

  public static final ThreadLocal<GenericDatumReader<IndexedRecord>> ROLLBACK_METADATA_DESERIALIZER =
      ThreadLocal.withInitial(() -> new GenericDatumReader<>(HoodieRollbackMetadata.getClassSchema()));

  public static final ThreadLocal<GenericDatumReader<IndexedRecord>> ROLLBACK_PLAN_DESERIALIZER =
      ThreadLocal.withInitial(() -> new GenericDatumReader<>(HoodieRollbackPlan.getClassSchema()));

  public static final ThreadLocal<GenericDatumReader<IndexedRecord>> RESTORE_PLAN_DESERIALIZER =
      ThreadLocal.withInitial(() -> new GenericDatumReader<>(HoodieRestorePlan.getClassSchema()));

  public static final ThreadLocal<GenericDatumReader<IndexedRecord>> RESTORE_METADATA_DESERIALIZER =
      ThreadLocal.withInitial(() -> new GenericDatumReader<>(HoodieRestoreMetadata.getClassSchema()));

  public static final ThreadLocal<GenericDatumReader<IndexedRecord>> SAVEPOINT_METADATA_DESERIALIZER =
      ThreadLocal.withInitial(() -> new GenericDatumReader<>(HoodieSavepointMetadata.getClassSchema()));

  public static final ThreadLocal<GenericDatumReader<IndexedRecord>> ARCHIVE_FILE_PLAN_DESERIALIZER =
      ThreadLocal.withInitial(() -> new GenericDatumReader<>(HoodieMergeArchiveFilePlan.getClassSchema()));

  public static final ThreadLocal<GenericDatumReader<IndexedRecord>> REPLACE_METADATA_DESERIALIZER =
      ThreadLocal.withInitial(() -> new GenericDatumReader<>(HoodieReplaceCommitMetadata.getClassSchema()));

  public static final ThreadLocal<GenericDatumReader<IndexedRecord>> REQUESTED_REPLACE_METADATA_DESERIALIZER =
      ThreadLocal.withInitial(() -> new GenericDatumReader<>(HoodieRequestedReplaceMetadata.getClassSchema()));

  public static final ThreadLocal<GenericDatumReader<IndexedRecord>> CLEAN_METADATA_DESERIALIZER =
      ThreadLocal.withInitial(() -> new GenericDatumReader<>(HoodieCleanMetadata.getClassSchema()));

  public static final ThreadLocal<GenericDatumReader<IndexedRecord>> CLEANER_PLAN_DESERIALIZER =
      ThreadLocal.withInitial(() -> new GenericDatumReader<>(HoodieCleanerPlan.getClassSchema()));

  public static final ThreadLocal<GenericDatumReader<IndexedRecord>> INDEX_PLAN_DESERIALIZER =
      ThreadLocal.withInitial(() -> new GenericDatumReader<>(HoodieIndexPlan.getClassSchema()));

  public static final ThreadLocal<GenericDatumReader<IndexedRecord>> ROLLBACK_PARTITION_METADATA_DESERIALIZER =
      ThreadLocal.withInitial(() -> new GenericDatumReader<>(HoodieRollbackPartitionMetadata.getClassSchema()));

  public static HoodieRollbackPlan deserializeHoodieRollbackPlan(byte[] data) throws IOException {
    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, null);
    GenericDatumReader<IndexedRecord> reader = ROLLBACK_PLAN_DESERIALIZER.get();
    GenericRecord record = (GenericRecord) reader.read(null, decoder).get(0);
    return HoodieRollbackPlan.newBuilder()
        .setInstantToRollback((HoodieInstantInfo) record.get("instantToRollback"))
        .setRollbackRequests((List<HoodieRollbackRequest>) record.get("rollbackRequests"))
        .setVersion((Integer) record.get("version"))
        .build();
  }

  public static HoodieRestorePlan deserializeHoodieRestorePlan(byte[] data) throws IOException {
    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, null);
    GenericDatumReader<IndexedRecord> reader = RESTORE_PLAN_DESERIALIZER.get();
    GenericRecord record = (GenericRecord) reader.read(null, decoder).get(0);
    return HoodieRestorePlan.newBuilder()
        .setInstantsToRollback((List<HoodieInstantInfo>) record.get("instantToRollback"))
        .setVersion((Integer) record.get("version"))
        .setSavepointToRestoreTimestamp((String) record.get("savepointToRestoreTimestamp"))
        .build();
  }

  public static HoodieRestoreMetadata deserializeHoodieRestoreMetadata(byte[] data) throws IOException {
    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, null);
    GenericDatumReader<IndexedRecord> reader = RESTORE_METADATA_DESERIALIZER.get();
    GenericRecord record = (GenericRecord) reader.read(null, decoder).get(0);
    return HoodieRestoreMetadata.newBuilder()
        .setStartRestoreTime((String) record.get("startRestoreTime"))
        .setTimeTakenInMillis((long) record.get("timeTakenInMillis"))
        .setInstantsToRollback((List<String>) record.get("instantsToRollback"))
        .setHoodieRestoreMetadata((Map<String, List<HoodieRollbackMetadata>>) record.get("hoodieRestoreMetadata"))
        .build();
  }

  public static HoodieRequestedReplaceMetadata deserializeHoodieRequestedReplaceMetadata(byte[] data) throws IOException {
    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, null);
    GenericDatumReader<IndexedRecord> reader = REQUESTED_REPLACE_METADATA_DESERIALIZER.get();
    GenericRecord plan = (GenericRecord) reader.read(null, decoder).get(0);
    return HoodieRequestedReplaceMetadata.newBuilder()
        .setOperationType((String) plan.get("operationType"))
        .setClusteringPlan((HoodieClusteringPlan) plan.get("clusteringPlan"))
        .setExtraMetadata((Map<String, String>) plan.get("extraMetadata"))
        .setVersion((Integer) plan.get("version"))
        .build();
  }

  public static HoodieIndexPlan deserializeHoodieIndexPlan(byte[] data) throws IOException {
    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, null);
    GenericDatumReader<IndexedRecord> reader = INDEX_PLAN_DESERIALIZER.get();
    GenericRecord plan = (GenericRecord) reader.read(null, decoder).get(0);
    return HoodieIndexPlan.newBuilder()
        .setVersion((Integer) plan.get("version"))
        .setIndexPartitionInfos((List<HoodieIndexPartitionInfo>) plan.get("indexPartitionInfos"))
        .build();
  }

  public static HoodieBootstrapIndexInfo deserializeHoodieBootstrapIndexInfo(byte[] data) throws IOException {
    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, null);
    GenericDatumReader<IndexedRecord> reader = BOOTSTRAP_INDEX_INFO_DESERIALIZER.get();
    GenericRecord plan = (GenericRecord) reader.read(null, decoder).get(0);
    return HoodieBootstrapIndexInfo.newBuilder()
        .setVersion((Integer) plan.get("version"))
        .setBootstrapBasePath((String) plan.get("bootstrapBasePath"))
        .setCreatedTimestamp((Long) plan.get("createdTimestamp"))
        .setNumKeys((Integer) plan.get("numKeys"))
        .build();
  }

  public static HoodieBootstrapPartitionMetadata deserializeHoodieBootstrapPartitionMetadata(byte[] data) throws IOException {
    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, null);
    GenericDatumReader<IndexedRecord> reader = BOOTSTRAP_PARTITION_METADATA_DESERIALIZER.get();
    GenericRecord record = (GenericRecord) reader.read(null, decoder).get(0);
    return HoodieBootstrapPartitionMetadata.newBuilder()
        .setVersion((Integer) record.get("version"))
        .setBootstrapPartitionPath((String) record.get("bootstrapPartitionPath"))
        .setPartitionPath((String) record.get("partitionPath"))
        .setFileIdToBootstrapFile(
            deserializeHoodieFileStatusMap((Map<String, GenericRecord>) record.get("fileIdToBootstrapFile")))
        .build();
  }

  public static org.apache.hudi.avro.model.HoodieCommitMetadata deserializeHoodieCommitMetadata(
      InputStream inputStream) throws IOException {
    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
    GenericDatumReader<IndexedRecord> reader = COMMIT_METADATA_DESERIALIZER.get();
    GenericRecord plan = (GenericRecord) reader.read(null, decoder).get(0);
    return org.apache.hudi.avro.model.HoodieCommitMetadata.newBuilder()
        .setPartitionToWriteStats(
            deserializeHoodieWriteStatsMap((Map<String, List<GenericRecord>>) plan.get("partitionToWriteStats")))
        .setCompacted((Boolean) plan.get("compacted"))
        .setExtraMetadata((Map<String, String>) plan.get("extraMetadata"))
        .setVersion((Integer) plan.get("version"))
        .setOperationType((String) plan.get("operationType"))
        .build();
  }

  public static HoodieRollbackMetadata deserializeHoodieRollbackMetadata(InputStream inputStream) throws IOException {
    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
    GenericDatumReader<IndexedRecord> reader = ROLLBACK_METADATA_DESERIALIZER.get();
    GenericRecord record = (GenericRecord) reader.read(null, decoder).get(0);
    return HoodieRollbackMetadata.newBuilder()
        .setStartRollbackTime((String) record.get("startRollbackTime"))
        .setTimeTakenInMillis((long) record.get("timeTakenInMillis"))
        .setTotalFilesDeleted((int) record.get("totalFilesDeleted"))
        .setCommitsRollback((List<String>) record.get("commitsRollback"))
        .setPartitionMetadata(
            deserializeHoodieRollbackPartitionMetadataMap((Map<String, GenericRecord>) record.get("partitionMetadata")))
        .setVersion((Integer) record.get("version"))
        .setInstantsRollback((List<HoodieInstantInfo>) record.get("instantsRollback"))
        .build();
  }

  public static HoodieMergeArchiveFilePlan deserializeHoodieMergeArchiveFilePlan(byte[] data) throws IOException {
    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, 0, data.length, null);
    GenericDatumReader<IndexedRecord> reader = ARCHIVE_FILE_PLAN_DESERIALIZER.get();
    GenericRecord plan = (GenericRecord) reader.read(null, decoder).get(0);
    return HoodieMergeArchiveFilePlan.newBuilder()
        .setVersion((Integer) plan.get("version"))
        .setCandidate((List<String>) plan.get("candidate"))
        .setMergedArchiveFileName((String) plan.get("mergedArchiveFileName"))
        .build();
  }

  public static HoodieSavepointMetadata deserializeHoodieSavepointMetadata(InputStream inputStream) throws IOException {
    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
    GenericDatumReader<IndexedRecord> reader = SAVEPOINT_METADATA_DESERIALIZER.get();
    GenericRecord record = (GenericRecord) reader.read(null, decoder).get(0);
    return HoodieSavepointMetadata.newBuilder()
        .setSavepointedBy((String) record.get("savepointedBy"))
        .setSavepointedAt((long) record.get("savepointedAt"))
        .setComments((String) record.get("comments"))
        .setPartitionMetadata(
            deserializeHoodieSavepointPartitionMetadataMap((Map<String, GenericRecord>) record.get("partitionMetadata")))
        .setVersion((Integer) record.get("version"))
        .build();
  }

  public static HoodieReplaceCommitMetadata deserializeHoodieReplaceCommitMetadata(InputStream inputStream) throws IOException {
    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
    GenericRecord record = (GenericRecord) REPLACE_METADATA_DESERIALIZER.get().read(null, decoder).get(0);
    return HoodieReplaceCommitMetadata.newBuilder()
        .setPartitionToWriteStats(
            deserializeHoodieWriteStatsMap((Map<String, List<GenericRecord>>) record.get("partitionToWriteStats")))
        .setCompacted((Boolean) record.get("compacted"))
        .setExtraMetadata((Map<String, String>) record.get("extraMetadata"))
        .setVersion((Integer) record.get("version"))
        .setOperationType((String) record.get("operationType"))
        .setPartitionToReplaceFileIds((Map<String, List<String>>) record.get("partitionToReplaceFileIds"))
        .build();
  }

  public static HoodieCleanMetadata deserializeHoodieCleanMetadata(InputStream inputStream) throws IOException {
    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
    GenericDatumReader<IndexedRecord> reader = CLEAN_METADATA_DESERIALIZER.get();
    GenericRecord record = (GenericRecord) reader.read(null, decoder).get(0);
    return HoodieCleanMetadata.newBuilder()
        .setStartCleanTime((String) record.get("startCleanTime"))
        .setTimeTakenInMillis((long) record.get("timeTakenInMillis"))
        .setTotalFilesDeleted((int) record.get("totalFilesDeleted"))
        .setEarliestCommitToRetain((String) record.get("earliestCommitToRetain"))
        .setLastCompletedCommitTimestamp((String) record.get("lastCompletedCommitTimestamp"))
        .setPartitionMetadata(
            deserializeCleanPartitionMetadataMap((Map<String, GenericRecord>) record.get("partitionMetadata")))
        .setVersion((Integer) record.get("version"))
        .setBootstrapPartitionMetadata(
            deserializeCleanPartitionMetadataMap((Map<String, GenericRecord>) record.get("bootstrapPartitionMetadata")))
        .setExtraMetadata((Map<String, String>) record.get("extraMetadata"))
        .build();
  }

  public static HoodieCleanerPlan deserializeHoodieCleanerPlanMetadata(InputStream inputStream) throws IOException {
    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
    GenericDatumReader<IndexedRecord> reader = CLEANER_PLAN_DESERIALIZER.get();
    GenericRecord record = (GenericRecord) reader.read(null, decoder).get(0);
    return HoodieCleanerPlan.newBuilder()
        .setEarliestInstantToRetain(
            deserializeHoodieActionInstant((GenericRecord) record.get("earliestInstantToRetain")))
        .setLastCompletedCommitTimestamp((String) record.get("lastCompletedCommitTimestamp"))
        .setPolicy((String) record.get("policy"))
        .setFilesToBeDeletedPerPartition((Map<String, List<String>>) record.get("filesToBeDeletedPerPartition"))
        .setVersion((Integer) record.get("version"))
        .setFilePathsToBeDeletedPerPartition(
            deserializeFilePathsToBeDeletedPerPartition((Map<String, List<GenericRecord>>) record.get("filePathsToBeDeletedPerPartition")))
        .setVersion((Integer) record.get("version"))
        .setPartitionsToBeDeleted((List<String>) record.get("partitionsToBeDeleted"))
        .setExtraMetadata((Map<String, String>) record.get("extraMetadata"))
        .build();
  }

  public static HoodieCompactionPlan deserializeHoodieCompactionPlan(InputStream inputStream) throws IOException {
    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
    GenericDatumReader<IndexedRecord> reader = COMPACT_PLAN_DESERIALIZER.get();
    GenericRecord record = (GenericRecord) reader.read(null, decoder).get(0);
    return HoodieCompactionPlan.newBuilder()
        .setOperations(
            deserializeHoodieCompactionOperationList((List<GenericRecord>) record.get("operations")))
        .setExtraMetadata((Map<String, String>) record.get("extraMetadata"))
        .setVersion((Integer) record.get("version"))
        .setStrategy(
            deserializeHoodieCompactionStrategy((GenericRecord) record.get("strategy")))
        .setPreserveHoodieMetadata((Boolean) record.get("preserveHoodieMetadata"))
        .setMissingSchedulePartitions((List<String>) record.get("missingSchedulePartitions"))
        .build();
  }

  public static HoodieBootstrapFilePartitionInfo deserializeHoodieBootstrapFilePartitionInfo(byte[] data) throws IOException {
    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, null);
    GenericDatumReader<IndexedRecord> reader = BOOTSTRP_FILE_PARTITION_INFO_DESERIALIZER.get();
    GenericRecord record = (GenericRecord) reader.read(null, decoder).get(0);
    return HoodieBootstrapFilePartitionInfo.newBuilder()
        .setVersion((Integer) record.get("version"))
        .setBootstrapPartitionPath((String) record.get("bootstrapPartitionPath"))
        .setBootstrapFileStatus(
            deserializeHoodieFileStatus((GenericRecord) record.get("bootstrapFileStatus")))
        .setPartitionPath((String) record.get("partitionPath"))
        .build();
  }

  public static HoodieRollbackPartitionMetadata deserializeHoodieRollbackPartitionMetadata(byte[] data) throws IOException {
    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, null);
    GenericDatumReader<IndexedRecord> reader = ROLLBACK_PARTITION_METADATA_DESERIALIZER.get();
    GenericRecord record = (GenericRecord) reader.read(null, decoder).get(0);
    return HoodieRollbackPartitionMetadata.newBuilder()
        .setPartitionPath((String) record.get("partitionPath"))
        .setSuccessDeleteFiles((List<String>) record.get("successDeleteFiles"))
        .setFailedDeleteFiles((List<String>) record.get("failedDeleteFiles"))
        .setRollbackLogFiles((Map<String, Long>) record.get("rollbackLogFiles"))
        .setLogFilesFromFailedCommit((Map<String, Long>) record.get("logFilesFromFailedCommit"))
        .build();
  }

  private static HoodieFileStatus deserializeHoodieFileStatus(GenericRecord record) {
    return HoodieFileStatus.newBuilder()
        .setVersion((Integer) record.get("version"))
        .setAccessTime((Long) record.get("accessTime"))
        .setGroup((String) record.get("group"))
        .setBlockReplication((Integer) record.get("blockReplication"))
        .setBlockSize((Long) record.get("blockSize"))
        .setIsDir((Boolean) record.get("isDir"))
        .setLength((Long) record.get("length"))
        .setOwner((String) record.get("Owner"))
        .setPath(
            deserializeHoodiePath((GenericRecord) record.get("path")))
        .setModificationTime((Long) record.get("modificationTime"))
        .setPermission(
            deserializeHoodieFSPermission((GenericRecord) record.get("permission")))
        .setSymlink(
            deserializeHoodiePath((GenericRecord) record.get("symlink")))
        .build();
  }

  private static HoodiePath deserializeHoodiePath(GenericRecord record) {
    return HoodiePath.newBuilder()
        .setVersion((Integer) record.get("version"))
        .setUri((String) record.get("uri"))
        .build();
  }

  private static HoodieFSPermission deserializeHoodieFSPermission(GenericRecord record) {
    return HoodieFSPermission.newBuilder()
        .setVersion((Integer) record.get("version"))
        .setGroupAction((String) record.get("groupAction"))
        .setOtherAction((String) record.get("otherAction"))
        .setStickyBit((Boolean) record.get("stickyBit"))
        .setUserAction((String) record.get("userAction"))
        .build();
  }

  private static List<HoodieCompactionOperation> deserializeHoodieCompactionOperationList(List<GenericRecord> records) {
    if (records == null) {
      return Collections.emptyList();
    }
    List<HoodieCompactionOperation> operations = new ArrayList<>();
    for (GenericRecord record : records) {
      HoodieCompactionOperation.Builder builder = HoodieCompactionOperation.newBuilder();
      builder.setBaseInstantTime((String) record.get("baseInstantTime"))
          .setMetrics((Map<String, Double>) record.get("metrics"))
          .setBootstrapFilePath((String) record.get("bootstrapFilePath"))
          .setDataFilePath((String) record.get("dataFilePath"))
          .setPartitionPath((String) record.get("partitionPath"))
          .setDeltaFilePaths((List<String>) record.get("deltaFilePaths"))
          .setFileId((String) record.get("fileId"))
          .setBaseInstantTime((String) record.get("baseInstantTime"));
      operations.add(builder.build());
    }
    return operations;
  }

  private static HoodieCompactionStrategy deserializeHoodieCompactionStrategy(GenericRecord record) {
    return HoodieCompactionStrategy.newBuilder()
        .setVersion((Integer) record.get("version"))
        .setStrategyParams((Map<String, String>) record.get("strategyParams"))
        .setCompactorClassName((String) record.get("compactorClassName"))
        .build();
  }

  private static HoodieActionInstant deserializeHoodieActionInstant(GenericRecord record) {
    return HoodieActionInstant.newBuilder()
        .setAction((String) record.get("action"))
        .setState((String) record.get("state"))
        .setTimestamp((String) record.get("timestamp"))
        .build();
  }

  private static Map<String, List<HoodieCleanFileInfo>> deserializeFilePathsToBeDeletedPerPartition(Map<String, List<GenericRecord>> recordMap) {
    if (recordMap == null) {
      return Collections.emptyMap();
    }
    Map<String, List<HoodieCleanFileInfo>> deserialized = new HashMap<>();
    for (Map.Entry<String, List<GenericRecord>> entry : recordMap.entrySet()) {
      List<GenericRecord> value = entry.getValue();
      List<HoodieCleanFileInfo> fileInfos = value.stream().map(DeserializationUtils::deserializeHoodieCleanFileInfo).collect(Collectors.toList());
      deserialized.put(entry.getKey(), fileInfos);
    }
    return deserialized;
  }

  private static HoodieCleanFileInfo deserializeHoodieCleanFileInfo(GenericRecord record) {
    return HoodieCleanFileInfo.newBuilder()
        .setFilePath((String) record.get("filePath"))
        .setIsBootstrapBaseFile((Boolean) record.get("isBootstrapBaseFile"))
        .build();
  }

  private static HoodieCleanPartitionMetadata deserializeHoodieCleanPartitionMetadata(GenericRecord record) {
    return HoodieCleanPartitionMetadata.newBuilder()
        .setPartitionPath((String) record.get("partitionPath"))
        .setPolicy((String) record.get("policy"))
        .setDeletePathPatterns((List<String>) record.get("deletePathPatterns"))
        .setFailedDeleteFiles((List<String>) record.get("failedDeleteFiles"))
        .setSuccessDeleteFiles((List<String>) record.get("successDeleteFiles"))
        .setIsPartitionDeleted((Boolean) record.get("isPartitionDeleted"))
        .build();
  }

  private static Map<String, HoodieCleanPartitionMetadata> deserializeCleanPartitionMetadataMap(Map<String, GenericRecord> recordMap) {
    if (recordMap == null) {
      return Collections.emptyMap();
    }
    return recordMap.entrySet().stream()
        .collect(Collectors.toMap(
            Map.Entry::getKey,
            entry -> deserializeHoodieCleanPartitionMetadata(entry.getValue())
        ));
  }

  private static HoodieWriteStat deserializeHoodieWriteStat(GenericRecord record) {
    HoodieWriteStat.Builder builder = HoodieWriteStat.newBuilder();

    builder.setFileId(record.get("fileId") != null ? record.get("fileId").toString() : null);
    builder.setPath(record.get("path") != null ? record.get("path").toString() : null);
    builder.setPrevCommit(record.get("prevCommit") != null ? record.get("prevCommit").toString() : null);
    builder.setNumWrites(record.get("numWrites") != null ? (Long) record.get("numWrites") : null);
    builder.setNumDeletes(record.get("numDeletes") != null ? (Long) record.get("numDeletes") : null);
    builder.setNumUpdateWrites(record.get("numUpdateWrites") != null ? (Long) record.get("numUpdateWrites") : null);
    builder.setTotalWriteBytes(record.get("totalWriteBytes") != null ? (Long) record.get("totalWriteBytes") : null);
    builder.setTotalWriteErrors(record.get("totalWriteErrors") != null ? (Long) record.get("totalWriteErrors") : null);
    builder.setPartitionPath(record.get("partitionPath") != null ? record.get("partitionPath").toString() : null);
    builder.setTotalLogRecords(record.get("totalLogRecords") != null ? (Long) record.get("totalLogRecords") : null);
    builder.setTotalLogFiles(record.get("totalLogFiles") != null ? (Long) record.get("totalLogFiles") : null);
    builder.setTotalUpdatedRecordsCompacted(record.get("totalUpdatedRecordsCompacted") != null ?
        (Long) record.get("totalUpdatedRecordsCompacted") : null);
    builder.setNumInserts(record.get("numInserts") != null ? (Long) record.get("numInserts") : null);
    builder.setTotalLogBlocks(record.get("totalLogBlocks") != null ? (Long) record.get("totalLogBlocks") : null);
    builder.setTotalCorruptLogBlock(record.get("totalCorruptLogBlock") != null ? (Long) record.get("totalCorruptLogBlock") : null);
    builder.setTotalRollbackBlocks(record.get("totalRollbackBlocks") != null ? (Long) record.get("totalRollbackBlocks") : null);
    builder.setFileSizeInBytes(record.get("fileSizeInBytes") != null ? (Long) record.get("fileSizeInBytes") : null);
    builder.setLogVersion(record.get("logVersion") != null ? (Integer) record.get("logVersion") : null);
    builder.setLogOffset(record.get("logOffset") != null ? (Long) record.get("logOffset") : null);
    builder.setBaseFile(record.get("baseFile") != null ? record.get("baseFile").toString() : null);

    // log files array
    if (record.get("logFiles") != null) {
      builder.setLogFiles(
          ((List<?>) record.get("logFiles")).stream()
              .map(Object::toString)
              .collect(Collectors.toList())
      );
    }

    // CDC stats map
    if (record.get("cdcStats") != null) {
      Map<String, Long> cdcStats = ((Map<?, ?>) record.get("cdcStats")).entrySet().stream()
          .collect(Collectors.toMap(e -> e.getKey().toString(), e -> (Long) e.getValue()));
      builder.setCdcStats(cdcStats);
    }

    builder.setPrevBaseFile(record.get("prevBaseFile") != null ? record.get("prevBaseFile").toString() : null);
    builder.setMinEventTime(record.get("minEventTime") != null ? (Long) record.get("minEventTime") : null);
    builder.setMaxEventTime(record.get("maxEventTime") != null ? (Long) record.get("maxEventTime") : null);

    // runtimeStats nested record
    if (record.get("runtimeStats") != null) {
      GenericRecord rsRecord = (GenericRecord) record.get("runtimeStats");
      HoodieWriteStat.HoodieRuntimeStats.Builder rsBuilder = HoodieWriteStat.HoodieRuntimeStats.newBuilder();
      rsBuilder.setTotalScanTime(rsRecord.get("totalScanTime") != null ? (Long) rsRecord.get("totalScanTime") : null);
      rsBuilder.setTotalCreateTime(rsRecord.get("totalCreateTime") != null ? (Long) rsRecord.get("totalCreateTime") : null);
      rsBuilder.setTotalUpsertTime(rsRecord.get("totalUpsertTime") != null ? (Long) rsRecord.get("totalUpsertTime") : null);
      builder.setRuntimeStats(rsBuilder.build());
    }

    builder.setTotalLogFilesCompacted(record.get("totalLogFilesCompacted") != null ? (Long) record.get("totalLogFilesCompacted") : null);
    builder.setTotalLogReadTimeMs(record.get("totalLogReadTimeMs") != null ? (Long) record.get("totalLogReadTimeMs") : null);
    builder.setTotalLogSizeCompacted(record.get("totalLogSizeCompacted") != null ? (Long) record.get("totalLogSizeCompacted") : null);
    builder.setTempPath(record.get("tempPath") != null ? record.get("tempPath").toString() : null);
    builder.setNumUpdates(record.get("numUpdates") != null ? (Long) record.get("numUpdates") : null);

    return builder.build();
  }

  private static Map<String, List<HoodieWriteStat>> deserializeHoodieWriteStatsMap(Map<String, List<GenericRecord>> recordMap) {
    if (recordMap == null) {
      return Collections.emptyMap();
    }
    return recordMap.entrySet().stream()
        .collect(Collectors.toMap(
            Map.Entry::getKey,
            entry -> entry.getValue().stream().map(DeserializationUtils::deserializeHoodieWriteStat).collect(Collectors.toList())
        ));
  }

  private static HoodieSavepointPartitionMetadata deserializeHoodieSavepointPartitionMetadata(GenericRecord record) {
    if (record == null) {
      return null;
    }
    return HoodieSavepointPartitionMetadata.newBuilder()
        .setPartitionPath((String) record.get("partitionPath"))
        .setSavepointDataFile((List<String>) record.get("savepointDataFile"))
        .build();
  }

  private static Map<String, HoodieSavepointPartitionMetadata> deserializeHoodieSavepointPartitionMetadataMap(Map<String, GenericRecord> recordMap) {
    if (recordMap == null) {
      return Collections.emptyMap();
    }
    return recordMap.entrySet().stream()
        .collect(Collectors.toMap(
            Map.Entry::getKey,
            entry -> deserializeHoodieSavepointPartitionMetadata(entry.getValue())
        ));
  }

  private static HoodieRollbackPartitionMetadata deserializeHoodieRollbackPartitionMetadata(GenericRecord record) {
    if (record == null) {
      return null;
    }
    return HoodieRollbackPartitionMetadata.newBuilder()
        .setPartitionPath((String) record.get("partitionPath"))
        .setSuccessDeleteFiles((List<String>) record.get("successDeleteFiles"))
        .setFailedDeleteFiles((List<String>) record.get("failedDeleteFiles"))
        .setRollbackLogFiles((Map<String, Long>) record.get("rollbackLogFiles"))
        .setLogFilesFromFailedCommit((Map<String, Long>) record.get("logFilesFromFailedCommit"))
        .build();
  }

  private static Map<String, HoodieRollbackPartitionMetadata> deserializeHoodieRollbackPartitionMetadataMap(Map<String, GenericRecord> recordMap) {
    if (recordMap == null) {
      return Collections.emptyMap();
    }
    return recordMap.entrySet().stream()
        .collect(Collectors.toMap(
            Map.Entry::getKey,
            entry -> deserializeHoodieRollbackPartitionMetadata(entry.getValue())
        ));
  }

  private static Map<String, HoodieFileStatus> deserializeHoodieFileStatusMap(Map<String, GenericRecord> recordMap) {
    if (recordMap == null) {
      return Collections.emptyMap();
    }
    return recordMap.entrySet().stream()
        .collect(Collectors.toMap(
            Map.Entry::getKey,
            entry -> deserializeHoodieFileStatus(entry.getValue())
        ));
  }
}
