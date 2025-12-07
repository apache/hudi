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

package org.apache.hudi.common.testutils;

import org.apache.hudi.avro.model.HoodieActionInstant;
import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCleanerPlan;
import org.apache.hudi.avro.model.HoodieClusteringGroup;
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.avro.model.HoodieClusteringStrategy;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.avro.model.HoodieInstantInfo;
import org.apache.hudi.avro.model.HoodieRequestedReplaceMetadata;
import org.apache.hudi.avro.model.HoodieRestoreMetadata;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.avro.model.HoodieRollbackPartitionMetadata;
import org.apache.hudi.avro.model.HoodieRollbackPlan;
import org.apache.hudi.avro.model.HoodieRollbackRequest;
import org.apache.hudi.avro.model.HoodieSavepointMetadata;
import org.apache.hudi.avro.model.HoodieSavepointPartitionMetadata;
import org.apache.hudi.avro.model.HoodieSliceInfo;
import org.apache.hudi.common.HoodieCleanStat;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.TestLogReaderUtils;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.InstantGenerator;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.table.timeline.TimelineUtils;
import org.apache.hudi.common.table.timeline.versioning.DefaultInstantGenerator;
import org.apache.hudi.common.table.timeline.versioning.clean.CleanPlanV2MigrationHandler;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import lombok.Getter;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.time.temporal.ChronoUnit.SECONDS;
import static java.util.Collections.singletonMap;
import static org.apache.hudi.common.model.HoodieTableType.MERGE_ON_READ;
import static org.apache.hudi.common.model.WriteOperationType.CLUSTER;
import static org.apache.hudi.common.model.WriteOperationType.COMPACT;
import static org.apache.hudi.common.model.WriteOperationType.UPSERT;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.CLEAN_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.CLUSTERING_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.REPLACE_COMMIT_ACTION;
import static org.apache.hudi.common.testutils.FileCreateUtils.baseFileName;
import static org.apache.hudi.common.testutils.FileCreateUtils.createCleanFile;
import static org.apache.hudi.common.testutils.FileCreateUtils.createCommit;
import static org.apache.hudi.common.testutils.FileCreateUtils.createDeltaCommit;
import static org.apache.hudi.common.testutils.FileCreateUtils.createInflightCleanFile;
import static org.apache.hudi.common.testutils.FileCreateUtils.createInflightClusterCommit;
import static org.apache.hudi.common.testutils.FileCreateUtils.createInflightCommit;
import static org.apache.hudi.common.testutils.FileCreateUtils.createInflightCompaction;
import static org.apache.hudi.common.testutils.FileCreateUtils.createInflightDeltaCommit;
import static org.apache.hudi.common.testutils.FileCreateUtils.createInflightReplaceCommit;
import static org.apache.hudi.common.testutils.FileCreateUtils.createInflightRollbackFile;
import static org.apache.hudi.common.testutils.FileCreateUtils.createInflightSavepoint;
import static org.apache.hudi.common.testutils.FileCreateUtils.createLogFileMarker;
import static org.apache.hudi.common.testutils.FileCreateUtils.createMarkerFile;
import static org.apache.hudi.common.testutils.FileCreateUtils.createReplaceCommit;
import static org.apache.hudi.common.testutils.FileCreateUtils.createRequestedCleanFile;
import static org.apache.hudi.common.testutils.FileCreateUtils.createRequestedClusterCommit;
import static org.apache.hudi.common.testutils.FileCreateUtils.createRequestedCommit;
import static org.apache.hudi.common.testutils.FileCreateUtils.createRequestedDeltaCommit;
import static org.apache.hudi.common.testutils.FileCreateUtils.createRequestedReplaceCommit;
import static org.apache.hudi.common.testutils.FileCreateUtils.createRequestedRollbackFile;
import static org.apache.hudi.common.testutils.FileCreateUtils.createRestoreFile;
import static org.apache.hudi.common.testutils.FileCreateUtils.createRollbackFile;
import static org.apache.hudi.common.testutils.FileCreateUtils.createSavepointCommit;
import static org.apache.hudi.common.testutils.FileCreateUtils.deleteSavepointCommit;
import static org.apache.hudi.common.testutils.FileCreateUtils.logFileName;
import static org.apache.hudi.common.testutils.HoodieCommonTestHarness.BASE_FILE_EXTENSION;
import static org.apache.hudi.common.util.CleanerUtils.convertCleanMetadata;
import static org.apache.hudi.common.util.CommitUtils.buildMetadata;
import static org.apache.hudi.common.util.CommitUtils.getCommitActionType;
import static org.apache.hudi.common.util.StringUtils.EMPTY_STRING;

/**
 * Test Hoodie Table for testing only.
 */
public class HoodieTestTable implements AutoCloseable {

  public static final String PHONY_TABLE_SCHEMA =
      "{\"namespace\": \"org.apache.hudi.avro.model\", \"type\": \"record\", \"name\": \"PhonyRecord\", \"fields\": []}";

  private static final Logger LOG = LoggerFactory.getLogger(HoodieTestTable.class);
  private static final Random RANDOM = new Random();

  protected static HoodieTestTableState testTableState;
  private final List<String> inflightCommits = new ArrayList<>();

  protected final String basePath;
  protected final HoodieStorage storage;
  protected final FileSystem fs;
  protected HoodieTableMetaClient metaClient;
  protected String currentInstantTime;
  @Getter
  private boolean isNonPartitioned = false;
  protected Option<HoodieEngineContext> context;
  protected final InstantGenerator instantGenerator = new DefaultInstantGenerator();

  protected HoodieTestTable(String basePath, HoodieStorage storage,
                            HoodieTableMetaClient metaClient) {
    this(basePath, storage, metaClient, Option.empty());
  }

  protected HoodieTestTable(String basePath, HoodieStorage storage,
                            HoodieTableMetaClient metaClient, Option<HoodieEngineContext> context) {
    ValidationUtils.checkArgument(Objects.equals(basePath, metaClient.getBasePath().toString()));
    ValidationUtils.checkArgument(Objects.equals(
        storage.getFileSystem(), metaClient.getRawStorage().getFileSystem()));
    this.basePath = basePath;
    this.storage = storage;
    this.fs = (FileSystem) storage.getFileSystem();
    this.metaClient = metaClient;
    testTableState = HoodieTestTableState.of();
    this.context = context;
  }

  public static HoodieTestTable of(HoodieTableMetaClient metaClient) {
    testTableState = HoodieTestTableState.of();
    return new HoodieTestTable(metaClient.getBasePath().toString(), metaClient.getRawStorage(), metaClient);
  }

  public void setNonPartitioned() {
    this.isNonPartitioned = true;
  }

  public static String makeNewCommitTime(int sequence, String instantFormat) {
    return String.format(instantFormat, sequence);
  }

  public static String makeNewCommitTime() {
    return makeNewCommitTime(Instant.now());
  }

  public static String makeNewCommitTime(Instant dateTime) {
    return TimelineUtils.formatDate(Date.from(dateTime));
  }

  public static List<String> makeIncrementalCommitTimes(int num, int firstOffsetSeconds, int deltaSecs) {
    final Instant now = Instant.now();
    return IntStream.range(0, num)
        .mapToObj(i -> makeNewCommitTime(now.plus(deltaSecs == 0 ? (firstOffsetSeconds + i) : (i == 0 ? (firstOffsetSeconds) : (i * deltaSecs) + i), SECONDS)))
        .collect(Collectors.toList());
  }

  public HoodieTestTable addRequestedCommit(String instantTime) throws Exception {
    createRequestedCommit(metaClient, instantTime);
    currentInstantTime = instantTime;
    return this;
  }

  public HoodieTestTable addInflightCommit(String instantTime) throws Exception {
    createRequestedCommit(metaClient, instantTime);
    createInflightCommit(metaClient, instantTime);
    inflightCommits.add(instantTime);
    currentInstantTime = instantTime;
    return this;
  }

  public HoodieTestTable addInflightDeltaCommit(String instantTime) throws Exception {
    createRequestedDeltaCommit(metaClient, instantTime);
    createInflightDeltaCommit(metaClient, instantTime);
    inflightCommits.add(instantTime);
    currentInstantTime = instantTime;
    return this;
  }

  public HoodieTestTable addCommit(String instantTime) throws Exception {
    return addCommit(instantTime, Option.empty());
  }

  public HoodieTestTable addCommit(String instantTime, Option<HoodieCommitMetadata> metadata) throws Exception {
    return addCommit(instantTime, Option.empty(), metadata);
  }

  public HoodieTestTable addCommit(String instantTime, Option<String> completionTime, Option<HoodieCommitMetadata> metadata) throws Exception {
    createRequestedCommit(metaClient, instantTime);
    createInflightCommit(metaClient, instantTime);
    createCommit(metaClient, metaClient.getTimelineLayout().getCommitMetadataSerDe(), instantTime, completionTime, metadata);
    currentInstantTime = instantTime;
    return this;
  }

  public HoodieTestTable addSavepointCommit(String instantTime, Option<String> completeTime, HoodieSavepointMetadata savepointMetadata) throws IOException {
    createInflightSavepoint(metaClient, instantTime);
    createSavepointCommit(metaClient, instantTime, completeTime, savepointMetadata);
    return this;
  }

  public HoodieCommitMetadata createCommitMetadata(String commitTime, WriteOperationType operationType,
                                                   List<String> partitions, int filesPerPartition, boolean bootstrap) {
    Map<String, List<Pair<String, Integer>>> partitionToFilesNameLengthMap = getPartitionFiles(partitions,
        filesPerPartition);
    HoodieTestTableState testTableState = getTestTableStateWithPartitionFileInfo(operationType,
        metaClient.getTableType(), commitTime, partitionToFilesNameLengthMap);
    return createCommitMetadata(operationType, commitTime, testTableState, bootstrap);
  }

  public HoodieCommitMetadata createCommitMetadata(WriteOperationType operationType, String commitTime,
                                                   HoodieTestTableState testTableState) {
    String actionType = getCommitActionType(operationType, metaClient.getTableType());
    return createCommitMetadata(operationType, commitTime, Collections.emptyMap(), testTableState, false, actionType);
  }

  public HoodieCommitMetadata createCommitMetadata(WriteOperationType operationType, String commitTime,
                                                   HoodieTestTableState testTableState, boolean bootstrap) {
    String actionType = getCommitActionType(operationType, metaClient.getTableType());
    return createCommitMetadata(operationType, commitTime, Collections.emptyMap(), testTableState, bootstrap,
        actionType);
  }

  public HoodieCommitMetadata createCommitMetadata(WriteOperationType operationType, String commitTime,
                                                   Map<String, List<String>> partitionToReplaceFileIds,
                                                   HoodieTestTableState testTableState, boolean bootstrap, String action) {
    List<HoodieWriteStat> writeStats = generateHoodieWriteStatForPartition(testTableState.getPartitionToBaseFileInfoMap(commitTime), commitTime, bootstrap);
    if (MERGE_ON_READ.equals(metaClient.getTableType()) && UPSERT.equals(operationType)) {
      writeStats.addAll(generateHoodieWriteStatForPartitionLogFiles(testTableState.getPartitionToLogFileInfoMap(commitTime), commitTime, bootstrap));
    }
    Map<String, String> extraMetadata = singletonMap("test", "test");
    return buildMetadata(writeStats, partitionToReplaceFileIds, Option.of(extraMetadata), operationType, PHONY_TABLE_SCHEMA, action);
  }

  public HoodieTestTable moveInflightCommitToComplete(String instantTime, HoodieCommitMetadata metadata) throws IOException {
    if (metaClient.getTableType() == HoodieTableType.COPY_ON_WRITE) {
      createCommit(metaClient, metaClient.getCommitMetadataSerDe(), instantTime, Option.of(metadata));
    } else {
      createDeltaCommit(metaClient, metaClient.getCommitMetadataSerDe(), instantTime, Option.empty(), metadata);
    }
    inflightCommits.remove(instantTime);
    currentInstantTime = instantTime;
    return this;
  }

  public void moveCompleteCommitToInflight(String instantTime) throws IOException {
    if (metaClient.getTableType() == HoodieTableType.COPY_ON_WRITE) {
      FileCreateUtils.deleteCommit(metaClient, instantTime);
    } else {
      FileCreateUtils.deleteDeltaCommit(metaClient, instantTime);
    }
  }

  public HoodieTestTable addDeltaCommit(String instantTime) throws Exception {
    createRequestedDeltaCommit(metaClient, instantTime);
    createInflightDeltaCommit(metaClient, instantTime);
    createDeltaCommit(metaClient, instantTime);
    currentInstantTime = instantTime;
    return this;
  }

  public HoodieTestTable addDeltaCommit(String instantTime, HoodieCommitMetadata metadata) throws Exception {
    createRequestedDeltaCommit(metaClient, instantTime);
    createInflightDeltaCommit(metaClient, instantTime);
    createDeltaCommit(metaClient, metaClient.getCommitMetadataSerDe(), instantTime, Option.empty(), metadata);
    currentInstantTime = instantTime;
    return this;
  }

  public HoodieTestTable addDeltaCommit(String instantTime, Option<String> completeTime, HoodieCommitMetadata metadata) throws Exception {
    createRequestedDeltaCommit(metaClient, instantTime);
    createInflightDeltaCommit(metaClient, instantTime);
    createDeltaCommit(metaClient, metaClient.getCommitMetadataSerDe(), instantTime, completeTime, metadata);
    currentInstantTime = instantTime;
    return this;
  }

  public HoodieTestTable addReplaceCommit(
      String instantTime,
      Option<HoodieRequestedReplaceMetadata> requestedReplaceMetadata,
      Option<HoodieCommitMetadata> inflightReplaceMetadata,
      HoodieReplaceCommitMetadata completeReplaceMetadata) throws Exception {
    return addReplaceCommit(instantTime, Option.empty(), requestedReplaceMetadata, inflightReplaceMetadata, completeReplaceMetadata);
  }

  public HoodieTestTable addReplaceCommit(
      String instantTime,
      Option<String> completeTime,
      Option<HoodieRequestedReplaceMetadata> requestedReplaceMetadata,
      Option<HoodieCommitMetadata> inflightReplaceMetadata,
      HoodieReplaceCommitMetadata completeReplaceMetadata) throws Exception {
    createRequestedReplaceCommit(metaClient, instantTime, requestedReplaceMetadata);
    createInflightReplaceCommit(metaClient, metaClient.getCommitMetadataSerDe(), instantTime, inflightReplaceMetadata);
    createReplaceCommit(metaClient, metaClient.getCommitMetadataSerDe(), instantTime, completeTime, completeReplaceMetadata);
    currentInstantTime = instantTime;
    return this;
  }

  public HoodieTestTable addPendingReplace(
      String instantTime, Option<HoodieRequestedReplaceMetadata> requestedReplaceMetadata, Option<HoodieCommitMetadata> inflightReplaceMetadata) throws Exception {
    createRequestedReplaceCommit(metaClient, instantTime, requestedReplaceMetadata);
    createInflightReplaceCommit(metaClient, metaClient.getCommitMetadataSerDe(), instantTime, inflightReplaceMetadata);
    currentInstantTime = instantTime;
    return this;
  }

  public HoodieTestTable addPendingCluster(String instantTime, HoodieRequestedReplaceMetadata requestedReplaceMetadata, Option<HoodieReplaceCommitMetadata> inflightReplaceMetadata) throws Exception {
    createRequestedClusterCommit(metaClient, instantTime, requestedReplaceMetadata);
    createInflightClusterCommit(metaClient, metaClient.getCommitMetadataSerDe(), instantTime, inflightReplaceMetadata);
    currentInstantTime = instantTime;
    return this;
  }

  public HoodieTestTable addRequestedCluster(String instantTime, HoodieRequestedReplaceMetadata requestedReplaceMetadata) throws Exception {
    createRequestedClusterCommit(metaClient, instantTime, requestedReplaceMetadata);
    currentInstantTime = instantTime;
    return this;
  }

  public HoodieTestTable addInflightCluster(String instantTime) throws Exception {
    createInflightClusterCommit(metaClient, metaClient.getCommitMetadataSerDe(), instantTime, Option.empty());
    currentInstantTime = instantTime;
    return this;
  }

  public HoodieTestTable addCluster(
      String instantTime,
      HoodieRequestedReplaceMetadata requestedReplaceMetadata,
      Option<HoodieReplaceCommitMetadata> inflightReplaceMetadata,
      HoodieReplaceCommitMetadata completeReplaceMetadata) throws Exception {
    createRequestedClusterCommit(metaClient, instantTime, requestedReplaceMetadata);
    createInflightClusterCommit(metaClient, metaClient.getCommitMetadataSerDe(), instantTime, inflightReplaceMetadata);
    createReplaceCommit(metaClient, metaClient.getCommitMetadataSerDe(), instantTime, completeReplaceMetadata);
    currentInstantTime = instantTime;
    return this;
  }

  public HoodieTestTable addCluster(
      String instantTime,
      HoodieRequestedReplaceMetadata requestedReplaceMetadata,
      Option<HoodieReplaceCommitMetadata> inflightReplaceMetadata,
      HoodieReplaceCommitMetadata completeReplaceMetadata,
      String completionTime) throws Exception {
    createRequestedClusterCommit(metaClient, instantTime, requestedReplaceMetadata);
    createInflightClusterCommit(metaClient, metaClient.getCommitMetadataSerDe(), instantTime, inflightReplaceMetadata);
    createReplaceCommit(metaClient, metaClient.getCommitMetadataSerDe(), instantTime, completionTime, completeReplaceMetadata);
    currentInstantTime = instantTime;
    return this;
  }

  public HoodieTestTable addRequestedReplace(String instantTime, Option<HoodieRequestedReplaceMetadata> requestedReplaceMetadata) throws Exception {
    createRequestedReplaceCommit(metaClient, instantTime, requestedReplaceMetadata);
    currentInstantTime = instantTime;
    return this;
  }

  public HoodieTestTable addInflightReplace(String instantTime, Option<HoodieCommitMetadata> inflightReplaceMetadata) throws Exception {
    createInflightReplaceCommit(metaClient, metaClient.getCommitMetadataSerDe(), instantTime, inflightReplaceMetadata);
    currentInstantTime = instantTime;
    return this;
  }

  public HoodieTestTable addInflightClean(String instantTime, HoodieCleanerPlan cleanerPlan) throws IOException {
    createRequestedCleanFile(metaClient, instantTime, cleanerPlan);
    createInflightCleanFile(metaClient, instantTime, cleanerPlan);
    currentInstantTime = instantTime;
    return this;
  }

  public HoodieTestTable addClean(String instantTime, HoodieCleanerPlan cleanerPlan, HoodieCleanMetadata metadata) throws IOException {
    return addClean(instantTime, Option.empty(), cleanerPlan, metadata, false, false);
  }

  public HoodieTestTable addClean(String instantTime, Option<String> completeTime, HoodieCleanerPlan cleanerPlan, HoodieCleanMetadata metadata) throws IOException {
    return addClean(instantTime, completeTime, cleanerPlan, metadata, false, false);
  }

  public HoodieTestTable addClean(
      String instantTime, Option<String> completeTime, HoodieCleanerPlan cleanerPlan, HoodieCleanMetadata metadata,
      boolean isEmptyForAll, boolean isEmptyCompleted) throws IOException {
    createRequestedCleanFile(metaClient, instantTime, cleanerPlan, isEmptyForAll);
    createInflightCleanFile(metaClient, instantTime, cleanerPlan, isEmptyForAll);
    createCleanFile(metaClient, instantTime, completeTime, metadata, isEmptyCompleted);
    currentInstantTime = instantTime;
    return this;
  }

  public HoodieTestTable addClean(
      String instantTime, HoodieCleanerPlan cleanerPlan, HoodieCleanMetadata metadata,
      boolean isEmptyForAll, boolean isEmptyCompleted) throws IOException {
    return addClean(instantTime, Option.empty(), cleanerPlan, metadata, isEmptyForAll, isEmptyCompleted);
  }

  public HoodieTestTable addClean(String instantTime) throws IOException {
    HoodieCleanerPlan cleanerPlan = new HoodieCleanerPlan(new HoodieActionInstant(EMPTY_STRING, EMPTY_STRING, EMPTY_STRING),
        EMPTY_STRING, EMPTY_STRING, new HashMap<>(), CleanPlanV2MigrationHandler.VERSION, new HashMap<>(), new ArrayList<>(), Collections.EMPTY_MAP);
    HoodieCleanStat cleanStats = new HoodieCleanStat(
        HoodieCleaningPolicy.KEEP_LATEST_FILE_VERSIONS,
        HoodieTestUtils.DEFAULT_PARTITION_PATHS[RANDOM.nextInt(HoodieTestUtils.DEFAULT_PARTITION_PATHS.length)],
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList(),
        instantTime,
        "");
    HoodieCleanMetadata cleanMetadata = convertCleanMetadata(instantTime, Option.of(0L), Collections.singletonList(cleanStats), Collections.EMPTY_MAP);
    return HoodieTestTable.of(metaClient).addClean(instantTime, cleanerPlan, cleanMetadata);
  }

  public Pair<HoodieCleanerPlan, HoodieCleanMetadata> getHoodieCleanMetadata(String commitTime, HoodieTestTableState testTableState) {
    HoodieCleanerPlan cleanerPlan = new HoodieCleanerPlan(new HoodieActionInstant(commitTime, CLEAN_ACTION, EMPTY_STRING),
        EMPTY_STRING, EMPTY_STRING, new HashMap<>(), CleanPlanV2MigrationHandler.VERSION, new HashMap<>(), new ArrayList<>(), Collections.EMPTY_MAP);
    List<HoodieCleanStat> cleanStats = new ArrayList<>();
    for (Map.Entry<String, List<String>> entry : testTableState.getPartitionToFileIdMapForCleaner(commitTime).entrySet()) {
      cleanStats.add(new HoodieCleanStat(HoodieCleaningPolicy.KEEP_LATEST_FILE_VERSIONS,
          entry.getKey(), entry.getValue(), entry.getValue(), Collections.emptyList(), commitTime, ""));
    }
    return Pair.of(cleanerPlan, convertCleanMetadata(commitTime, Option.of(0L), cleanStats, Collections.EMPTY_MAP));
  }

  public HoodieTestTable addRequestedRollback(String instantTime, HoodieRollbackPlan plan) throws IOException {
    createRequestedRollbackFile(metaClient, instantTime, plan);
    currentInstantTime = instantTime;
    return this;
  }

  public HoodieTestTable addInflightRollback(String instantTime) throws IOException {
    createInflightRollbackFile(metaClient, instantTime);
    currentInstantTime = instantTime;
    return this;
  }

  public HoodieTestTable addRollback(String instantTime, HoodieRollbackMetadata rollbackMetadata, HoodieRollbackPlan rollbackPlan) throws IOException {
    return addRollback(instantTime, rollbackMetadata, false, rollbackPlan);
  }

  public HoodieTestTable addRollback(String instantTime, HoodieRollbackMetadata rollbackMetadata, boolean isEmpty, HoodieRollbackPlan rollbackPlan) throws IOException {
    if (rollbackPlan != null) {
      createRequestedRollbackFile(metaClient, instantTime, rollbackPlan);
    } else {
      createRequestedRollbackFile(metaClient, instantTime);
    }
    createInflightRollbackFile(metaClient, instantTime);
    // createRollbackFile(metaClient, instantTime, rollbackMetadata, isEmpty);
    currentInstantTime = instantTime;
    return this;
  }

  public HoodieTestTable addRollbackCompleted(String instantTime, HoodieRollbackMetadata rollbackMetadata, boolean isEmpty) throws IOException {
    createRollbackFile(metaClient, instantTime, rollbackMetadata, isEmpty);
    currentInstantTime = instantTime;
    return this;
  }

  public HoodieTestTable addRestore(String instantTime, HoodieRestoreMetadata restoreMetadata) throws IOException {
    createRestoreFile(metaClient, instantTime, restoreMetadata);
    currentInstantTime = instantTime;
    return this;
  }

  public HoodieRollbackMetadata getRollbackMetadata(String instantTimeToDelete, Map<String, List<String>> partitionToFilesMeta, boolean shouldAddRollbackLogFile) throws Exception {
    HoodieRollbackMetadata rollbackMetadata = new HoodieRollbackMetadata();
    rollbackMetadata.setCommitsRollback(Collections.singletonList(instantTimeToDelete));
    rollbackMetadata.setStartRollbackTime(instantTimeToDelete);
    Map<String, HoodieRollbackPartitionMetadata> partitionMetadataMap = new HashMap<>();
    for (Map.Entry<String, List<String>> entry : partitionToFilesMeta.entrySet()) {
      HoodieRollbackPartitionMetadata rollbackPartitionMetadata = new HoodieRollbackPartitionMetadata();
      rollbackPartitionMetadata.setPartitionPath(entry.getKey());
      rollbackPartitionMetadata.setSuccessDeleteFiles(entry.getValue());
      rollbackPartitionMetadata.setFailedDeleteFiles(new ArrayList<>());
      if (shouldAddRollbackLogFile) {
        long rollbackLogFileSize = 50 + RANDOM.nextInt(500);
        String fileId = UUID.randomUUID().toString();
        String logFileName = logFileName(instantTimeToDelete, fileId, 0);
        FileCreateUtils.createLogFile(metaClient, entry.getKey(), instantTimeToDelete, fileId, 0, (int) rollbackLogFileSize);
        rollbackPartitionMetadata.setRollbackLogFiles(singletonMap(logFileName, rollbackLogFileSize));
      }
      partitionMetadataMap.put(entry.getKey(), rollbackPartitionMetadata);
    }
    rollbackMetadata.setPartitionMetadata(partitionMetadataMap);
    rollbackMetadata.setInstantsRollback(Collections.singletonList(new HoodieInstantInfo(instantTimeToDelete, HoodieTimeline.ROLLBACK_ACTION)));
    return rollbackMetadata;
  }

  /**
   * Return a map of log file name to file size that were expected to be rolled back in that partition.
   */
  private Map<String, Long> getWrittenLogFiles(String instant, Map.Entry<String, List<String>> entry) {
    Map<String, Long> writtenLogFiles = new HashMap<>();
    for (String fileName : entry.getValue()) {
      if (HadoopFSUtils.isLogFile(new Path(fileName))) {
        if (testTableState.getPartitionToLogFileInfoMap(instant) != null
            && testTableState.getPartitionToLogFileInfoMap(instant).containsKey(entry.getKey())) {
          List<Pair<String, Integer[]>> fileInfos = testTableState.getPartitionToLogFileInfoMap(instant).get(entry.getKey());
          for (Pair<String, Integer[]> fileInfo : fileInfos) {
            if (fileName.equals(logFileName(instant, fileInfo.getLeft(), fileInfo.getRight()[0]))) {
              writtenLogFiles.put(fileName, Long.valueOf(fileInfo.getRight()[1]));
            }
          }
        }
      }
    }
    return writtenLogFiles;
  }

  public HoodieSavepointMetadata getSavepointMetadata(String instant, Map<String, List<String>> partitionToFilesMeta) {
    HoodieSavepointMetadata savepointMetadata = new HoodieSavepointMetadata();
    savepointMetadata.setSavepointedAt(12345L);
    Map<String, HoodieSavepointPartitionMetadata> partitionMetadataMap = new HashMap<>();
    for (Map.Entry<String, List<String>> entry : partitionToFilesMeta.entrySet()) {
      HoodieSavepointPartitionMetadata savepointPartitionMetadata = new HoodieSavepointPartitionMetadata();
      savepointPartitionMetadata.setPartitionPath(entry.getKey());
      savepointPartitionMetadata.setSavepointDataFile(entry.getValue());
      partitionMetadataMap.put(entry.getKey(), savepointPartitionMetadata);
    }
    savepointMetadata.setPartitionMetadata(partitionMetadataMap);
    savepointMetadata.setSavepointedBy("test");
    savepointMetadata.setComments("test_comment");
    return savepointMetadata;
  }

  public HoodieTestTable addRequestedCompaction(String instantTime) {
    List<FileSlice> fileSlices = new ArrayList<>();
    fileSlices.add(new FileSlice("par1", instantTime, "fg-1"));
    fileSlices.add(new FileSlice("par2", instantTime, "fg-2"));
    HoodieCompactionPlan compactionPlan = CompactionUtils
        .buildFromFileSlices(fileSlices.stream().map(fs -> Pair.of(fs.getPartitionPath(), fs))
            .collect(Collectors.toList()), Option.empty(), Option.empty());
    HoodieInstant compactionInstant = instantGenerator.createNewInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, instantTime);
    metaClient.getActiveTimeline().saveToCompactionRequested(compactionInstant, compactionPlan);
    currentInstantTime = instantTime;
    return this;
  }

  public HoodieTestTable addRequestedCompaction(String instantTime, HoodieCompactionPlan compactionPlan) {
    HoodieInstant compactionInstant = instantGenerator.createNewInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, instantTime);
    metaClient.getActiveTimeline().saveToCompactionRequested(compactionInstant, compactionPlan);
    currentInstantTime = instantTime;
    return this;
  }

  public HoodieTestTable addRequestedCompaction(String instantTime, FileSlice... fileSlices) {
    HoodieCompactionPlan plan = CompactionUtils
        .buildFromFileSlices(Arrays.stream(fileSlices).map(fs -> Pair.of(fs.getPartitionPath(), fs))
            .collect(Collectors.toList()), Option.empty(), Option.empty());
    return addRequestedCompaction(instantTime, plan);
  }

  public HoodieTestTable addInflightCompaction(String instantTime, HoodieCommitMetadata commitMetadata) throws Exception {
    List<FileSlice> fileSlices = new ArrayList<>();
    for (Map.Entry<String, List<HoodieWriteStat>> entry : commitMetadata.getPartitionToWriteStats().entrySet()) {
      for (HoodieWriteStat stat : entry.getValue()) {
        fileSlices.add(new FileSlice(entry.getKey(), instantTime, stat.getPath()));
      }
    }
    this.addRequestedCompaction(instantTime, fileSlices.toArray(new FileSlice[0]));
    createInflightCompaction(metaClient, instantTime);
    inflightCommits.add(instantTime);
    currentInstantTime = instantTime;
    return this;
  }

  public HoodieTestTable addCompaction(String instantTime, HoodieCommitMetadata commitMetadata) throws Exception {
    addInflightCompaction(instantTime, commitMetadata);
    this.inflightCommits.remove(instantTime);
    createCommit(metaClient, metaClient.getCommitMetadataSerDe(), instantTime, Option.of(commitMetadata));
    return this;
  }

  public HoodieTestTable addCompaction(String instantTime, Option<String> completeTime, HoodieCommitMetadata commitMetadata) throws Exception {
    addInflightCompaction(instantTime, commitMetadata);
    this.inflightCommits.remove(instantTime);
    createCommit(metaClient, metaClient.getCommitMetadataSerDe(), instantTime, completeTime, Option.of(commitMetadata));
    return this;
  }

  public HoodieTestTable addDeletePartitionCommit(String instantTime, String partition, List<String> fileIds) throws Exception {
    forReplaceCommit(instantTime);
    WriteOperationType operationType = WriteOperationType.DELETE_PARTITION;
    Pair<HoodieRequestedReplaceMetadata, HoodieReplaceCommitMetadata> metas =
        generateReplaceCommitMetadata(instantTime, partition, fileIds, Option.empty(), operationType);
    return addReplaceCommit(instantTime, Option.of(metas.getLeft()), Option.empty(), metas.getRight());
  }

  private Pair<HoodieRequestedReplaceMetadata, HoodieReplaceCommitMetadata> generateReplaceCommitMetadata(
      String instantTime, String partition, List<String> replacedFileIds, Option<String> newFileId, WriteOperationType operationType) {
    HoodieRequestedReplaceMetadata requestedReplaceMetadata = new HoodieRequestedReplaceMetadata();
    requestedReplaceMetadata.setOperationType(operationType.toString());
    requestedReplaceMetadata.setVersion(1);
    List<HoodieSliceInfo> sliceInfos = replacedFileIds.stream()
        .map(replacedFileId -> HoodieSliceInfo.newBuilder().setFileId(replacedFileId).build())
        .collect(Collectors.toList());
    List<HoodieClusteringGroup> clusteringGroups = new ArrayList<>();
    clusteringGroups.add(HoodieClusteringGroup.newBuilder()
        .setVersion(1).setNumOutputFileGroups(1).setMetrics(Collections.emptyMap())
        .setSlices(sliceInfos).build());
    requestedReplaceMetadata.setExtraMetadata(Collections.emptyMap());
    requestedReplaceMetadata.setClusteringPlan(HoodieClusteringPlan.newBuilder()
        .setVersion(1).setExtraMetadata(Collections.emptyMap())
        .setStrategy(HoodieClusteringStrategy.newBuilder().setStrategyClassName("").setVersion(1).build())
        .setInputGroups(clusteringGroups).build());

    HoodieReplaceCommitMetadata replaceMetadata = new HoodieReplaceCommitMetadata();
    replaceMetadata.addMetadata(HoodieCommitMetadata.SCHEMA_KEY, HoodieTestTable.PHONY_TABLE_SCHEMA);
    replacedFileIds.forEach(replacedFileId -> replaceMetadata.addReplaceFileId(partition, replacedFileId));
    replaceMetadata.setOperationType(operationType);
    if (newFileId.isPresent() && !StringUtils.isNullOrEmpty(newFileId.get())) {
      HoodieWriteStat writeStat = new HoodieWriteStat();
      writeStat.setPartitionPath(partition);
      writeStat.setPath(partition + "/" + FSUtils.makeBaseFileName(instantTime, "1-0-1", newFileId.get(), BASE_FILE_EXTENSION));
      writeStat.setFileId(newFileId.get());
      writeStat.setTotalWriteBytes(1);
      writeStat.setFileSizeInBytes(1);
      replaceMetadata.addWriteStat(partition, writeStat);
    }
    return Pair.of(requestedReplaceMetadata, replaceMetadata);
  }

  public HoodieTestTable moveInflightCompactionToComplete(String instantTime, HoodieCommitMetadata metadata) throws IOException {
    createCommit(metaClient, metaClient.getCommitMetadataSerDe(), instantTime, Option.of(metadata));
    inflightCommits.remove(instantTime);
    currentInstantTime = instantTime;
    return this;
  }

  public HoodieTestTable addSavepoint(String instantTime, HoodieSavepointMetadata savepointMetadata) throws IOException {
    return addSavepoint(instantTime, Option.empty(), savepointMetadata);
  }

  public HoodieTestTable addSavepoint(String instantTime, Option<String> completeTime, HoodieSavepointMetadata savepointMetadata) throws IOException {
    createInflightSavepoint(metaClient, instantTime);
    createSavepointCommit(metaClient, instantTime, completeTime, savepointMetadata);
    return this;
  }

  public HoodieTestTable deleteSavepoint(String instantTime) throws IOException {
    deleteSavepointCommit(metaClient, instantTime, storage);
    return this;
  }

  public HoodieTestTable forCommit(String instantTime) {
    currentInstantTime = instantTime;
    return this;
  }

  public HoodieTestTable forDeltaCommit(String instantTime) {
    currentInstantTime = instantTime;
    return this;
  }

  public HoodieTestTable forReplaceCommit(String instantTime) {
    currentInstantTime = instantTime;
    return this;
  }

  public HoodieTestTable withPartitionMetaFiles(String... partitionPaths) throws IOException {
    for (String partitionPath : partitionPaths) {
      FileCreateUtils.createPartitionMetaFile(basePath, partitionPath);
    }
    return this;
  }

  public HoodieTestTable withPartitionMetaFiles(List<String> partitionPaths) throws IOException {
    for (String partitionPath : partitionPaths) {
      FileCreateUtils.createPartitionMetaFile(basePath, partitionPath);
    }
    return this;
  }

  public HoodieTestTable withMarkerFile(String partitionPath, String fileId, IOType ioType) throws IOException {
    createMarkerFile(metaClient, partitionPath, currentInstantTime, fileId, ioType);
    return this;
  }

  public HoodieTestTable withMarkerFiles(String partitionPath, int num, IOType ioType) throws IOException {
    String[] fileIds = IntStream.range(0, num).mapToObj(i -> UUID.randomUUID().toString()).toArray(String[]::new);
    return withMarkerFiles(partitionPath, fileIds, ioType);
  }

  public HoodieTestTable withMarkerFiles(String partitionPath, String[] fileIds, IOType ioType) throws IOException {
    for (String fileId : fileIds) {
      createMarkerFile(metaClient, partitionPath, currentInstantTime, fileId, ioType);
    }
    return this;
  }

  public HoodieTestTable withLogMarkerFile(String partitionPath, String fileId, IOType ioType) throws IOException {
    String logFileName = FSUtils.makeLogFileName(fileId, HoodieLogFile.DELTA_EXTENSION, currentInstantTime, HoodieLogFile.LOGFILE_BASE_VERSION, HoodieLogFormat.UNKNOWN_WRITE_TOKEN);
    String markerFileName = FileCreateUtils.markerFileName(logFileName, ioType);
    FileCreateUtils.createMarkerFile(metaClient, partitionPath, currentInstantTime, markerFileName);
    return this;
  }

  public HoodieTestTable withLogMarkerFile(String baseInstantTime, String partitionPath, String fileId, IOType ioType, int logVersion)
      throws IOException {
    createLogFileMarker(basePath, partitionPath, baseInstantTime, currentInstantTime, fileId, ioType, logVersion);
    return this;
  }

  public HoodieTestTable withLogMarkerFile(String partitionPath, String fileName) throws IOException {
    createLogFileMarker(metaClient, partitionPath, currentInstantTime, fileName);
    return this;
  }

  /**
   * Insert one base file to each of the given distinct partitions.
   *
   * @return A {@link Map} of partition and its newly inserted file's id.
   */
  public Map<String, String> getFileIdsWithBaseFilesInPartitions(String... partitions) throws Exception {
    Map<String, String> partitionFileIdMap = new HashMap<>();
    for (String p : partitions) {
      String fileId = UUID.randomUUID().toString();
      FileCreateUtils.createBaseFile(metaClient, p, currentInstantTime, fileId);
      partitionFileIdMap.put(p, fileId);
    }
    return partitionFileIdMap;
  }

  public Pair<HoodieTestTable, List<String>> withBaseFilesInPartitions(Map<String, String> partitionAndFileId) throws Exception {
    List<String> files = new ArrayList<>();
    for (Map.Entry<String, String> pair : partitionAndFileId.entrySet()) {
      files.addAll(withBaseFilesInPartition(pair.getKey(), pair.getValue()).getValue());
    }
    return Pair.of(this, files);
  }

  public Pair<HoodieTestTable, List<String>> withBaseFilesInPartition(String partition, String... fileIds) throws Exception {
    List<String> files = new ArrayList<>();
    for (String f : fileIds) {
      files.add(FileCreateUtils.createBaseFile(metaClient, partition, currentInstantTime, f));
    }
    return Pair.of(this, files);
  }

  public HoodieTestTable withBaseFilesInPartition(String partition, int... lengths) throws Exception {
    for (int l : lengths) {
      String fileId = UUID.randomUUID().toString();
      FileCreateUtils.createBaseFile(metaClient, partition, currentInstantTime, fileId, l);
    }
    return this;
  }

  public HoodieTestTable withBaseFilesInPartition(String partition, List<Pair<String, Integer>> fileInfos) throws Exception {
    for (Pair<String, Integer> fileInfo : fileInfos) {
      FileCreateUtils.createBaseFile(metaClient, partition, currentInstantTime, fileInfo.getKey(), fileInfo.getValue());
    }
    return this;
  }

  public String getFileIdWithLogFile(String partitionPath) throws Exception {
    String fileId = UUID.randomUUID().toString();
    withLogFile(partitionPath, fileId);
    return fileId;
  }

  public Pair<HoodieTestTable, List<String>> withLogFile(String partitionPath, String fileId) throws Exception {
    return withLogFile(partitionPath, fileId, 0);
  }

  public Pair<HoodieTestTable, List<String>> withLogFile(String partitionPath, String fileId, int... versions) throws Exception {
    return withLogFile(partitionPath, fileId, currentInstantTime, versions);
  }

  public Pair<HoodieTestTable, List<String>> withLogFile(String partitionPath, String fileId,
                                                         String instantTime, int... versions) throws Exception {
    List<String> logFiles = new ArrayList<>();
    for (int version : versions) {
      logFiles.add(FileCreateUtils.createLogFile(metaClient, partitionPath, instantTime, fileId, version));
    }
    return Pair.of(this, logFiles);
  }

  /**
   * Writes log files in the partition.
   *
   * @param partition partition to write log files
   * @param fileInfos list of pairs of file ID, log version, and file size of the log files
   * @return {@link HoodieTestTable} instance
   * @throws Exception upon error
   */
  public HoodieTestTable withLogFilesInPartition(String partition, List<Pair<String, Integer[]>> fileInfos) throws Exception {
    return withLogFilesAndBaseInstantTimeInPartition(partition,
        fileInfos.stream().map(e -> Pair.of(Pair.of(currentInstantTime, e.getLeft()), e.getRight())).collect(Collectors.toList()));
  }

  /**
   * Writes log files in the partition.
   *
   * @param partition partition to write log files
   * @param fileInfos list of pairs of base instant time, file ID, log version, and file size of the log files
   * @return {@link HoodieTestTable} instance
   * @throws Exception upon error
   */
  public HoodieTestTable withLogFilesAndBaseInstantTimeInPartition(String partition, List<Pair<Pair<String, String>, Integer[]>> fileInfos)
      throws Exception {
    for (Pair<Pair<String, String>, Integer[]> fileInfo : fileInfos) {
      FileCreateUtils.createLogFile(
          metaClient, partition, fileInfo.getKey().getKey(), fileInfo.getKey().getValue(), fileInfo.getValue()[0], fileInfo.getValue()[1]);
    }
    return this;
  }

  public boolean inflightCommitExists(String instantTime) {
    try {
      return fs.exists(getInflightCommitFilePath(instantTime));
    } catch (IOException e) {
      throw new HoodieTestTableException(e);
    }
  }

  public boolean commitExists(String instantTime) {
    try {
      getCommitFilePath(instantTime);
      return true;
    } catch (HoodieIOException e) {
      return false;
    }
  }

  public boolean baseFilesExist(Map<String, String> partitionAndFileId, String instantTime) {
    return partitionAndFileId.entrySet().stream().allMatch(entry -> {
      String partition = entry.getKey();
      String fileId = entry.getValue();
      return baseFileExists(partition, instantTime, fileId);
    });
  }

  public boolean baseFileExists(String partition, String instantTime, String fileId) {
    try {
      return fs.exists(new Path(Paths.get(basePath, partition, baseFileName(instantTime, fileId)).toString()));
    } catch (IOException e) {
      throw new HoodieTestTableException(e);
    }
  }

  public boolean logFilesExist(String partition, String instantTime, String fileId, int... versions) {
    return Arrays.stream(versions).allMatch(v -> logFileExists(partition, instantTime, fileId, v));
  }

  public boolean logFileExists(String partition, String instantTime, String fileId, int version) {
    try {
      return fs.exists(new Path(
          Paths.get(basePath, partition, logFileName(instantTime, fileId, version)).toString()));
    } catch (IOException e) {
      throw new HoodieTestTableException(e);
    }
  }

  public Path getInflightCommitFilePath(String instantTime) {
    return new Path(Paths.get(metaClient.getTimelinePath().toUri().getPath(),
        instantTime + HoodieTimeline.INFLIGHT_COMMIT_EXTENSION).toUri());
  }

  public StoragePath getCommitFilePath(String instantTime) {
    return HoodieTestUtils.getCompleteInstantPath(storage, metaClient.getTimelinePath(), instantTime, HoodieTimeline.COMMIT_ACTION);
  }

  public Path getRequestedCompactionFilePath(String instantTime) {
    return new Path(Paths.get(metaClient.getMetaAuxiliaryPath(),
        instantTime + HoodieTimeline.REQUESTED_COMPACTION_EXTENSION).toUri());
  }

  public Path getPartitionPath(String partition) {
    return new Path(Paths.get(basePath, partition).toUri());
  }

  public List<java.nio.file.Path> getAllPartitionPaths() throws IOException {
    java.nio.file.Path basePathPath = Paths.get(basePath);
    return FileCreateUtils.getPartitionPaths(basePathPath);
  }

  public Path getBaseFilePath(String partition, String fileId) {
    return new Path(Paths.get(basePath, partition, getBaseFileNameById(fileId)).toUri());
  }

  public String getBaseFileNameById(String fileId) {
    return baseFileName(currentInstantTime, fileId);
  }

  public Path getLogFilePath(String partition, String fileId, int version) {
    return new Path(Paths.get(basePath, partition, getLogFileNameById(currentInstantTime, fileId, version)).toString());
  }

  public String getLogFileNameById(String fileId, int version) {
    return logFileName(currentInstantTime, fileId, version);
  }

  public String getLogFileNameById(String baseInstantTime, String fileId, int version) {
    return logFileName(baseInstantTime, fileId, version);
  }

  public List<String> getEarliestFilesInPartition(String partition, int count) throws IOException {
    List<FileStatus> fileStatuses = Arrays.asList(listAllFilesInPartition(partition));
    fileStatuses.sort(Comparator.comparing(FileStatus::getModificationTime));
    return fileStatuses.subList(0, count).stream().map(entry -> entry.getPath().getName()).collect(Collectors.toList());
  }

  public List<String> inflightCommits() {
    return this.inflightCommits;
  }

  public List<StoragePathInfo> listAllBaseFiles() throws IOException {
    return listAllBaseFiles(HoodieFileFormat.PARQUET.getFileExtension());
  }

  public List<StoragePathInfo> listAllBaseFiles(String fileExtension) throws IOException {
    return listRecursive(storage, new StoragePath(basePath)).stream()
        .filter(fileInfo -> fileInfo.getPath().getName().endsWith(fileExtension))
        .collect(Collectors.toList());
  }

  public static List<FileStatus> listRecursive(FileSystem fs, Path path) throws IOException {
    return listFiles(fs, path, true);
  }

  public static List<FileStatus> listFiles(FileSystem fs, Path path, boolean recursive) throws IOException {
    RemoteIterator<LocatedFileStatus> itr = fs.listFiles(path, recursive);
    List<FileStatus> statuses = new ArrayList<>();
    while (itr.hasNext()) {
      statuses.add(itr.next());
    }
    return statuses;
  }

  public static List<StoragePathInfo> listRecursive(HoodieStorage storage, StoragePath path)
      throws IOException {
    return listFiles(storage, path);
  }

  public static List<StoragePathInfo> listFiles(HoodieStorage storage, StoragePath path)
      throws IOException {
    return storage.listFiles(path);
  }

  public static String readLastLineFromResourceFile(String resourceName) throws IOException {
    try (InputStream inputStream = TestLogReaderUtils.class.getResourceAsStream(resourceName)) {
      List<String> lines = FileIOUtils.readAsUTFStringLines(inputStream);
      return lines.get(lines.size() - 1);
    }
  }

  public List<StoragePathInfo> listAllLogFiles() throws IOException {
    return listAllLogFiles(HoodieFileFormat.HOODIE_LOG.getFileExtension());
  }

  public List<StoragePathInfo> listAllLogFiles(String fileExtension) throws IOException {
    return listRecursive(storage, new StoragePath(basePath)).stream()
        .filter(
            fileInfo -> !fileInfo.getPath().toString()
                .contains(HoodieTableMetaClient.METAFOLDER_NAME))
        .filter(fileInfo -> fileInfo.getPath().getName().contains(fileExtension))
        .collect(Collectors.toList());
  }

  public List<StoragePathInfo> listAllBaseAndLogFiles() throws IOException {
    List<StoragePathInfo> result = new ArrayList<>(listAllBaseFiles());
    result.addAll(listAllLogFiles());
    return result;
  }

  public FileStatus[] listAllFilesInPartition(String partitionPath) throws IOException {
    return listRecursive(fs,
            new Path(Paths.get(basePath, partitionPath).toString())).stream()
        .filter(entry -> {
          boolean toReturn = true;
          String filePath = entry.getPath().toString();
          String fileName = entry.getPath().getName();
          if (fileName.startsWith(HoodiePartitionMetadata.HOODIE_PARTITION_METAFILE_PREFIX)
              || !FileCreateUtils.isBaseOrLogFilename(fileName)
              || filePath.contains("metadata")) {
            toReturn = false;
          } else {
            for (String inflight : inflightCommits) {
              if (fileName.contains(inflight)) {
                toReturn = false;
                break;
              }
            }
          }
          return toReturn;
        }).toArray(FileStatus[]::new);
  }

  public FileStatus[] listAllFilesInTempFolder() throws IOException {
    return listRecursive(fs, new Path(metaClient.getTempFolderPath())).toArray(new FileStatus[0]);
  }

  public void deleteFilesInPartition(String partitionPath, List<String> filesToDelete) throws IOException {
    FileStatus[] allFiles = listAllFilesInPartition(partitionPath);
    Arrays.stream(allFiles).filter(entry -> filesToDelete.contains(entry.getPath().getName())).forEach(entry -> {
      try {
        Files.delete(Paths.get(basePath, partitionPath, entry.getPath().getName()));
      } catch (IOException e) {
        throw new HoodieTestTableException(e);
      }
    });
  }

  public HoodieTestTable doRollback(String commitTimeToRollback, String commitTime) throws Exception {
    metaClient = HoodieTableMetaClient.reload(metaClient);
    Option<HoodieCommitMetadata> commitMetadata = getMetadataForInstant(commitTimeToRollback);
    if (!commitMetadata.isPresent()) {
      throw new IllegalArgumentException("Instant to rollback not present in timeline: " + commitTimeToRollback);
    }
    Map<String, List<String>> partitionFiles = getPartitionFiles(commitMetadata.get());
    HoodieRollbackMetadata rollbackMetadata = getRollbackMetadata(commitTimeToRollback, partitionFiles, false);
    for (Map.Entry<String, List<String>> entry : partitionFiles.entrySet()) {
      deleteFilesInPartition(entry.getKey(), entry.getValue());
    }
    HoodieRollbackPlan rollbackPlan = getHoodieRollbackPlan(commitTime, partitionFiles);
    HoodieTestTable testTable = addRollback(commitTime, rollbackMetadata, rollbackPlan);
    return testTable.addRollbackCompleted(commitTime, rollbackMetadata, false);
  }

  private HoodieRollbackPlan getHoodieRollbackPlan(String commitTime, Map<String, List<String>> partitionFiles) {
    HoodieRollbackPlan rollbackPlan = new HoodieRollbackPlan();
    List<HoodieRollbackRequest> rollbackRequestList = partitionFiles.keySet().stream()
        .map(partition -> new HoodieRollbackRequest(partition, EMPTY_STRING, EMPTY_STRING,
                partitionFiles.get(partition), Collections.emptyMap()))
        .collect(Collectors.toList());
    rollbackPlan.setRollbackRequests(rollbackRequestList);
    rollbackPlan.setInstantToRollback(new HoodieInstantInfo(commitTime, HoodieTimeline.COMMIT_ACTION));
    return rollbackPlan;
  }

  public HoodieTestTable doRollbackWithExtraFiles(String commitTimeToRollback, String commitTime, Map<String, List<String>> extraFiles) throws Exception {
    metaClient = HoodieTableMetaClient.reload(metaClient);
    Option<HoodieCommitMetadata> commitMetadata = getMetadataForInstant(commitTimeToRollback);
    if (!commitMetadata.isPresent()) {
      throw new IllegalArgumentException("Instant to rollback not present in timeline: " + commitTimeToRollback);
    }
    Map<String, List<String>> partitionFiles = getPartitionFiles(commitMetadata.get());
    for (Map.Entry<String, List<String>> entry : partitionFiles.entrySet()) {
      deleteFilesInPartition(entry.getKey(), entry.getValue());
    }
    for (Map.Entry<String, List<String>> entry : extraFiles.entrySet()) {
      if (partitionFiles.containsKey(entry.getKey())) {
        partitionFiles.get(entry.getKey()).addAll(entry.getValue());
      }
    }
    HoodieRollbackMetadata rollbackMetadata = getRollbackMetadata(commitTimeToRollback, partitionFiles, false);
    HoodieRollbackPlan rollbackPlan = getHoodieRollbackPlan(commitTime, partitionFiles);
    HoodieTestTable testTable = addRollback(commitTime, rollbackMetadata, rollbackPlan);
    return testTable.addRollbackCompleted(commitTime, rollbackMetadata, false);
  }

  public HoodieTestTable doRestore(String commitToRestoreTo, String restoreTime) throws Exception {
    metaClient = HoodieTableMetaClient.reload(metaClient);
    List<HoodieInstant> commitsToRollback = metaClient.getActiveTimeline().getCommitsTimeline()
        .filterCompletedInstants().findInstantsAfter(commitToRestoreTo).getReverseOrderedInstants().collect(Collectors.toList());
    Map<String, List<HoodieRollbackMetadata>> rollbackMetadataMap = new HashMap<>();
    for (HoodieInstant commitInstantToRollback : commitsToRollback) {
      Option<HoodieCommitMetadata> commitMetadata = getCommitMeta(commitInstantToRollback);
      if (!commitMetadata.isPresent()) {
        throw new IllegalArgumentException("Instant to rollback not present in timeline: " + commitInstantToRollback.requestedTime());
      }
      Map<String, List<String>> partitionFiles = getPartitionFiles(commitMetadata.get());
      rollbackMetadataMap.put(commitInstantToRollback.requestedTime(),
          Collections.singletonList(getRollbackMetadata(commitInstantToRollback.requestedTime(), partitionFiles, false)));
      for (Map.Entry<String, List<String>> entry : partitionFiles.entrySet()) {
        deleteFilesInPartition(entry.getKey(), entry.getValue());
      }
    }

    HoodieRestoreMetadata restoreMetadata = TimelineMetadataUtils.convertRestoreMetadata(restoreTime, 1000L,
        commitsToRollback, rollbackMetadataMap);
    return addRestore(restoreTime, restoreMetadata);
  }

  public HoodieReplaceCommitMetadata doCluster(String commitTime, Map<String, List<String>> partitionToReplaceFileIds, List<String> partitions, int filesPerPartition) throws Exception {
    HoodieTestTableState testTableState = getTestTableStateWithPartitionFileInfo(CLUSTER, metaClient.getTableType(), commitTime, partitions, filesPerPartition);
    this.currentInstantTime = commitTime;
    Map<String, List<Pair<String, Integer>>> partitionToReplaceFileIdsWithLength = new HashMap<>();
    for (Map.Entry<String, List<String>> entry : partitionToReplaceFileIds.entrySet()) {
      String partition = entry.getKey();
      partitionToReplaceFileIdsWithLength.put(entry.getKey(), new ArrayList<>());
      for (String fileId : entry.getValue()) {
        int length = 100 + RANDOM.nextInt(500);
        partitionToReplaceFileIdsWithLength.get(partition).add(Pair.of(fileId, length));
      }
    }
    List<HoodieWriteStat> writeStats = generateHoodieWriteStatForPartition(testTableState.getPartitionToBaseFileInfoMap(commitTime), commitTime, false);
    for (String partition : testTableState.getPartitionToBaseFileInfoMap(commitTime).keySet()) {
      this.withBaseFilesInPartition(partition, testTableState.getPartitionToBaseFileInfoMap(commitTime).get(partition));
    }
    HoodieReplaceCommitMetadata replaceMetadata =
        (HoodieReplaceCommitMetadata) buildMetadata(writeStats, partitionToReplaceFileIds, Option.empty(), CLUSTER, PHONY_TABLE_SCHEMA,
            REPLACE_COMMIT_ACTION);
    HoodieRequestedReplaceMetadata requestedReplaceMetadata = HoodieRequestedReplaceMetadata.newBuilder()
        .setOperationType(WriteOperationType.CLUSTER.name())
        .setExtraMetadata(Collections.emptyMap())
        .setClusteringPlan(new HoodieClusteringPlan())
        .build();
    addCluster(commitTime, requestedReplaceMetadata, Option.empty(), replaceMetadata);
    return replaceMetadata;
  }

  public HoodieCleanMetadata doClean(String commitTime, Map<String, Integer> partitionFileCountsToDelete) throws IOException {
    return doClean(commitTime, partitionFileCountsToDelete, Collections.emptyMap());
  }

  public HoodieCleanMetadata doClean(String commitTime, Map<String, Integer> partitionFileCountsToDelete, Map<String, String> extraMetadata) throws IOException {
    Map<String, List<String>> partitionFilesToDelete = new HashMap<>();
    for (Map.Entry<String, Integer> entry : partitionFileCountsToDelete.entrySet()) {
      partitionFilesToDelete.put(entry.getKey(), getEarliestFilesInPartition(entry.getKey(), entry.getValue()));
    }
    HoodieTestTableState testTableState = new HoodieTestTableState();
    for (Map.Entry<String, List<String>> entry : partitionFilesToDelete.entrySet()) {
      testTableState = testTableState.createTestTableStateForCleaner(commitTime, entry.getKey(), entry.getValue());
      deleteFilesInPartition(entry.getKey(), entry.getValue());
    }
    Pair<HoodieCleanerPlan, HoodieCleanMetadata> cleanerMeta = getHoodieCleanMetadata(commitTime, testTableState);
    HoodieCleanMetadata cleanMetadata = cleanerMeta.getValue();
    cleanerMeta.getKey().setExtraMetadata(extraMetadata);
    cleanMetadata.setExtraMetadata(extraMetadata);
    addClean(commitTime, cleanerMeta.getKey(), cleanMetadata);
    return cleanMetadata;
  }

  /**
   * Repeats the same cleaning based on the cleaner plan and clean commit metadata.
   *
   * @param cleanCommitTime new clean commit time to use.
   * @param cleanerPlan     cleaner plan to write to the metadata.
   * @param cleanMetadata   clean metadata in data table to use.
   */
  public void repeatClean(String cleanCommitTime,
                          HoodieCleanerPlan cleanerPlan,
                          HoodieCleanMetadata cleanMetadata) throws IOException {
    addClean(cleanCommitTime, cleanerPlan, cleanMetadata);
  }

  public HoodieCleanMetadata doCleanBasedOnCommits(String cleanCommitTime, List<String> commitsToClean) throws IOException {
    Map<String, Integer> partitionFileCountsToDelete = new HashMap<>();
    for (String commitTime : commitsToClean) {
      Option<HoodieCommitMetadata> commitMetadata = getMetadataForInstant(commitTime);
      if (commitMetadata.isPresent()) {
        Map<String, List<String>> partitionFiles = getPartitionFiles(commitMetadata.get());
        for (String partition : partitionFiles.keySet()) {
          partitionFileCountsToDelete.put(partition, partitionFiles.get(partition).size() + partitionFileCountsToDelete.getOrDefault(partition, 0));
        }
      }
    }
    return doClean(cleanCommitTime, partitionFileCountsToDelete);
  }

  public HoodieSavepointMetadata doSavepoint(String commitTime) throws IOException {
    Option<HoodieCommitMetadata> commitMetadata = getMetadataForInstant(commitTime);
    if (!commitMetadata.isPresent()) {
      throw new IllegalArgumentException("Instant to rollback not present in timeline: " + commitTime);
    }
    Map<String, List<String>> partitionFiles = getPartitionFiles(commitMetadata.get());
    HoodieSavepointMetadata savepointMetadata = getSavepointMetadata(commitTime, partitionFiles);
    for (Map.Entry<String, List<String>> entry : partitionFiles.entrySet()) {
      deleteFilesInPartition(entry.getKey(), entry.getValue());
    }
    return savepointMetadata;
  }

  public HoodieCommitMetadata doCompaction(String commitTime, List<String> partitions) throws Exception {
    return doCompaction(commitTime, partitions, false);
  }

  public HoodieCommitMetadata doCompaction(String commitTime, List<String> partitions, boolean inflight) throws Exception {
    this.currentInstantTime = commitTime;
    if (partitions.isEmpty()) {
      partitions = Collections.singletonList(EMPTY_STRING);
    }
    HoodieTestTableState testTableState = getTestTableStateWithPartitionFileInfo(COMPACT, metaClient.getTableType(), commitTime, partitions, 1);
    HoodieCommitMetadata commitMetadata = createCommitMetadata(COMPACT, commitTime, testTableState);
    for (String partition : partitions) {
      this.withBaseFilesInPartition(partition, testTableState.getPartitionToBaseFileInfoMap(commitTime).get(partition));
    }
    if (inflight) {
      this.addInflightCompaction(commitTime, commitMetadata);
    } else {
      this.addCompaction(commitTime, commitMetadata);
    }
    return commitMetadata;
  }

  public HoodieCommitMetadata doWriteOperation(String commitTime, WriteOperationType operationType,
                                               List<String> partitions, int filesPerPartition) throws Exception {
    return doWriteOperation(commitTime, operationType, Collections.emptyList(), partitions, filesPerPartition, false);
  }

  public HoodieCommitMetadata doWriteOperation(String commitTime, WriteOperationType operationType,
                                               List<String> newPartitionsToAdd, List<String> partitions,
                                               int filesPerPartition) throws Exception {
    return doWriteOperation(commitTime, operationType, newPartitionsToAdd, partitions, filesPerPartition, false);
  }

  public HoodieCommitMetadata doWriteOperation(String commitTime, WriteOperationType operationType,
                                               List<String> newPartitionsToAdd, List<String> partitions,
                                               int filesPerPartition, boolean bootstrap) throws Exception {
    return doWriteOperation(commitTime, operationType, newPartitionsToAdd, partitions, filesPerPartition, bootstrap, false);
  }

  public HoodieCommitMetadata doWriteOperation(String commitTime, WriteOperationType operationType,
                                               List<String> partitions, int filesPerPartition, boolean bootstrap) throws Exception {
    return doWriteOperation(commitTime, operationType, Collections.emptyList(), partitions, filesPerPartition,
        bootstrap, false);
  }

  public HoodieCommitMetadata doWriteOperation(String commitTime, WriteOperationType operationType,
                                               List<String> newPartitionsToAdd, List<String> partitions,
                                               int filesPerPartition, boolean bootstrap,
                                               boolean createInflightCommit) throws Exception {
    if (partitions.isEmpty()) {
      partitions = Collections.singletonList(EMPTY_STRING);
    }

    Map<String, List<Pair<String, Integer>>> partitionToFilesNameLengthMap = getPartitionFiles(partitions,
        filesPerPartition);
    return doWriteOperation(commitTime, operationType, newPartitionsToAdd, partitionToFilesNameLengthMap, bootstrap,
        createInflightCommit);
  }

  /**
   * Add commits to the requested partitions.
   *
   * @param commitTime                    - Commit time for the operation
   * @param operationType                 - Operation type
   * @param newPartitionsToAdd            - New partitions to add for the operation
   * @param partitionToFilesNameLengthMap - Map of partition names to its list of files name and length pair
   * @param bootstrap                     - Whether bootstrapping needed for the operation
   * @param createInflightCommit          - Whether in flight commit needed for the operation
   * @return Commit metadata for the commit operation performed.
   * @throws Exception
   */
  public HoodieCommitMetadata doWriteOperation(String commitTime, WriteOperationType operationType,
                                               List<String> newPartitionsToAdd,
                                               Map<String, List<Pair<String, Integer>>> partitionToFilesNameLengthMap,
                                               boolean bootstrap, boolean createInflightCommit) throws Exception {
    if (partitionToFilesNameLengthMap.isEmpty()) {
      partitionToFilesNameLengthMap = singletonMap(EMPTY_STRING, Collections.emptyList());
    }
    HoodieTestTableState testTableState = getTestTableStateWithPartitionFileInfo(operationType,
        metaClient.getTableType(), commitTime, partitionToFilesNameLengthMap);
    HoodieCommitMetadata commitMetadata = createCommitMetadata(operationType, commitTime, testTableState, bootstrap);
    for (String str : newPartitionsToAdd) {
      this.withPartitionMetaFiles(str);
    }
    if (createInflightCommit) {
      if (metaClient.getTableType() == HoodieTableType.COPY_ON_WRITE) {
        this.addInflightCommit(commitTime);
      } else {
        this.addInflightDeltaCommit(commitTime);
      }
    } else {
      if (metaClient.getTableType() == HoodieTableType.COPY_ON_WRITE) {
        this.addCommit(commitTime, Option.of(commitMetadata));
      } else {
        this.addDeltaCommit(commitTime, commitMetadata);
      }
    }
    for (Map.Entry<String, List<Pair<String, Integer>>> entry : partitionToFilesNameLengthMap.entrySet()) {
      String partition = entry.getKey();
      this.withPartitionMetaFiles(partition); // needed by the metadata table initialization.
      this.withBaseFilesInPartition(partition, testTableState.getPartitionToBaseFileInfoMap(commitTime).get(partition));
      if (MERGE_ON_READ.equals(metaClient.getTableType()) && UPSERT.equals(operationType)) {
        this.withLogFilesInPartition(partition, testTableState.getPartitionToLogFileInfoMap(commitTime).get(partition));
      }
    }
    return commitMetadata;
  }

  public Option<HoodieCommitMetadata> getMetadataForInstant(String instantTime) {
    metaClient = HoodieTableMetaClient.reload(metaClient);
    Option<HoodieInstant> hoodieInstant = metaClient.getActiveTimeline().getCommitsTimeline()
        .filterCompletedInstants().filter(i -> i.requestedTime().equals(instantTime)).firstInstant();
    try {
      if (hoodieInstant.isPresent()) {
        return getCommitMeta(hoodieInstant.get());
      } else {
        return Option.empty();
      }
    } catch (IOException io) {
      throw new HoodieIOException("Unable to read metadata for instant " + hoodieInstant.get(), io);
    }
  }

  private Option<HoodieCommitMetadata> getCommitMeta(HoodieInstant hoodieInstant) throws IOException {
    switch (hoodieInstant.getAction()) {
      case REPLACE_COMMIT_ACTION:
      case CLUSTERING_ACTION:
        return Option.of(
            metaClient.getActiveTimeline().readReplaceCommitMetadata(hoodieInstant));
      case HoodieTimeline.DELTA_COMMIT_ACTION:
      case HoodieTimeline.COMMIT_ACTION:
        return Option.of(
            metaClient.getActiveTimeline().readCommitMetadata(hoodieInstant));
      default:
        throw new IllegalArgumentException("Unknown instant action" + hoodieInstant.getAction());
    }
  }

  private static Map<String, List<String>> getPartitionFiles(HoodieCommitMetadata commitMetadata) {
    Map<String, List<String>> partitionFilesToDelete = new HashMap<>();
    Map<String, List<HoodieWriteStat>> partitionToWriteStats = commitMetadata.getPartitionToWriteStats();
    for (Map.Entry<String, List<HoodieWriteStat>> entry : partitionToWriteStats.entrySet()) {
      partitionFilesToDelete.put(entry.getKey(), new ArrayList<>());
      entry.getValue().forEach(writeStat -> partitionFilesToDelete.get(entry.getKey()).add(writeStat.getFileId()));
    }
    return partitionFilesToDelete;
  }

  /**
   * Generate partition files names and length details.
   *
   * @param partitions        - List of partition for which file details need to be generated
   * @param filesPerPartition - File count per partition
   * @return Map of partition to its collection of files name and length pair
   */
  protected static Map<String, List<Pair<String, Integer>>> getPartitionFiles(List<String> partitions,
                                                                              int filesPerPartition) {
    Map<String, List<Pair<String, Integer>>> partitionToFilesNameLengthMap = new HashMap<>();
    for (String partition : partitions) {
      Stream<Integer> fileLengths = IntStream.range(0, filesPerPartition).map(i -> 100 + RANDOM.nextInt(500)).boxed();
      List<Pair<String, Integer>> fileNameAndLengthList =
          fileLengths.map(len -> Pair.of(UUID.randomUUID().toString(), len)).collect(Collectors.toList());
      partitionToFilesNameLengthMap.put(partition, fileNameAndLengthList);
    }
    return partitionToFilesNameLengthMap;
  }

  /**
   * Get Test table state for the requested partitions and file count.
   *
   * @param operationType     - Table write operation type
   * @param tableType         - Hudi table type
   * @param commitTime        - Write commit time
   * @param partitions        - List of partition names
   * @param filesPerPartition - Total file count per partition
   * @return Test table state for the requested partitions and file count
   */
  private static HoodieTestTableState getTestTableStateWithPartitionFileInfo(WriteOperationType operationType,
                                                                             HoodieTableType tableType,
                                                                             String commitTime,
                                                                             List<String> partitions,
                                                                             int filesPerPartition) {
    Map<String, List<Pair<String, Integer>>> partitionToFilesNameLengthMap = getPartitionFiles(partitions,
        filesPerPartition);
    return getTestTableStateWithPartitionFileInfo(operationType, tableType, commitTime, partitionToFilesNameLengthMap);
  }

  /**
   * Get Test table state for the requested partitions and files.
   *
   * @param operationType                 - Table write operation type
   * @param tableType                     - Hudi table type
   * @param commitTime                    - Write commit time
   * @param partitionToFilesNameLengthMap - Map of partition names to its list of files and their lengths
   * @return Test table state for the requested partitions and files
   */
  private static HoodieTestTableState getTestTableStateWithPartitionFileInfo(WriteOperationType operationType,
                                                                             HoodieTableType tableType,
                                                                             String commitTime,
                                                                             Map<String, List<Pair<String, Integer>>> partitionToFilesNameLengthMap) {
    if (partitionToFilesNameLengthMap.isEmpty()) {
      return testTableState;
    }

    for (Map.Entry<String, List<Pair<String, Integer>>> partitionEntry : partitionToFilesNameLengthMap.entrySet()) {
      String partitionName = partitionEntry.getKey();
      List<Pair<String, Integer>> fileNameAndLengthList = partitionEntry.getValue();
      if (MERGE_ON_READ.equals(tableType) && UPSERT.equals(operationType)) {
        List<Pair<Integer, Integer>> fileVersionAndLength =
            fileNameAndLengthList.stream().map(nameLengthPair -> Pair.of(0, nameLengthPair.getRight())).collect(Collectors.toList());
        testTableState = testTableState.createTestTableStateForBaseAndLogFiles(commitTime, partitionName,
            fileVersionAndLength);
      } else {
        testTableState = testTableState.createTestTableStateForBaseFilesOnly(commitTime, partitionName,
            fileNameAndLengthList);
      }
    }
    return testTableState;
  }

  public static List<HoodieWriteStat> generateHoodieWriteStatForPartition(Map<String, List<Pair<String, Integer>>> partitionToFileIdMap,
                                                                          String commitTime, boolean bootstrap) {
    List<HoodieWriteStat> writeStats = new ArrayList<>();
    if (partitionToFileIdMap == null || partitionToFileIdMap.isEmpty()) {
      return writeStats;
    }

    for (Map.Entry<String, List<Pair<String, Integer>>> entry : partitionToFileIdMap.entrySet()) {
      String partition = entry.getKey();
      for (Pair<String, Integer> fileIdInfo : entry.getValue()) {
        HoodieWriteStat writeStat = new HoodieWriteStat();
        String fileName = bootstrap ? fileIdInfo.getKey() :
            FileCreateUtils.baseFileName(commitTime, fileIdInfo.getKey());
        writeStat.setFileId(fileName);
        writeStat.setPartitionPath(partition);
        writeStat.setPath(StringUtils.isNullOrEmpty(partition) ? fileName : partition + "/" + fileName);
        writeStat.setTotalWriteBytes(fileIdInfo.getValue());
        writeStat.setFileSizeInBytes(fileIdInfo.getValue());
        writeStats.add(writeStat);
      }
    }
    return writeStats;
  }

  /**
   * Returns the write stats for log files in the partition. Since log file has version associated with it, the {@param partitionToFileIdMap}
   * contains list of Pair<String, Integer[]> where the Integer[] array has both file version and file size.
   */
  private static List<HoodieWriteStat> generateHoodieWriteStatForPartitionLogFiles(Map<String, List<Pair<String, Integer[]>>> partitionToFileIdMap, String commitTime, boolean bootstrap) {
    List<HoodieWriteStat> writeStats = new ArrayList<>();
    if (partitionToFileIdMap == null) {
      return writeStats;
    }
    for (Map.Entry<String, List<Pair<String, Integer[]>>> entry : partitionToFileIdMap.entrySet()) {
      String partition = entry.getKey();
      for (Pair<String, Integer[]> fileIdInfo : entry.getValue()) {
        HoodieWriteStat writeStat = new HoodieWriteStat();
        String fileName = bootstrap ? fileIdInfo.getKey() :
            FileCreateUtils.logFileName(commitTime, fileIdInfo.getKey(), fileIdInfo.getValue()[0]);
        writeStat.setFileId(fileName);
        writeStat.setPartitionPath(partition);
        writeStat.setPath(StringUtils.isNullOrEmpty(partition) ? fileName : partition + "/" + fileName);
        writeStat.setTotalWriteBytes(fileIdInfo.getValue()[1]);
        writeStat.setFileSizeInBytes(fileIdInfo.getValue()[1]);
        writeStats.add(writeStat);
      }
    }
    return writeStats;
  }

  @Override
  public void close() throws Exception {
    // no-op
  }

  /**
   * Exception for {@link HoodieTestTable}.
   */
  public static class HoodieTestTableException extends RuntimeException {
    public HoodieTestTableException(Throwable t) {
      super(t);
    }
  }

  static class HoodieTestTableState {
    /**
     * Map<commitTime, Map<partitionPath, List<filesToDelete>>>
     * Used in building CLEAN metadata.
     */
    Map<String, Map<String, List<String>>> commitsToPartitionToFileIdForCleaner = new HashMap<>();
    /**
     * Map<commitTime, Map<partitionPath, List<Pair<fileName, fileLength>>>>
     * Used to build commit metadata for base files for several write operations.
     */
    Map<String, Map<String, List<Pair<String, Integer>>>> commitsToPartitionToBaseFileInfoStats = new HashMap<>();
    /**
     * Map<commitTime, Map<partitionPath, List<Pair<fileName, [fileVersion, fileLength]>>>>
     * Used to build commit metadata for log files for several write operations.
     */
    Map<String, Map<String, List<Pair<String, Integer[]>>>> commitsToPartitionToLogFileInfoStats = new HashMap<>();

    HoodieTestTableState() {
    }

    static HoodieTestTableState of() {
      return new HoodieTestTableState();
    }

    HoodieTestTableState createTestTableStateForCleaner(String commitTime, String partitionPath, List<String> filesToClean) {
      if (!commitsToPartitionToFileIdForCleaner.containsKey(commitTime)) {
        commitsToPartitionToFileIdForCleaner.put(commitTime, new HashMap<>());
      }
      if (!this.commitsToPartitionToFileIdForCleaner.get(commitTime).containsKey(partitionPath)) {
        this.commitsToPartitionToFileIdForCleaner.get(commitTime).put(partitionPath, new ArrayList<>());
      }

      this.commitsToPartitionToFileIdForCleaner.get(commitTime).get(partitionPath).addAll(filesToClean);
      return this;
    }

    Map<String, List<String>> getPartitionToFileIdMapForCleaner(String commitTime) {
      return this.commitsToPartitionToFileIdForCleaner.get(commitTime);
    }

    HoodieTestTableState createTestTableStateForBaseFileLengthsOnly(String commitTime, String partitionPath,
                                                                    List<Integer> lengths) {
      List<Pair<String, Integer>> fileNameLengthList = new ArrayList<>();
      for (int length : lengths) {
        fileNameLengthList.add(Pair.of(UUID.randomUUID().toString(), length));
      }
      return createTestTableStateForBaseFilesOnly(commitTime, partitionPath, fileNameLengthList);
    }

    HoodieTestTableState createTestTableStateForBaseFilesOnly(String commitTime, String partitionPath,
                                                              List<Pair<String, Integer>> fileNameAndLengthList) {
      if (!commitsToPartitionToBaseFileInfoStats.containsKey(commitTime)) {
        commitsToPartitionToBaseFileInfoStats.put(commitTime, new HashMap<>());
      }
      if (!this.commitsToPartitionToBaseFileInfoStats.get(commitTime).containsKey(partitionPath)) {
        this.commitsToPartitionToBaseFileInfoStats.get(commitTime).put(partitionPath, new ArrayList<>());
      }

      this.commitsToPartitionToBaseFileInfoStats.get(commitTime).get(partitionPath).addAll(fileNameAndLengthList);
      return this;
    }

    HoodieTestTableState createTestTableStateForBaseAndLogFiles(String commitTime, String partitionPath,
                                                                List<Pair<Integer, Integer>> versionsAndLengths) {
      if (!commitsToPartitionToBaseFileInfoStats.containsKey(commitTime)) {
        createTestTableStateForBaseFileLengthsOnly(commitTime, partitionPath,
            versionsAndLengths.stream().map(Pair::getRight).collect(Collectors.toList()));
      }
      if (!this.commitsToPartitionToBaseFileInfoStats.get(commitTime).containsKey(partitionPath)) {
        createTestTableStateForBaseFileLengthsOnly(commitTime, partitionPath,
            versionsAndLengths.stream().map(Pair::getRight).collect(Collectors.toList()));
      }
      if (!commitsToPartitionToLogFileInfoStats.containsKey(commitTime)) {
        commitsToPartitionToLogFileInfoStats.put(commitTime, new HashMap<>());
      }
      if (!this.commitsToPartitionToLogFileInfoStats.get(commitTime).containsKey(partitionPath)) {
        this.commitsToPartitionToLogFileInfoStats.get(commitTime).put(partitionPath, new ArrayList<>());
      }

      List<Pair<String, Integer[]>> fileInfos = new ArrayList<>();
      for (int i = 0; i < versionsAndLengths.size(); i++) {
        Pair<Integer, Integer> versionAndLength = versionsAndLengths.get(i);
        String fileId = FSUtils.getFileId(commitsToPartitionToBaseFileInfoStats.get(commitTime).get(partitionPath).get(i).getLeft());
        fileInfos.add(Pair.of(fileId, new Integer[] {versionAndLength.getLeft(), versionAndLength.getRight()}));
      }
      this.commitsToPartitionToLogFileInfoStats.get(commitTime).get(partitionPath).addAll(fileInfos);
      return this;
    }

    Map<String, List<Pair<String, Integer>>> getPartitionToBaseFileInfoMap(String commitTime) {
      return this.commitsToPartitionToBaseFileInfoStats.get(commitTime);
    }

    Map<String, List<Pair<String, Integer[]>>> getPartitionToLogFileInfoMap(String commitTime) {
      return this.commitsToPartitionToLogFileInfoStats.get(commitTime);
    }
  }

}
