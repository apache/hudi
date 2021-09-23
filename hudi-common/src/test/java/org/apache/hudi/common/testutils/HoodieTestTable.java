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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.avro.model.HoodieActionInstant;
import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCleanerPlan;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.avro.model.HoodieInstantInfo;
import org.apache.hudi.avro.model.HoodieRequestedReplaceMetadata;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.avro.model.HoodieRollbackPartitionMetadata;
import org.apache.hudi.avro.model.HoodieSavepointMetadata;
import org.apache.hudi.avro.model.HoodieSavepointPartitionMetadata;
import org.apache.hudi.common.HoodieCleanStat;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.table.timeline.versioning.clean.CleanPlanV2MigrationHandler;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
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
import static org.apache.hudi.common.model.HoodieTableType.MERGE_ON_READ;
import static org.apache.hudi.common.model.WriteOperationType.CLUSTER;
import static org.apache.hudi.common.model.WriteOperationType.COMPACT;
import static org.apache.hudi.common.model.WriteOperationType.UPSERT;
import static org.apache.hudi.common.table.timeline.HoodieActiveTimeline.COMMIT_FORMATTER;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.CLEAN_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.REPLACE_COMMIT_ACTION;
import static org.apache.hudi.common.testutils.FileCreateUtils.baseFileName;
import static org.apache.hudi.common.testutils.FileCreateUtils.createCleanFile;
import static org.apache.hudi.common.testutils.FileCreateUtils.createCommit;
import static org.apache.hudi.common.testutils.FileCreateUtils.createDeltaCommit;
import static org.apache.hudi.common.testutils.FileCreateUtils.createInflightCleanFile;
import static org.apache.hudi.common.testutils.FileCreateUtils.createInflightCommit;
import static org.apache.hudi.common.testutils.FileCreateUtils.createInflightCompaction;
import static org.apache.hudi.common.testutils.FileCreateUtils.createInflightDeltaCommit;
import static org.apache.hudi.common.testutils.FileCreateUtils.createInflightReplaceCommit;
import static org.apache.hudi.common.testutils.FileCreateUtils.createInflightRollbackFile;
import static org.apache.hudi.common.testutils.FileCreateUtils.createMarkerFile;
import static org.apache.hudi.common.testutils.FileCreateUtils.createReplaceCommit;
import static org.apache.hudi.common.testutils.FileCreateUtils.createRequestedCleanFile;
import static org.apache.hudi.common.testutils.FileCreateUtils.createRequestedCommit;
import static org.apache.hudi.common.testutils.FileCreateUtils.createRequestedCompaction;
import static org.apache.hudi.common.testutils.FileCreateUtils.createRequestedDeltaCommit;
import static org.apache.hudi.common.testutils.FileCreateUtils.createRequestedReplaceCommit;
import static org.apache.hudi.common.testutils.FileCreateUtils.createRollbackFile;
import static org.apache.hudi.common.testutils.FileCreateUtils.logFileName;
import static org.apache.hudi.common.util.CleanerUtils.convertCleanMetadata;
import static org.apache.hudi.common.util.CollectionUtils.createImmutableMap;
import static org.apache.hudi.common.util.CommitUtils.buildMetadata;
import static org.apache.hudi.common.util.CommitUtils.getCommitActionType;
import static org.apache.hudi.common.util.StringUtils.EMPTY_STRING;

public class HoodieTestTable {

  private static final Logger LOG = LogManager.getLogger(HoodieTestTable.class);
  private static final Random RANDOM = new Random();
  private static HoodieTestTableState testTableState;
  private final List<String> inflightCommits = new ArrayList<>();

  protected final String basePath;
  protected final FileSystem fs;
  protected HoodieTableMetaClient metaClient;
  protected String currentInstantTime;

  protected HoodieTestTable(String basePath, FileSystem fs, HoodieTableMetaClient metaClient) {
    ValidationUtils.checkArgument(Objects.equals(basePath, metaClient.getBasePath()));
    ValidationUtils.checkArgument(Objects.equals(fs, metaClient.getRawFs()));
    this.basePath = basePath;
    this.fs = fs;
    this.metaClient = metaClient;
  }

  public static HoodieTestTable of(HoodieTableMetaClient metaClient) {
    testTableState = HoodieTestTableState.of();
    return new HoodieTestTable(metaClient.getBasePath(), metaClient.getRawFs(), metaClient);
  }

  public static String makeNewCommitTime(int sequence) {
    return String.format("%09d", sequence);
  }

  public static String makeNewCommitTime() {
    return makeNewCommitTime(Instant.now());
  }

  public static String makeNewCommitTime(Instant dateTime) {
    return COMMIT_FORMATTER.format(Date.from(dateTime));
  }

  public static List<String> makeIncrementalCommitTimes(int num) {
    return makeIncrementalCommitTimes(num, 1);
  }

  public static List<String> makeIncrementalCommitTimes(int num, int firstOffsetSeconds) {
    final Instant now = Instant.now();
    return IntStream.range(0, num)
        .mapToObj(i -> makeNewCommitTime(now.plus(firstOffsetSeconds + i, SECONDS)))
        .collect(Collectors.toList());
  }

  public HoodieTestTable addRequestedCommit(String instantTime) throws Exception {
    createRequestedCommit(basePath, instantTime);
    currentInstantTime = instantTime;
    metaClient = HoodieTableMetaClient.reload(metaClient);
    return this;
  }

  public HoodieTestTable addInflightCommit(String instantTime) throws Exception {
    createRequestedCommit(basePath, instantTime);
    createInflightCommit(basePath, instantTime);
    inflightCommits.add(instantTime);
    currentInstantTime = instantTime;
    metaClient = HoodieTableMetaClient.reload(metaClient);
    return this;
  }

  public HoodieTestTable addCommit(String instantTime) throws Exception {
    createRequestedCommit(basePath, instantTime);
    createInflightCommit(basePath, instantTime);
    createCommit(basePath, instantTime);
    currentInstantTime = instantTime;
    metaClient = HoodieTableMetaClient.reload(metaClient);
    return this;
  }

  public HoodieCommitMetadata createCommitMetadata(WriteOperationType operationType, String commitTime, HoodieTestTableState testTableState) {
    String actionType = getCommitActionType(operationType, metaClient.getTableType());
    return createCommitMetadata(operationType, commitTime, Collections.emptyMap(), testTableState, false, actionType);
  }

  public HoodieCommitMetadata createCommitMetadata(WriteOperationType operationType, String commitTime,
                                                   HoodieTestTableState testTableState, boolean bootstrap) {
    String actionType = getCommitActionType(operationType, metaClient.getTableType());
    return createCommitMetadata(operationType, commitTime, Collections.emptyMap(), testTableState, bootstrap, actionType);
  }

  public HoodieCommitMetadata createCommitMetadata(WriteOperationType operationType, String commitTime,
                                                   Map<String, List<String>> partitionToReplaceFileIds,
                                                   HoodieTestTableState testTableState, boolean bootstrap, String action) {
    List<HoodieWriteStat> writeStats = generateHoodieWriteStatForPartition(testTableState.getPartitionToBaseFileInfoMap(commitTime), commitTime, bootstrap);
    if (MERGE_ON_READ.equals(metaClient.getTableType()) && UPSERT.equals(operationType)) {
      writeStats.addAll(generateHoodieWriteStatForPartitionLogFiles(testTableState.getPartitionToLogFileInfoMap(commitTime), commitTime, bootstrap));
    }
    Map<String, String> extraMetadata = createImmutableMap("test", "test");
    return buildMetadata(writeStats, partitionToReplaceFileIds, Option.of(extraMetadata), operationType, EMPTY_STRING, action);
  }

  public HoodieTestTable addCommit(String instantTime, HoodieCommitMetadata metadata) throws Exception {
    createRequestedCommit(basePath, instantTime);
    createInflightCommit(basePath, instantTime);
    createCommit(basePath, instantTime, metadata);
    currentInstantTime = instantTime;
    metaClient = HoodieTableMetaClient.reload(metaClient);
    return this;
  }

  public HoodieTestTable moveInflightCommitToComplete(String instantTime, HoodieCommitMetadata metadata) throws IOException {
    createCommit(basePath, instantTime, metadata);
    inflightCommits.remove(instantTime);
    currentInstantTime = instantTime;
    metaClient = HoodieTableMetaClient.reload(metaClient);
    return this;
  }

  public HoodieTestTable addDeltaCommit(String instantTime) throws Exception {
    createRequestedDeltaCommit(basePath, instantTime);
    createInflightDeltaCommit(basePath, instantTime);
    createDeltaCommit(basePath, instantTime);
    currentInstantTime = instantTime;
    metaClient = HoodieTableMetaClient.reload(metaClient);
    return this;
  }

  public HoodieTestTable addReplaceCommit(
      String instantTime,
      Option<HoodieRequestedReplaceMetadata> requestedReplaceMetadata,
      Option<HoodieCommitMetadata> inflightReplaceMetadata,
      HoodieReplaceCommitMetadata completeReplaceMetadata) throws Exception {
    createRequestedReplaceCommit(basePath, instantTime, requestedReplaceMetadata);
    createInflightReplaceCommit(basePath, instantTime, inflightReplaceMetadata);
    createReplaceCommit(basePath, instantTime, completeReplaceMetadata);
    currentInstantTime = instantTime;
    metaClient = HoodieTableMetaClient.reload(metaClient);
    return this;
  }

  public HoodieTestTable addRequestedReplace(String instantTime, Option<HoodieRequestedReplaceMetadata> requestedReplaceMetadata) throws Exception {
    createRequestedReplaceCommit(basePath, instantTime, requestedReplaceMetadata);
    currentInstantTime = instantTime;
    metaClient = HoodieTableMetaClient.reload(metaClient);
    return this;
  }

  public HoodieTestTable addInflightClean(String instantTime, HoodieCleanerPlan cleanerPlan) throws IOException {
    createRequestedCleanFile(basePath, instantTime, cleanerPlan);
    createInflightCleanFile(basePath, instantTime, cleanerPlan);
    currentInstantTime = instantTime;
    metaClient = HoodieTableMetaClient.reload(metaClient);
    return this;
  }

  public HoodieTestTable addClean(String instantTime, HoodieCleanerPlan cleanerPlan, HoodieCleanMetadata metadata) throws IOException {
    createRequestedCleanFile(basePath, instantTime, cleanerPlan);
    createInflightCleanFile(basePath, instantTime, cleanerPlan);
    createCleanFile(basePath, instantTime, metadata);
    currentInstantTime = instantTime;
    metaClient = HoodieTableMetaClient.reload(metaClient);
    return this;
  }

  public HoodieTestTable addClean(String instantTime) throws IOException {
    HoodieCleanerPlan cleanerPlan = new HoodieCleanerPlan(new HoodieActionInstant(EMPTY_STRING, EMPTY_STRING, EMPTY_STRING), EMPTY_STRING, new HashMap<>(),
        CleanPlanV2MigrationHandler.VERSION, new HashMap<>());
    HoodieCleanStat cleanStats = new HoodieCleanStat(
        HoodieCleaningPolicy.KEEP_LATEST_FILE_VERSIONS,
        HoodieTestUtils.DEFAULT_PARTITION_PATHS[RANDOM.nextInt(HoodieTestUtils.DEFAULT_PARTITION_PATHS.length)],
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList(),
        instantTime);
    HoodieCleanMetadata cleanMetadata = convertCleanMetadata(instantTime, Option.of(0L), Collections.singletonList(cleanStats));
    return HoodieTestTable.of(metaClient).addClean(instantTime, cleanerPlan, cleanMetadata);
  }

  public Pair<HoodieCleanerPlan, HoodieCleanMetadata> getHoodieCleanMetadata(String commitTime, HoodieTestTableState testTableState) {
    HoodieCleanerPlan cleanerPlan = new HoodieCleanerPlan(new HoodieActionInstant(commitTime, CLEAN_ACTION, EMPTY_STRING), EMPTY_STRING, new HashMap<>(),
        CleanPlanV2MigrationHandler.VERSION, new HashMap<>());
    List<HoodieCleanStat> cleanStats = new ArrayList<>();
    for (Map.Entry<String, List<String>> entry : testTableState.getPartitionToFileIdMapForCleaner(commitTime).entrySet()) {
      cleanStats.add(new HoodieCleanStat(HoodieCleaningPolicy.KEEP_LATEST_FILE_VERSIONS,
          entry.getKey(), entry.getValue(), entry.getValue(), Collections.emptyList(), commitTime));
    }
    return Pair.of(cleanerPlan, convertCleanMetadata(commitTime, Option.of(0L), cleanStats));
  }

  public HoodieTestTable addInflightRollback(String instantTime) throws IOException {
    createInflightRollbackFile(basePath, instantTime);
    currentInstantTime = instantTime;
    metaClient = HoodieTableMetaClient.reload(metaClient);
    return this;
  }

  public HoodieTestTable addRollback(String instantTime, HoodieRollbackMetadata rollbackMetadata) throws IOException {
    createInflightRollbackFile(basePath, instantTime);
    createRollbackFile(basePath, instantTime, rollbackMetadata);
    currentInstantTime = instantTime;
    metaClient = HoodieTableMetaClient.reload(metaClient);
    return this;
  }

  public HoodieRollbackMetadata getRollbackMetadata(String instantTimeToDelete, Map<String, List<String>> partitionToFilesMeta) throws Exception {
    HoodieRollbackMetadata rollbackMetadata = new HoodieRollbackMetadata();
    rollbackMetadata.setCommitsRollback(Collections.singletonList(instantTimeToDelete));
    rollbackMetadata.setStartRollbackTime(instantTimeToDelete);
    Map<String, HoodieRollbackPartitionMetadata> partitionMetadataMap = new HashMap<>();
    for (Map.Entry<String, List<String>> entry : partitionToFilesMeta.entrySet()) {
      HoodieRollbackPartitionMetadata rollbackPartitionMetadata = new HoodieRollbackPartitionMetadata();
      rollbackPartitionMetadata.setPartitionPath(entry.getKey());
      rollbackPartitionMetadata.setSuccessDeleteFiles(entry.getValue());
      rollbackPartitionMetadata.setFailedDeleteFiles(new ArrayList<>());
      rollbackPartitionMetadata.setWrittenLogFiles(getWrittenLogFiles(instantTimeToDelete, entry));
      rollbackPartitionMetadata.setRollbackLogFiles(createImmutableMap(logFileName(instantTimeToDelete, UUID.randomUUID().toString(), 0), (long) (100 + RANDOM.nextInt(500))));
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
      if (FSUtils.isLogFile(new Path(fileName))) {
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
    savepointMetadata.setSavepointedAt(Long.valueOf(instant));
    Map<String, HoodieSavepointPartitionMetadata> partitionMetadataMap = new HashMap<>();
    for (Map.Entry<String, List<String>> entry : partitionToFilesMeta.entrySet()) {
      HoodieSavepointPartitionMetadata savepointPartitionMetadata = new HoodieSavepointPartitionMetadata();
      savepointPartitionMetadata.setPartitionPath(entry.getKey());
      savepointPartitionMetadata.setSavepointDataFile(entry.getValue());
      partitionMetadataMap.put(entry.getKey(), savepointPartitionMetadata);
    }
    savepointMetadata.setPartitionMetadata(partitionMetadataMap);
    savepointMetadata.setSavepointedBy("test");
    return savepointMetadata;
  }

  public HoodieTestTable addRequestedCompaction(String instantTime) throws IOException {
    createRequestedCompaction(basePath, instantTime);
    currentInstantTime = instantTime;
    metaClient = HoodieTableMetaClient.reload(metaClient);
    return this;
  }

  public HoodieTestTable addRequestedCompaction(String instantTime, HoodieCompactionPlan compactionPlan) throws IOException {
    HoodieInstant compactionInstant = new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, instantTime);
    metaClient.getActiveTimeline().saveToCompactionRequested(compactionInstant,
        TimelineMetadataUtils.serializeCompactionPlan(compactionPlan));
    return addRequestedCompaction(instantTime);
  }

  public HoodieTestTable addRequestedCompaction(String instantTime, FileSlice... fileSlices) throws IOException {
    HoodieCompactionPlan plan = CompactionUtils
        .buildFromFileSlices(Arrays.stream(fileSlices).map(fs -> Pair.of(fs.getPartitionPath(), fs))
            .collect(Collectors.toList()), Option.empty(), Option.empty());
    return addRequestedCompaction(instantTime, plan);
  }

  public HoodieTestTable addCompaction(String instantTime, HoodieCommitMetadata commitMetadata) throws Exception {
    createRequestedCompaction(basePath, instantTime);
    createInflightCompaction(basePath, instantTime);
    return HoodieTestTable.of(metaClient)
        .addCommit(instantTime, commitMetadata);
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

  public HoodieTestTable withMarkerFile(String partitionPath, String fileId, IOType ioType) throws IOException {
    createMarkerFile(basePath, partitionPath, currentInstantTime, fileId, ioType);
    return this;
  }

  public HoodieTestTable withMarkerFiles(String partitionPath, int num, IOType ioType) throws IOException {
    String[] fileIds = IntStream.range(0, num).mapToObj(i -> UUID.randomUUID().toString()).toArray(String[]::new);
    return withMarkerFiles(partitionPath, fileIds, ioType);
  }

  public HoodieTestTable withMarkerFiles(String partitionPath, String[] fileIds, IOType ioType) throws IOException {
    for (String fileId : fileIds) {
      createMarkerFile(basePath, partitionPath, currentInstantTime, fileId, ioType);
    }
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
      FileCreateUtils.createBaseFile(basePath, p, currentInstantTime, fileId);
      partitionFileIdMap.put(p, fileId);
    }
    return partitionFileIdMap;
  }

  public HoodieTestTable withBaseFilesInPartitions(Map<String, String> partitionAndFileId) throws Exception {
    for (Map.Entry<String, String> pair : partitionAndFileId.entrySet()) {
      withBaseFilesInPartition(pair.getKey(), pair.getValue());
    }
    return this;
  }

  public HoodieTestTable withBaseFilesInPartition(String partition, String... fileIds) throws Exception {
    for (String f : fileIds) {
      FileCreateUtils.createBaseFile(basePath, partition, currentInstantTime, f);
    }
    return this;
  }

  public HoodieTestTable withBaseFilesInPartition(String partition, int... lengths) throws Exception {
    for (int l : lengths) {
      String fileId = UUID.randomUUID().toString();
      FileCreateUtils.createBaseFile(basePath, partition, currentInstantTime, fileId, l);
    }
    return this;
  }

  public HoodieTestTable withBaseFilesInPartition(String partition, List<Pair<String, Integer>> fileInfos) throws Exception {
    for (Pair<String, Integer> fileInfo : fileInfos) {
      FileCreateUtils.createBaseFile(basePath, partition, currentInstantTime, fileInfo.getKey(), fileInfo.getValue());
    }
    return this;
  }

  public String getFileIdWithLogFile(String partitionPath) throws Exception {
    String fileId = UUID.randomUUID().toString();
    withLogFile(partitionPath, fileId);
    return fileId;
  }

  public HoodieTestTable withLogFile(String partitionPath, String fileId) throws Exception {
    return withLogFile(partitionPath, fileId, 0);
  }

  public HoodieTestTable withLogFile(String partitionPath, String fileId, int... versions) throws Exception {
    for (int version : versions) {
      FileCreateUtils.createLogFile(basePath, partitionPath, currentInstantTime, fileId, version);
    }
    return this;
  }

  public HoodieTestTable withLogFilesInPartition(String partition, List<Pair<String, Integer[]>> fileInfos) throws Exception {
    for (Pair<String, Integer[]> fileInfo : fileInfos) {
      FileCreateUtils.createLogFile(basePath, partition, currentInstantTime, fileInfo.getKey(), fileInfo.getValue()[0], fileInfo.getValue()[1]);
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
      return fs.exists(getCommitFilePath(instantTime));
    } catch (IOException e) {
      throw new HoodieTestTableException(e);
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
      return fs.exists(new Path(Paths.get(basePath, partition, logFileName(instantTime, fileId, version)).toString()));
    } catch (IOException e) {
      throw new HoodieTestTableException(e);
    }
  }

  public Path getInflightCommitFilePath(String instantTime) {
    return new Path(Paths.get(basePath, HoodieTableMetaClient.METAFOLDER_NAME, instantTime + HoodieTimeline.INFLIGHT_COMMIT_EXTENSION).toUri());
  }

  public Path getCommitFilePath(String instantTime) {
    return new Path(Paths.get(basePath, HoodieTableMetaClient.METAFOLDER_NAME, instantTime + HoodieTimeline.COMMIT_EXTENSION).toUri());
  }

  public Path getRequestedCompactionFilePath(String instantTime) {
    return new Path(Paths.get(basePath, HoodieTableMetaClient.AUXILIARYFOLDER_NAME, instantTime + HoodieTimeline.REQUESTED_COMPACTION_EXTENSION).toUri());
  }

  public Path getPartitionPath(String partition) {
    return new Path(Paths.get(basePath, partition).toUri());
  }

  public List<java.nio.file.Path> getAllPartitionPaths() throws IOException {
    java.nio.file.Path basePathPath = Paths.get(basePath, HoodieTableMetaClient.TEMPFOLDER_NAME).getParent().getParent();
    return FileCreateUtils.getPartitionPaths(basePathPath);
  }

  public Path getBaseFilePath(String partition, String fileId) {
    return new Path(Paths.get(basePath, partition, getBaseFileNameById(fileId)).toUri());
  }

  public String getBaseFileNameById(String fileId) {
    return baseFileName(currentInstantTime, fileId);
  }

  public Path getLogFilePath(String partition, String fileId, int version) {
    return new Path(Paths.get(basePath, partition, getLogFileNameById(fileId, version)).toString());
  }

  public String getLogFileNameById(String fileId, int version) {
    return logFileName(currentInstantTime, fileId, version);
  }

  public List<String> getEarliestFilesInPartition(String partition, int count) throws IOException {
    List<FileStatus> fileStatuses = Arrays.asList(listAllFilesInPartition(partition));
    fileStatuses.sort(Comparator.comparing(FileStatus::getModificationTime));
    return fileStatuses.subList(0, count).stream().map(entry -> entry.getPath().getName()).collect(Collectors.toList());
  }

  public List<String> inflightCommits() {
    return this.inflightCommits;
  }

  public FileStatus[] listAllBaseFiles() throws IOException {
    return listAllBaseFiles(HoodieTableConfig.BASE_FILE_FORMAT.defaultValue().getFileExtension());
  }

  public FileStatus[] listAllBaseFiles(String fileExtension) throws IOException {
    return FileSystemTestUtils.listRecursive(fs, new Path(basePath)).stream()
        .filter(status -> status.getPath().getName().endsWith(fileExtension))
        .toArray(FileStatus[]::new);
  }

  public FileStatus[] listAllLogFiles() throws IOException {
    return listAllLogFiles(HoodieFileFormat.HOODIE_LOG.getFileExtension());
  }

  public FileStatus[] listAllLogFiles(String fileExtension) throws IOException {
    return FileSystemTestUtils.listRecursive(fs, new Path(basePath)).stream()
        .filter(status -> status.getPath().getName().contains(fileExtension))
        .toArray(FileStatus[]::new);
  }

  public FileStatus[] listAllBaseAndLogFiles() throws IOException {
    return Stream.concat(Stream.of(listAllBaseFiles()), Stream.of(listAllLogFiles())).toArray(FileStatus[]::new);
  }

  public FileStatus[] listAllFilesInPartition(String partitionPath) throws IOException {
    return FileSystemTestUtils.listRecursive(fs, new Path(Paths.get(basePath, partitionPath).toString())).stream()
        .filter(entry -> {
          boolean toReturn = true;
          String fileName = entry.getPath().getName();
          if (fileName.equals(HoodiePartitionMetadata.HOODIE_PARTITION_METAFILE)) {
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
    return FileSystemTestUtils.listRecursive(fs, new Path(Paths.get(basePath, HoodieTableMetaClient.TEMPFOLDER_NAME).toString())).toArray(new FileStatus[0]);
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
    Option<HoodieCommitMetadata> commitMetadata = getMetadataForInstant(commitTimeToRollback);
    if (!commitMetadata.isPresent()) {
      throw new IllegalArgumentException("Instant to rollback not present in timeline: " + commitTimeToRollback);
    }
    Map<String, List<String>> partitionFiles = getPartitionFiles(commitMetadata.get());
    HoodieRollbackMetadata rollbackMetadata = getRollbackMetadata(commitTimeToRollback, partitionFiles);
    for (Map.Entry<String, List<String>> entry : partitionFiles.entrySet()) {
      deleteFilesInPartition(entry.getKey(), entry.getValue());
    }
    return addRollback(commitTime, rollbackMetadata);
  }

  public HoodieTestTable doCluster(String commitTime, Map<String, List<String>> partitionToReplaceFileIds) throws Exception {
    Map<String, List<Pair<String, Integer>>> partitionToReplaceFileIdsWithLength = new HashMap<>();
    for (Map.Entry<String, List<String>> entry : partitionToReplaceFileIds.entrySet()) {
      String partition = entry.getKey();
      partitionToReplaceFileIdsWithLength.put(entry.getKey(), new ArrayList<>());
      for (String fileId : entry.getValue()) {
        int length = 100 + RANDOM.nextInt(500);
        partitionToReplaceFileIdsWithLength.get(partition).add(Pair.of(fileId, length));
      }
    }
    List<HoodieWriteStat> writeStats = generateHoodieWriteStatForPartition(partitionToReplaceFileIdsWithLength, commitTime, false);
    HoodieReplaceCommitMetadata replaceMetadata =
        (HoodieReplaceCommitMetadata) buildMetadata(writeStats, partitionToReplaceFileIds, Option.empty(), CLUSTER, EMPTY_STRING, REPLACE_COMMIT_ACTION);
    return addReplaceCommit(commitTime, Option.empty(), Option.empty(), replaceMetadata);
  }

  public HoodieCleanMetadata doClean(String commitTime, Map<String, Integer> partitionFileCountsToDelete) throws IOException {
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
    addClean(commitTime, cleanerMeta.getKey(), cleanerMeta.getValue());
    return cleanerMeta.getValue();
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

  public HoodieTestTable doCompaction(String commitTime, List<String> partitions) throws Exception {
    this.currentInstantTime = commitTime;
    if (partitions.isEmpty()) {
      partitions = Collections.singletonList(EMPTY_STRING);
    }
    HoodieTestTableState testTableState = getTestTableStateWithPartitionFileInfo(COMPACT, metaClient.getTableType(), commitTime, partitions, 1);
    HoodieCommitMetadata commitMetadata = createCommitMetadata(COMPACT, commitTime, testTableState);
    for (String partition : partitions) {
      this.withBaseFilesInPartition(partition, testTableState.getPartitionToBaseFileInfoMap(commitTime).get(partition));
    }
    return addCompaction(commitTime, commitMetadata);
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
    return doWriteOperation(commitTime, operationType, Collections.emptyList(), partitions, filesPerPartition, bootstrap, false);
  }

  public HoodieCommitMetadata doWriteOperation(String commitTime, WriteOperationType operationType,
                                               List<String> newPartitionsToAdd, List<String> partitions,
                                               int filesPerPartition, boolean bootstrap, boolean createInflightCommit) throws Exception {
    if (partitions.isEmpty()) {
      partitions = Collections.singletonList(EMPTY_STRING);
    }
    HoodieTestTableState testTableState = getTestTableStateWithPartitionFileInfo(operationType, metaClient.getTableType(), commitTime, partitions, filesPerPartition);
    HoodieCommitMetadata commitMetadata = createCommitMetadata(operationType, commitTime, testTableState, bootstrap);
    for (String str : newPartitionsToAdd) {
      this.withPartitionMetaFiles(str);
    }
    if (createInflightCommit) {
      this.addInflightCommit(commitTime);
    } else {
      this.addCommit(commitTime, commitMetadata);
    }
    for (String partition : partitions) {
      this.withBaseFilesInPartition(partition, testTableState.getPartitionToBaseFileInfoMap(commitTime).get(partition));
      if (MERGE_ON_READ.equals(metaClient.getTableType()) && UPSERT.equals(operationType)) {
        this.withLogFilesInPartition(partition, testTableState.getPartitionToLogFileInfoMap(commitTime).get(partition));
      }
    }
    return commitMetadata;
  }

  private Option<HoodieCommitMetadata> getMetadataForInstant(String instantTime) {
    Option<HoodieInstant> hoodieInstant = metaClient.getActiveTimeline().getCommitsTimeline()
        .filterCompletedInstants().filter(i -> i.getTimestamp().equals(instantTime)).firstInstant();
    try {
      if (hoodieInstant.isPresent()) {
        switch (hoodieInstant.get().getAction()) {
          case HoodieTimeline.REPLACE_COMMIT_ACTION:
            HoodieReplaceCommitMetadata replaceCommitMetadata = HoodieReplaceCommitMetadata
                .fromBytes(metaClient.getActiveTimeline().getInstantDetails(hoodieInstant.get()).get(), HoodieReplaceCommitMetadata.class);
            return Option.of(replaceCommitMetadata);
          case HoodieTimeline.DELTA_COMMIT_ACTION:
          case HoodieTimeline.COMMIT_ACTION:
            HoodieCommitMetadata commitMetadata = HoodieCommitMetadata
                .fromBytes(metaClient.getActiveTimeline().getInstantDetails(hoodieInstant.get()).get(), HoodieCommitMetadata.class);
            return Option.of(commitMetadata);
          default:
            throw new IllegalArgumentException("Unknown instant action" + hoodieInstant.get().getAction());
        }
      } else {
        return Option.empty();
      }
    } catch (IOException io) {
      throw new HoodieIOException("Unable to read metadata for instant " + hoodieInstant.get(), io);
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

  private static HoodieTestTableState getTestTableStateWithPartitionFileInfo(WriteOperationType operationType, HoodieTableType tableType, String commitTime,
          List<String> partitions, int filesPerPartition) {
    for (String partition : partitions) {
      Stream<Integer> fileLengths = IntStream.range(0, filesPerPartition).map(i -> 100 + RANDOM.nextInt(500)).boxed();
      if (MERGE_ON_READ.equals(tableType) && UPSERT.equals(operationType)) {
        List<Pair<Integer, Integer>> fileVersionAndLength = fileLengths.map(len -> Pair.of(0, len)).collect(Collectors.toList());
        testTableState = testTableState.createTestTableStateForBaseAndLogFiles(commitTime, partition, fileVersionAndLength);
      } else {
        testTableState = testTableState.createTestTableStateForBaseFilesOnly(commitTime, partition, fileLengths.collect(Collectors.toList()));
      }
    }
    return testTableState;
  }

  private static List<HoodieWriteStat> generateHoodieWriteStatForPartition(Map<String, List<Pair<String, Integer>>> partitionToFileIdMap,
                                                                           String commitTime, boolean bootstrap) {
    List<HoodieWriteStat> writeStats = new ArrayList<>();
    for (Map.Entry<String, List<Pair<String, Integer>>> entry : partitionToFileIdMap.entrySet()) {
      String partition = entry.getKey();
      for (Pair<String, Integer> fileIdInfo : entry.getValue()) {
        HoodieWriteStat writeStat = new HoodieWriteStat();
        String fileName = bootstrap ? fileIdInfo.getKey() :
            FileCreateUtils.baseFileName(commitTime, fileIdInfo.getKey());
        writeStat.setFileId(fileName);
        writeStat.setPartitionPath(partition);
        writeStat.setPath(partition + "/" + fileName);
        writeStat.setTotalWriteBytes(fileIdInfo.getValue());
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
        writeStat.setPath(partition + "/" + fileName);
        writeStat.setTotalWriteBytes(fileIdInfo.getValue()[1]);
        writeStats.add(writeStat);
      }
    }
    return writeStats;
  }

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

    HoodieTestTableState createTestTableStateForBaseFilesOnly(String commitTime, String partitionPath, List<Integer> lengths) {
      if (!commitsToPartitionToBaseFileInfoStats.containsKey(commitTime)) {
        commitsToPartitionToBaseFileInfoStats.put(commitTime, new HashMap<>());
      }
      if (!this.commitsToPartitionToBaseFileInfoStats.get(commitTime).containsKey(partitionPath)) {
        this.commitsToPartitionToBaseFileInfoStats.get(commitTime).put(partitionPath, new ArrayList<>());
      }

      List<Pair<String, Integer>> fileInfos = new ArrayList<>();
      for (int length : lengths) {
        fileInfos.add(Pair.of(UUID.randomUUID().toString(), length));
      }
      this.commitsToPartitionToBaseFileInfoStats.get(commitTime).get(partitionPath).addAll(fileInfos);
      return this;
    }

    HoodieTestTableState createTestTableStateForBaseAndLogFiles(String commitTime, String partitionPath, List<Pair<Integer, Integer>> versionsAndLengths) {
      if (!commitsToPartitionToBaseFileInfoStats.containsKey(commitTime)) {
        createTestTableStateForBaseFilesOnly(commitTime, partitionPath, versionsAndLengths.stream().map(Pair::getRight).collect(Collectors.toList()));
      }
      if (!this.commitsToPartitionToBaseFileInfoStats.get(commitTime).containsKey(partitionPath)) {
        createTestTableStateForBaseFilesOnly(commitTime, partitionPath, versionsAndLengths.stream().map(Pair::getRight).collect(Collectors.toList()));
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
