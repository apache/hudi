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
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.avro.model.HoodieInstantInfo;
import org.apache.hudi.avro.model.HoodieRequestedReplaceMetadata;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.avro.model.HoodieRollbackPartitionMetadata;
import org.apache.hudi.common.HoodieCleanStat;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import static org.apache.hudi.common.table.timeline.HoodieActiveTimeline.COMMIT_FORMATTER;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.CLEAN_ACTION;
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

public class HoodieTestTable {

  private static final Logger LOG = LogManager.getLogger(HoodieTestTable.class);
  private static final Random RANDOM = new Random();
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

  public HoodieCommitMetadata createCommitMetadata(WriteOperationType operationType, String commitTime,
                                                   Map<String, List<Pair<String, Integer>>> partitionToFileIdMap) {
    return createCommitMetadata(operationType, commitTime, partitionToFileIdMap, false);
  }

  public HoodieCommitMetadata createCommitMetadata(WriteOperationType operationType, String commitTime,
                                                   Map<String, List<Pair<String, Integer>>> partitionToFileIdMap,
                                                   boolean bootstrap) {
    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    commitMetadata.addMetadata("test", "test");
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
        commitMetadata.addWriteStat(partition, writeStat);
      }
    }
    commitMetadata.setOperationType(operationType);
    return commitMetadata;
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
    HoodieCleanerPlan cleanerPlan = new HoodieCleanerPlan(new HoodieActionInstant("", "", ""), "", new HashMap<>(),
        CleanPlanV2MigrationHandler.VERSION, new HashMap<>());
    HoodieCleanStat cleanStats = new HoodieCleanStat(
        HoodieCleaningPolicy.KEEP_LATEST_FILE_VERSIONS,
        HoodieTestUtils.DEFAULT_PARTITION_PATHS[new Random().nextInt(HoodieTestUtils.DEFAULT_PARTITION_PATHS.length)],
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList(),
        instantTime);
    HoodieCleanMetadata cleanMetadata = convertCleanMetadata(instantTime, Option.of(0L), Collections.singletonList(cleanStats));
    return HoodieTestTable.of(metaClient).addClean(instantTime, cleanerPlan, cleanMetadata);
  }

  public Pair<HoodieCleanerPlan, HoodieCleanMetadata> getHoodieCleanMetadata(String instantTime, Map<String, List<String>> partitionToDeletedFiles) {
    HoodieCleanerPlan cleanerPlan = new HoodieCleanerPlan(new HoodieActionInstant(instantTime, CLEAN_ACTION, ""), "", new HashMap<>(),
        CleanPlanV2MigrationHandler.VERSION, new HashMap<>());
    List<HoodieCleanStat> cleanStats = new ArrayList<>();
    for (Map.Entry<String, List<String>> entry : partitionToDeletedFiles.entrySet()) {
      cleanStats.add(new HoodieCleanStat(HoodieCleaningPolicy.KEEP_LATEST_FILE_VERSIONS,
          entry.getKey(), entry.getValue(), entry.getValue(), Collections.emptyList(), instantTime));
    }
    return Pair.of(cleanerPlan, convertCleanMetadata(instantTime, Option.of(0L), cleanStats));
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

  public HoodieRollbackMetadata getRollbackMetadata(String instantTimeToDelete, String commitTime, Map<String, List<String>> partitionToFilesMeta) throws Exception {
    HoodieRollbackMetadata rollbackMetadata = new HoodieRollbackMetadata();
    rollbackMetadata.setCommitsRollback(Arrays.asList(instantTimeToDelete));
    rollbackMetadata.setStartRollbackTime(instantTimeToDelete);
    Map<String, HoodieRollbackPartitionMetadata> partitionMetadataMap = new HashMap<>();
    for (Map.Entry<String, List<String>> entry : partitionToFilesMeta.entrySet()) {
      HoodieRollbackPartitionMetadata rollbackPartitionMetadata = new HoodieRollbackPartitionMetadata();
      rollbackPartitionMetadata.setPartitionPath(entry.getKey());
      rollbackPartitionMetadata.setSuccessDeleteFiles(entry.getValue());
      rollbackPartitionMetadata.setFailedDeleteFiles(new ArrayList<>());
      rollbackPartitionMetadata.setWrittenLogFiles(new HashMap<>());
      rollbackPartitionMetadata.setRollbackLogFiles(new HashMap<>());
      partitionMetadataMap.put(entry.getKey(), rollbackPartitionMetadata);
    }
    rollbackMetadata.setPartitionMetadata(partitionMetadataMap);
    rollbackMetadata.setInstantsRollback(Collections.singletonList(new HoodieInstantInfo(instantTimeToDelete, HoodieTimeline.ROLLBACK_ACTION)));
    return rollbackMetadata;
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

  public HoodieTestTable addCompaction(String instantTime) throws Exception {
    createRequestedCompaction(basePath, instantTime);
    String fileId1 = "file-" + instantTime + "-1";
    String fileId2 = "file-" + instantTime + "-2";

    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    commitMetadata.addMetadata("test", "test");
    commitMetadata.setOperationType(WriteOperationType.COMPACT);
    commitMetadata.setCompacted(true);
    HoodieWriteStat writeStat = new HoodieWriteStat();
    writeStat.setFileId(fileId1);
    commitMetadata.addWriteStat(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, writeStat);
    return HoodieTestTable.of(metaClient)
        .addCommit(instantTime, commitMetadata)
        .withBaseFilesInPartition(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, fileId1, fileId2);
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
    List<java.nio.file.Path> toReturn = FileCreateUtils.getPartitionPaths(basePathPath);
    return toReturn;
  }

  public Path getBaseFilePath(String partition, String fileId) {
    return new Path(Paths.get(basePath, partition, getBaseFileNameById(fileId)).toUri());
  }

  public String getBaseFileNameById(String fileId) {
    return baseFileName(currentInstantTime, fileId);
  }

  public List<String> getEarliestFilesInPartition(String partition, int count) throws IOException {
    List<FileStatus> fileStatuses = Arrays.asList(listAllFilesInPartition(partition));
    Collections.sort(fileStatuses, new Comparator<FileStatus>() {
      @Override
      public int compare(FileStatus o1, FileStatus o2) {
        if (o1.getModificationTime() > o2.getModificationTime()) {
          return 1;
        } else if (o1.getModificationTime() == o2.getModificationTime()) {
          return 0;
        } else {
          return -1;
        }
      }
    });
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
              }
            }
          }
          return toReturn;
        }).collect(Collectors.toList()).toArray(new FileStatus[0]);
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
        e.printStackTrace();
      }
    });
  }

  public HoodieCleanMetadata doClean(HoodieTestTable testTable, String commitTime, Map<String, Integer> partitionFileCountsToDelete) throws IOException {
    Map<String, List<String>> partitionFilesToDelete = new HashMap<>();
    for (Map.Entry<String, Integer> entry : partitionFileCountsToDelete.entrySet()) {
      partitionFilesToDelete.put(entry.getKey(), testTable.getEarliestFilesInPartition(entry.getKey(), entry.getValue()));
    }
    PartitionDeleteFileList partitionDeleteFileList = new PartitionDeleteFileList();
    for (Map.Entry<String, List<String>> entry : partitionFilesToDelete.entrySet()) {
      partitionDeleteFileList = partitionDeleteFileList.addPartitionAndBasefiles(commitTime, entry.getKey(), entry.getValue());
      testTable.deleteFilesInPartition(entry.getKey(), entry.getValue());
    }
    Pair<HoodieCleanerPlan, HoodieCleanMetadata> cleanerMeta = testTable.getHoodieCleanMetadata(commitTime, partitionDeleteFileList.getPartitionToFileIdMap(commitTime));
    testTable.addClean(commitTime, cleanerMeta.getKey(), cleanerMeta.getValue());
    return cleanerMeta.getValue();
  }

  public HoodieTestTable doCompaction(HoodieTestTable testTable, String commitTime, List<String> partitions) throws Exception {
    this.currentInstantTime = commitTime;
    PartitionFileInfoMap partitionFileInfoMap = new PartitionFileInfoMap();
    for (String partition : partitions) {
      partitionFileInfoMap = partitionFileInfoMap.addPartitionAndBasefiles(commitTime, partition, Arrays.asList(100 + RANDOM.nextInt(500)));
    }
    HoodieCommitMetadata commitMetadata = testTable.createCommitMetadata(WriteOperationType.COMPACT, commitTime, partitionFileInfoMap.getPartitionToFileIdMap(commitTime));
    for (String partition : partitions) {
      testTable = testTable.withBaseFilesInPartition(partition, partitionFileInfoMap.getPartitionToFileIdMap(commitTime).get(partition));
    }
    return testTable.addCompaction(commitTime, commitMetadata);
  }

  public Pair<HoodieCommitMetadata, PartitionFileInfoMap> doWriteOperation(HoodieTestTable testTable, String commitTime, WriteOperationType operationType,
                                                                           List<String> newPartitionsToAdd, List<String> partitions, int filestoAddPerPartition) throws Exception {
    return doWriteOperation(testTable, commitTime, operationType, newPartitionsToAdd, partitions, filestoAddPerPartition, false);
  }
  public Pair<HoodieCommitMetadata, PartitionFileInfoMap> doWriteOperation(HoodieTestTable testTable, String commitTime, WriteOperationType operationType,
                                                                           List<String> newPartitionsToAdd, List<String> partitions, int filestoAddPerPartition, boolean bootstrap) throws Exception {
    return doWriteOperation(testTable, commitTime, operationType, newPartitionsToAdd, partitions, filestoAddPerPartition, bootstrap, false);
  }

  public Pair<HoodieCommitMetadata, PartitionFileInfoMap> doWriteOperation(HoodieTestTable testTable, String commitTime, WriteOperationType operationType,
                                                                           List<String> newPartitionsToAdd, List<String> partitions, int filestoAddPerPartition, boolean bootstrap,
                                                                           boolean createInflightCommit) throws Exception {
    PartitionFileInfoMap partitionFileInfoMap = new PartitionFileInfoMap();
    for (String partition : partitions) {
      List<Integer> fileLengths = new ArrayList<>();
      for (int i = 0; i < filestoAddPerPartition; i++) {
        fileLengths.add(10 + RANDOM.nextInt(500));
      }
      partitionFileInfoMap = partitionFileInfoMap.addPartitionAndBasefiles(commitTime, partition, fileLengths);
    }
    HoodieCommitMetadata commitMetadata = testTable.createCommitMetadata(operationType, commitTime, partitionFileInfoMap.getPartitionToFileIdMap(commitTime), bootstrap);
    for (String str : newPartitionsToAdd) {
      testTable = testTable.withPartitionMetaFiles(str);
    }
    if (createInflightCommit) {
      testTable = testTable.addInflightCommit(commitTime);
    } else {
      testTable = testTable
          .addCommit(commitTime, commitMetadata);
    }
    for (String partition : partitions) {
      testTable = testTable.withBaseFilesInPartition(partition, partitionFileInfoMap.getPartitionToFileIdMap(commitTime).get(partition));
    }
    return Pair.of(commitMetadata, partitionFileInfoMap);
  }

  public static class HoodieTestTableException extends RuntimeException {
    public HoodieTestTableException(Throwable t) {
      super(t);
    }
  }
}
