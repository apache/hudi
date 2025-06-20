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

package org.apache.hudi.client.functional;

import org.apache.hudi.avro.model.HoodieActionInstant;
import org.apache.hudi.avro.model.HoodieCleanFileInfo;
import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCleanerPlan;
import org.apache.hudi.avro.model.HoodieMetadataColumnStats;
import org.apache.hudi.avro.model.IntWrapper;
import org.apache.hudi.avro.model.StringWrapper;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.HoodieCleanStat;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.model.HoodieColumnRangeMetadata;
import org.apache.hudi.common.model.HoodieDeltaWriteStat;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.CleanerUtils;
import org.apache.hudi.common.util.ExternalFilePathUtil;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.HoodieBackedTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;
import org.apache.hudi.table.action.clean.CleanPlanner;
import org.apache.hudi.testutils.HoodieClientTestBase;

import org.apache.avro.Schema;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.apache.hudi.index.HoodieIndex.IndexType.INMEMORY;

/**
 * Asserts that tables initialized from file paths created outside Hudi can properly be loaded.
 */
public class TestExternalPathHandling extends HoodieClientTestBase {

  private static final String FIELD_1 = "field1";
  private static final String FIELD_2 = "field2";
  private HoodieWriteConfig writeConfig;

  @ParameterizedTest
  @MethodSource("getArgs")
  public void testFlow(FileIdAndNameGenerator fileIdAndNameGenerator, List<String> partitions) throws Exception {
    metaClient = HoodieTableMetaClient.reload(metaClient);
    Properties properties = new Properties();
    properties.setProperty(HoodieMetadataConfig.AUTO_INITIALIZE.key(), "false");
    List<Schema.Field> fields = new ArrayList<>();
    fields.add(new Schema.Field(FIELD_1, Schema.create(Schema.Type.STRING), null, null));
    fields.add(new Schema.Field(FIELD_2, Schema.create(Schema.Type.STRING), null, null));
    Schema simpleSchema = Schema.createRecord("simpleSchema", null, null, false, fields);

    writeConfig = HoodieWriteConfig.newBuilder()
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(INMEMORY).build())
        .withPath(metaClient.getBasePath())
        .withEmbeddedTimelineServerEnabled(false)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .withMaxNumDeltaCommitsBeforeCompaction(2)
            .enable(true)
            .withMetadataIndexColumnStats(true)
            .withColumnStatsIndexForColumns(FIELD_1 + "," + FIELD_2)
            .withProperties(properties)
            .build())
        .withArchivalConfig(HoodieArchivalConfig.newBuilder().archiveCommitsWith(1, 2).build())
        .withTableServicesEnabled(true)
        .withSchema(simpleSchema.toString())
        .build();

    writeClient = getHoodieWriteClient(writeConfig);
    writeClient.setOperationType(WriteOperationType.INSERT_OVERWRITE);
    String instantTime1 = writeClient.startCommit(HoodieTimeline.REPLACE_COMMIT_ACTION, metaClient);
    String partitionPath1 = partitions.get(0);
    Pair<String, String> fileIdAndName1 = fileIdAndNameGenerator.generate(1, instantTime1);
    String fileId1 = fileIdAndName1.getLeft();
    String fileName1 = fileIdAndName1.getRight();
    String filePath1 = getPath(partitionPath1, fileName1);
    WriteStatus writeStatus1 = createWriteStatus(instantTime1, partitionPath1, filePath1, fileId1);
    JavaRDD<WriteStatus> rdd1 = createRdd(Collections.singletonList(writeStatus1));
    metaClient.getActiveTimeline().transitionReplaceRequestedToInflight(
        INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.REPLACE_COMMIT_ACTION, instantTime1), Option.empty());
    writeClient.commit(instantTime1, rdd1, Option.empty(), HoodieTimeline.REPLACE_COMMIT_ACTION,  Collections.emptyMap());

    assertFileGroupCorrectness(instantTime1, partitionPath1, filePath1, fileId1, 1);

    // add a new file and remove the old one
    String instantTime2 = writeClient.startCommit(HoodieTimeline.REPLACE_COMMIT_ACTION, metaClient);
    Pair<String, String> fileIdAndName2 = fileIdAndNameGenerator.generate(2, instantTime2);
    String fileId2 = fileIdAndName2.getLeft();
    String fileName2 = fileIdAndName2.getRight();
    String filePath2 = getPath(partitionPath1, fileName2);
    WriteStatus newWriteStatus = createWriteStatus(instantTime2, partitionPath1, filePath2, fileId2);
    JavaRDD<WriteStatus> rdd2 = createRdd(Collections.singletonList(newWriteStatus));
    Map<String, List<String>> partitionToReplacedFileIds = new HashMap<>();
    partitionToReplacedFileIds.put(partitionPath1, Collections.singletonList(fileId1));
    metaClient.getActiveTimeline().transitionReplaceRequestedToInflight(
        INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.REPLACE_COMMIT_ACTION, instantTime2), Option.empty());
    writeClient.commit(instantTime2, rdd2, Option.empty(), HoodieTimeline.REPLACE_COMMIT_ACTION, partitionToReplacedFileIds);

    assertFileGroupCorrectness(instantTime2, partitionPath1, filePath2, fileId2, 1);

    // Add file to a new partition
    String partitionPath2 = partitions.get(1);
    String instantTime3 = writeClient.startCommit(HoodieTimeline.REPLACE_COMMIT_ACTION, metaClient);
    Pair<String, String> fileIdAndName3 = fileIdAndNameGenerator.generate(3, instantTime3);
    String fileId3 = fileIdAndName3.getLeft();
    String fileName3 = fileIdAndName3.getRight();
    String filePath3 = getPath(partitionPath2, fileName3);
    WriteStatus writeStatus3 = createWriteStatus(instantTime3, partitionPath2, filePath3, fileId3);
    JavaRDD<WriteStatus> rdd3 = createRdd(Collections.singletonList(writeStatus3));
    metaClient.getActiveTimeline().transitionReplaceRequestedToInflight(
        INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.REPLACE_COMMIT_ACTION, instantTime3), Option.empty());
    writeClient.commit(instantTime3, rdd3, Option.empty(), HoodieTimeline.REPLACE_COMMIT_ACTION, Collections.emptyMap());

    assertFileGroupCorrectness(instantTime3, partitionPath2, filePath3, fileId3, partitionPath2.isEmpty() ? 2 : 1);

    // clean first commit
    String cleanTime = writeClient.createNewInstantTime();
    HoodieCleanerPlan cleanerPlan = cleanerPlan(new HoodieActionInstant(instantTime2, HoodieTimeline.REPLACE_COMMIT_ACTION, HoodieInstant.State.COMPLETED.name()), instantTime3,
        Collections.singletonMap(partitionPath1, Collections.singletonList(new HoodieCleanFileInfo(filePath1, false))));
    metaClient.getActiveTimeline().saveToCleanRequested(INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.CLEAN_ACTION, cleanTime), Option.of(cleanerPlan));
    HoodieInstant inflightClean = metaClient.getActiveTimeline().transitionCleanRequestedToInflight(
        INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.CLEAN_ACTION, cleanTime));
    List<HoodieCleanStat> cleanStats = Collections.singletonList(createCleanStat(partitionPath1, Arrays.asList(filePath1), instantTime2, instantTime3));
    HoodieCleanMetadata cleanMetadata = CleanerUtils.convertCleanMetadata(
        cleanTime,
        Option.empty(),
        cleanStats,
        Collections.EMPTY_MAP);
    try (HoodieTableMetadataWriter hoodieTableMetadataWriter = (HoodieTableMetadataWriter) writeClient.initTable(WriteOperationType.UPSERT, Option.of(cleanTime)).getMetadataWriter(cleanTime).get()) {
      hoodieTableMetadataWriter.update(cleanMetadata, cleanTime);
      metaClient.getActiveTimeline().transitionCleanInflightToComplete(true, inflightClean, Option.of(cleanMetadata));
      // make sure we still get the same results as before
      assertFileGroupCorrectness(instantTime2, partitionPath1, filePath2, fileId2, partitionPath2.isEmpty() ? 2 : 1);
      assertFileGroupCorrectness(instantTime3, partitionPath2, filePath3, fileId3, partitionPath2.isEmpty() ? 2 : 1);

      // trigger archiver manually
      writeClient.archive();
      // assert commit was archived
      Assertions.assertEquals(1, metaClient.getArchivedTimeline().reload().filterCompletedInstants().countInstants());
      // make sure we still get the same results as before
      assertFileGroupCorrectness(instantTime2, partitionPath1, filePath2, fileId2, partitionPath2.isEmpty() ? 2 : 1);
      assertFileGroupCorrectness(instantTime3, partitionPath2, filePath3, fileId3, partitionPath2.isEmpty() ? 2 : 1);

      // assert that column stats are correct
      HoodieBackedTableMetadata hoodieBackedTableMetadata = new HoodieBackedTableMetadata(
          context, metaClient.getStorage(), writeConfig.getMetadataConfig(), writeConfig.getBasePath(), true);
      assertEmptyColStats(hoodieBackedTableMetadata, partitionPath1, fileName1);
      assertColStats(hoodieBackedTableMetadata, partitionPath1, fileName2);
      assertColStats(hoodieBackedTableMetadata, partitionPath2, fileName3);
    }

  }

  static Stream<Arguments> getArgs() {
    FileIdAndNameGenerator external = (index, instantTime) -> {
      String fileName = String.format("file_%d.parquet", index);
      String fileId = fileName;
      return Pair.of(fileId, fileName);
    };
    List<String> partitionedTable = Arrays.asList("americas/brazil", "americas/argentina");
    List<String> unpartitionedTable = Arrays.asList("", "");
    return Stream.of(Arguments.of(external, partitionedTable), Arguments.of(external, unpartitionedTable));
  }

  private String getPath(String partitionPath, String fileName) {
    if (partitionPath.isEmpty()) {
      return fileName;
    }
    return String.format("%s/%s", partitionPath, fileName);
  }

  @FunctionalInterface
  private interface FileIdAndNameGenerator {
    Pair<String, String> generate(int iteration, String instantTime);
  }

  private void assertFileGroupCorrectness(String instantTime, String partitionPath, String filePath, String fileId, int expectedSize) {
    HoodieTableMetadata tableMetadata = metaClient.getTableFormat().getMetadataFactory().create(
        context, metaClient.getStorage(), writeConfig.getMetadataConfig(), metaClient.getBasePath().toString());
    HoodieTableFileSystemView fsView = new HoodieTableFileSystemView(tableMetadata, metaClient, metaClient.reloadActiveTimeline());
    List<HoodieFileGroup> fileGroups = fsView.getAllFileGroups(partitionPath).collect(Collectors.toList());
    Assertions.assertEquals(expectedSize, fileGroups.size());
    Option<HoodieFileGroup> fileGroupOption = Option.fromJavaOptional(fileGroups.stream().filter(fg -> fg.getFileGroupId().getFileId().equals(fileId)).findFirst());
    Assertions.assertTrue(fileGroupOption.isPresent());
    HoodieFileGroup fileGroup = fileGroupOption.get();
    Assertions.assertEquals(fileId, fileGroup.getFileGroupId().getFileId());
    Assertions.assertEquals(partitionPath, fileGroup.getPartitionPath());
    HoodieBaseFile baseFile = fileGroup.getAllBaseFiles().findFirst().get();
    Assertions.assertEquals(instantTime, baseFile.getCommitTime());
    Assertions.assertEquals(metaClient.getBasePath() + "/" + filePath, baseFile.getPath());
  }

  private void assertEmptyColStats(HoodieBackedTableMetadata hoodieBackedTableMetadata, String partitionPath, String fileName) {
    Assertions.assertTrue(hoodieBackedTableMetadata.getColumnStats(Collections.singletonList(Pair.of(partitionPath, fileName)), FIELD_1).isEmpty());
    Assertions.assertTrue(hoodieBackedTableMetadata.getColumnStats(Collections.singletonList(Pair.of(partitionPath, fileName)), FIELD_2).isEmpty());
  }

  private void assertColStats(HoodieBackedTableMetadata hoodieBackedTableMetadata, String partitionPath, String fileName) {
    Map<Pair<String, String>, HoodieMetadataColumnStats> field1ColStats = hoodieBackedTableMetadata.getColumnStats(Collections.singletonList(Pair.of(partitionPath, fileName)), FIELD_1);
    Assertions.assertEquals(1, field1ColStats.size());
    HoodieMetadataColumnStats column1stats = field1ColStats.get(Pair.of(partitionPath, fileName));
    Assertions.assertEquals(FIELD_1, column1stats.getColumnName());
    Assertions.assertEquals(fileName, column1stats.getFileName());
    Assertions.assertEquals(new IntWrapper(1), column1stats.getMinValue());
    Assertions.assertEquals(new IntWrapper(2), column1stats.getMaxValue());
    Assertions.assertEquals(2, column1stats.getValueCount());
    Assertions.assertEquals(0, column1stats.getNullCount());
    Assertions.assertEquals(5, column1stats.getTotalSize());
    Assertions.assertEquals(10, column1stats.getTotalUncompressedSize());


    Map<Pair<String, String>, HoodieMetadataColumnStats> field2ColStats = hoodieBackedTableMetadata.getColumnStats(Collections.singletonList(Pair.of(partitionPath, fileName)), FIELD_2);
    Assertions.assertEquals(1, field2ColStats.size());
    HoodieMetadataColumnStats column2stats = field2ColStats.get(Pair.of(partitionPath, fileName));
    Assertions.assertEquals(FIELD_2, column2stats.getColumnName());
    Assertions.assertEquals(fileName, column2stats.getFileName());
    Assertions.assertEquals(new StringWrapper("a"), column2stats.getMinValue());
    Assertions.assertEquals(new StringWrapper("b"), column2stats.getMaxValue());
    Assertions.assertEquals(3, column2stats.getValueCount());
    Assertions.assertEquals(1, column2stats.getNullCount());
    Assertions.assertEquals(10, column2stats.getTotalSize());
    Assertions.assertEquals(20, column2stats.getTotalUncompressedSize());
  }

  private JavaRDD<WriteStatus> createRdd(List<WriteStatus> writeStatuses) {
    return jsc.parallelize(writeStatuses, 1);
  }

  private WriteStatus createWriteStatus(String commitTime, String partitionPath, String filePath, String fileId) {
    WriteStatus writeStatus = new WriteStatus();
    writeStatus.setFileId(fileId);
    writeStatus.setPartitionPath(partitionPath);
    HoodieDeltaWriteStat writeStat = new HoodieDeltaWriteStat();
    writeStat.setFileId(fileId);
    writeStat.setPath(ExternalFilePathUtil.appendCommitTimeAndExternalFileMarker(filePath, commitTime));
    writeStat.setPartitionPath(partitionPath);
    writeStat.setNumWrites(3);
    writeStat.setNumDeletes(0);
    writeStat.setNumUpdateWrites(0);
    writeStat.setNumInserts(3);
    writeStat.setTotalWriteBytes(400);
    writeStat.setTotalWriteErrors(0);
    writeStat.setFileSizeInBytes(400);
    writeStat.setTotalLogBlocks(0);
    writeStat.setTotalLogRecords(0);
    writeStat.setTotalLogFilesCompacted(0);
    writeStat.setTotalLogSizeCompacted(0);
    writeStat.setTotalUpdatedRecordsCompacted(0);
    writeStat.setTotalCorruptLogBlock(0);
    writeStat.setTotalRollbackBlocks(0);
    Map<String, HoodieColumnRangeMetadata<Comparable>> stats = new HashMap<>();
    stats.put(FIELD_1, HoodieColumnRangeMetadata.<Comparable>create(filePath, FIELD_1, 1, 2, 0, 2, 5, 10));
    stats.put(FIELD_2, HoodieColumnRangeMetadata.<Comparable>create(filePath, FIELD_2, "a", "b", 1, 3, 10, 20));
    writeStat.putRecordsStats(stats);
    writeStatus.setStat(writeStat);
    return writeStatus;
  }

  private HoodieCleanStat createCleanStat(String partitionPath, List<String> deletePaths, String earliestCommitToRetain, String lastCompletedCommitTimestamp) {
    return new HoodieCleanStat(HoodieCleaningPolicy.KEEP_LATEST_COMMITS, partitionPath, deletePaths, deletePaths, Collections.emptyList(),
        earliestCommitToRetain, lastCompletedCommitTimestamp);
  }

  private HoodieCleanerPlan cleanerPlan(HoodieActionInstant earliestInstantToRetain, String latestCommit, Map<String, List<HoodieCleanFileInfo>> filePathsToBeDeletedPerPartition) {
    return new HoodieCleanerPlan(earliestInstantToRetain,
        latestCommit,
        writeConfig.getCleanerPolicy().name(), Collections.emptyMap(),
        CleanPlanner.LATEST_CLEAN_PLAN_VERSION, filePathsToBeDeletedPerPartition, Collections.emptyList(), Collections.EMPTY_MAP);
  }
}
