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

package org.apache.hudi.utilities;

import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.fs.HoodieWrapperFileSystem;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieColumnRangeMetadata;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.block.HoodieAvroDataBlock;
import org.apache.hudi.common.table.log.block.HoodieCommandBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieDefaultTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.SerializationUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.metadata.FileSystemBackedTableMetadata;
import org.apache.hudi.testutils.HoodieSparkClientTestBase;

import jodd.io.FileUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.hadoop.fs.FileUtil.copy;
import static org.apache.hudi.common.testutils.RawTripTestPayload.recordToString;
import static org.apache.hudi.common.testutils.SchemaTestUtil.getSimpleSchema;
import static org.apache.hudi.common.util.StringUtils.toStringWithThreshold;
import static org.apache.hudi.common.util.TestStringUtils.generateRandomString;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestHoodieMetadataTableValidator extends HoodieSparkClientTestBase {
  private static final Random RANDOM = new Random();

  private static Stream<Arguments> lastNFileSlicesTestArgs() {
    return Stream.of(-1, 1, 3, 4, 5).flatMap(i -> Stream.of(Arguments.of(i, true), Arguments.of(i, false)));
  }

  private final int logDetailMaxLength = new HoodieMetadataTableValidator.Config().logDetailMaxLength;

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testMetadataTableValidation(boolean includeUncommittedLogFiles) throws Exception {
    Map<String, String> writeOptions = new HashMap<>();
    writeOptions.put(DataSourceWriteOptions.TABLE_NAME().key(), "test_table");
    writeOptions.put("hoodie.table.name", "test_table");
    writeOptions.put(DataSourceWriteOptions.TABLE_TYPE().key(), "MERGE_ON_READ");
    writeOptions.put(DataSourceWriteOptions.RECORDKEY_FIELD().key(), "_row_key");
    writeOptions.put(DataSourceWriteOptions.PRECOMBINE_FIELD().key(), "timestamp");
    writeOptions.put(DataSourceWriteOptions.PARTITIONPATH_FIELD().key(), "partition_path");

    Dataset<Row> inserts = makeInsertDf("000", 5).cache();
    inserts.write().format("hudi").options(writeOptions)
        .option(DataSourceWriteOptions.OPERATION().key(), WriteOperationType.BULK_INSERT.value())
        .option(HoodieMetadataConfig.RECORD_INDEX_ENABLE_PROP.key(), "true")
        .option(HoodieMetadataConfig.RECORD_INDEX_MIN_FILE_GROUP_COUNT_PROP.key(), "1")
        .option(HoodieMetadataConfig.RECORD_INDEX_MAX_FILE_GROUP_COUNT_PROP.key(), "1")
        .mode(SaveMode.Overwrite)
        .save(basePath);
    Dataset<Row> updates = makeUpdateDf("001", 5).cache();
    updates.write().format("hudi").options(writeOptions)
        .option(DataSourceWriteOptions.OPERATION().key(), WriteOperationType.UPSERT.value())
        .option(HoodieMetadataConfig.RECORD_INDEX_ENABLE_PROP.key(), "true")
        .option(HoodieMetadataConfig.RECORD_INDEX_MIN_FILE_GROUP_COUNT_PROP.key(), "1")
        .option(HoodieMetadataConfig.RECORD_INDEX_MAX_FILE_GROUP_COUNT_PROP.key(), "1")
        .mode(SaveMode.Append)
        .save(basePath);

    if (includeUncommittedLogFiles) {
      // add uncommitted log file to simulate task retry
      RemoteIterator<LocatedFileStatus> fileStatusIterator = fs.listFiles(new Path(basePath), true);
      while (fileStatusIterator.hasNext()) {
        Path path = fileStatusIterator.next().getPath();
        if (FSUtils.isLogFile(path)) {
          String modifiedPath = path.toString().substring(0, path.toString().lastIndexOf("-") + 1) + "000";
          fs.copyFromLocalFile(path, new Path(modifiedPath));
          break;
        }
      }
    }

    // validate MDT
    HoodieMetadataTableValidator.Config config = new HoodieMetadataTableValidator.Config();
    config.basePath = "file:" + basePath;
    config.validateLatestFileSlices = true;
    config.validateAllFileGroups = true;
    HoodieMetadataTableValidator validator = new HoodieMetadataTableValidator(jsc, config);
    assertTrue(validator.run());
    assertFalse(validator.hasValidationFailure());
    assertTrue(validator.getThrowables().isEmpty());
  }

  @Test
  void missingLogFileFailsValidation() throws Exception {
    Map<String, String> writeOptions = new HashMap<>();
    writeOptions.put(DataSourceWriteOptions.TABLE_NAME().key(), "test_table");
    writeOptions.put("hoodie.table.name", "test_table");
    writeOptions.put(DataSourceWriteOptions.TABLE_TYPE().key(), "MERGE_ON_READ");
    writeOptions.put(DataSourceWriteOptions.RECORDKEY_FIELD().key(), "_row_key");
    writeOptions.put(DataSourceWriteOptions.PRECOMBINE_FIELD().key(), "timestamp");
    writeOptions.put(DataSourceWriteOptions.PARTITIONPATH_FIELD().key(), "partition_path");

    Dataset<Row> inserts = makeInsertDf("000", 5).cache();
    inserts.write().format("hudi").options(writeOptions)
        .option(DataSourceWriteOptions.OPERATION().key(), WriteOperationType.BULK_INSERT.value())
        .option(HoodieMetadataConfig.RECORD_INDEX_ENABLE_PROP.key(), "true")
        .option(HoodieMetadataConfig.RECORD_INDEX_MIN_FILE_GROUP_COUNT_PROP.key(), "1")
        .option(HoodieMetadataConfig.RECORD_INDEX_MAX_FILE_GROUP_COUNT_PROP.key(), "1")
        .mode(SaveMode.Overwrite)
        .save(basePath);

    // copy the metadata dir to a separate dir before update and then replace the proper table with this out of date version
    String metadataPath = basePath + "/.hoodie/metadata";
    String backupDir = tempDir.resolve("backup").toString();
    copy(fs, new Path(metadataPath), fs, new Path(backupDir), false, hadoopConf);

    Dataset<Row> updates = makeUpdateDf("001", 5).cache();
    updates.write().format("hudi").options(writeOptions)
        .option(DataSourceWriteOptions.OPERATION().key(), WriteOperationType.UPSERT.value())
        .option(HoodieMetadataConfig.RECORD_INDEX_ENABLE_PROP.key(), "true")
        .option(HoodieMetadataConfig.RECORD_INDEX_MIN_FILE_GROUP_COUNT_PROP.key(), "1")
        .option(HoodieMetadataConfig.RECORD_INDEX_MAX_FILE_GROUP_COUNT_PROP.key(), "1")
        .mode(SaveMode.Append)
        .save(basePath);

    // clear MDT and replace with old copy
    fs.delete(new Path(metadataPath), true);
    copy(fs, new Path(backupDir), fs, new Path(metadataPath), true, hadoopConf);

    // validate MDT is out of date
    HoodieMetadataTableValidator.Config config = new HoodieMetadataTableValidator.Config();
    config.basePath = "file:" + basePath;
    config.validateLatestFileSlices = true;
    config.validateAllFileGroups = true;
    config.ignoreFailed = true;
    HoodieMetadataTableValidator validator = new HoodieMetadataTableValidator(jsc, config);
    assertFalse(validator.run());
    assertTrue(validator.hasValidationFailure());
    assertFalse(validator.getThrowables().isEmpty());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testAdditionalPartitionsinMDT(boolean testFailureCase) throws IOException {
    Map<String, String> writeOptions = new HashMap<>();
    writeOptions.put(DataSourceWriteOptions.TABLE_NAME().key(), "test_table");
    writeOptions.put("hoodie.table.name", "test_table");
    writeOptions.put(DataSourceWriteOptions.TABLE_TYPE().key(), "MERGE_ON_READ");
    writeOptions.put(DataSourceWriteOptions.RECORDKEY_FIELD().key(), "_row_key");
    writeOptions.put(DataSourceWriteOptions.PRECOMBINE_FIELD().key(), "timestamp");
    writeOptions.put(DataSourceWriteOptions.PARTITIONPATH_FIELD().key(), "partition_path");

    // constructor of HoodieMetadataValidator instantiates HoodieTableMetaClient. hence creating an actual table. but rest of tests is mocked.
    Dataset<Row> inserts = makeInsertDf("000", 5).cache();
    inserts.write().format("hudi").options(writeOptions)
        .option(DataSourceWriteOptions.OPERATION().key(), WriteOperationType.BULK_INSERT.value())
        .mode(SaveMode.Overwrite)
        .save(basePath);

    String partition1 = "PARTITION1";
    String partition2 = "PARTITION2";
    String partition3 = "PARTITION3";

    HoodieMetadataTableValidator.Config config = new HoodieMetadataTableValidator.Config();
    config.basePath = basePath;
    config.validateLatestFileSlices = true;
    config.validateAllFileGroups = true;
    MockHoodieMetadataTableValidator validator = new MockHoodieMetadataTableValidator(jsc, config);
    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieWrapperFileSystem fs = mock(HoodieWrapperFileSystem.class);
    when(metaClient.getFs()).thenReturn(fs);
    when(fs.exists(new Path(basePath + "/" + partition1))).thenReturn(true);
    when(fs.exists(new Path(basePath + "/" + partition2))).thenReturn(true);
    when(fs.exists(new Path(basePath + "/" + partition3))).thenReturn(true);

    // mock list of partitions to return from MDT to have 1 additional partition compared to FS based listing.
    List<String> mdtPartitions = Arrays.asList(partition1, partition2, partition3);
    validator.setMetadataPartitionsToReturn(mdtPartitions);
    List<String> fsPartitions = Arrays.asList(partition1, partition2);
    validator.setFsPartitionsToReturn(fsPartitions);

    // mock completed timeline.
    HoodieTimeline commitsTimeline = mock(HoodieTimeline.class);
    HoodieTimeline completedTimeline = mock(HoodieTimeline.class);
    when(metaClient.getCommitsTimeline()).thenReturn(commitsTimeline);
    when(commitsTimeline.filterCompletedInstants()).thenReturn(completedTimeline);

    if (testFailureCase) {
      // 3rd partition which is additional in MDT should have creation time before last instant in timeline.
      String partition3CreationTime = HoodieActiveTimeline.createNewInstantTime();
      String lastIntantCreationTime = HoodieInstantTimeGenerator.createNewInstantTime(100);

      HoodieInstant lastInstant = new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, lastIntantCreationTime);
      when(completedTimeline.lastInstant()).thenReturn(Option.of(lastInstant));
      validator.setPartitionCreationTime(Option.of(partition3CreationTime));
      // validate that exception is thrown since MDT has one additional partition.
      assertThrows(HoodieValidationException.class, () -> {
        validator.validatePartitions(engineContext, basePath, metaClient);
      });
    } else {
      // 3rd partition creation time is > last completed instant
      HoodieInstant lastInstant = new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, HoodieActiveTimeline.createNewInstantTime());
      when(completedTimeline.lastInstant()).thenReturn(Option.of(lastInstant));
      validator.setPartitionCreationTime(Option.of(HoodieInstantTimeGenerator.createNewInstantTime(100)));

      // validate that all 3 partitions are returned
      assertEquals(mdtPartitions, validator.validatePartitions(engineContext, basePath, metaClient));
    }
  }

  @ParameterizedTest
  @MethodSource("lastNFileSlicesTestArgs")
  public void testAdditionalFilesinMetadata(Integer lastNFileSlices, boolean ignoreFailed) throws IOException {
    Map<String, String> writeOptions = new HashMap<>();
    writeOptions.put(DataSourceWriteOptions.TABLE_NAME().key(), "test_table");
    writeOptions.put("hoodie.table.name", "test_table");
    writeOptions.put(DataSourceWriteOptions.TABLE_TYPE().key(), "MERGE_ON_READ");
    writeOptions.put(DataSourceWriteOptions.RECORDKEY_FIELD().key(), "_row_key");
    writeOptions.put(DataSourceWriteOptions.PRECOMBINE_FIELD().key(), "timestamp");
    writeOptions.put(HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key(),"2");

    Dataset<Row> inserts = makeInsertDf("000", 10).cache();
    inserts.write().format("hudi").options(writeOptions)
        .mode(SaveMode.Overwrite)
        .save(basePath);

    // Perform updates to generate log files
    inserts.write().format("hudi").options(writeOptions)
        .mode(SaveMode.Append)
        .save(basePath);
    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setBasePath(basePath).setConf(engineContext.getHadoopConf().get()).build();

    // let's add a log file entry to the commit history and filesystem by directly modifying the commit so FS based listing and MDT based listing diverges.
    HoodieActiveTimeline timeline = metaClient.getActiveTimeline();
    HoodieInstant instantToOverwrite = timeline.getInstants().get(1);
    HoodieCommitMetadata commitMetadata = timeline.deserializeInstantContent(instantToOverwrite, HoodieCommitMetadata.class);
    HoodieWriteStat writeStatToCopy = commitMetadata.getPartitionToWriteStats().entrySet().stream().flatMap(entry -> entry.getValue().stream())
        .filter(writeStat -> FSUtils.isLogFile(writeStat.getPath())).findFirst().get();
    String newLogFilePath = writeStatToCopy.getPath() + "1";
    HoodieWriteStat writeStatCopy = SerializationUtils.deserialize(SerializationUtils.serialize(writeStatToCopy));
    writeStatCopy.setPath(newLogFilePath);
    commitMetadata.addWriteStat(writeStatCopy.getPartitionPath(), writeStatCopy);
    FileSystem fs = FSUtils.getFs(newLogFilePath, new Configuration(false));
    fs.copyFromLocalFile(new Path(basePath, writeStatToCopy.getPath()), new Path(basePath, newLogFilePath));
    // remove the existing instant and rewrite with the new metadata
    assertTrue(fs.delete(new Path(basePath, String.format(".hoodie/%s", instantToOverwrite.getFileName()))));
    timeline.saveAsComplete(new HoodieInstant(HoodieInstant.State.INFLIGHT, instantToOverwrite.getAction(), instantToOverwrite.getTimestamp()), Option.of(commitMetadata));

    for (int i = 0; i < 5; i++) {
      inserts.write().format("hudi").options(writeOptions)
          .mode(SaveMode.Append)
          .save(basePath);
    }

    HoodieMetadataTableValidator.Config config = new HoodieMetadataTableValidator.Config();
    config.basePath = "file:" + basePath;
    config.validateLatestFileSlices = true;
    config.validateAllFileGroups = true;
    config.ignoreFailed = true;

    HoodieMetadataTableValidator.Config finalConfig = config;
    HoodieMetadataTableValidator localValidator = new HoodieMetadataTableValidator(jsc, finalConfig);
    localValidator.run();
    assertTrue(localValidator.hasValidationFailure());
    assertTrue(localValidator.getThrowables().get(0) instanceof HoodieValidationException);

    // lets set lastN file slices to 2 and so validation should succeed. (bcoz, there will be mis-match only on first file slice)
    config = new HoodieMetadataTableValidator.Config();
    config.basePath = "file:" + basePath;
    config.validateLatestFileSlices = true;
    config.validateAllFileGroups = true;
    if (lastNFileSlices != -1) {
      config.validateLastNFileSlices = lastNFileSlices;
    }
    config.ignoreFailed = true;
    HoodieMetadataTableValidator validator = new HoodieMetadataTableValidator(jsc, config);
    validator.run();
    if (lastNFileSlices != -1 && lastNFileSlices < 4) {
      assertFalse(validator.hasValidationFailure());
    } else {
      assertTrue(validator.hasValidationFailure());
      assertTrue(validator.getThrowables().get(0) instanceof HoodieValidationException);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testAdditionalPartitionsinMdtEndToEnd(boolean ignoreFailed) throws IOException {
    Map<String, String> writeOptions = new HashMap<>();
    writeOptions.put(DataSourceWriteOptions.TABLE_NAME().key(), "test_table");
    writeOptions.put("hoodie.table.name", "test_table");
    writeOptions.put(DataSourceWriteOptions.TABLE_TYPE().key(), "MERGE_ON_READ");
    writeOptions.put(DataSourceWriteOptions.RECORDKEY_FIELD().key(), "_row_key");
    writeOptions.put(DataSourceWriteOptions.PRECOMBINE_FIELD().key(), "timestamp");
    writeOptions.put(DataSourceWriteOptions.PARTITIONPATH_FIELD().key(),"partition_path");
    writeOptions.put(HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key(), "2");

    Dataset<Row> inserts = makeInsertDf("000", 100).cache();
    inserts.write().format("hudi").options(writeOptions)
        .mode(SaveMode.Overwrite)
        .save(basePath);

    for (int i = 0; i < 6; i++) {
      inserts.write().format("hudi").options(writeOptions)
          .mode(SaveMode.Append)
          .save(basePath);
    }

    HoodieMetadataTableValidator.Config config = new HoodieMetadataTableValidator.Config();
    config.basePath = "file:" + basePath;
    config.validateLatestFileSlices = true;
    config.validateAllFileGroups = true;
    config.ignoreFailed = ignoreFailed;
    HoodieMetadataTableValidator validator = new HoodieMetadataTableValidator(jsc, config);
    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setBasePath(basePath).setConf(engineContext.getHadoopConf().get()).build();

    validator.run();
    assertFalse(validator.hasValidationFailure());

    // let's delete one of the partitions, so validation fails
    metaClient.getFs().delete(new Path(basePath + "/" + HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH));

    config = new HoodieMetadataTableValidator.Config();
    config.basePath = "file:" + basePath;
    config.validateLatestFileSlices = true;
    config.ignoreFailed = ignoreFailed;

    HoodieMetadataTableValidator localValidator = new HoodieMetadataTableValidator(jsc, config);
    if (ignoreFailed) {
      localValidator.run();
      assertTrue(localValidator.hasValidationFailure());
      assertTrue(localValidator.getThrowables().get(0) instanceof HoodieValidationException);
    } else {
      assertThrows(HoodieValidationException.class, localValidator::run);
    }
  }

  @Test
  void testHasCommittedLogFiles() throws IOException, InterruptedException {
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    when(metaClient.getBasePathV2()).thenReturn(new Path(tempDir.toString()));
    HoodieMetadataTableValidator.Config config = new HoodieMetadataTableValidator.Config();
    config.basePath = basePath;
    config.validateLatestFileSlices = true;
    config.validateAllFileGroups = true;
    MockHoodieMetadataTableValidator validator = new MockHoodieMetadataTableValidator(jsc, config);
    Map<String, Set<String>> committedFilesMap = new HashMap<>();
    String baseInstantTime = HoodieActiveTimeline.createNewInstantTime();
    String logInstantTime = HoodieActiveTimeline.createNewInstantTime();
    String newInstantTime = HoodieActiveTimeline.createNewInstantTime();

    // Empty log file set
    assertEquals(Pair.of(false, ""), validator.hasCommittedLogFiles(
        fs, Collections.emptySet(), metaClient, committedFilesMap));
    // Empty log file
    HoodieLogFile logFile = new HoodieLogFile(new Path(
        tempDir.toString(),
        FSUtils.makeLogFileName(
            UUID.randomUUID().toString(), HoodieLogFile.DELTA_EXTENSION, logInstantTime, 1, "1-0-1")));
    fs.create(logFile.getPath()).close();
    prepareTimelineAndValidate(metaClient, validator, Collections.emptyList(),
        logFile, committedFilesMap, Pair.of(false, ""));

    // Log file with command log block
    logFile = prepareLogFile(
        UUID.randomUUID().toString(), baseInstantTime, logInstantTime, false);
    prepareTimelineAndValidate(metaClient, validator, Collections.emptyList(),
        logFile, committedFilesMap, Pair.of(false, ""));

    // Log file with data log block
    logFile = prepareLogFile(
        UUID.randomUUID().toString(), baseInstantTime, logInstantTime, true);
    // Log block created by completed delta commit in active timeline
    committedFilesMap.put(logInstantTime, Collections.emptySet());
    prepareTimelineAndValidate(metaClient, validator,
        Collections.singletonList(new HoodieInstant(
            HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, logInstantTime)),
        logFile, committedFilesMap, Pair.of(false, ""));

    // Log block created by completed delta commit but not included in the commit metadata
    committedFilesMap.put(logInstantTime,
        new HashSet<>(Collections.singletonList(logFile.getPath().getName())));
    prepareTimelineAndValidate(metaClient, validator,
        Collections.singletonList(new HoodieInstant(
            HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, logInstantTime)),
        logFile, committedFilesMap,
        Pair.of(true,
            String.format("Log file is committed in an instant in active timeline: instantTime=%s %s",
                logInstantTime, logFile.getPath().toString())));
    committedFilesMap.clear();

    // Log block created by completed delta commit before active timeline starts
    prepareTimelineAndValidate(metaClient, validator,
        Collections.singletonList(new HoodieInstant(
            HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, newInstantTime)),
        logFile, committedFilesMap,
        Pair.of(true,
            String.format("Log file is committed in an instant in archived timeline: instantTime=%s %s",
                logInstantTime, logFile.getPath().toString())));

    // Log block created by inflight delta commit in active timeline
    prepareTimelineAndValidate(metaClient, validator,
        Collections.singletonList(new HoodieInstant(
            HoodieInstant.State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, logInstantTime)),
        logFile, committedFilesMap, Pair.of(false, ""));

    // Log block created by a delta commit not in active timeline
    prepareTimelineAndValidate(metaClient, validator,
        Collections.singletonList(new HoodieInstant(
            HoodieInstant.State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, baseInstantTime)),
        logFile, committedFilesMap, Pair.of(false, ""));
  }

  private void prepareTimelineAndValidate(HoodieTableMetaClient metaClient,
                                          MockHoodieMetadataTableValidator validator,
                                          List<HoodieInstant> instantList,
                                          HoodieLogFile logFile,
                                          Map<String, Set<String>> committedFilesMap,
                                          Pair<Boolean, String> expected) {
    when(metaClient.getCommitsTimeline()).thenReturn(new HoodieDefaultTimeline(
        instantList.stream(), null));
    assertEquals(expected,
        validator.hasCommittedLogFiles(
            fs, new HashSet<>(Collections.singletonList(logFile.getPath().toString())),
            metaClient, committedFilesMap));
  }

  private HoodieLogFile prepareLogFile(String fileId,
                                       String baseInstantTime,
                                       String instantTime,
                                       boolean writeDataBlock) throws IOException, InterruptedException {
    try (HoodieLogFormat.Writer writer = HoodieLogFormat.newWriterBuilder()
        .onParentPath(new Path(tempDir.toString()))
        .withFileExtension(HoodieLogFile.DELTA_EXTENSION)
        .withFileId(fileId).overBaseCommit(baseInstantTime)
        .withFs(fs)
        .withSizeThreshold(Long.MAX_VALUE).build()) {
      Map<HoodieLogBlock.HeaderMetadataType, String> header =
          new EnumMap<>(HoodieLogBlock.HeaderMetadataType.class);
      if (writeDataBlock) {
        header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, instantTime);
        header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, getSimpleSchema().toString());
        writer.appendBlock(new HoodieAvroDataBlock(
            Collections.emptyList(), header, HoodieRecord.RECORD_KEY_METADATA_FIELD));
      } else {
        header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, instantTime);
        header.put(HoodieLogBlock.HeaderMetadataType.TARGET_INSTANT_TIME, baseInstantTime);
        header.put(HoodieLogBlock.HeaderMetadataType.COMMAND_BLOCK_TYPE,
            String.valueOf(HoodieCommandBlock.HoodieCommandBlockTypeEnum.ROLLBACK_BLOCK.ordinal()));
        writer.appendBlock(new HoodieCommandBlock(header));
      }
      return writer.getLogFile();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testValidate(boolean oversizeList) {
    HoodieMetadataTableValidator.Config config = new HoodieMetadataTableValidator.Config();
    config.basePath = basePath;
    config.validateLatestFileSlices = true;
    config.validateAllFileGroups = true;
    MockHoodieMetadataTableValidator validator = new MockHoodieMetadataTableValidator(jsc, config);
    int listSize = oversizeList ? 5000 : 100;
    String partition = "partition10";
    String label = "metadata item";

    // Base file list
    Pair<List<HoodieBaseFile>, List<HoodieBaseFile>> filelistPair = generateTwoEqualBaseFileList(listSize);
    runValidateAndVerify(
        validator, oversizeList, partition, label, filelistPair.getLeft(), filelistPair.getRight(),
        generateRandomBaseFile().getLeft());

    // Column stats list
    Pair<List<HoodieColumnRangeMetadata<Comparable>>, List<HoodieColumnRangeMetadata<Comparable>>> statsListPair =
        generateTwoEqualColumnStatsList(listSize);
    runValidateAndVerify(
        validator, oversizeList, partition, label, statsListPair.getLeft(), statsListPair.getRight(),
        generateRandomColumnStats().getLeft());
  }

  private <T> void runValidateAndVerify(HoodieMetadataTableValidator validator,
                                        boolean oversizeList, String partition, String label,
                                        List<T> listMdt, List<T> listFs, T newItem) {
    assertEquals(
        oversizeList,
        toStringWithThreshold(listMdt, Integer.MAX_VALUE).length() > logDetailMaxLength);
    assertEquals(
        oversizeList,
        toStringWithThreshold(listFs, Integer.MAX_VALUE).length() > logDetailMaxLength);
    // Equal case
    assertDoesNotThrow(() ->
        validator.validate(listMdt, listFs, partition, label));
    // Size mismatch
    listFs.add(newItem);
    Exception exception = assertThrows(
        HoodieValidationException.class,
        () -> validator.validate(listMdt, listFs, partition, label));
    assertEquals(
        String.format(
            "Validation of %s for partition %s failed for table: %s. "
                + "Number of %s based on the file system does not match that based on "
                + "the metadata table. File system-based listing (%s): %s; "
                + "MDT-based listing (%s): %s.",
            label, partition, basePath, label, listFs.size(),
            toStringWithThreshold(listFs, logDetailMaxLength),
            listMdt.size(), toStringWithThreshold(listMdt, logDetailMaxLength)),
        exception.getMessage());
    listFs.remove(listFs.size() - 1);
    // Item mismatch
    int i = 35;
    listFs.set(i, newItem);
    exception = assertThrows(
        HoodieValidationException.class,
        () -> validator.validate(listMdt, listFs, partition, label));
    assertEquals(
        String.format(
            "Validation of %s for partition %s failed for table: %s. "
                + "%s mismatch. File slice from file system-based listing: %s; "
                + "File slice from MDT-based listing: %s.",
            label, partition, basePath, label, listFs.get(i), listMdt.get(i)),
        exception.getMessage());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testValidateFileSlices(boolean oversizeList) {
    HoodieMetadataTableValidator.Config config = new HoodieMetadataTableValidator.Config();
    config.basePath = basePath;
    config.validateLatestFileSlices = true;
    config.validateAllFileGroups = true;
    MockHoodieMetadataTableValidator validator = new MockHoodieMetadataTableValidator(jsc, config);
    int listSize = oversizeList ? 500 : 50;
    String partition = "partition10";
    String label = "metadata item";

    // Base file list
    Pair<List<FileSlice>, List<FileSlice>> filelistPair = generateTwoEqualFileSliceList(listSize);
    List<FileSlice> listMdt = filelistPair.getLeft();
    List<FileSlice> listFs = filelistPair.getRight();
    // Equal case
    assertDoesNotThrow(() ->
        validator.validateFileSlices(listMdt, listFs, partition, metaClient, label));
    // Size mismatch
    listFs.add(generateRandomFileSlice().getLeft());
    assertEquals(
        oversizeList,
        toStringWithThreshold(listMdt, Integer.MAX_VALUE).length() > logDetailMaxLength);
    assertEquals(
        oversizeList,
        toStringWithThreshold(listFs, Integer.MAX_VALUE).length() > logDetailMaxLength);
    Exception exception = assertThrows(
        HoodieValidationException.class,
        () -> validator.validateFileSlices(listMdt, listFs, partition, metaClient, label));
    assertEquals(
        String.format(
            "Validation of %s for partition %s failed for table: %s. "
                + "Number of file slices based on the file system does not match that based on the "
                + "metadata table. File system-based listing (%s file slices): %s; "
                + "MDT-based listing (%s file slices): %s.",
            label, partition, basePath, listFs.size(),
            toStringWithThreshold(listFs, logDetailMaxLength),
            listMdt.size(), toStringWithThreshold(listMdt, logDetailMaxLength)),
        exception.getMessage());
    listFs.remove(listFs.size() - 1);
    // Item mismatch
    int i = 35;
    // Instant time mismatch
    FileSlice originalFileSlice = listMdt.get(i);
    FileSlice mismatchFileSlice = new FileSlice(
        originalFileSlice.getFileGroupId(),
        HoodieActiveTimeline.createNewInstantTime(),
        originalFileSlice.getBaseFile().get(),
        originalFileSlice.getLogFiles().collect(Collectors.toList()));
    listMdt.set(i, mismatchFileSlice);
    exception = assertThrows(
        HoodieValidationException.class,
        () -> validator.validateFileSlices(listMdt, listFs, partition, metaClient, label));
    assertEquals(
        String.format(
            "Validation of %s for partition %s failed for table: %s. "
                + "File group ID (missing a file group in MDT) "
                + "or base instant time mismatches. File slice from file system-based listing: %s; "
                + "File slice from MDT-based listing: %s.",
            label, partition, basePath, listFs.get(i), listMdt.get(i)),
        exception.getMessage());
    // base file mismatch
    mismatchFileSlice = new FileSlice(
        originalFileSlice.getFileGroupId(),
        originalFileSlice.getBaseInstantTime(),
        generateRandomBaseFile().getLeft(),
        originalFileSlice.getLogFiles().collect(Collectors.toList()));
    listMdt.set(i, mismatchFileSlice);
    exception = assertThrows(
        HoodieValidationException.class,
        () -> validator.validateFileSlices(listMdt, listFs, partition, metaClient, label));
    assertEquals(
        String.format(
            "Validation of %s for partition %s failed for table: %s. "
                + "Base files mismatch. "
                + "File slice from file system-based listing: %s; "
                + "File slice from MDT-based listing: %s.",
            label, partition, basePath, listFs.get(i), listMdt.get(i)),
        exception.getMessage());
  }

  Pair<List<HoodieBaseFile>, List<HoodieBaseFile>> generateTwoEqualBaseFileList(int size) {
    List<HoodieBaseFile> list1 = new ArrayList<>();
    List<HoodieBaseFile> list2 = new ArrayList<>();
    IntStream.range(0, size).forEach(i -> {
      Pair<HoodieBaseFile, HoodieBaseFile> pair = generateRandomBaseFile();
      list1.add(pair.getLeft());
      list2.add(pair.getRight());
    });
    return Pair.of(
        list1.stream().sorted(new HoodieMetadataTableValidator.HoodieBaseFileComparator())
            .collect(Collectors.toList()),
        list2.stream().sorted(new HoodieMetadataTableValidator.HoodieBaseFileComparator())
            .collect(Collectors.toList()));
  }

  Pair<List<HoodieColumnRangeMetadata<Comparable>>,
      List<HoodieColumnRangeMetadata<Comparable>>> generateTwoEqualColumnStatsList(int size) {
    List<HoodieColumnRangeMetadata<Comparable>> list1 = new ArrayList<>();
    List<HoodieColumnRangeMetadata<Comparable>> list2 = new ArrayList<>();
    IntStream.range(0, size).forEach(i -> {
      Pair<HoodieColumnRangeMetadata, HoodieColumnRangeMetadata> pair = generateRandomColumnStats();
      list1.add(pair.getLeft());
      list2.add(pair.getRight());
    });
    return Pair.of(
        list1.stream().sorted(new HoodieMetadataTableValidator.HoodieColumnRangeMetadataComparator())
            .collect(Collectors.toList()),
        list2.stream().sorted(new HoodieMetadataTableValidator.HoodieColumnRangeMetadataComparator())
            .collect(Collectors.toList()));
  }

  Pair<List<FileSlice>, List<FileSlice>> generateTwoEqualFileSliceList(int size) {
    List<FileSlice> list1 = new ArrayList<>();
    List<FileSlice> list2 = new ArrayList<>();
    IntStream.range(0, size).forEach(i -> {
      Pair<FileSlice, FileSlice> pair = generateRandomFileSlice();
      list1.add(pair.getLeft());
      list2.add(pair.getRight());
    });
    return Pair.of(
        list1.stream().sorted(new HoodieMetadataTableValidator.FileSliceComparator())
            .collect(Collectors.toList()),
        list2.stream().sorted(new HoodieMetadataTableValidator.FileSliceComparator())
            .collect(Collectors.toList()));
  }

  private Pair<HoodieBaseFile, HoodieBaseFile> generateRandomBaseFile() {
    String filePath = "/dummy/base/" + FSUtils.makeBaseFileName(
        "001", "1-0-1", UUID.randomUUID().toString(), HoodieFileFormat.PARQUET.getFileExtension());
    return Pair.of(new HoodieBaseFile(filePath), new HoodieBaseFile(new String(filePath)));
  }

  private Pair<HoodieColumnRangeMetadata, HoodieColumnRangeMetadata> generateRandomColumnStats() {
    long count = RANDOM.nextLong();
    long size = RANDOM.nextLong();
    switch (RANDOM.nextInt(3)) {
      case 0:
        HoodieColumnRangeMetadata<Integer> intMetadata = HoodieColumnRangeMetadata.create(
            generateRandomString(30), generateRandomString(5),
            RANDOM.nextInt() % 30, RANDOM.nextInt() % 1000_000_000 + 30,
            count / 3L, count, size, size / 8L);
        return Pair.of(intMetadata,
            HoodieColumnRangeMetadata.create(
                new String(intMetadata.getFilePath()), new String(intMetadata.getColumnName()),
                (int) intMetadata.getMinValue(), (int) intMetadata.getMaxValue(),
                count / 3L, count, size, size / 8L));
      case 1:
        HoodieColumnRangeMetadata<Long> longMetadata = HoodieColumnRangeMetadata.create(
            generateRandomString(30), generateRandomString(5),
            RANDOM.nextLong() % 30L, RANDOM.nextInt() % 1000_000_000_000_000L + 30L,
            count / 3L, count, size, size / 8L);
        return Pair.of(longMetadata,
            HoodieColumnRangeMetadata.create(
                new String(longMetadata.getFilePath()), new String(longMetadata.getColumnName()),
                (long) longMetadata.getMinValue(), (long) longMetadata.getMaxValue(),
                count / 3L, count, size, size / 8L));
      default:
        String stringValue1 = generateRandomString(20);
        String stringValue2 = generateRandomString(20);
        HoodieColumnRangeMetadata<String> stringMetadata = HoodieColumnRangeMetadata.create(
            generateRandomString(30), generateRandomString(5),
            stringValue1, stringValue2,
            count / 3L, count, size, size / 8L);
        return Pair.of(stringMetadata,
            HoodieColumnRangeMetadata.create(
                new String(stringMetadata.getFilePath()), new String(stringMetadata.getColumnName()),
                new String(stringValue1), new String(stringValue2),
                count / 3L, count, size, size / 8L));
    }
  }

  private Pair<FileSlice, FileSlice> generateRandomFileSlice() {
    String partition = "partition";
    String fileId = UUID.randomUUID().toString();
    String baseInstantTime = HoodieActiveTimeline.createNewInstantTime();
    Pair<HoodieBaseFile, HoodieBaseFile> baseFilePair = generateRandomBaseFile();
    List<HoodieLogFile> logFileList = new ArrayList<>();
    logFileList.add(generateRandomLogFile(fileId, HoodieActiveTimeline.createNewInstantTime()));
    logFileList.add(generateRandomLogFile(fileId, HoodieActiveTimeline.createNewInstantTime()));
    FileSlice fileSlice = new FileSlice(
        new HoodieFileGroupId(partition, fileId), baseInstantTime,
        baseFilePair.getLeft(), logFileList);
    return Pair.of(fileSlice,
        new FileSlice(new HoodieFileGroupId(partition, fileId),
            new String(baseInstantTime), baseFilePair.getRight(),
            logFileList.stream().map(HoodieLogFile::new).collect(Collectors.toList())));
  }

  private HoodieLogFile generateRandomLogFile(String fileId, String instantTime) {
    return new HoodieLogFile("/dummy/base/" + FSUtils.makeLogFileName(
        fileId, HoodieLogFile.DELTA_EXTENSION, instantTime, 1, "1-0-1"));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testRecordIndexMismatch(boolean ignoreFailed) throws IOException {
    Map<String, String> writeOptions = new HashMap<>();
    writeOptions.put(DataSourceWriteOptions.TABLE_NAME().key(), "test_table");
    writeOptions.put("hoodie.table.name", "test_table");
    writeOptions.put(DataSourceWriteOptions.TABLE_TYPE().key(), "COPY_ON_WRITE");
    writeOptions.put(DataSourceWriteOptions.RECORDKEY_FIELD().key(), "_row_key");
    writeOptions.put(DataSourceWriteOptions.PRECOMBINE_FIELD().key(), "timestamp");
    writeOptions.put(DataSourceWriteOptions.OPERATION().key(),"bulk_insert");
    writeOptions.put(HoodieMetadataConfig.RECORD_INDEX_ENABLE_PROP.key(), "true");

    Dataset<Row> inserts = makeInsertDf("000", 50).cache();
    inserts.write().format("hudi").options(writeOptions)
        .mode(SaveMode.Overwrite)
        .save(basePath);

    for (int i = 0; i < 6; i++) {
      makeInsertDf("000", (i + 1) * 100).write().format("hudi").options(writeOptions)
          .mode(SaveMode.Append)
          .save(basePath);
    }

    HoodieMetadataTableValidator.Config config = new HoodieMetadataTableValidator.Config();
    config.basePath = "file:" + basePath;
    config.validateLatestFileSlices = true;
    config.ignoreFailed = ignoreFailed;
    HoodieMetadataTableValidator validator = new HoodieMetadataTableValidator(jsc, config);
    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setBasePath(basePath).setConf(engineContext.getHadoopConf().get()).build();

    validator.run();
    assertFalse(validator.hasValidationFailure());

    // lets override one of the latest base file w/ another. so that file slice validation succeeds, but record index comparison fails.
    HoodieTableFileSystemView fsView = new HoodieTableFileSystemView(
        new FileSystemBackedTableMetadata(engineContext, metaClient.getTableConfig(), metaClient.getSerializableHadoopConf(), metaClient.getBasePathV2().toString(), false),
        metaClient, metaClient.getActiveTimeline().filterCompletedAndCompactionInstants(), false);
    List<HoodieBaseFile> allBaseFiles = fsView.getLatestBaseFiles(StringUtils.EMPTY_STRING).collect(Collectors.toList());
    metaClient.getFs().copyFromLocalFile(allBaseFiles.get(0).getHadoopPath(), allBaseFiles.get(1).getHadoopPath());

    config = new HoodieMetadataTableValidator.Config();
    config.basePath = "file:" + basePath;
    config.validateLatestFileSlices = true;
    config.ignoreFailed = ignoreFailed;

    HoodieMetadataTableValidator localValidator = new HoodieMetadataTableValidator(jsc, config);
    if (ignoreFailed) {
      localValidator.run();
      assertTrue(localValidator.hasValidationFailure());
      assertTrue(localValidator.getThrowables().get(0) instanceof HoodieValidationException);
    } else {
      assertThrows(HoodieValidationException.class, localValidator::run);
    }
  }

  class MockHoodieMetadataTableValidator extends HoodieMetadataTableValidator {

    private List<String> metadataPartitionsToReturn;
    private List<String> fsPartitionsToReturn;
    private Option<String> partitionCreationTime;

    public MockHoodieMetadataTableValidator(JavaSparkContext jsc, Config cfg) {
      super(jsc, cfg);
    }

    void setMetadataPartitionsToReturn(List<String> metadataPartitionsToReturn) {
      this.metadataPartitionsToReturn = metadataPartitionsToReturn;
    }

    void setFsPartitionsToReturn(List<String> fsPartitionsToReturn) {
      this.fsPartitionsToReturn = fsPartitionsToReturn;
    }

    void setPartitionCreationTime(Option<String> partitionCreationTime) {
      this.partitionCreationTime = partitionCreationTime;
    }

    @Override
    List<String> getPartitionsFromFileSystem(HoodieEngineContext engineContext, String basePath, boolean assumeDatePartitioning,
                                             FileSystem fs, HoodieTimeline completedTimeline) {
      return fsPartitionsToReturn;
    }

    @Override
    List<String> getPartitionsFromMDT(HoodieEngineContext engineContext, String basePath, boolean assumeDatePartitioning) {
      return metadataPartitionsToReturn;
    }

    @Override
    Option<String> getPartitionCreationInstant(FileSystem fs, String basePath, String partition) {
      return this.partitionCreationTime;
    }
  }

  @Test
  public void testRliValidationFalsePositiveCase() throws IOException {
    Map<String, String> writeOptions = new HashMap<>();
    writeOptions.put(DataSourceWriteOptions.TABLE_NAME().key(), "test_table");
    writeOptions.put("hoodie.table.name", "test_table");
    writeOptions.put(DataSourceWriteOptions.TABLE_TYPE().key(), "MERGE_ON_READ");
    writeOptions.put(DataSourceWriteOptions.RECORDKEY_FIELD().key(), "_row_key");
    writeOptions.put(DataSourceWriteOptions.PRECOMBINE_FIELD().key(), "timestamp");
    writeOptions.put(DataSourceWriteOptions.PARTITIONPATH_FIELD().key(), "partition_path");

    Dataset<Row> inserts = makeInsertDf("000", 5).cache();
    inserts.write().format("hudi").options(writeOptions)
        .option(DataSourceWriteOptions.OPERATION().key(), WriteOperationType.BULK_INSERT.value())
        .option(HoodieMetadataConfig.RECORD_INDEX_ENABLE_PROP.key(), "true")
        .option(HoodieMetadataConfig.RECORD_INDEX_MIN_FILE_GROUP_COUNT_PROP.key(), "1")
        .option(HoodieMetadataConfig.RECORD_INDEX_MAX_FILE_GROUP_COUNT_PROP.key(), "1")
        .mode(SaveMode.Overwrite)
        .save(basePath);
    Dataset<Row> updates = makeUpdateDf("001", 5).cache();
    updates.write().format("hudi").options(writeOptions)
        .option(DataSourceWriteOptions.OPERATION().key(), WriteOperationType.UPSERT.value())
        .option(HoodieMetadataConfig.RECORD_INDEX_ENABLE_PROP.key(), "true")
        .option(HoodieMetadataConfig.RECORD_INDEX_MIN_FILE_GROUP_COUNT_PROP.key(), "1")
        .option(HoodieMetadataConfig.RECORD_INDEX_MAX_FILE_GROUP_COUNT_PROP.key(), "1")
        .mode(SaveMode.Append)
        .save(basePath);

    Dataset<Row> inserts2 = makeInsertDf("002", 5).cache();
    inserts2.write().format("hudi").options(writeOptions)
        .option(DataSourceWriteOptions.OPERATION().key(), WriteOperationType.BULK_INSERT.value())
        .option(HoodieMetadataConfig.RECORD_INDEX_ENABLE_PROP.key(), "true")
        .option(HoodieMetadataConfig.RECORD_INDEX_MIN_FILE_GROUP_COUNT_PROP.key(), "1")
        .option(HoodieMetadataConfig.RECORD_INDEX_MAX_FILE_GROUP_COUNT_PROP.key(), "1")
        .mode(SaveMode.Append)
        .save(basePath);

    // validate MDT
    HoodieMetadataTableValidator.Config config = new HoodieMetadataTableValidator.Config();
    config.basePath = "file://" + basePath;
    config.validateLatestFileSlices = true;
    config.validateAllFileGroups = true;

    // lets ensure we have a pending commit when FS based polling is done. and the commit completes when MDT is polled.
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setBasePath(basePath).setConf(jsc.hadoopConfiguration()).build();
    // moving out the completed commit meta file to a temp location
    HoodieInstant lastInstant = metaClient.getActiveTimeline().filterCompletedInstants().lastInstant().get();
    String latestCompletedCommitMetaFile = basePath + "/.hoodie/" + lastInstant.getFileName();
    String tempDir = getTempLocation();
    String destFilePath = tempDir + "/" + lastInstant.getFileName();
    FileUtil.move(latestCompletedCommitMetaFile, destFilePath);

    MockHoodieMetadataTableValidatorForRli validator = new MockHoodieMetadataTableValidatorForRli(jsc, config);
    validator.setOriginalFilePath(latestCompletedCommitMetaFile);
    validator.setDestFilePath(destFilePath);
    assertTrue(validator.run());
    assertFalse(validator.hasValidationFailure());
    assertTrue(validator.getThrowables().isEmpty());
  }

  /**
   * Class to assist with testing a false positive case with RLI validation.
   */
  static class MockHoodieMetadataTableValidatorForRli extends HoodieMetadataTableValidator {

    private String destFilePath;
    private String originalFilePath;

    public MockHoodieMetadataTableValidatorForRli(JavaSparkContext jsc, Config cfg) {
      super(jsc, cfg);
    }

    @Override
    JavaPairRDD<String, Pair<String, String>> getRecordLocationsFromRLI(HoodieSparkEngineContext sparkEngineContext,
                                                                        String basePath,
                                                                        String latestCompletedCommit) {
      // move the completed file back to ".hoodie" to simuate the false positive case.
      try {
        FileUtil.move(destFilePath, originalFilePath);
        return super.getRecordLocationsFromRLI(sparkEngineContext, basePath, latestCompletedCommit);
      } catch (IOException e) {
        throw new HoodieException("Move should not have failed");
      }
    }

    public void setDestFilePath(String destFilePath) {
      this.destFilePath = destFilePath;
    }

    public void setOriginalFilePath(String originalFilePath) {
      this.originalFilePath = originalFilePath;
    }
  }

  private String getTempLocation() {
    try {
      String folderName = "temp_location";
      java.nio.file.Path tempPath = tempDir.resolve(folderName);
      java.nio.file.Files.createDirectories(tempPath);
      return tempPath.toAbsolutePath().toString();
    } catch (IOException ioe) {
      throw new HoodieIOException(ioe.getMessage(), ioe);
    }
  }

  protected Dataset<Row> makeInsertDf(String instantTime, Integer n) {
    List<String> records = dataGen.generateInserts(instantTime, n).stream()
        .map(r -> recordToString(r).get()).collect(Collectors.toList());
    JavaRDD<String> rdd = jsc.parallelize(records);
    return sparkSession.read().json(rdd);
  }

  protected Dataset<Row> makeUpdateDf(String instantTime, Integer n) {
    try {
      List<String> records = dataGen.generateUpdates(instantTime, n).stream()
          .map(r -> recordToString(r).get()).collect(Collectors.toList());
      JavaRDD<String> rdd = jsc.parallelize(records);
      return sparkSession.read().json(rdd);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  void testLogDetailMaxLength() {
    Map<String, String> writeOptions = new HashMap<>();
    writeOptions.put(DataSourceWriteOptions.TABLE_NAME().key(), "test_table");
    writeOptions.put("hoodie.table.name", "test_table");
    writeOptions.put(DataSourceWriteOptions.TABLE_TYPE().key(), "MERGE_ON_READ");
    writeOptions.put(DataSourceWriteOptions.RECORDKEY_FIELD().key(), "_row_key");
    writeOptions.put(DataSourceWriteOptions.PRECOMBINE_FIELD().key(), "timestamp");
    writeOptions.put(DataSourceWriteOptions.PARTITIONPATH_FIELD().key(), "partition_path");

    // Create a large dataset to generate long validation messages
    Dataset<Row> inserts = makeInsertDf("000", 1000).cache();
    inserts.write().format("hudi").options(writeOptions)
        .option(DataSourceWriteOptions.OPERATION().key(), WriteOperationType.BULK_INSERT.value())
        .mode(SaveMode.Overwrite)
        .save(basePath);

    // Test with default max length
    HoodieMetadataTableValidator.Config config = new HoodieMetadataTableValidator.Config();
    config.basePath = "file:" + basePath;
    config.validateLatestFileSlices = true;
    config.validateAllFileGroups = true;
    MockHoodieMetadataTableValidator validator = new MockHoodieMetadataTableValidator(jsc, config);

    // Generate two unequal lists to trigger validation error
    Pair<List<FileSlice>, List<FileSlice>> filelistPair = generateTwoEqualFileSliceList(500);
    List<FileSlice> listMdt = filelistPair.getLeft();
    List<FileSlice> listFs = new ArrayList<>(filelistPair.getRight());
    listFs.add(generateRandomFileSlice().getLeft());

    // Verify default behavior (100,000 chars)
    MockHoodieMetadataTableValidator finalValidator = validator;
    Exception exception = assertThrows(
        HoodieValidationException.class,
        () -> finalValidator.validateFileSlices(listMdt, listFs, "partition", metaClient, "test"));
    // The message include 3 parts: Truncated file slice list of MDT, truncated file slice list of File system, other exception message strings.
    assertTrue(exception.getMessage().length() <= 100_000 * 2 + 1000);

    // Test with custom small max length
    config.logDetailMaxLength = 1000;
    validator = new MockHoodieMetadataTableValidator(jsc, config);
    MockHoodieMetadataTableValidator finalValidator1 = validator;
    exception = assertThrows(
        HoodieValidationException.class,
        () -> finalValidator1.validateFileSlices(listMdt, listFs, "partition", metaClient, "test"));
    // The message include 3 parts: Truncated file slice list of MDT, truncated file slice list of File system, other exception message strings.
    assertTrue(exception.getMessage().length() <= 1000 * 2 + 1000);

    // Test with custom large max length
    config.logDetailMaxLength = 200_000;
    validator = new MockHoodieMetadataTableValidator(jsc, config);
    MockHoodieMetadataTableValidator finalValidator2 = validator;
    exception = assertThrows(
        HoodieValidationException.class,
        () -> finalValidator2.validateFileSlices(listMdt, listFs, "partition", metaClient, "test"));
    // The message include 3 parts: Truncated file slice list of MDT, truncated file slice list of File system, other exception message strings.
    assertTrue(exception.getMessage().length() <= 200_000 * 2 + 1000);
  }

  @Test
  void testValidatePartitionsTruncation() throws IOException {
    // Setup mock objects
    HoodieMetadataTableValidator.Config config = new HoodieMetadataTableValidator.Config();
    config.basePath = basePath;
    config.logDetailMaxLength = 100; // Small length to force truncation
    
    MockHoodieMetadataTableValidator validator = new MockHoodieMetadataTableValidator(jsc, config);
    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieWrapperFileSystem fs = mock(HoodieWrapperFileSystem.class);
    
    // Generate long partition lists that will exceed the truncation threshold
    List<String> mdtPartitions = new ArrayList<>();
    List<String> fsPartitions = new ArrayList<>();
    for (int i = 0; i < 20; i++) {
      mdtPartitions.add("partition_" + generateRandomString(20));
    }
    for (int i = 0; i < 15; i++) {
      fsPartitions.add("partition_" + generateRandomString(20));
    }
    
    // Setup mocks
    when(metaClient.getFs()).thenReturn(fs);
    for (String partition : mdtPartitions) {
      when(fs.exists(new Path(basePath + "/" + partition))).thenReturn(true);
    }
    
    // Mock timeline
    HoodieTimeline commitsTimeline = mock(HoodieTimeline.class);
    HoodieTimeline completedTimeline = mock(HoodieTimeline.class);
    when(metaClient.getCommitsTimeline()).thenReturn(commitsTimeline);
    when(commitsTimeline.filterCompletedInstants()).thenReturn(completedTimeline);
    
    // Setup validator with test data
    validator.setMetadataPartitionsToReturn(mdtPartitions);
    validator.setFsPartitionsToReturn(fsPartitions);
    
    // Test validation with truncation
    HoodieValidationException exception = assertThrows(HoodieValidationException.class, () -> {
      validator.validatePartitions(engineContext, basePath, metaClient);
    });
    
    // Verify truncation in error message
    String errorMsg = exception.getMessage();
    assertTrue(errorMsg.contains("..."));  // Should contain truncation indicator
    assertTrue(errorMsg.length() <= config.logDetailMaxLength * 2 + 1000); // Account for both lists and additional message text
    
    // Verify the error message contains the count of partitions
    assertTrue(errorMsg.contains(String.format("Additional %d partitions from FS, but missing from MDT : ",
        fsPartitions.size())));
    assertTrue(errorMsg.contains(String.format("additional %d partitions from MDT, but missing from FS listing :",
        mdtPartitions.size())));
  }

  @Test
  void testValidateFileSlicesTruncation() {
    // Setup mock objects
    HoodieMetadataTableValidator.Config config = new HoodieMetadataTableValidator.Config();
    config.basePath = basePath;
    config.logDetailMaxLength = 100; // Small length to force truncation
    
    MockHoodieMetadataTableValidator validator = new MockHoodieMetadataTableValidator(jsc, config);
    
    // Generate large lists of file slices that will exceed truncation threshold
    String partition = "partition_" + generateRandomString(10);
    List<FileSlice> mdtFileSlices = new ArrayList<>();
    List<FileSlice> fsFileSlices = new ArrayList<>();
    
    // Generate 20 file slices for MDT and 15 for FS to ensure they're different
    for (int i = 0; i < 20; i++) {
      String fileId = UUID.randomUUID().toString();
      String baseInstantTime = HoodieActiveTimeline.createNewInstantTime();

      // Create file slice with base file and log files
      HoodieBaseFile baseFile = new HoodieBaseFile(FSUtils.makeBaseFileName(
          baseInstantTime, "1-0-1", fileId, HoodieFileFormat.PARQUET.getFileExtension()));
      List<HoodieLogFile> logFiles = Arrays.asList(
        new HoodieLogFile(FSUtils.makeLogFileName(fileId, HoodieLogFile.DELTA_EXTENSION, baseInstantTime, 1, "1-0-1")),
        new HoodieLogFile(FSUtils.makeLogFileName(fileId, HoodieLogFile.DELTA_EXTENSION, baseInstantTime, 2, "1-0-1"))
      );

      FileSlice slice = new FileSlice(new HoodieFileGroupId(partition, fileId), baseInstantTime);
      slice.setBaseFile(baseFile);
      logFiles.forEach(slice::addLogFile);
      mdtFileSlices.add(slice);

      // Add to FS list for first 15 entries
      if (i < 15) {
        fsFileSlices.add(new FileSlice(slice));
      }
    }
    
    // Test validation with truncation
    HoodieValidationException exception = assertThrows(
        HoodieValidationException.class,
        () -> validator.validateFileSlices(mdtFileSlices, fsFileSlices, partition, metaClient, "test"));
    
    String errorMsg = exception.getMessage();
    
    // Verify truncation behavior
    assertTrue(errorMsg.contains("..."));  // Should contain truncation indicator
    assertTrue(errorMsg.length() <= config.logDetailMaxLength * 2 + 1000); // Account for both lists and additional message text
    
    // Verify error message contains file slice counts
    assertTrue(errorMsg.contains(String.format("Number of file slices based on the file system does not match that based on the metadata table. File system-based listing (%d file slices)",
        fsFileSlices.size())));
    assertTrue(errorMsg.contains(String.format("MDT-based listing (%d file slices)", 
        mdtFileSlices.size())));
  }
}
