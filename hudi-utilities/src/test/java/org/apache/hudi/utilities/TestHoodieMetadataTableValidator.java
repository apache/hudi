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
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
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
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hadoop.fs.FileUtil.copy;
import static org.apache.hudi.common.testutils.RawTripTestPayload.recordToString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestHoodieMetadataTableValidator extends HoodieSparkClientTestBase {

  private static Stream<Arguments> lastNFileSlicesTestArgs() {
    return Stream.of(-1, 1, 3, 4, 5).flatMap(i -> Stream.of(Arguments.of(i, true), Arguments.of(i, false)));
  }

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
    timeline.saveAsComplete(new HoodieInstant(HoodieInstant.State.INFLIGHT, instantToOverwrite.getAction(), instantToOverwrite.getTimestamp()),
        Option.of(commitMetadata.toJsonString().getBytes(StandardCharsets.UTF_8)));

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
}
