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
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.testutils.HoodieSparkClientTestBase;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.common.testutils.RawTripTestPayload.recordToString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestHoodieMetadataTableValidator extends HoodieSparkClientTestBase {

  @Test
  public void testMetadataTableValidation() {
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

    // validate MDT
    HoodieMetadataTableValidator.Config config = new HoodieMetadataTableValidator.Config();
    config.basePath = basePath;
    config.validateLatestFileSlices = true;
    config.validateAllFileGroups = true;
    HoodieMetadataTableValidator validator = new HoodieMetadataTableValidator(jsc, config);
    assertTrue(validator.run());
    assertFalse(validator.hasValidationFailure());
    assertTrue(validator.getThrowables().isEmpty());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testAdditionalPartitionsinMDT(boolean testFailureCase) throws InterruptedException {
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

    HoodieMetadataTableValidator.Config config = new HoodieMetadataTableValidator.Config();
    config.basePath = basePath;
    config.validateLatestFileSlices = true;
    config.validateAllFileGroups = true;
    MockHoodieMetadataTableValidator validator = new MockHoodieMetadataTableValidator(jsc, config);
    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);

    String partition1 = "PARTITION1";
    String partition2 = "PARTITION2";
    String partition3 = "PARTITION3";

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

    StoragePath baseStoragePath = new StoragePath(basePath);

    if (testFailureCase) {
      // 3rd partition which is additional in MDT should have creation time before last instant in timeline.

      String partition3CreationTime = HoodieActiveTimeline.createNewInstantTime();
      Thread.sleep(100);
      String lastIntantCreationTime = HoodieActiveTimeline.createNewInstantTime();

      HoodieInstant lastInstant = new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, lastIntantCreationTime);
      when(completedTimeline.lastInstant()).thenReturn(Option.of(lastInstant));
      validator.setPartitionCreationTime(Option.of(partition3CreationTime));
      // validate that exception is thrown since MDT has one additional partition.
      assertThrows(HoodieValidationException.class, () -> {
        validator.validatePartitions(engineContext, baseStoragePath, metaClient);
      });
    } else {
      // 3rd partition creation time is > last completed instant
      HoodieInstant lastInstant = new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, HoodieActiveTimeline.createNewInstantTime());
      when(completedTimeline.lastInstant()).thenReturn(Option.of(lastInstant));
      Thread.sleep(100);
      validator.setPartitionCreationTime(Option.of(HoodieActiveTimeline.createNewInstantTime()));

      // validate that all 3 partitions are returned
      assertEquals(mdtPartitions, validator.validatePartitions(engineContext, baseStoragePath, metaClient));
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
    List<String> getPartitionsFromFileSystem(HoodieEngineContext engineContext, StoragePath basePath, HoodieStorage storage, HoodieTimeline completedTimeline) {
      return fsPartitionsToReturn;
    }

    @Override
    List<String> getPartitionsFromMDT(HoodieEngineContext engineContext, StoragePath basePath, HoodieStorage storage) {
      return metadataPartitionsToReturn;
    }

    @Override
    Option<String> getPartitionCreationInstant(HoodieStorage storage, StoragePath basePath, String partition) {
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
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setBasePath(basePath).setConf(HadoopFSUtils.getStorageConfWithCopy(jsc.hadoopConfiguration())).build();
    // moving out the completed commit meta file to a temp location
    HoodieInstant lastInstant = metaClient.getActiveTimeline().filterCompletedInstants().lastInstant().get();
    String latestCompletedCommitMetaFile = basePath + "/.hoodie/" + lastInstant.getFileName();
    String tempDir = getTempLocation();
    String destFilePath = tempDir + "/" + lastInstant.getFileName();
    new File(latestCompletedCommitMetaFile).renameTo(new File(destFilePath));

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
      new File(destFilePath).renameTo(new File(originalFilePath));
      return super.getRecordLocationsFromRLI(sparkEngineContext, basePath, latestCompletedCommit);
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
