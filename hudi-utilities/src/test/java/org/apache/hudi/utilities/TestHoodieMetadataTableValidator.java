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
import org.apache.hudi.common.config.HoodieTimeGeneratorConfig;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimeGenerator;
import org.apache.hudi.common.table.timeline.TimeGenerators;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.testutils.HoodieSparkClientTestBase;

import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.common.testutils.RawTripTestPayload.recordToString;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
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
        .mode(SaveMode.Overwrite)
        .save(basePath);
    Dataset<Row> updates = makeUpdateDf("001", 5).cache();
    updates.write().format("hudi").options(writeOptions)
        .option(DataSourceWriteOptions.OPERATION().key(), WriteOperationType.UPSERT.value())
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

    TimeGenerator timeGenerator = TimeGenerators
        .getTimeGenerator(HoodieTimeGeneratorConfig.defaultConfig(basePath), jsc.hadoopConfiguration());

    if (testFailureCase) {
      // 3rd partition which is additional in MDT should have creation time before last instant in timeline.

      String partition3CreationTime = HoodieActiveTimeline.createNewInstantTime(true, timeGenerator);
      Thread.sleep(100);
      String lastIntantCreationTime = HoodieActiveTimeline.createNewInstantTime(true, timeGenerator);

      HoodieInstant lastInstant = new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, lastIntantCreationTime);
      when(completedTimeline.lastInstant()).thenReturn(Option.of(lastInstant));
      validator.setPartitionCreationTime(Option.of(partition3CreationTime));
      // validate that exception is thrown since MDT has one additional partition.
      assertThrows(HoodieValidationException.class, () -> {
        validator.validatePartitions(engineContext, basePath, metaClient);
      });
    } else {
      // 3rd partition creation time is > last completed instant
      HoodieInstant lastInstant = new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, HoodieActiveTimeline.createNewInstantTime(true, timeGenerator));
      when(completedTimeline.lastInstant()).thenReturn(Option.of(lastInstant));
      Thread.sleep(100);
      validator.setPartitionCreationTime(Option.of(HoodieActiveTimeline.createNewInstantTime(true, timeGenerator)));

      // validate that all 3 partitions are returned
      assertEquals(mdtPartitions, validator.validatePartitions(engineContext, basePath, metaClient));
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
    List<String> getPartitionsFromFileSystem(HoodieEngineContext engineContext, String basePath, FileSystem fs, HoodieTimeline completedTimeline) {
      return fsPartitionsToReturn;
    }

    @Override
    List<String> getPartitionsFromMDT(HoodieEngineContext engineContext, String basePath) {
      return metadataPartitionsToReturn;
    }

    @Override
    Option<String> getPartitionCreationInstant(FileSystem fs, String basePath, String partition) {
      return this.partitionCreationTime;
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
