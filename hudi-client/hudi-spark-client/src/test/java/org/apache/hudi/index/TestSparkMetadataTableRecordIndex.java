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

package org.apache.hudi.index;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.data.HoodieListPairData;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordGlobalLocation;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.collection.ImmutablePair;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.config.internal.OnehouseInternalConfig;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.table.HoodieSparkCopyOnWriteTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;

import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.apache.hudi.common.model.HoodieTableType.COPY_ON_WRITE;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class TestSparkMetadataTableRecordIndex extends SparkClientFunctionalTestHarness {

  private String basePath;

  private void initBasePath() {
    basePath = basePath().substring(7);
  }

  @Test
  public <T> void testValidateRLIAgainstFilesPartition() throws IOException {
    initBasePath();
    HoodieWriteConfig writeConfig = getConfig();
    HoodieTableMetaClient metaClient = getHoodieMetaClient(COPY_ON_WRITE);
    SparkRDDWriteClient client =  getHoodieWriteClient(writeConfig);
    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();

    //normal insert
    List<HoodieRecord> records = insertRecords(client, dataGen, HoodieActiveTimeline.createNewInstantTime());

    SparkMetadataTableRecordIndex index = new SparkMetadataTableRecordIndex(writeConfig);
    HoodieTable table = HoodieSparkCopyOnWriteTable.create(writeConfig, context(), metaClient);
    JavaRDD<HoodieRecord<T>> recordRDD = jsc().parallelize(records.subList(0, 10), 1).map(record -> (HoodieRecord<T>)record);

    // lets collect all tagged records.
    List<HoodieRecord<T>> output = index.tagLocation(HoodieJavaRDD.of(recordRDD), context(), table).collectAsList();
    String fileId = output.stream().map(record -> record.getCurrentLocation().getFileId()).findFirst().get();

    // prepare input data to mayBeValidateAgainstFilesPartition
    HoodiePairData<String, HoodieRecordGlobalLocation> validDataPair = HoodieListPairData.lazy(Arrays.asList(
        ImmutablePair.of(output.get(0).getRecordKey(), new HoodieRecordGlobalLocation(output.get(0).getPartitionPath(), output.get(0).getCurrentLocation().getInstantTime(),
            output.get(0).getCurrentLocation().getFileId())
    )));

    metaClient = HoodieTableMetaClient.reload(metaClient);
    table = HoodieSparkCopyOnWriteTable.create(writeConfig, context(), metaClient);
    // expected to not throw any execption
    index.mayBeValidateAgainstFilesPartition(validDataPair, table);

    // now lets test w/ an errorneous fileId.
    // compute errorneousFileId
    String errorneousFileId = fileId.substring(0, fileId.length() - 1);
    HoodiePairData<String, HoodieRecordGlobalLocation> invalidDataPair = HoodieListPairData.lazy(Arrays.asList(
        ImmutablePair.of(output.get(0).getRecordKey(), new HoodieRecordGlobalLocation(output.get(0).getPartitionPath(), output.get(0).getCurrentLocation().getInstantTime(),
            errorneousFileId)
        )));

    try {
      index.mayBeValidateAgainstFilesPartition(invalidDataPair, table);
      fail("Should have failed");
    } catch (HoodieIndexException e) {
      assertTrue(e.getMessage().contains("There are few FileIDs found in RLI which are not referenced in FILES partition in MDT ["
          + errorneousFileId + "]"));
    }

    // test by disbaling the config and not exception should be thrown.
    HoodieWriteConfig config2 = getConfigByDisablingValidation();
    index = new SparkMetadataTableRecordIndex(config2);

    // even though we pass in invalid entries, no exception should be thrown
    index.mayBeValidateAgainstFilesPartition(invalidDataPair,table);
  }

  protected List<HoodieRecord> insertRecords(SparkRDDWriteClient client, HoodieTestDataGenerator dataGen, String commitTime) {
    client.startCommitWithTime(commitTime);
    List<HoodieRecord> records = dataGen.generateInserts(commitTime, 100);
    JavaRDD<HoodieRecord> writeRecords = jsc().parallelize(records, 1);
    List<WriteStatus> statuses = client.upsert(writeRecords, commitTime).collect();
    assertNoWriteErrors(statuses);
    return records;
  }

  protected HoodieWriteConfig getConfig() {
    return HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withProperties(getPropertiesForKeyGen(true))
        .withSchema(TRIP_EXAMPLE_SCHEMA)
        .withParallelism(2, 2)
        .withAutoCommit(true)
        .withEmbeddedTimelineServerEnabled(false)
        .forTable("test-trip-table")
        .withRollbackUsingMarkers(true)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(true).withEnableRecordIndex(true).withRecordIndexFileGroupCount(1,1).build())
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.RECORD_INDEX).build())
        .withOnehouseInternalConfig(OnehouseInternalConfig.newBuilder().withRecordIndexValidateAgainstFilesPartitions(true).build())
        .build();
  }

  protected HoodieWriteConfig getConfigByDisablingValidation() {
    return HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withProperties(getPropertiesForKeyGen(true))
        .withSchema(TRIP_EXAMPLE_SCHEMA)
        .withParallelism(2, 2)
        .withAutoCommit(true)
        .withEmbeddedTimelineServerEnabled(false)
        .forTable("test-trip-table")
        .withRollbackUsingMarkers(true)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(true).withEnableRecordIndex(true).withRecordIndexFileGroupCount(1,1).build())
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.RECORD_INDEX).build())
        .withOnehouseInternalConfig(OnehouseInternalConfig.newBuilder().withRecordIndexValidateAgainstFilesPartitions(false).build())
        .build();
  }
}
