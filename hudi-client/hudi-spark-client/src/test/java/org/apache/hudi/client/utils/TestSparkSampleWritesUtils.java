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

package org.apache.hudi.client.utils;

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;

import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestSparkSampleWritesUtils extends SparkClientFunctionalTestHarness {

  private HoodieTestDataGenerator dataGen;
  private HoodieTableMetaClient metaClient;

  @BeforeEach
  public void setUp() throws IOException {
    dataGen = new HoodieTestDataGenerator(0xDEED);
    metaClient = getHoodieMetaClient(HoodieTableType.COPY_ON_WRITE);
  }

  @AfterEach
  public void tearDown() {
    dataGen.close();
  }

  /*
   * TODO remove this and fix parent class (HUDI-6042)
   */
  @Override
  public String basePath() {
    return tempDir.toAbsolutePath().toString();
  }

  @Test
  public void skipOverwriteRecordSizeEstimateWhenTimelineNonEmpty() throws Exception {
    String commitTime = HoodieTestTable.makeNewCommitTime();
    HoodieTestTable.of(metaClient).addCommit(commitTime);
    int originalRecordSize = 100;
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath(basePath())
        .withSampleWritesEnabled(true)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().approxRecordSize(originalRecordSize).build())
        .build();
    JavaRDD<HoodieRecord> records = jsc().parallelize(dataGen.generateInserts(commitTime, 1), 1);
    SparkSampleWritesUtils.overwriteRecordSizeEstimateIfNeeded(jsc(), records, writeConfig, commitTime);
    assertEquals(originalRecordSize, writeConfig.getCopyOnWriteRecordSizeEstimate(), "Original record size estimate should not be changed.");
  }

  @Test
  public void overwriteRecordSizeEstimateForEmptyTable() {
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .forTable("foo")
        .withPath(basePath())
        .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
        .withSampleWritesEnabled(true)
        .withSampleWritesSize(2000)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().approxRecordSize(100).build())
        .build();

    String commitTime = HoodieTestDataGenerator.getCommitTimeAtUTC(1);
    JavaRDD<HoodieRecord> records = jsc().parallelize(dataGen.generateInserts(commitTime, 2000), 2);
    SparkSampleWritesUtils.overwriteRecordSizeEstimateIfNeeded(jsc(), records, writeConfig, commitTime);
    assertEquals(779, writeConfig.getCopyOnWriteRecordSizeEstimate());
  }
}
