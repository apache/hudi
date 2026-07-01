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

package org.apache.hudi.utilities.deltastreamer;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;
import org.apache.hudi.utilities.config.HoodieStreamerConfig;
import org.apache.hudi.utilities.streamer.SparkSampleWritesUtils;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
    TypedProperties props = new TypedProperties();
    props.put(HoodieStreamerConfig.SAMPLE_WRITES_ENABLED.key(), "true");
    props.put(HoodieCompactionConfig.COPY_ON_WRITE_RECORD_SIZE_ESTIMATE.key(), String.valueOf(originalRecordSize));
    HoodieWriteConfig originalWriteConfig = HoodieWriteConfig.newBuilder()
        .withProperties(props)
        .withPath(basePath())
        .build();
    JavaRDD<HoodieRecord> records = jsc().parallelize(dataGen.generateInserts(commitTime, 1), 1);
    Option<HoodieWriteConfig> writeConfigOpt = SparkSampleWritesUtils.getWriteConfigWithRecordSizeEstimate(jsc(), Option.of(records), originalWriteConfig);
    assertFalse(writeConfigOpt.isPresent());
    assertEquals(originalRecordSize, originalWriteConfig.getCopyOnWriteRecordSizeEstimate(), "Original record size estimate should not be changed.");
  }

  @Test
  void overwriteRecordSizeEstimateForEmptyTable() throws IOException {
    int originalRecordSize = 100;
    TypedProperties props = new TypedProperties();
    props.put(HoodieStreamerConfig.SAMPLE_WRITES_ENABLED.key(), "true");
    props.put(HoodieCompactionConfig.COPY_ON_WRITE_RECORD_SIZE_ESTIMATE.key(), String.valueOf(originalRecordSize));
    HoodieWriteConfig originalWriteConfig = HoodieWriteConfig.newBuilder()
        .withProperties(props)
        .forTable("foo")
        .withPath(basePath())
        .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
        .build();

    String commitTime = HoodieTestDataGenerator.getCommitTimeAtUTC(1);
    // Input spans multiple source partitions; the sample-writes table must still be non-partitioned.
    JavaRDD<HoodieRecord> records = jsc().parallelize(dataGen.generateInserts(commitTime, 2000), 2);
    Option<HoodieWriteConfig> writeConfigOpt = SparkSampleWritesUtils.getWriteConfigWithRecordSizeEstimate(jsc(), Option.of(records), originalWriteConfig);
    assertTrue(writeConfigOpt.isPresent());
    assertEquals(337.0, writeConfigOpt.get().getCopyOnWriteRecordSizeEstimate(), 10.0);
    assertSampleWritesNonPartitioned();
  }

  @Test
  void sampleWritesAreNonPartitionedEvenForManyPartitionInput() throws IOException {
    int recordsPerPartition = 50;
    String[] partitionPaths = IntStream.range(0, 20)
        .mapToObj(i -> String.format("year=2024/month=01/day=%02d", i + 1))
        .toArray(String[]::new);
    HoodieTestDataGenerator manyPartitionGen = new HoodieTestDataGenerator(partitionPaths);

    TypedProperties props = new TypedProperties();
    props.put(HoodieStreamerConfig.SAMPLE_WRITES_ENABLED.key(), "true");
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withProperties(props)
        .forTable("foo")
        .withPath(basePath())
        .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
        .build();

    String commitTime = HoodieTestDataGenerator.getCommitTimeAtUTC(1);
    List<HoodieRecord> allRecords = new ArrayList<>();
    for (String partition : partitionPaths) {
      allRecords.addAll(manyPartitionGen.generateInsertsForPartition(commitTime, recordsPerPartition, partition));
    }
    JavaRDD<HoodieRecord> records = jsc().parallelize(allRecords, 4);

    Option<HoodieWriteConfig> writeConfigOpt = SparkSampleWritesUtils.getWriteConfigWithRecordSizeEstimate(jsc(), Option.of(records), writeConfig);
    assertTrue(writeConfigOpt.isPresent(), "Sample write should produce a record-size estimate.");
    assertSampleWritesNonPartitioned();
  }

  /**
   * Fails if the sample-writes folder contains any source-partition subdirectory, i.e. the
   * sample write was not flattened into a single non-partitioned file.
   */
  private void assertSampleWritesNonPartitioned() throws IOException {
    Path sampleWritesPath = new Path(basePath(), ".hoodie/.aux/.sample_writes");
    FileSystem fs = sampleWritesPath.getFileSystem(jsc().hadoopConfiguration());
    assertTrue(fs.exists(sampleWritesPath), "Sample-writes folder should exist after a sample write.");
    FileStatus[] runs = fs.listStatus(sampleWritesPath);
    assertTrue(runs.length > 0, "Sample-writes folder should contain at least one run.");
    for (FileStatus run : runs) {
      List<String> partitionDirs = new ArrayList<>();
      for (FileStatus entry : fs.listStatus(run.getPath())) {
        if (entry.isDirectory() && !entry.getPath().getName().equals(".hoodie")) {
          partitionDirs.add(entry.getPath().getName());
        }
      }
      assertTrue(partitionDirs.isEmpty(),
          "Sample-writes run at " + run.getPath() + " should have no source partition subdirectories, but found: "
              + Arrays.toString(partitionDirs.toArray()));
    }
  }
}
