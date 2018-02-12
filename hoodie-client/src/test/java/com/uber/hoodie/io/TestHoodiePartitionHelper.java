/*
 *  Copyright (c) 2017 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.uber.hoodie.io;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.uber.hoodie.HoodieWriteClient;
import com.uber.hoodie.common.HoodieClientTestUtils;
import com.uber.hoodie.common.HoodieTestDataGenerator;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieTableType;
import com.uber.hoodie.common.model.HoodieTestUtils;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.config.HoodieIndexConfig;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.index.HoodieIndex;
import com.uber.hoodie.index.bloom.HoodieBloomIndex;
import com.uber.hoodie.table.HoodieTable;
import com.uber.hoodie.table.WorkloadProfile;
import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import scala.Tuple3;

/**
 * Test HoodiePartitionHelper.
 */
public class TestHoodiePartitionHelper {
  private transient JavaSparkContext jsc = null;
  private transient FileSystem fs;
  private String basePath = null;
  private transient HoodieTestDataGenerator dataGen = null;
  private FastDateFormat PARTITION_STRING_FORMATTER = FastDateFormat.getInstance("yyyy/mm/dd");

  @Before
  public void init() throws IOException, ParseException {
    // Initialize a local spark env
    jsc = new JavaSparkContext(HoodieClientTestUtils.getSparkConfForTest("TestHoodiePartitionHelper"));

    // Create a temp folder as the base path
    TemporaryFolder folder = new TemporaryFolder();
    folder.create();
    basePath = folder.getRoot().getAbsolutePath();
    fs = FSUtils.getFs(basePath, HoodieTestUtils.getDefaultHadoopConf());
    HoodieTestUtils.initTableType(fs, basePath, HoodieTableType.COPY_ON_WRITE);

    // Create HoodieTestDataGenerator
    Date startPartition = PARTITION_STRING_FORMATTER.parse("2018/01/01");

    // Generate 20 partitions
    List<String> partitions = new ArrayList<>();
    for (int i = 0; i < 20; ++i) {
      partitions.add(PARTITION_STRING_FORMATTER.format(DateUtils.addDays(startPartition, i)));
    }
    dataGen = new HoodieTestDataGenerator(partitions.toArray(new String[partitions.size()]));
  }

  @After
  public void clean() {
    if (basePath != null) {
      new File(basePath).delete();
    }
    if (jsc != null) {
      jsc.stop();
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testHoodiePartitionHelper() throws Exception {
    // insert 1000 records
    HoodieWriteConfig config = getConfig();
    HoodieWriteClient writeClient = new HoodieWriteClient(jsc, config);
    String newCommitTime = "100";
    writeClient.startCommitWithTime(newCommitTime);
    List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 1000);
    JavaRDD<HoodieRecord> recordsRDD = jsc.parallelize(records, 1);
    writeClient.insert(recordsRDD, newCommitTime).collect();

    // Update all the 1000 records
    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(),
        basePath);
    HoodieTable table = HoodieTable.getHoodieTable(metaClient, config);
    newCommitTime = "101";
    writeClient.startCommitWithTime(newCommitTime);
    List<HoodieRecord> updatedRecords = dataGen.generateUpdates(newCommitTime, records);
    JavaRDD<HoodieRecord> updatedRecordsRDD = jsc.parallelize(updatedRecords, 1);
    HoodieIndex index = new HoodieBloomIndex<>(config, jsc);
    JavaRDD<HoodieRecord> preppedRecords = index.tagLocation(updatedRecordsRDD, table);

    WorkloadProfile profile = new WorkloadProfile(preppedRecords);

    // Test with no workload partitioning
    List<Tuple3<String, String, Long>> conditions = HoodiePartitionHelper.splitWorkload(
        profile, true, dataGen.getPartitionPaths().length);
    assertTrue("Conditions should be empty", conditions.isEmpty());
    long totalRecords = preppedRecords.count();

    // Test with workload partitioning with parallelisms from 1 to 10
    for (int maxPartitions = 1; maxPartitions <= 10; ++maxPartitions) {
      long totalRecords2 = 0;
      conditions = HoodiePartitionHelper.splitWorkload(profile, true, maxPartitions);
      int numConditions = (dataGen.getPartitionPaths().length + maxPartitions - 1) /
          maxPartitions;

      assertEquals("Number of conditions should be match", numConditions, conditions.size());
      for (Tuple3<String, String, Long> condition : conditions) {
        // Step 1: filter input records based on condition
        JavaRDD<HoodieRecord> filteredPreppedRecords = preppedRecords.filter(record ->
            HoodiePartitionHelper.filterHoodieRecord(record, condition));
        totalRecords2 += filteredPreppedRecords.count();
        assertTrue("Each split should not exceed maxPartitions",
            condition._3() <= maxPartitions);
      }
      assertEquals("Total count should match", totalRecords, totalRecords2);
    }
  }

  private HoodieWriteConfig getConfig() {
    return getConfigBuilder().build();
  }

  private HoodieWriteConfig.Builder getConfigBuilder() {
    return HoodieWriteConfig.newBuilder().withPath(basePath)
        .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
        .withParallelism(2, 2)
        .forTable("test-trip-table")
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(
            HoodieIndex.IndexType.BLOOM).build());
  }
}
