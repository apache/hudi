/*
 * Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.io;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;

import com.uber.hoodie.HoodieWriteClient;
import com.uber.hoodie.WriteStatus;
import com.uber.hoodie.common.HoodieClientTestUtils;
import com.uber.hoodie.common.HoodieTestDataGenerator;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieTestUtils;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.table.timeline.HoodieActiveTimeline;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.config.HoodieCompactionConfig;
import com.uber.hoodie.config.HoodieIndexConfig;
import com.uber.hoodie.config.HoodieStorageConfig;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.index.HoodieIndex;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

@SuppressWarnings("unchecked")
public class TestHoodieMergeHandleDuplicateRecords {

  protected transient JavaSparkContext jsc = null;
  protected transient SQLContext sqlContext;
  protected transient FileSystem fs;
  protected String basePath = null;
  protected transient HoodieTestDataGenerator dataGen = null;

  @Before
  public void init() throws IOException {
    // Initialize a local spark env
    jsc = new JavaSparkContext(HoodieClientTestUtils.getSparkConfForTest("TestHoodieMergeHandleDuplicateRecords"));

    //SQLContext stuff
    sqlContext = new SQLContext(jsc);

    // Create a temp folder as the base path
    TemporaryFolder folder = new TemporaryFolder();
    folder.create();
    basePath = folder.getRoot().getAbsolutePath();
    fs = FSUtils.getFs(basePath, jsc.hadoopConfiguration());
    HoodieTestUtils.init(jsc.hadoopConfiguration(), basePath);
    dataGen = new HoodieTestDataGenerator();
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
  public void testUpsertsForMultipleRecordsInSameFile() throws Exception {
    // Create records in a single partition
    String partitionPath = HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS[0];
    dataGen = new HoodieTestDataGenerator(new String[]{partitionPath});

    // Build a write config with bulkinsertparallelism set
    HoodieWriteConfig cfg = getConfigBuilder().build();
    HoodieWriteClient client = new HoodieWriteClient(jsc, cfg);
    FileSystem fs = FSUtils.getFs(basePath, jsc.hadoopConfiguration());

    /**
     * Write 1 (only inserts)
     * This will do a bulk insert of 44 records of which there are 2 records repeated 21 times each.
     * id1 (21 records), id2 (21 records), id3, id4
     */
    String newCommitTime = "001";
    client.startCommitWithTime(newCommitTime);
    List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 4);
    HoodieRecord record1 = records.get(0);
    HoodieRecord record2 = records.get(1);
    for (int i = 0; i < 20; i++) {
      HoodieRecord dup = dataGen.generateUpdateRecord(record1.getKey(), newCommitTime);
      records.add(dup);
    }
    for (int i = 0; i < 20; i++) {
      HoodieRecord dup = dataGen.generateUpdateRecord(record2.getKey(), newCommitTime);
      records.add(dup);
    }
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);
    List<WriteStatus> statuses = client.bulkInsert(writeRecords, newCommitTime).collect();
    assertNoWriteErrors(statuses);

    // verify that there is a commit
    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), basePath);
    HoodieTimeline timeline = new HoodieActiveTimeline(metaClient).getCommitTimeline();
    assertEquals("Expecting a single commit.", 1, timeline.findInstantsAfter("000", Integer.MAX_VALUE).countInstants());
    assertEquals("Latest commit should be 001", newCommitTime, timeline.lastInstant().get().getTimestamp());
    assertEquals("Must contain 44 records",
        records.size(),
        HoodieClientTestUtils.readCommit(basePath, sqlContext, timeline, newCommitTime).count());

    /**
     * Write 2 (insert)
     * This will do a bulk insert of 1 record with the same row_key as record1 in the previous insert - id1.
     * At this point, we will have 2 files with the row_keys as shown here -
     * File 1 - id1 (21 records), id2 (21 records), id3, id4
     * File 2 - id1
     */
    newCommitTime = "002";
    client.startCommitWithTime(newCommitTime);

    // Do 1 more bulk insert with the same dup record1
    List<HoodieRecord> newRecords = new ArrayList<>();
    HoodieRecord sameAsRecord1 = dataGen.generateUpdateRecord(record1.getKey(), newCommitTime);
    newRecords.add(sameAsRecord1);
    writeRecords = jsc.parallelize(newRecords, 1);
    statuses = client.bulkInsert(writeRecords, newCommitTime).collect();
    assertNoWriteErrors(statuses);

    // verify that there are 2 commits
    metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), basePath);
    timeline = new HoodieActiveTimeline(metaClient).getCommitTimeline();
    assertEquals("Expecting two commits.", 2, timeline.findInstantsAfter("000", Integer.MAX_VALUE)
        .countInstants());
    assertEquals("Latest commit should be 002", newCommitTime, timeline.lastInstant().get().getTimestamp());
    Dataset<Row> dataSet = getRecords();
    assertEquals("Must contain 45 records", 45, dataSet.count());

    /**
     * Write 3 (insert)
     * This will bulk insert 2 new completely new records.
     * At this point, we will have 2 files with the row_keys as shown here -
     * File 1 - id1 (21 records), id2 (21 records), id3, id4
     * File 2 - id1
     * File 3 - id5, id6
     */
    newCommitTime = "003";
    client.startCommitWithTime(newCommitTime);
    newRecords = dataGen.generateInserts(newCommitTime, 2);
    writeRecords = jsc.parallelize(newRecords, 1);
    statuses = client.bulkInsert(writeRecords, newCommitTime).collect();
    assertNoWriteErrors(statuses);

    // verify that there are now 3 commits
    metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), basePath);
    timeline = new HoodieActiveTimeline(metaClient).getCommitTimeline();
    assertEquals("Expecting three commits.", 3, timeline.findInstantsAfter("000", Integer.MAX_VALUE)
        .countInstants());
    assertEquals("Latest commit should be 003", newCommitTime, timeline.lastInstant().get().getTimestamp());
    dataSet = getRecords();
    assertEquals("Must contain 47 records", 47, dataSet.count());

    /**
     * Write 4 (updates)
     * This will generate 2 upsert records with id1 and id2. The rider and driver names in the update records
     * will be rider-004 and driver-004.
     * After the upsert is complete, all the records with id1 in File 1 and File 2 must be updated, all the records
     * with id2 in File 2 must also be updated.
     * Also, none of the other records in File 1, File 2 and File 3 must be updated.
     */
    newCommitTime = "004";
    client.startCommitWithTime(newCommitTime);
    List<HoodieRecord> updateRecords = new ArrayList<>();

    // This exists in 001 and 002 and should be updated in both
    sameAsRecord1 = dataGen.generateUpdateRecord(record1.getKey(), newCommitTime);
    updateRecords.add(sameAsRecord1);

    // This exists in 001 and should be updated
    HoodieRecord sameAsRecord2 = dataGen.generateUpdateRecord(record2.getKey(), newCommitTime);
    updateRecords.add(sameAsRecord2);
    JavaRDD<HoodieRecord> updateRecordsRDD = jsc.parallelize(updateRecords, 1);
    statuses = client.upsert(updateRecordsRDD, newCommitTime).collect();

    // Verify there are no errors
    assertNoWriteErrors(statuses);

    // verify there are now 4 commits
    timeline = new HoodieActiveTimeline(metaClient).getCommitTimeline();
    assertEquals("Expecting four commits.", 4, timeline.findInstantsAfter("000", Integer.MAX_VALUE)
        .countInstants());
    assertEquals("Latest commit should be 004", timeline.lastInstant().get().getTimestamp(), newCommitTime);

    // Check the entire dataset has 47 records still
    dataSet = getRecords();
    assertEquals("Must contain 47 records", 47, dataSet.count());
    Row[] rows = (Row[]) dataSet.collect();
    int record1Count = 0;
    int record2Count = 0;
    for (Row row : rows) {
      if (row.getAs("_hoodie_record_key").equals(record1.getKey().getRecordKey())) {
        record1Count++;
        // assert each duplicate record is updated
        assertEquals(row.getAs("rider"), "rider-004");
        assertEquals(row.getAs("driver"), "driver-004");
      } else if (row.getAs("_hoodie_record_key").equals(record2.getKey().getRecordKey())) {
        record2Count++;
        // assert each duplicate record is updated
        assertEquals(row.getAs("rider"), "rider-004");
        assertEquals(row.getAs("driver"), "driver-004");
      } else {
        assertNotEquals(row.getAs("rider"), "rider-004");
        assertNotEquals(row.getAs("driver"), "rider-004");
      }
    }
    // Assert that id1 record count which has been updated to rider-004 and driver-004 is 22, which is the total
    // number of records with row_key id1
    assertEquals(22, record1Count);

    // Assert that id2 record count which has been updated to rider-004 and driver-004 is 21, which is the total
    // number of records with row_key id2
    assertEquals(21, record2Count);
  }

  private Dataset<Row> getRecords() {
    // Check the entire dataset has 8 records still
    String[] fullPartitionPaths = new String[dataGen.getPartitionPaths().length];
    for (int i = 0; i < fullPartitionPaths.length; i++) {
      fullPartitionPaths[i] = String.format("%s/%s/*", basePath, dataGen.getPartitionPaths()[i]);
    }
    Dataset<Row> dataSet = HoodieClientTestUtils.read(jsc, basePath, sqlContext, fs,
        fullPartitionPaths);
    return dataSet;
  }

  /**
   * Assert no failures in writing hoodie files
   *
   * @param statuses List of Write Status
   */
  void assertNoWriteErrors(List<WriteStatus> statuses) {
    // Verify there are no errors
    for (WriteStatus status : statuses) {
      assertFalse("Errors found in write of " + status.getFileId(), status.hasErrors());
    }
  }

  HoodieWriteConfig.Builder getConfigBuilder() {
    return HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
        .withParallelism(2, 2)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().compactionSmallFileSize(1024 * 1024).build())
        .withStorageConfig(HoodieStorageConfig.newBuilder().limitFileSize(1024 * 1024).build())
        .forTable("test-trip-table")
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build())
        .withBulkInsertParallelism(2);
  }
}
