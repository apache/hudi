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

package org.apache.hudi;

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;

import org.apache.spark.api.java.JavaRDD;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertTrue;

@SuppressWarnings("unchecked")
/**
 * Test-cases for covering HoodieReadClient APIs
 */
public class TestHoodieReadClient extends TestHoodieClientBase {

  /**
   * Test ReadFilter API after writing new records using HoodieWriteClient.insert.
   */
  @Test
  public void testReadFilterExistAfterInsert() throws Exception {
    testReadFilterExist(getConfig(), HoodieWriteClient::insert);
  }

  /**
   * Test ReadFilter API after writing new records using HoodieWriteClient.insertPrepped.
   */
  @Test
  public void testReadFilterExistAfterInsertPrepped() throws Exception {
    testReadFilterExist(getConfig(), HoodieWriteClient::insertPreppedRecords);
  }

  /**
   * Test ReadFilter API after writing new records using HoodieWriteClient.bulkInsert.
   */
  @Test
  public void testReadFilterExistAfterBulkInsert() throws Exception {
    testReadFilterExist(getConfigBuilder().withBulkInsertParallelism(1).build(), HoodieWriteClient::bulkInsert);
  }

  /**
   * Test ReadFilter API after writing new records using HoodieWriteClient.bulkInsertPrepped.
   */
  @Test
  public void testReadFilterExistAfterBulkInsertPrepped() throws Exception {
    testReadFilterExist(getConfigBuilder().withBulkInsertParallelism(1).build(),
        (writeClient, recordRDD, commitTime) -> {
          return writeClient.bulkInsertPreppedRecords(recordRDD, commitTime, Option.empty());
        });
  }

  /**
   * Helper to write new records using one of HoodieWriteClient's write API and use ReadClient to test filterExists()
   * API works correctly.
   *
   * @param config Hoodie Write Config
   * @param writeFn Write Function for writing records
   * @throws Exception in case of error
   */
  private void testReadFilterExist(HoodieWriteConfig config,
      Function3<JavaRDD<WriteStatus>, HoodieWriteClient, JavaRDD<HoodieRecord>, String> writeFn) throws Exception {
    try (HoodieWriteClient writeClient = getHoodieWriteClient(config);
        HoodieReadClient readClient = getHoodieReadClient(config.getBasePath());) {
      String newCommitTime = writeClient.startCommit();
      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 100);
      JavaRDD<HoodieRecord> recordsRDD = jsc.parallelize(records, 1);

      JavaRDD<HoodieRecord> filteredRDD = readClient.filterExists(recordsRDD);

      // Should not find any files
      assertTrue(filteredRDD.collect().size() == 100);

      JavaRDD<HoodieRecord> smallRecordsRDD = jsc.parallelize(records.subList(0, 75), 1);
      // We create three parquet file, each having one record. (3 different partitions)
      List<WriteStatus> statuses = writeFn.apply(writeClient, smallRecordsRDD, newCommitTime).collect();
      // Verify there are no errors
      assertNoWriteErrors(statuses);

      try (HoodieReadClient anotherReadClient = getHoodieReadClient(config.getBasePath());) {
        filteredRDD = anotherReadClient.filterExists(recordsRDD);
        List<HoodieRecord> result = filteredRDD.collect();
        // Check results
        Assert.assertEquals(25, result.size());
      }
    }
  }

  /**
   * Test tagLocation API after insert().
   */
  @Test
  public void testTagLocationAfterInsert() throws Exception {
    testTagLocation(getConfig(), HoodieWriteClient::insert, HoodieWriteClient::upsert, false);
  }

  /**
   * Test tagLocation API after insertPrepped().
   */
  @Test
  public void testTagLocationAfterInsertPrepped() throws Exception {
    testTagLocation(getConfig(), HoodieWriteClient::insertPreppedRecords, HoodieWriteClient::upsertPreppedRecords,
        true);
  }

  /**
   * Test tagLocation API after bulk-insert().
   */
  @Test
  public void testTagLocationAfterBulkInsert() throws Exception {
    testTagLocation(getConfigBuilder().withBulkInsertParallelism(1).build(), HoodieWriteClient::bulkInsert,
        HoodieWriteClient::upsert, false);
  }

  /**
   * Test tagLocation API after bulkInsertPrepped().
   */
  @Test
  public void testTagLocationAfterBulkInsertPrepped() throws Exception {
    testTagLocation(
        getConfigBuilder().withBulkInsertParallelism(1).build(), (writeClient, recordRDD, commitTime) -> writeClient
            .bulkInsertPreppedRecords(recordRDD, commitTime, Option.empty()),
        HoodieWriteClient::upsertPreppedRecords, true);
  }

  /**
   * Helper method to test tagLocation after using different HoodieWriteClient write APIS.
   *
   * @param hoodieWriteConfig Write Config
   * @param insertFn Hoodie Write Client first Insert API
   * @param updateFn Hoodie Write Client upsert API
   * @param isPrepped isPrepped flag.
   * @throws Exception in case of error
   */
  private void testTagLocation(HoodieWriteConfig hoodieWriteConfig,
      Function3<JavaRDD<WriteStatus>, HoodieWriteClient, JavaRDD<HoodieRecord>, String> insertFn,
      Function3<JavaRDD<WriteStatus>, HoodieWriteClient, JavaRDD<HoodieRecord>, String> updateFn, boolean isPrepped)
      throws Exception {
    try (HoodieWriteClient client = getHoodieWriteClient(hoodieWriteConfig);) {
      // Write 1 (only inserts)
      String newCommitTime = "001";
      String initCommitTime = "000";
      int numRecords = 200;
      JavaRDD<WriteStatus> result = insertFirstBatch(hoodieWriteConfig, client, newCommitTime, initCommitTime,
          numRecords, insertFn, isPrepped, true, numRecords);
      // Construct HoodieRecord from the WriteStatus but set HoodieKey, Data and HoodieRecordLocation accordingly
      // since they have been modified in the DAG
      JavaRDD<HoodieRecord> recordRDD =
          jsc.parallelize(result.collect().stream().map(WriteStatus::getWrittenRecords).flatMap(Collection::stream)
              .map(record -> new HoodieRecord(record.getKey(), null)).collect(Collectors.toList()));
      // Should have 100 records in table (check using Index), all in locations marked at commit
      HoodieReadClient readClient = getHoodieReadClient(hoodieWriteConfig.getBasePath());
      List<HoodieRecord> taggedRecords = readClient.tagLocation(recordRDD).collect();
      checkTaggedRecords(taggedRecords, newCommitTime);

      // Write 2 (updates)
      String prevCommitTime = newCommitTime;
      newCommitTime = "004";
      numRecords = 100;
      String commitTimeBetweenPrevAndNew = "002";
      result = updateBatch(hoodieWriteConfig, client, newCommitTime, prevCommitTime,
          Option.of(Arrays.asList(commitTimeBetweenPrevAndNew)), initCommitTime, numRecords, updateFn, isPrepped, true,
          numRecords, 200, 2);
      recordRDD =
          jsc.parallelize(result.collect().stream().map(WriteStatus::getWrittenRecords).flatMap(Collection::stream)
              .map(record -> new HoodieRecord(record.getKey(), null)).collect(Collectors.toList()));
      // Index should be able to locate all updates in correct locations.
      readClient = getHoodieReadClient(hoodieWriteConfig.getBasePath());
      taggedRecords = readClient.tagLocation(recordRDD).collect();
      checkTaggedRecords(taggedRecords, newCommitTime);
    }
  }
}
