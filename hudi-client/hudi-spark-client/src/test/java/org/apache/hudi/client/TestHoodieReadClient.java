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

package org.apache.hudi.client;

import org.apache.hudi.common.model.HoodieLegacyAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.testutils.HoodieClientTestBase;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Test-cases for covering HoodieReadClient APIs
 */
@SuppressWarnings("unchecked")
public class TestHoodieReadClient extends HoodieClientTestBase {

  @Override
  protected void initPath() {
    try {
      java.nio.file.Path basePath = tempDir.resolve("dataset");
      java.nio.file.Files.createDirectories(basePath);
      this.basePath = basePath.toUri().toString();
    } catch (IOException ioe) {
      throw new HoodieIOException(ioe.getMessage(), ioe);
    }
  }

  /**
   * Test ReadFilter API after writing new records using HoodieWriteClient.insert.
   */
  @Test
  public void testReadFilterExistAfterInsert() throws Exception {
    testReadFilterExist(getConfig(), SparkRDDWriteClient::insert);
  }

  /**
   * Test ReadFilter API after writing new records using HoodieWriteClient.insertPrepped.
   */
  @Test
  public void testReadFilterExistAfterInsertPrepped() throws Exception {
    testReadFilterExist(getConfig(), SparkRDDWriteClient::insertPreppedRecords);
  }

  /**
   * Test ReadFilter API after writing new records using HoodieWriteClient.bulkInsert.
   */
  @Test
  public void testReadFilterExistAfterBulkInsert() throws Exception {
    testReadFilterExist(getConfigBuilder().withBulkInsertParallelism(1).build(), SparkRDDWriteClient::bulkInsert);
  }

  /**
   * Test ReadFilter API after writing new records using HoodieWriteClient.bulkInsertPrepped.
   */
  @Test
  public void testReadFilterExistAfterBulkInsertPrepped() throws Exception {
    testReadFilterExist(getConfigBuilder().withBulkInsertParallelism(1).build(),
        (writeClient, recordRDD, instantTime) -> {
          return writeClient.bulkInsertPreppedRecords(recordRDD, instantTime, Option.empty());
        });
  }

  @Test
  public void testReadROViewFailsWithoutSqlContext() {
    SparkRDDReadClient readClient = new SparkRDDReadClient(context, getConfig());
    JavaRDD<HoodieKey> recordsRDD = jsc.parallelize(new ArrayList<>(), 1);
    assertThrows(IllegalStateException.class, () -> {
      readClient.readROView(recordsRDD, 1);
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
      Function3<JavaRDD<WriteStatus>, SparkRDDWriteClient, JavaRDD<HoodieRecord>, String> writeFn) throws Exception {
    try (SparkRDDWriteClient writeClient = getHoodieWriteClient(config);) {
      SparkRDDReadClient readClient = getHoodieReadClient(config.getBasePath());
      String newCommitTime = writeClient.startCommit();
      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 100);
      JavaRDD<HoodieRecord> recordsRDD = jsc.parallelize(records, 1);

      JavaRDD<HoodieRecord> filteredRDD = readClient.filterExists(recordsRDD);

      // Should not find any files
      assertEquals(100, filteredRDD.collect().size());

      JavaRDD<HoodieRecord> smallRecordsRDD = jsc.parallelize(records.subList(0, 75), 1);
      // We create three base file, each having one record. (3 different partitions)
      List<WriteStatus> statuses = writeFn.apply(writeClient, smallRecordsRDD, newCommitTime).collect();
      // Verify there are no errors
      assertNoWriteErrors(statuses);

      SparkRDDReadClient anotherReadClient = getHoodieReadClient(config.getBasePath());
      filteredRDD = anotherReadClient.filterExists(recordsRDD);
      List<HoodieRecord> result = filteredRDD.collect();
      // Check results
      assertEquals(25, result.size());

      // check path exists for written keys
      JavaPairRDD<HoodieKey, Option<String>> keyToPathPair =
              anotherReadClient.checkExists(recordsRDD.map(HoodieRecord::getKey));
      JavaRDD<HoodieKey> keysWithPaths = keyToPathPair.filter(keyPath -> keyPath._2.isPresent())
              .map(keyPath -> keyPath._1);
      assertEquals(75, keysWithPaths.count());

      // verify rows match inserted records
      Dataset<Row> rows = anotherReadClient.readROView(keysWithPaths, 1);
      assertEquals(75, rows.count());

      JavaRDD<HoodieKey> keysWithoutPaths = keyToPathPair.filter(keyPath -> !keyPath._2.isPresent())
          .map(keyPath -> keyPath._1);

      assertThrows(AnalysisException.class, () -> {
        anotherReadClient.readROView(keysWithoutPaths, 1);
      });

      // Actual tests of getPendingCompactions method are in TestAsyncCompaction
      // This is just testing empty list
      assertEquals(0, anotherReadClient.getPendingCompactions().size());
    }
  }

  /**
   * Test tagLocation API after insert().
   */
  @Test
  public void testTagLocationAfterInsert() throws Exception {
    testTagLocation(getConfig(), SparkRDDWriteClient::insert, SparkRDDWriteClient::upsert, false);
  }

  /**
   * Test tagLocation API after insertPrepped().
   */
  @Test
  public void testTagLocationAfterInsertPrepped() throws Exception {
    testTagLocation(getConfig(), SparkRDDWriteClient::insertPreppedRecords, SparkRDDWriteClient::upsertPreppedRecords,
        true);
  }

  /**
   * Test tagLocation API after bulk-insert().
   */
  @Test
  public void testTagLocationAfterBulkInsert() throws Exception {
    testTagLocation(getConfigBuilder().withBulkInsertParallelism(1).build(), SparkRDDWriteClient::bulkInsert,
        SparkRDDWriteClient::upsert, false);
  }

  /**
   * Test tagLocation API after bulkInsertPrepped().
   */
  @Test
  public void testTagLocationAfterBulkInsertPrepped() throws Exception {
    testTagLocation(
        getConfigBuilder().withBulkInsertParallelism(1).build(), (writeClient, recordRDD, instantTime) -> writeClient
            .bulkInsertPreppedRecords(recordRDD, instantTime, Option.empty()),
        SparkRDDWriteClient::upsertPreppedRecords, true);
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
      Function3<JavaRDD<WriteStatus>, SparkRDDWriteClient, JavaRDD<HoodieRecord>, String> insertFn,
      Function3<JavaRDD<WriteStatus>, SparkRDDWriteClient, JavaRDD<HoodieRecord>, String> updateFn, boolean isPrepped)
      throws Exception {
    try (SparkRDDWriteClient client = getHoodieWriteClient(hoodieWriteConfig);) {
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
              .map(record -> new HoodieLegacyAvroRecord(record.getKey(), null)).collect(Collectors.toList()));
      // Should have 100 records in table (check using Index), all in locations marked at commit
      SparkRDDReadClient readClient = getHoodieReadClient(hoodieWriteConfig.getBasePath());
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
              .map(record -> new HoodieLegacyAvroRecord(record.getKey(), null)).collect(Collectors.toList()));
      // Index should be able to locate all updates in correct locations.
      readClient = getHoodieReadClient(hoodieWriteConfig.getBasePath());
      taggedRecords = readClient.tagLocation(recordRDD).collect();
      checkTaggedRecords(taggedRecords, newCommitTime);
    }
  }
}
