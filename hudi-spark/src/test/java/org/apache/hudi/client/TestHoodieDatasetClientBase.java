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

import static org.apache.hudi.common.HoodieTestDataGenerator.AVRO_SCHEMA_1;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.AvroConversionUtils;
import org.apache.hudi.common.HoodieClientTestUtils;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class TestHoodieDatasetClientBase extends TestHoodieClientBase {

  protected HoodieDatasetWriteClient getHoodieDatasetWriteClient(HoodieWriteConfig cfg) {
    return new HoodieDatasetWriteClient(jsc, cfg, false, HoodieIndex.createIndex(cfg, jsc));
  }

  /**
   * Helper to insert first batch of records and do regular assertions on the state after successful completion.
   *
   * @param writeConfig Hoodie Write Config
   * @param client Hoodie Write Client
   * @param newCommitTime New Commit Timestamp to be used
   * @param initCommitTime Begin Timestamp (usually "000")
   * @param numRecordsInThisCommit Number of records to be added in the new commit
   * @param writeFn Write Function to be used for insertion
   * @param isPreppedAPI Boolean flag to indicate writeFn expects prepped records
   * @param assertForCommit Enable Assertion of Writes
   * @param expRecordsInThisCommit Expected number of records in this commit
   * @return RDD of write-status
   * @throws Exception in case of error
   */
  Dataset<EncodableWriteStatus> insertFirstBatchDataset(HoodieWriteConfig writeConfig, HoodieDatasetWriteClient client, String newCommitTime,
      String initCommitTime, int numRecordsInThisCommit,
      Function3<Dataset<EncodableWriteStatus>, HoodieDatasetWriteClient, Dataset<Row>, String> writeFn, boolean isPreppedAPI,
      boolean assertForCommit, int expRecordsInThisCommit) throws Exception {
    final Function2<List<GenericRecord>, String, Integer> recordGenFunction =
        generateWrapRowsFn(isPreppedAPI, writeConfig, dataGen::generateInsertsGenRec);

    return writeBatchDataset(client, newCommitTime, initCommitTime, Option.empty(), initCommitTime, numRecordsInThisCommit,
        recordGenFunction, writeFn, assertForCommit, expRecordsInThisCommit, expRecordsInThisCommit, 1);
  }

  /**
   * Helper to insert/upsert batch of records and do regular assertions on the state after successful completion.
   *
   * @param client Hoodie Write Client
   * @param newCommitTime New Commit Timestamp to be used
   * @param prevCommitTime Commit Timestamp used in previous commit
   * @param commitTimesBetweenPrevAndNew Sample of Timestamps between prevCommitTime and newCommitTime
   * @param initCommitTime Begin Timestamp (usually "000")
   * @param numRecordsInThisCommit Number of records to be added in the new commit
   * @param recordGenFunction Records Generation Function
   * @param writeFn Write Function to be used for upsert
   * @param assertForCommit Enable Assertion of Writes
   * @param expRecordsInThisCommit Expected number of records in this commit
   * @param expTotalRecords Expected number of records when scanned
   * @param expTotalCommits Expected number of commits (including this commit)
   * @throws Exception in case of error
   */
  Dataset<EncodableWriteStatus> writeBatchDataset(HoodieDatasetWriteClient client, String newCommitTime, String prevCommitTime,
      Option<List<String>> commitTimesBetweenPrevAndNew, String initCommitTime, int numRecordsInThisCommit,
      Function2<List<GenericRecord>, String, Integer> recordGenFunction,
      Function3<Dataset<EncodableWriteStatus>, HoodieDatasetWriteClient, Dataset<Row>, String> writeFn,
      boolean assertForCommit, int expRecordsInThisCommit, int expTotalRecords, int expTotalCommits) throws Exception {

    // Write 1 (only inserts)
    client.startCommitWithTime(newCommitTime);

    List<GenericRecord> records = recordGenFunction.apply(newCommitTime, numRecordsInThisCommit);
    JavaRDD<GenericRecord> writeRecords = jsc.parallelize(records, 1);
    Dataset<Row> rowDataset = AvroConversionUtils
        .createDataFrame(writeRecords.rdd(), AVRO_SCHEMA_1.toString(), sqlContext.sparkSession());
    List<Row> rows = rowDataset.collectAsList();
    Dataset<EncodableWriteStatus> result = writeFn.apply(client, rowDataset, newCommitTime);
    List<EncodableWriteStatus> statuses = result.collectAsList();
    assertNoWriteErrorsEncWriteStatus(statuses);

    // check the partition metadata is written out
    assertPartitionMetadataForDataset(records, fs);

    // verify that there is a commit
    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), basePath);
    HoodieTimeline timeline = new HoodieActiveTimeline(metaClient).getCommitTimeline();

    if (assertForCommit) {
      assertEquals(expTotalCommits, timeline.findInstantsAfter(initCommitTime, Integer.MAX_VALUE).countInstants(),
          "Expecting " + expTotalCommits + " commits.");
      assertEquals(newCommitTime, timeline.lastInstant().get().getTimestamp(),
          "Latest commit should be " + newCommitTime);
      assertEquals(expRecordsInThisCommit, HoodieClientTestUtils
              .readCommit(basePath, sqlContext, timeline, newCommitTime).count(),
          "Must contain " + expRecordsInThisCommit + " records");

      // Check the entire dataset has all records still
      String[] fullPartitionPaths = new String[dataGen.getPartitionPaths().length];
      for (int i = 0; i < fullPartitionPaths.length; i++) {
        fullPartitionPaths[i] = String.format("%s/%s/*", basePath, dataGen.getPartitionPaths()[i]);
      }
      assertEquals(expTotalRecords, HoodieClientTestUtils.read(jsc, basePath, sqlContext, fs, fullPartitionPaths).count(),
          "Must contain " + expTotalRecords + " records");

      // Check that the incremental consumption from prevCommitTime
      assertEquals(HoodieClientTestUtils.readCommit(basePath, sqlContext, timeline, newCommitTime).count(),
          HoodieClientTestUtils.readSince(basePath, sqlContext, timeline, prevCommitTime).count(),
          "Incremental consumption from " + prevCommitTime + " should give all records in latest commit");
      if (commitTimesBetweenPrevAndNew.isPresent()) {
        commitTimesBetweenPrevAndNew.get().forEach(ct -> {
          assertEquals(HoodieClientTestUtils.readCommit(basePath, sqlContext, timeline, newCommitTime).count(),
              HoodieClientTestUtils.readSince(basePath, sqlContext, timeline, ct).count(),
              "Incremental consumption from " + ct + " should give all records in latest commit");
        });
      }
    }
    return result;
  }

}
