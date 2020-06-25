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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hudi.common.model.HoodieTestUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

public class TestHoodieDatasetWriteClient extends TestHoodieDatasetClientBase {

  /**
   * Test Auto Commit behavior for HoodieWriteClient bulk-insert API.
   */
  @Test
  public void testAutoCommitOnBulkInsertDataset() throws Exception {
    testAutoCommitDataset(HoodieDatasetWriteClient::bulkInsertDataset, false);
  }

  /**
   * Test auto-commit by applying write function.
   *
   * @param writeFn One of HoodieWriteClient Write API
   * @throws Exception in case of failure
   */
  private void testAutoCommitDataset(Function3<Dataset<EncodableWriteStatus>, HoodieDatasetWriteClient, Dataset<Row>, String> writeFn,
      boolean isPrepped) throws Exception {
    // Set autoCommit false
    HoodieWriteConfig cfg = getConfigBuilder().build();
    try (HoodieDatasetWriteClient client = getHoodieDatasetWriteClient(cfg);) {

      String prevCommitTime = "000";
      String newCommitTime = "001";
      int numRecords = 10;
      Dataset<EncodableWriteStatus> result = insertFirstBatchDataset(cfg, client, newCommitTime, prevCommitTime, numRecords, writeFn,
          isPrepped, true, numRecords);

      assertFalse(HoodieTestUtils.doesCommitExist(basePath, newCommitTime),
          "If Autocommit is false, then commit should not be made automatically");
      assertTrue(client.commitDataset(newCommitTime, result), "Commit should succeed");
      assertTrue(HoodieTestUtils.doesCommitExist(basePath, newCommitTime),
          "After explicit commit, commit file should be created");
    }
  }

  //TODO: Add unit-tests
  // 1. 1 round of bulkInsert
  // 2. N rounds of bulkInsert
  // 3. Test Rollback of failed bulkInsert
}
