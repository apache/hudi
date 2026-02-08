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

package org.apache.hudi.client.utils;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteClientTestUtils;
import org.apache.hudi.client.validator.SqlQuerySingleResultPreCommitValidator;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodiePreCommitValidatorConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.testutils.HoodieClientTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.apache.spark.api.java.JavaRDD;

import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;

/**
 * Tests for {@link SparkValidatorUtils}.
 * Ported from commit 94cf96d3 (HUDI-6630): SparkValidatorUtils should not throw
 * "Column ... does not exist" when running a precommit validation query against an empty write.
 */
public class TestSparkValidatorUtils extends HoodieClientTestBase {

  /**
   * When a SQL validation query references column names (e.g. _row_key) and the write has
   * no new base files (empty commit), getRecordsFromPendingCommits returns an empty DataFrame.
   * Without the fix, that DataFrame has no schema, so the validator fails with
   * AnalysisException: Column '_row_key' does not exist.
   * This test ensures validation still succeeds by using an inferred schema for the empty DataFrame.
   */
  @Test
  public void testSqlQueryValidatorWithNoRecords() throws Exception {
    HoodieWriteConfig writeConfig = getConfigBuilder().build();
    try (SparkRDDWriteClient writeClient = getHoodieWriteClient(writeConfig)) {

      // Create initial commit with data so table has schema and data
      String firstCommit = "001";
      writeBatch(
          writeClient,
          firstCommit,
          "000",
          Option.empty(),
          "000",
          10,
          generateWrapRecordsFn(false, writeConfig, dataGen::generateInserts),
          SparkRDDWriteClient::bulkInsert,
          true,
          10,
          10,
          1,
          false,
          INSTANT_GENERATOR);

      // Empty insert so the commit goes through the executor and runs pre-commit validators.
      // Start the commit (creates REQUESTED) so the executor can transition to INFLIGHT; then insert(empty)
      // runs the executor, which runs validators. getRecordsFromPendingCommits returns empty DataFrame
      // with table schema; validator must not throw.
      metaClient.reloadActiveTimeline();
      String secondCommit = "002";
      HoodieWriteConfig configWithValidator = getConfigBuilder()
          .withPreCommitValidatorConfig(
              HoodiePreCommitValidatorConfig.newBuilder()
                  .withPreCommitValidator(SqlQuerySingleResultPreCommitValidator.class.getName())
                  .withPrecommitValidatorSingleResultSqlQueries(
                      "SELECT COUNT(1) AS result FROM (SELECT _row_key FROM <TABLE_NAME> GROUP BY _row_key HAVING COUNT(1) > 1) a#0")
                  .build())
          .build();

      try (SparkRDDWriteClient validatorClient = getHoodieWriteClient(configWithValidator)) {
        WriteClientTestUtils.startCommitWithTime(validatorClient, secondCommit);
        JavaRDD<HoodieRecord> emptyRecords = jsc.emptyRDD();
        validatorClient.insert(emptyRecords, secondCommit);
      }

      metaClient.reloadActiveTimeline();
      Assertions.assertEquals(2, metaClient.getActiveTimeline().countInstants(),
          "Should have 2 commits (one with data, one empty)");
    }
  }
}
