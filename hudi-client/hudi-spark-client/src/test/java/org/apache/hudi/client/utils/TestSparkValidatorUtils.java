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
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.validator.SparkPreCommitValidator;
import org.apache.hudi.client.validator.SqlQuerySingleResultPreCommitValidator;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodiePreCommitValidatorConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.testutils.HoodieClientTestBase;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for {@link SparkValidatorUtils}.
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

  /**
   * Verifies that two custom validators are both invoked in parallel via ExecutorServiceBasedEngineContext
   * without ClassNotFoundException, confirming that classloader-loaded user validator classes execute correctly.
   */
  @Test
  public void testTwoValidatorsBothInvoked() throws Exception {
    CountingValidator.INVOCATION_COUNT.set(0);

    HoodieWriteConfig configWithTwoValidators = getConfigBuilder()
        .withPreCommitValidatorConfig(
            HoodiePreCommitValidatorConfig.newBuilder()
                .withPreCommitValidator(
                    CountingValidator.class.getName() + "," + CountingValidator.class.getName())
                .build())
        .build();

    try (SparkRDDWriteClient writeClient = getHoodieWriteClient(configWithTwoValidators)) {
      String commit = "001";
      writeBatch(
          writeClient,
          commit,
          "000",
          Option.empty(),
          "000",
          5,
          generateWrapRecordsFn(false, configWithTwoValidators, dataGen::generateInserts),
          SparkRDDWriteClient::bulkInsert,
          true,
          5,
          5,
          1,
          false,
          INSTANT_GENERATOR);
    }

    Assertions.assertEquals(2, CountingValidator.INVOCATION_COUNT.get(),
        "Both configured validators must have been invoked in parallel");
  }

  /**
   * Verifies that when a validator throws {@link HoodieValidationException}, it surfaces somewhere
   * in the exception cause chain of the write operation.
   * The write client wraps validator exceptions in a {@code HoodieInsertException}, so we walk
   * the cause chain rather than asserting the top-level type.
   */
  @Test
  public void testValidatorFailurePropagatesException() throws Exception {
    HoodieWriteConfig configWithFailingValidator = getConfigBuilder()
        .withPreCommitValidatorConfig(
            HoodiePreCommitValidatorConfig.newBuilder()
                .withPreCommitValidator(FailingValidator.class.getName())
                .build())
        .build();

    try (SparkRDDWriteClient writeClient = getHoodieWriteClient(configWithFailingValidator)) {
      String commit = "001";
      Exception thrown = assertThrows(Exception.class, () ->
          writeBatch(
              writeClient,
              commit,
              "000",
              Option.empty(),
              "000",
              5,
              generateWrapRecordsFn(false, configWithFailingValidator, dataGen::generateInserts),
              SparkRDDWriteClient::bulkInsert,
              true,
              5,
              5,
              1,
              false,
              INSTANT_GENERATOR),
          "A failing validator must cause the write operation to throw");

      // Walk the cause chain: bulkInsert wraps the HoodieValidationException in HoodieInsertException.
      Throwable cause = thrown;
      while (cause != null && !(cause instanceof HoodieValidationException)) {
        cause = cause.getCause();
      }
      Assertions.assertNotNull(cause,
          "HoodieValidationException must appear somewhere in the exception cause chain");
      Assertions.assertInstanceOf(HoodieValidationException.class, cause);
    }
  }

  /**
   * Verifies that when a validator throws an unexpected RuntimeException (e.g. NPE or
   * IllegalStateException — a validator bug), the exception is NOT silently swallowed as a
   * generic "validation failed" message. The original exception must appear in the cause chain
   * so operators can diagnose the real problem.
   */
  @Test
  public void testUnexpectedValidatorExceptionIsNotSilenced() throws Exception {
    HoodieWriteConfig configWithBuggyValidator = getConfigBuilder()
        .withPreCommitValidatorConfig(
            HoodiePreCommitValidatorConfig.newBuilder()
                .withPreCommitValidator(BuggyValidator.class.getName())
                .build())
        .build();

    try (SparkRDDWriteClient writeClient = getHoodieWriteClient(configWithBuggyValidator)) {
      String commit = "001";
      Exception thrown = assertThrows(Exception.class, () ->
          writeBatch(
              writeClient,
              commit,
              "000",
              Option.empty(),
              "000",
              5,
              generateWrapRecordsFn(false, configWithBuggyValidator, dataGen::generateInserts),
              SparkRDDWriteClient::bulkInsert,
              true,
              5,
              5,
              1,
              false,
              INSTANT_GENERATOR),
          "A buggy validator must still cause the write to fail");

      // The original IllegalStateException must be visible somewhere in the cause chain.
      // It must NOT be silently converted into a plain "At least one pre-commit validation failed".
      Throwable cause = thrown;
      boolean foundOriginal = false;
      while (cause != null) {
        if (cause instanceof IllegalStateException
            && "simulated bug in validator".equals(cause.getMessage())) {
          foundOriginal = true;
          break;
        }
        cause = cause.getCause();
      }
      Assertions.assertTrue(foundOriginal,
          "The original IllegalStateException from the buggy validator must appear in the cause chain, "
              + "not be buried under a generic 'validation failed' message. Full exception: " + thrown);
    }
  }

  /**
   * Minimal validator that records each invocation. Must be a public static class so that
   * ReflectionUtils can instantiate it by name during runValidators.
   */
  public static class CountingValidator<T, I, K, O extends HoodieData<WriteStatus>>
      extends SparkPreCommitValidator<T, I, K, O> {

    static final AtomicInteger INVOCATION_COUNT = new AtomicInteger(0);

    public CountingValidator(HoodieSparkTable<T> table, HoodieEngineContext context,
                             HoodieWriteConfig config) {
      super(table, context, config);
    }

    @Override
    protected void validateRecordsBeforeAndAfter(Dataset<Row> before, Dataset<Row> after,
                                                 Set<String> partitionsAffected) {
      INVOCATION_COUNT.incrementAndGet();
    }
  }

  /**
   * Validator that always fails with {@link HoodieValidationException}. Must be a public static
   * class so that ReflectionUtils can instantiate it by name during runValidators.
   */
  public static class FailingValidator<T, I, K, O extends HoodieData<WriteStatus>>
      extends SparkPreCommitValidator<T, I, K, O> {

    public FailingValidator(HoodieSparkTable<T> table, HoodieEngineContext context,
                            HoodieWriteConfig config) {
      super(table, context, config);
    }

    @Override
    protected void validateRecordsBeforeAndAfter(Dataset<Row> before, Dataset<Row> after,
                                                 Set<String> partitionsAffected) {
      throw new HoodieValidationException("intentional failure from FailingValidator");
    }
  }

  /**
   * Validator that throws an unexpected RuntimeException (simulates a validator bug such as NPE).
   * Must be a public static class so that ReflectionUtils can instantiate it by name.
   */
  public static class BuggyValidator<T, I, K, O extends HoodieData<WriteStatus>>
      extends SparkPreCommitValidator<T, I, K, O> {

    public BuggyValidator(HoodieSparkTable<T> table, HoodieEngineContext context,
                          HoodieWriteConfig config) {
      super(table, context, config);
    }

    @Override
    protected void validateRecordsBeforeAndAfter(Dataset<Row> before, Dataset<Row> after,
                                                 Set<String> partitionsAffected) {
      throw new IllegalStateException("simulated bug in validator");
    }
  }
}
