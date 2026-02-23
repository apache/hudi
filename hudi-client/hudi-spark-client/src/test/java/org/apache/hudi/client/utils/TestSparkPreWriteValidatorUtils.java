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
import org.apache.hudi.client.validator.PreWriteValidator;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.config.HoodiePreWriteValidatorConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.testutils.HoodieClientTestBase;

import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for pre-write validators with Spark write client.
 * These tests verify that pre-write validators are invoked during write operations
 * and receive the records being written.
 */
public class TestSparkPreWriteValidatorUtils extends HoodieClientTestBase {

  @Test
  public void testPreWriteValidatorReceivesRecordsOnInsert() throws Exception {
    // Reset the static state
    RecordCapturingValidator.reset();

    HoodieWriteConfig writeConfig = getConfigBuilder()
        .withProps(HoodiePreWriteValidatorConfig.newBuilder()
            .withPreWriteValidator(RecordCapturingValidator.class.getName())
            .build().getProps())
        .build();

    try (SparkRDDWriteClient writeClient = getHoodieWriteClient(writeConfig)) {
      String instantTime = "001";
      WriteClientTestUtils.startCommitWithTime(writeClient, instantTime);

      List<HoodieRecord> records = dataGen.generateInserts(instantTime, 10);
      JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 2);

      writeClient.insert(writeRecords, instantTime);

      // Verify validator was called
      assertTrue(RecordCapturingValidator.wasValidateCalled(), "Validator should have been called");
      assertEquals(WriteOperationType.INSERT, RecordCapturingValidator.getCapturedOperationType());
      assertNotNull(RecordCapturingValidator.getCapturedRecords(), "Records should not be null");
      assertEquals(10, RecordCapturingValidator.getCapturedRecordCount(), "Should have received 10 records");
    }
  }

  @Test
  public void testPreWriteValidatorReceivesRecordsOnUpsert() throws Exception {
    // Reset the static state
    RecordCapturingValidator.reset();

    HoodieWriteConfig writeConfig = getConfigBuilder()
        .withProps(HoodiePreWriteValidatorConfig.newBuilder()
            .withPreWriteValidator(RecordCapturingValidator.class.getName())
            .build().getProps())
        .build();

    try (SparkRDDWriteClient writeClient = getHoodieWriteClient(writeConfig)) {
      String instantTime = "001";
      WriteClientTestUtils.startCommitWithTime(writeClient, instantTime);

      List<HoodieRecord> records = dataGen.generateInserts(instantTime, 5);
      JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 2);

      writeClient.upsert(writeRecords, instantTime);

      // Verify validator was called
      assertTrue(RecordCapturingValidator.wasValidateCalled(), "Validator should have been called");
      assertEquals(WriteOperationType.UPSERT, RecordCapturingValidator.getCapturedOperationType());
      assertNotNull(RecordCapturingValidator.getCapturedRecords(), "Records should not be null");
      assertEquals(5, RecordCapturingValidator.getCapturedRecordCount(), "Should have received 5 records");
    }
  }

  @Test
  public void testPreWriteValidatorFailureBlocksWrite() throws Exception {
    // Reset the static state
    FailingValidator.reset();

    HoodieWriteConfig writeConfig = getConfigBuilder()
        .withProps(HoodiePreWriteValidatorConfig.newBuilder()
            .withPreWriteValidator(FailingValidator.class.getName())
            .build().getProps())
        .build();

    try (SparkRDDWriteClient writeClient = getHoodieWriteClient(writeConfig)) {
      String instantTime = "001";
      WriteClientTestUtils.startCommitWithTime(writeClient, instantTime);

      List<HoodieRecord> records = dataGen.generateInserts(instantTime, 5);
      JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 2);

      // The validator should throw an exception, blocking the write
      assertThrows(HoodieValidationException.class, () -> {
        writeClient.insert(writeRecords, instantTime);
      });

      assertTrue(FailingValidator.wasValidateCalled(), "Validator should have been called before failure");
    }
  }

  @Test
  public void testMultiplePreWriteValidatorsAreInvoked() throws Exception {
    // Reset static states
    RecordCapturingValidator.reset();
    CountingValidator.reset();

    String validators = RecordCapturingValidator.class.getName() + "," + CountingValidator.class.getName();
    HoodieWriteConfig writeConfig = getConfigBuilder()
        .withProps(HoodiePreWriteValidatorConfig.newBuilder()
            .withPreWriteValidator(validators)
            .build().getProps())
        .build();

    try (SparkRDDWriteClient writeClient = getHoodieWriteClient(writeConfig)) {
      String instantTime = "001";
      WriteClientTestUtils.startCommitWithTime(writeClient, instantTime);

      List<HoodieRecord> records = dataGen.generateInserts(instantTime, 7);
      JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 2);

      writeClient.insert(writeRecords, instantTime);

      // Verify both validators were called
      assertTrue(RecordCapturingValidator.wasValidateCalled(), "RecordCapturingValidator should have been called");
      assertTrue(CountingValidator.wasValidateCalled(), "CountingValidator should have been called");
      assertEquals(1, CountingValidator.getInvocationCount(), "CountingValidator should have been called once");
    }
  }

  /**
   * A test validator that captures records and operation details.
   */
  public static class RecordCapturingValidator implements PreWriteValidator {
    private static volatile boolean validateCalled = false;
    private static volatile WriteOperationType capturedOperationType = null;
    private static volatile HoodieData<?> capturedRecords = null;
    private static volatile long capturedRecordCount = 0;

    public static void reset() {
      validateCalled = false;
      capturedOperationType = null;
      capturedRecords = null;
      capturedRecordCount = 0;
    }

    public static boolean wasValidateCalled() {
      return validateCalled;
    }

    public static WriteOperationType getCapturedOperationType() {
      return capturedOperationType;
    }

    public static HoodieData<?> getCapturedRecords() {
      return capturedRecords;
    }

    public static long getCapturedRecordCount() {
      return capturedRecordCount;
    }

    @Override
    public <T> void validate(String instantTime,
                             WriteOperationType writeOperationType,
                             HoodieTableMetaClient metaClient,
                             HoodieWriteConfig writeConfig,
                             HoodieEngineContext engineContext,
                             HoodieData<HoodieRecord<T>> records) throws HoodieValidationException {
      validateCalled = true;
      capturedOperationType = writeOperationType;
      capturedRecords = records;
      if (records != null) {
        capturedRecordCount = records.count();
      }
    }

    @Override
    public String getName() {
      return "RecordCapturingValidator";
    }
  }

  /**
   * A test validator that always fails.
   */
  public static class FailingValidator implements PreWriteValidator {
    private static volatile boolean validateCalled = false;

    public static void reset() {
      validateCalled = false;
    }

    public static boolean wasValidateCalled() {
      return validateCalled;
    }

    @Override
    public <T> void validate(String instantTime,
                             WriteOperationType writeOperationType,
                             HoodieTableMetaClient metaClient,
                             HoodieWriteConfig writeConfig,
                             HoodieEngineContext engineContext,
                             HoodieData<HoodieRecord<T>> records) throws HoodieValidationException {
      validateCalled = true;
      throw new HoodieValidationException("Intentional validation failure for testing");
    }

    @Override
    public String getName() {
      return "FailingValidator";
    }
  }

  /**
   * A test validator that counts invocations.
   */
  public static class CountingValidator implements PreWriteValidator {
    private static volatile boolean validateCalled = false;
    private static volatile int invocationCount = 0;

    public static void reset() {
      validateCalled = false;
      invocationCount = 0;
    }

    public static boolean wasValidateCalled() {
      return validateCalled;
    }

    public static int getInvocationCount() {
      return invocationCount;
    }

    @Override
    public <T> void validate(String instantTime,
                             WriteOperationType writeOperationType,
                             HoodieTableMetaClient metaClient,
                             HoodieWriteConfig writeConfig,
                             HoodieEngineContext engineContext,
                             HoodieData<HoodieRecord<T>> records) throws HoodieValidationException {
      validateCalled = true;
      invocationCount++;
    }

    @Override
    public String getName() {
      return "CountingValidator";
    }
  }
}
