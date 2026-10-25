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
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodiePreWriteValidatorConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.testutils.HoodieClientTestBase;

import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for pre-write validators with Spark write client.
 * These tests verify that pre-write validators are invoked during write operations.
 */
public class TestSparkPreWriteValidatorUtils extends HoodieClientTestBase {

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
    FirstPassingValidator.reset();
    SecondPassingValidator.reset();

    String validators = FirstPassingValidator.class.getName() + "," + SecondPassingValidator.class.getName();
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
      assertTrue(FirstPassingValidator.wasValidateCalled(), "FirstPassingValidator should have been called");
      assertTrue(SecondPassingValidator.wasValidateCalled(), "SecondPassingValidator should have been called");
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
                             Option<HoodieData<HoodieRecord<T>>> recordsOpt) throws HoodieValidationException {
      validateCalled = true;
      throw new HoodieValidationException("Intentional validation failure for testing");
    }

    @Override
    public String getName() {
      return "FailingValidator";
    }
  }

  /**
   * A test validator that always passes.
   */
  public static class FirstPassingValidator implements PreWriteValidator {
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
                             Option<HoodieData<HoodieRecord<T>>> recordsOpt) throws HoodieValidationException {
      validateCalled = true;
    }

    @Override
    public String getName() {
      return "FirstPassingValidator";
    }
  }

  /**
   * A second test validator that always passes.
   */
  public static class SecondPassingValidator implements PreWriteValidator {
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
                             Option<HoodieData<HoodieRecord<T>>> recordsOpt) throws HoodieValidationException {
      validateCalled = true;
    }

    @Override
    public String getName() {
      return "SecondPassingValidator";
    }
  }
}
