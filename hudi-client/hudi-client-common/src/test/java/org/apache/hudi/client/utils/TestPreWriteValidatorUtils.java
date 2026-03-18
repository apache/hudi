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

import org.apache.hudi.client.validator.PreWriteValidator;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieValidationException;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link PreWriteValidatorUtils}.
 */
public class TestPreWriteValidatorUtils {

  @Test
  public void testRunValidatorsWithNoValidatorsConfigured() {
    HoodieWriteConfig config = Mockito.mock(HoodieWriteConfig.class);
    HoodieTableMetaClient metaClient = Mockito.mock(HoodieTableMetaClient.class);
    HoodieEngineContext engineContext = Mockito.mock(HoodieEngineContext.class);

    when(config.getPreWriteValidators()).thenReturn("");

    assertDoesNotThrow(() ->
        PreWriteValidatorUtils.runValidators(config, "001", WriteOperationType.INSERT, metaClient, engineContext, Option.empty()));
  }

  @Test
  public void testRunValidatorsWithNullValidatorsConfigured() {
    HoodieWriteConfig config = Mockito.mock(HoodieWriteConfig.class);
    HoodieTableMetaClient metaClient = Mockito.mock(HoodieTableMetaClient.class);
    HoodieEngineContext engineContext = Mockito.mock(HoodieEngineContext.class);

    when(config.getPreWriteValidators()).thenReturn(null);

    assertDoesNotThrow(() ->
        PreWriteValidatorUtils.runValidators(config, "001", WriteOperationType.INSERT, metaClient, engineContext, Option.empty()));
  }

  @Test
  public void testRunValidatorsWithPassingValidator() {
    HoodieWriteConfig config = Mockito.mock(HoodieWriteConfig.class);
    HoodieTableMetaClient metaClient = Mockito.mock(HoodieTableMetaClient.class);
    HoodieEngineContext engineContext = Mockito.mock(HoodieEngineContext.class);

    when(config.getPreWriteValidators()).thenReturn(PassingPreWriteValidator.class.getName());

    assertDoesNotThrow(() ->
        PreWriteValidatorUtils.runValidators(config, "001", WriteOperationType.INSERT, metaClient, engineContext, Option.empty()));
  }

  @Test
  public void testRunValidatorsWithFailingValidator() {
    HoodieWriteConfig config = Mockito.mock(HoodieWriteConfig.class);
    HoodieTableMetaClient metaClient = Mockito.mock(HoodieTableMetaClient.class);
    HoodieEngineContext engineContext = Mockito.mock(HoodieEngineContext.class);

    when(config.getPreWriteValidators()).thenReturn(FailingPreWriteValidator.class.getName());

    assertThrows(HoodieValidationException.class, () ->
        PreWriteValidatorUtils.runValidators(config, "001", WriteOperationType.INSERT, metaClient, engineContext, Option.empty()));
  }

  @Test
  public void testRunValidatorsWithMultipleValidators() {
    HoodieWriteConfig config = Mockito.mock(HoodieWriteConfig.class);
    HoodieTableMetaClient metaClient = Mockito.mock(HoodieTableMetaClient.class);
    HoodieEngineContext engineContext = Mockito.mock(HoodieEngineContext.class);

    String validators = PassingPreWriteValidator.class.getName() + "," + PassingPreWriteValidator.class.getName();
    when(config.getPreWriteValidators()).thenReturn(validators);

    assertDoesNotThrow(() ->
        PreWriteValidatorUtils.runValidators(config, "001", WriteOperationType.INSERT, metaClient, engineContext, Option.empty()));
  }

  @Test
  public void testRunValidatorsWithOnePassingOneFailing() {
    HoodieWriteConfig config = Mockito.mock(HoodieWriteConfig.class);
    HoodieTableMetaClient metaClient = Mockito.mock(HoodieTableMetaClient.class);
    HoodieEngineContext engineContext = Mockito.mock(HoodieEngineContext.class);

    String validators = PassingPreWriteValidator.class.getName() + "," + FailingPreWriteValidator.class.getName();
    when(config.getPreWriteValidators()).thenReturn(validators);

    assertThrows(HoodieValidationException.class, () ->
        PreWriteValidatorUtils.runValidators(config, "001", WriteOperationType.INSERT, metaClient, engineContext, Option.empty()));
  }

  @Test
  public void testRunValidatorsWithInvalidClassName() {
    HoodieWriteConfig config = Mockito.mock(HoodieWriteConfig.class);
    HoodieTableMetaClient metaClient = Mockito.mock(HoodieTableMetaClient.class);
    HoodieEngineContext engineContext = Mockito.mock(HoodieEngineContext.class);

    when(config.getPreWriteValidators()).thenReturn("com.invalid.NonExistentValidator");

    // Should throw an exception when trying to load invalid class
    assertThrows(Exception.class, () ->
        PreWriteValidatorUtils.runValidators(config, "001", WriteOperationType.INSERT, metaClient, engineContext, Option.empty()));
  }

  @Test
  public void testRunValidatorsWithWhitespaceInClassNames() {
    HoodieWriteConfig config = Mockito.mock(HoodieWriteConfig.class);
    HoodieTableMetaClient metaClient = Mockito.mock(HoodieTableMetaClient.class);
    HoodieEngineContext engineContext = Mockito.mock(HoodieEngineContext.class);

    // Add whitespace around class names
    String validators = "  " + PassingPreWriteValidator.class.getName() + " , " + PassingPreWriteValidator.class.getName() + "  ";
    when(config.getPreWriteValidators()).thenReturn(validators);

    assertDoesNotThrow(() ->
        PreWriteValidatorUtils.runValidators(config, "001", WriteOperationType.INSERT, metaClient, engineContext, Option.empty()));
  }

  @Test
  public void testRunValidatorsWithRecords() {
    HoodieWriteConfig config = Mockito.mock(HoodieWriteConfig.class);
    HoodieTableMetaClient metaClient = Mockito.mock(HoodieTableMetaClient.class);
    HoodieEngineContext engineContext = Mockito.mock(HoodieEngineContext.class);
    HoodieData<HoodieRecord<Object>> records = Mockito.mock(HoodieData.class);

    when(config.getPreWriteValidators()).thenReturn(RecordCheckingValidator.class.getName());

    assertDoesNotThrow(() ->
        PreWriteValidatorUtils.runValidators(config, "001", WriteOperationType.UPSERT, metaClient, engineContext, Option.of(records)));
  }

  @Test
  public void testRunValidatorsWithDifferentOperationTypes() {
    HoodieWriteConfig config = Mockito.mock(HoodieWriteConfig.class);
    HoodieTableMetaClient metaClient = Mockito.mock(HoodieTableMetaClient.class);
    HoodieEngineContext engineContext = Mockito.mock(HoodieEngineContext.class);

    when(config.getPreWriteValidators()).thenReturn(OperationTypeCheckingValidator.class.getName());

    // Test with different operation types
    for (WriteOperationType opType : new WriteOperationType[]{
        WriteOperationType.INSERT, WriteOperationType.UPSERT, WriteOperationType.DELETE,
        WriteOperationType.BULK_INSERT, WriteOperationType.INSERT_OVERWRITE}) {
      assertDoesNotThrow(() ->
          PreWriteValidatorUtils.runValidators(config, "001", opType, metaClient, engineContext, Option.empty()));
    }
  }

  @Test
  public void testRunValidatorsWithEmptyStringEntries() {
    HoodieWriteConfig config = Mockito.mock(HoodieWriteConfig.class);
    HoodieTableMetaClient metaClient = Mockito.mock(HoodieTableMetaClient.class);
    HoodieEngineContext engineContext = Mockito.mock(HoodieEngineContext.class);

    // Test with empty entries in the validator list (should be filtered out)
    String validators = PassingPreWriteValidator.class.getName() + ",,," + PassingPreWriteValidator.class.getName();
    when(config.getPreWriteValidators()).thenReturn(validators);

    assertDoesNotThrow(() ->
        PreWriteValidatorUtils.runValidators(config, "001", WriteOperationType.INSERT, metaClient, engineContext, Option.empty()));
  }

  @Test
  public void testRunValidatorsWithNonValidationException() {
    HoodieWriteConfig config = Mockito.mock(HoodieWriteConfig.class);
    HoodieTableMetaClient metaClient = Mockito.mock(HoodieTableMetaClient.class);
    HoodieEngineContext engineContext = Mockito.mock(HoodieEngineContext.class);

    when(config.getPreWriteValidators()).thenReturn(RuntimeExceptionValidator.class.getName());

    // Should catch and handle non-HoodieValidationException errors
    assertThrows(HoodieValidationException.class, () ->
        PreWriteValidatorUtils.runValidators(config, "001", WriteOperationType.INSERT, metaClient, engineContext, Option.empty()));
  }

  @Test
  public void testRunValidatorsInParallel() {
    HoodieWriteConfig config = Mockito.mock(HoodieWriteConfig.class);
    HoodieTableMetaClient metaClient = Mockito.mock(HoodieTableMetaClient.class);
    HoodieEngineContext engineContext = Mockito.mock(HoodieEngineContext.class);

    // Reset counters
    SlowValidator.reset();

    String validators = SlowValidator.class.getName() + "," + SlowValidator.class.getName() + "," + SlowValidator.class.getName();
    when(config.getPreWriteValidators()).thenReturn(validators);

    long startTime = System.currentTimeMillis();
    assertDoesNotThrow(() ->
        PreWriteValidatorUtils.runValidators(config, "001", WriteOperationType.INSERT, metaClient, engineContext, Option.empty()));
    long duration = System.currentTimeMillis() - startTime;

    // Verify all validators were called
    assertEquals(3, SlowValidator.getCallCount());

    // If running in parallel, total time should be closer to 100ms than 300ms
    // Allow some buffer for test execution overhead
    assertTrue(duration < 250, "Validators should run in parallel. Duration: " + duration + "ms");
  }

  @Test
  public void testRunValidatorsAllCalled() {
    HoodieWriteConfig config = Mockito.mock(HoodieWriteConfig.class);
    HoodieTableMetaClient metaClient = Mockito.mock(HoodieTableMetaClient.class);
    HoodieEngineContext engineContext = Mockito.mock(HoodieEngineContext.class);

    // Reset counters
    SlowValidator.reset();

    String validators = SlowValidator.class.getName() + "," + SlowValidator.class.getName() + "," + SlowValidator.class.getName();
    when(config.getPreWriteValidators()).thenReturn(validators);

    assertDoesNotThrow(() ->
        PreWriteValidatorUtils.runValidators(config, "001", WriteOperationType.INSERT, metaClient, engineContext, Option.empty()));

    // Verify all 3 validators were called
    assertEquals(3, SlowValidator.getCallCount());
  }

  @Test
  public void testRunValidatorsWithCustomValidatorName() {
    HoodieWriteConfig config = Mockito.mock(HoodieWriteConfig.class);
    HoodieTableMetaClient metaClient = Mockito.mock(HoodieTableMetaClient.class);
    HoodieEngineContext engineContext = Mockito.mock(HoodieEngineContext.class);

    when(config.getPreWriteValidators()).thenReturn(CustomNameValidator.class.getName());

    assertDoesNotThrow(() ->
        PreWriteValidatorUtils.runValidators(config, "001", WriteOperationType.INSERT, metaClient, engineContext, Option.empty()));
  }

  @Test
  public void testRunValidatorsMultipleFailures() {
    HoodieWriteConfig config = Mockito.mock(HoodieWriteConfig.class);
    HoodieTableMetaClient metaClient = Mockito.mock(HoodieTableMetaClient.class);
    HoodieEngineContext engineContext = Mockito.mock(HoodieEngineContext.class);

    // Multiple failing validators
    String validators = FailingPreWriteValidator.class.getName() + "," + FailingPreWriteValidator.class.getName();
    when(config.getPreWriteValidators()).thenReturn(validators);

    assertThrows(HoodieValidationException.class, () ->
        PreWriteValidatorUtils.runValidators(config, "001", WriteOperationType.INSERT, metaClient, engineContext, Option.empty()));
  }

  /**
   * A test validator that always passes.
   */
  public static class PassingPreWriteValidator implements PreWriteValidator {
    @Override
    public <T> void validate(String instantTime,
                             WriteOperationType writeOperationType,
                             HoodieTableMetaClient metaClient,
                             HoodieWriteConfig writeConfig,
                             HoodieEngineContext engineContext,
                             Option<HoodieData<HoodieRecord<T>>> recordsOpt) throws HoodieValidationException {
      // Always passes - do nothing
    }

    @Override
    public String getName() {
      return "PassingPreWriteValidator";
    }
  }

  /**
   * A test validator that always fails.
   */
  public static class FailingPreWriteValidator implements PreWriteValidator {
    @Override
    public <T> void validate(String instantTime,
                             WriteOperationType writeOperationType,
                             HoodieTableMetaClient metaClient,
                             HoodieWriteConfig writeConfig,
                             HoodieEngineContext engineContext,
                             Option<HoodieData<HoodieRecord<T>>> recordsOpt) throws HoodieValidationException {
      throw new HoodieValidationException("Validation failed for testing");
    }

    @Override
    public String getName() {
      return "FailingPreWriteValidator";
    }
  }

  /**
   * A test validator that checks if records are present.
   */
  public static class RecordCheckingValidator implements PreWriteValidator {
    @Override
    public <T> void validate(String instantTime,
                             WriteOperationType writeOperationType,
                             HoodieTableMetaClient metaClient,
                             HoodieWriteConfig writeConfig,
                             HoodieEngineContext engineContext,
                             Option<HoodieData<HoodieRecord<T>>> recordsOpt) throws HoodieValidationException {
      // Just verify we can access the recordsOpt parameter
      recordsOpt.isPresent();
    }

    @Override
    public String getName() {
      return "RecordCheckingValidator";
    }
  }

  /**
   * A test validator that checks the operation type.
   */
  public static class OperationTypeCheckingValidator implements PreWriteValidator {
    @Override
    public <T> void validate(String instantTime,
                             WriteOperationType writeOperationType,
                             HoodieTableMetaClient metaClient,
                             HoodieWriteConfig writeConfig,
                             HoodieEngineContext engineContext,
                             Option<HoodieData<HoodieRecord<T>>> recordsOpt) throws HoodieValidationException {
      // Just verify we can access the writeOperationType parameter
      if (writeOperationType == null) {
        throw new HoodieValidationException("WriteOperationType is null");
      }
    }

    @Override
    public String getName() {
      return "OperationTypeCheckingValidator";
    }
  }

  /**
   * A test validator that throws a non-HoodieValidationException.
   */
  public static class RuntimeExceptionValidator implements PreWriteValidator {
    @Override
    public <T> void validate(String instantTime,
                             WriteOperationType writeOperationType,
                             HoodieTableMetaClient metaClient,
                             HoodieWriteConfig writeConfig,
                             HoodieEngineContext engineContext,
                             Option<HoodieData<HoodieRecord<T>>> recordsOpt) {
      throw new RuntimeException("Unexpected runtime exception");
    }

    @Override
    public String getName() {
      return "RuntimeExceptionValidator";
    }
  }

  /**
   * A test validator that sleeps to test parallel execution.
   */
  public static class SlowValidator implements PreWriteValidator {
    private static final AtomicInteger CALL_COUNT = new AtomicInteger(0);

    public static void reset() {
      CALL_COUNT.set(0);
    }

    public static int getCallCount() {
      return CALL_COUNT.get();
    }

    @Override
    public <T> void validate(String instantTime,
                             WriteOperationType writeOperationType,
                             HoodieTableMetaClient metaClient,
                             HoodieWriteConfig writeConfig,
                             HoodieEngineContext engineContext,
                             Option<HoodieData<HoodieRecord<T>>> recordsOpt) throws HoodieValidationException {
      CALL_COUNT.incrementAndGet();
      try {
        Thread.sleep(100); // Sleep for 100ms
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new HoodieValidationException("Interrupted", e);
      }
    }

    @Override
    public String getName() {
      return "SlowValidator";
    }
  }

  /**
   * A test validator with a custom name.
   */
  public static class CustomNameValidator implements PreWriteValidator {
    @Override
    public <T> void validate(String instantTime,
                             WriteOperationType writeOperationType,
                             HoodieTableMetaClient metaClient,
                             HoodieWriteConfig writeConfig,
                             HoodieEngineContext engineContext,
                             Option<HoodieData<HoodieRecord<T>>> recordsOpt) throws HoodieValidationException {
      // Do nothing
    }

    @Override
    public String getName() {
      return "MyCustomValidatorName";
    }
  }
}
