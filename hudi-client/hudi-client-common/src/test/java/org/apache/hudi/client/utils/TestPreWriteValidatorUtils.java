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
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieValidationException;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
        PreWriteValidatorUtils.runValidators(config, "001", WriteOperationType.INSERT, metaClient, engineContext, null));
  }

  @Test
  public void testRunValidatorsWithNullValidatorsConfigured() {
    HoodieWriteConfig config = Mockito.mock(HoodieWriteConfig.class);
    HoodieTableMetaClient metaClient = Mockito.mock(HoodieTableMetaClient.class);
    HoodieEngineContext engineContext = Mockito.mock(HoodieEngineContext.class);

    when(config.getPreWriteValidators()).thenReturn(null);

    assertDoesNotThrow(() ->
        PreWriteValidatorUtils.runValidators(config, "001", WriteOperationType.INSERT, metaClient, engineContext, null));
  }

  @Test
  public void testRunValidatorsWithPassingValidator() {
    HoodieWriteConfig config = Mockito.mock(HoodieWriteConfig.class);
    HoodieTableMetaClient metaClient = Mockito.mock(HoodieTableMetaClient.class);
    HoodieEngineContext engineContext = Mockito.mock(HoodieEngineContext.class);

    when(config.getPreWriteValidators()).thenReturn(PassingPreWriteValidator.class.getName());

    assertDoesNotThrow(() ->
        PreWriteValidatorUtils.runValidators(config, "001", WriteOperationType.INSERT, metaClient, engineContext, null));
  }

  @Test
  public void testRunValidatorsWithFailingValidator() {
    HoodieWriteConfig config = Mockito.mock(HoodieWriteConfig.class);
    HoodieTableMetaClient metaClient = Mockito.mock(HoodieTableMetaClient.class);
    HoodieEngineContext engineContext = Mockito.mock(HoodieEngineContext.class);

    when(config.getPreWriteValidators()).thenReturn(FailingPreWriteValidator.class.getName());

    assertThrows(HoodieValidationException.class, () ->
        PreWriteValidatorUtils.runValidators(config, "001", WriteOperationType.INSERT, metaClient, engineContext, null));
  }

  @Test
  public void testRunValidatorsWithMultipleValidators() {
    HoodieWriteConfig config = Mockito.mock(HoodieWriteConfig.class);
    HoodieTableMetaClient metaClient = Mockito.mock(HoodieTableMetaClient.class);
    HoodieEngineContext engineContext = Mockito.mock(HoodieEngineContext.class);

    String validators = PassingPreWriteValidator.class.getName() + "," + PassingPreWriteValidator.class.getName();
    when(config.getPreWriteValidators()).thenReturn(validators);

    assertDoesNotThrow(() ->
        PreWriteValidatorUtils.runValidators(config, "001", WriteOperationType.INSERT, metaClient, engineContext, null));
  }

  @Test
  public void testRunValidatorsWithOnePassingOneFailing() {
    HoodieWriteConfig config = Mockito.mock(HoodieWriteConfig.class);
    HoodieTableMetaClient metaClient = Mockito.mock(HoodieTableMetaClient.class);
    HoodieEngineContext engineContext = Mockito.mock(HoodieEngineContext.class);

    String validators = PassingPreWriteValidator.class.getName() + "," + FailingPreWriteValidator.class.getName();
    when(config.getPreWriteValidators()).thenReturn(validators);

    assertThrows(HoodieValidationException.class, () ->
        PreWriteValidatorUtils.runValidators(config, "001", WriteOperationType.INSERT, metaClient, engineContext, null));
  }

  @Test
  public void testRunValidatorsWithInvalidClassName() {
    HoodieWriteConfig config = Mockito.mock(HoodieWriteConfig.class);
    HoodieTableMetaClient metaClient = Mockito.mock(HoodieTableMetaClient.class);
    HoodieEngineContext engineContext = Mockito.mock(HoodieEngineContext.class);

    when(config.getPreWriteValidators()).thenReturn("com.invalid.NonExistentValidator");

    // Should throw an exception when trying to load invalid class
    assertThrows(Exception.class, () ->
        PreWriteValidatorUtils.runValidators(config, "001", WriteOperationType.INSERT, metaClient, engineContext, null));
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
        PreWriteValidatorUtils.runValidators(config, "001", WriteOperationType.INSERT, metaClient, engineContext, null));
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
                             HoodieData<HoodieRecord<T>> records) throws HoodieValidationException {
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
                             HoodieData<HoodieRecord<T>> records) throws HoodieValidationException {
      throw new HoodieValidationException("Validation failed for testing");
    }

    @Override
    public String getName() {
      return "FailingPreWriteValidator";
    }
  }
}
