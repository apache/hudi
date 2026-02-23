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
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieValidationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Utility class for running pre-write validators.
 */
public class PreWriteValidatorUtils {
  private static final Logger LOG = LoggerFactory.getLogger(PreWriteValidatorUtils.class);

  /**
   * Run all configured pre-write validators.
   * Validators are run sequentially, and if any validator fails,
   * a HoodieValidationException is thrown.
   *
   * @param config             The write configuration
   * @param instantTime        The instant time for the write operation
   * @param writeOperationType The type of write operation
   * @param metaClient         The HoodieTableMetaClient
   * @param engineContext      The Hoodie engine context
   * @throws HoodieValidationException if any validation fails
   */
  public static void runValidators(HoodieWriteConfig config,
                                   String instantTime,
                                   WriteOperationType writeOperationType,
                                   HoodieTableMetaClient metaClient,
                                   HoodieEngineContext engineContext) {
    String validatorClassNames = config.getPreWriteValidators();

    if (StringUtils.isNullOrEmpty(validatorClassNames)) {
      LOG.debug("No pre-write validators configured.");
      return;
    }

    HoodieTimer timer = HoodieTimer.start();
    List<PreWriteValidator> validators = Arrays.stream(validatorClassNames.split(","))
        .map(String::trim)
        .filter(className -> !className.isEmpty())
        .map(className -> (PreWriteValidator) ReflectionUtils.loadClass(className))
        .collect(Collectors.toList());

    LOG.info("Running {} pre-write validators for instant {}", validators.size(), instantTime);

    List<String> failedValidators = validators.stream()
        .filter(validator -> !runValidator(validator, instantTime, writeOperationType, metaClient, config, engineContext))
        .map(PreWriteValidator::getName)
        .collect(Collectors.toList());

    long duration = timer.endTimer();
    LOG.info("Pre-write validation completed in {} ms", duration);

    if (!failedValidators.isEmpty()) {
      String failedMessage = String.join(", ", failedValidators);
      throw new HoodieValidationException(
          "Pre-write validation failed for validators: " + failedMessage);
    }
  }

  /**
   * Run a single validator.
   *
   * @return true if validation passed, false if validation failed
   */
  private static boolean runValidator(PreWriteValidator validator,
                                      String instantTime,
                                      WriteOperationType writeOperationType,
                                      HoodieTableMetaClient metaClient,
                                      HoodieWriteConfig writeConfig,
                                      HoodieEngineContext engineContext) {
    String validatorName = validator.getName();
    LOG.info("Running pre-write validator: {}", validatorName);

    try {
      HoodieTimer timer = HoodieTimer.start();
      validator.validate(instantTime, writeOperationType, metaClient, writeConfig, engineContext);
      long duration = timer.endTimer();
      LOG.info("Pre-write validator {} completed successfully in {} ms", validatorName, duration);
      return true;
    } catch (HoodieValidationException e) {
      LOG.error("Pre-write validation failed for validator {}: {}", validatorName, e.getMessage(), e);
      return false;
    } catch (Exception e) {
      LOG.error("Unexpected error running pre-write validator {}: {}", validatorName, e.getMessage(), e);
      return false;
    }
  }
}
