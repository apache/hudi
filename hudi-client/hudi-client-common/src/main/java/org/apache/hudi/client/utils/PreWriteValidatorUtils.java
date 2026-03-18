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
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieValidationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Utility class for running pre-write validators.
 */
public class PreWriteValidatorUtils {
  private static final Logger LOG = LoggerFactory.getLogger(PreWriteValidatorUtils.class);

  /**
   * Run all configured pre-write validators.
   * Validators are run asynchronously in parallel, and if any validator fails,
   * a HoodieValidationException is thrown.
   *
   * @param config             The write configuration
   * @param instantTime        The instant time for the write operation
   * @param writeOperationType The type of write operation
   * @param metaClient         The HoodieTableMetaClient
   * @param engineContext      The Hoodie engine context
   * @param recordsOpt         Option of HoodieData of records to be written, empty for operations
   *                           without input records (e.g., compact, cluster, delete)
   * @param <T>                The payload type of the records
   * @throws HoodieValidationException if any validation fails
   */
  public static <T> void runValidators(HoodieWriteConfig config,
                                       String instantTime,
                                       WriteOperationType writeOperationType,
                                       HoodieTableMetaClient metaClient,
                                       HoodieEngineContext engineContext,
                                       Option<HoodieData<HoodieRecord<T>>> recordsOpt) {
    String validatorClassNames = config.getPreWriteValidators();

    if (StringUtils.isNullOrEmpty(validatorClassNames)) {
      LOG.debug("No pre-write validators configured.");
      return;
    }

    HoodieTimer timer = HoodieTimer.start();
    Stream<PreWriteValidator> validators = Arrays.stream(validatorClassNames.split(","))
        .map(String::trim)
        .filter(className -> !className.isEmpty())
        .map(className -> (PreWriteValidator) ReflectionUtils.loadClass(className));

    LOG.info("Running pre-write validators for instant {}", instantTime);

    // Collect all futures first to ensure parallel execution
    List<CompletableFuture<Boolean>> futures = validators
        .map(validator -> runValidatorAsync(validator, instantTime, writeOperationType, metaClient, config, engineContext, recordsOpt))
        .collect(Collectors.toList());

    // Wait for all validators to complete
    boolean allSuccess = futures.stream()
        .map(CompletableFuture::join)
        .reduce(true, Boolean::logicalAnd);

    long duration = timer.endTimer();
    LOG.info("Pre-write validation completed in {} ms", duration);

    if (allSuccess) {
      LOG.info("All pre-write validations succeeded");
    } else {
      LOG.error("At least one pre-write validation failed");
      throw new HoodieValidationException("At least one pre-write validation failed");
    }
  }

  /**
   * Run a single validator asynchronously in a separate thread pool for parallelism.
   *
   * @return CompletableFuture that resolves to true if validation passed, false if validation failed
   */
  private static <T> CompletableFuture<Boolean> runValidatorAsync(PreWriteValidator validator,
                                                                   String instantTime,
                                                                   WriteOperationType writeOperationType,
                                                                   HoodieTableMetaClient metaClient,
                                                                   HoodieWriteConfig writeConfig,
                                                                   HoodieEngineContext engineContext,
                                                                   Option<HoodieData<HoodieRecord<T>>> recordsOpt) {
    return CompletableFuture.supplyAsync(() -> {
      String validatorName = validator.getName();
      LOG.info("Running pre-write validator: {}", validatorName);

      try {
        HoodieTimer timer = HoodieTimer.start();
        validator.validate(instantTime, writeOperationType, metaClient, writeConfig, engineContext, recordsOpt);
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
    });
  }
}
