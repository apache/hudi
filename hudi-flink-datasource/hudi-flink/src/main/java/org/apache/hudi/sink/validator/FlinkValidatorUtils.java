/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.sink.validator;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.validator.BasePreCommitValidator;
import org.apache.hudi.client.validator.ValidationContext;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodiePreCommitValidatorConfig;
import org.apache.hudi.exception.HoodieValidationException;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Utility for running pre-commit validators in the Flink commit flow.
 *
 * <p>Instantiates and executes validators configured via
 * {@code hoodie.precommit.validators}. Each validator must extend
 * {@link BasePreCommitValidator} and have a constructor that accepts
 * {@link TypedProperties}.</p>
 *
 * <p>Called from {@code StreamWriteOperatorCoordinator.doCommit()} before
 * the commit is finalized.</p>
 */
@Slf4j
public class FlinkValidatorUtils {

  /**
   * Run all configured pre-commit validators.
   *
   * @param conf Flink configuration containing validator class names
   * @param instant Commit instant time
   * @param allWriteStatus Write statuses from all operators
   * @param checkpointCommitMetadata Extra metadata being committed (contains checkpoint info)
   * @param previousCommitMetadataSupplier Supplier for metadata from the previous completed commit (lazily evaluated)
   * @throws HoodieValidationException if any validator fails with FAIL policy
   */
  public static void runValidators(Configuration conf,
                                   String instant,
                                   List<WriteStatus> allWriteStatus,
                                   Map<String, String> checkpointCommitMetadata,
                                   Supplier<Option<HoodieCommitMetadata>> previousCommitMetadataSupplier) {
    String validatorClassNames = conf.getString(
        HoodiePreCommitValidatorConfig.VALIDATOR_CLASS_NAMES.key(),
        HoodiePreCommitValidatorConfig.VALIDATOR_CLASS_NAMES.defaultValue());

    if (StringUtils.isNullOrEmpty(validatorClassNames)) {
      return;
    }

    // Fetch previous commit metadata only when validators are configured
    Option<HoodieCommitMetadata> previousCommitMetadata = previousCommitMetadataSupplier.get();

    // Build ValidationContext from available data
    HoodieCommitMetadata currentMetadata = buildCommitMetadata(allWriteStatus, checkpointCommitMetadata);
    List<HoodieWriteStat> writeStats = allWriteStatus.stream()
        .map(WriteStatus::getStat)
        .collect(Collectors.toList());

    ValidationContext context = new FlinkValidationContext(
        instant,
        Option.of(currentMetadata),
        Option.of(writeStats),
        previousCommitMetadata);

    // Build config properties for validators
    TypedProperties props = TypedProperties.fromMap(conf.toMap());

    // Instantiate and run each validator
    List<String> classNames = Arrays.stream(validatorClassNames.split(","))
        .map(String::trim)
        .filter(s -> !s.isEmpty())
        .collect(Collectors.toList());

    for (String className : classNames) {
      try {
        BasePreCommitValidator validator = (BasePreCommitValidator)
            ReflectionUtils.loadClass(className, new Class<?>[] {TypedProperties.class}, props);
        log.info("Running pre-commit validator: {} for instant: {}", className, instant);
        validator.validateWithMetadata(context);
        log.info("Pre-commit validator {} passed for instant: {}", className, instant);
      } catch (HoodieValidationException e) {
        log.error("Pre-commit validator {} failed for instant: {}", className, instant, e);
        throw e;
      } catch (Exception e) {
        log.error("Failed to instantiate or run validator: {}", className, e);
        throw new HoodieValidationException(
            "Failed to run pre-commit validator: " + className, e);
      }
    }
  }

  /**
   * Build HoodieCommitMetadata from write statuses and extra metadata.
   * This constructs the metadata object that would be committed, giving
   * validators access to the same data.
   */
  private static HoodieCommitMetadata buildCommitMetadata(
      List<WriteStatus> writeStatuses, Map<String, String> extraMetadata) {
    HoodieCommitMetadata metadata = new HoodieCommitMetadata();

    // Add write stats
    for (WriteStatus status : writeStatuses) {
      HoodieWriteStat stat = status.getStat();
      if (stat != null) {
        metadata.addWriteStat(stat.getPartitionPath(), stat);
      }
    }

    // Add extra metadata (includes checkpoint info like HoodieMetadataKey)
    if (extraMetadata != null) {
      extraMetadata.forEach(metadata::addMetadata);
    }

    return metadata;
  }

  private FlinkValidatorUtils() {
    // Utility class
  }
}
