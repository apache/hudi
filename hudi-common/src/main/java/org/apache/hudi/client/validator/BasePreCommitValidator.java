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

package org.apache.hudi.client.validator;

import org.apache.hudi.ApiMaturityLevel;
import org.apache.hudi.PublicAPIClass;
import org.apache.hudi.PublicAPIMethod;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.exception.HoodieValidationException;

/**
 * Base class for all pre-commit validators across all engines (Spark, Flink, Java).
 * Engine-specific implementations extend this class and implement ValidationContext.
 *
 * <p>This is the foundation for engine-agnostic validation logic that can access
 * commit metadata, timeline, and write statistics.</p>
 *
 * <p>Integration with existing framework:
 * In Phase 3, the existing {@code SparkPreCommitValidator} will be refactored to extend
 * this class, and {@code SparkValidatorUtils.runValidators()} will be updated to invoke
 * {@link #validateWithMetadata(ValidationContext)} for validators that extend this class.
 * The existing validator class names configured via
 * {@code HoodiePreCommitValidatorConfig.VALIDATOR_CLASS_NAMES} will continue to work.</p>
 *
 * <p>Phase 1: Core framework in hudi-common</p>
 * <p>Phase 2: Flink-specific implementations in hudi-flink-datasource</p>
 * <p>Phase 3: Spark-specific implementations in hudi-client/hudi-spark-client</p>
 */
@PublicAPIClass(maturity = ApiMaturityLevel.EVOLVING)
public abstract class BasePreCommitValidator {

  protected final TypedProperties config;

  /**
   * Create a pre-commit validator with configuration.
   *
   * @param config Typed properties containing validator configuration
   */
  protected BasePreCommitValidator(TypedProperties config) {
    this.config = config;
  }

  /**
   * Perform validation using commit metadata, timeline, and write statistics.
   * This method is called by the engine-specific orchestration layer.
   *
   * <p>Subclasses should override this method to implement validation logic that:</p>
   * <ul>
   *   <li>Accesses commit metadata (checkpoints, custom metadata)</li>
   *   <li>Navigates timeline (previous commits)</li>
   *   <li>Analyzes write statistics (record counts, partition info)</li>
   * </ul>
   *
   * @param context Validation context providing access to metadata (engine-specific implementation)
   * @throws HoodieValidationException if validation fails
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  protected void validateWithMetadata(ValidationContext context) throws HoodieValidationException {
    // Default no-op implementation
    // Concrete validators override this to implement validation logic
  }

  /**
   * Get the validator configuration.
   *
   * @return Typed properties with validator settings
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public TypedProperties getConfig() {
    return config;
  }
}
