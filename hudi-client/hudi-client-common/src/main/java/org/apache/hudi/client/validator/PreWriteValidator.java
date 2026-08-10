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

package org.apache.hudi.client.validator;

import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieValidationException;

/**
 * Interface for pre-write validators.
 * Pre-write validators are invoked before the write operation begins,
 * similar to pre-commit validators but at an earlier stage.
 *
 * <p>Implementations can perform various validations such as:
 * <ul>
 *   <li>Schema compatibility checks</li>
 *   <li>Data quality validations</li>
 *   <li>Permission validations</li>
 *   <li>Resource availability checks</li>
 * </ul>
 */
public interface PreWriteValidator {

  /**
   * Validates the state before a write operation.
   * This method is called in the preWrite phase of the write client.
   *
   * @param instantTime        The instant time for the write operation
   * @param writeOperationType The type of write operation being performed
   * @param metaClient         The HoodieTableMetaClient for accessing table metadata
   * @param writeConfig        The write configuration
   * @param engineContext      The Hoodie engine context
   * @param recordsOpt         Option of HoodieData of records to be written, empty for operations
   *                           without input records (e.g., compact, cluster, delete)
   * @param <T>                The payload type of the records
   * @throws HoodieValidationException if validation fails and the write should not proceed
   */
  <T> void validate(String instantTime,
                    WriteOperationType writeOperationType,
                    HoodieTableMetaClient metaClient,
                    HoodieWriteConfig writeConfig,
                    HoodieEngineContext engineContext,
                    Option<HoodieData<HoodieRecord<T>>> recordsOpt) throws HoodieValidationException;

  /**
   * Returns a descriptive name for this validator.
   * Used for logging and metrics purposes.
   *
   * @return The validator name
   */
  default String getName() {
    return getClass().getSimpleName();
  }
}
