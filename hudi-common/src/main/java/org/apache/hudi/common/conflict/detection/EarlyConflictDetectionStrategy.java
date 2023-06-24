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

package org.apache.hudi.common.conflict.detection;

import org.apache.hudi.ApiMaturityLevel;
import org.apache.hudi.PublicAPIClass;
import org.apache.hudi.exception.HoodieEarlyConflictDetectionException;

/**
 * Interface for pluggable strategy of early conflict detection for multiple writers.
 */
@PublicAPIClass(maturity = ApiMaturityLevel.EVOLVING)
public interface EarlyConflictDetectionStrategy {
  /**
   * Detects and resolves the write conflict if necessary.
   */
  void detectAndResolveConflictIfNecessary() throws HoodieEarlyConflictDetectionException;

  /**
   * @return whether there's a write conflict based on markers.
   */
  boolean hasMarkerConflict();

  /**
   * Resolves a write conflict.
   *
   * @param basePath      Base path of the table.
   * @param partitionPath Relative partition path.
   * @param dataFileName  Data file name.
   */
  void resolveMarkerConflict(String basePath, String partitionPath, String dataFileName);
}
