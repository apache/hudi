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

package org.apache.hudi.storage;

import java.util.Map;
import java.util.Set;

public interface HoodieStorageStrategy {
  /**
   * Return a storage location for the given path
   *
   * @param path
   * @param configMap
   * @return Append the appropriate prefix based on the Path and return
   */
  StoragePath storageLocation(String path, Map<String, String> configMap);

  /**
   * Return all possible StoragePaths
   *
   * @param partitionPath
   * @param checkExist check if StoragePath is truly existed or not.
   * @return a st of storage partition path
   */
  Set<StoragePath> getAllLocations(String partitionPath, boolean checkExist);

  /**
   * Return RelativePath base on path and locations.
   *
   * @param path
   * @return relative path
   */
  StoragePath getRelativePath(String path, Map<String, String> configMap);
}
