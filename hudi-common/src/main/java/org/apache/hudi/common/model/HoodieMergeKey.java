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

package org.apache.hudi.common.model;

import java.io.Serializable;

/**
 * Defines a standard for all merge keys to ensure consistent handling including simple keys and composite keys.
 * It includes methods for retrieving the key and partition path.
 */
public interface HoodieMergeKey extends Serializable {

  /**
   * Get the partition path.
   */
  String getPartitionPath();


  /**
   * Get the record key.
   */
  Serializable getRecordKey();

  /**
   * Get the hoodie key.
   * For simple merge keys, this is used to directly fetch the {@link HoodieKey}, which is a combination of record key and partition path.
   */
  default HoodieKey getHoodieKey() {
    return new HoodieKey(getRecordKey().toString(), getPartitionPath());
  }
}
