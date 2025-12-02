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

package org.apache.hudi.index.record;

import org.apache.hudi.common.model.HoodieIndexDefinition;

import java.util.Map;

/**
 * Interface representing a record index in Hudi which provides options and other parameters used for record index.
 */
public interface HoodieRecordIndex {
  String IS_PARTITIONED_OPTION = "isPartitioned";

  static boolean isPartitioned(HoodieIndexDefinition indexDefinition) {
    return Boolean.parseBoolean(indexDefinition.getIndexOptions().getOrDefault(IS_PARTITIONED_OPTION, Boolean.FALSE.toString()));
  }

  static boolean isPartitioned(Map<String, String> options) {
    return Boolean.parseBoolean(options.getOrDefault(IS_PARTITIONED_OPTION, Boolean.FALSE.toString()));
  }
}
