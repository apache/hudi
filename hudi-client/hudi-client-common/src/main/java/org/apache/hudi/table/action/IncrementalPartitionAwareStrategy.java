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

package org.apache.hudi.table.action;

import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;

import java.util.List;

/**
 * Marking strategy interface.
 *
 * Any Strategy implement this `IncrementalPartitionAwareStrategy` could have the ability to perform incremental partitions processing.
 * At this time, Incremental partitions should be passed to the current strategy.
 */
public interface IncrementalPartitionAwareStrategy {
  
  /**
   * Filter the given incremental partitions.
   * @param writeConfig
   * @param incrementalPartitions
   * @return Pair of final processing partition paths and filtered partitions which will be recorded as missing partitions.
   * Different strategies can individually implement whether to record, or which partitions to record as missing partitions.
   */
  Pair<List<String>, List<String>> filterPartitionPaths(HoodieWriteConfig writeConfig, List<String> partitions);
}
