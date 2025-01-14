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
 * Marking interface for table service strategy that utilize incremental partitions.
 *
 * <p> Any strategy class that implements this `IncrementalPartitionAwareStrategy` could have the ability to perform incremental partitions processing.
 * Currently, Incremental partitions will be passed to the strategy instance as a best-effort. In the following cases, the partitions would fallback to full partition list:
 *
 * <ul>
 *   <li> Executing Table Service for the first time. </li>
 *   <li> The last completed table service instant is archived. </li>
 *   <li> Any exception thrown during retrieval of incremental partitions. </li>
 * </ul>
 */
public interface IncrementalPartitionAwareStrategy {
  
  /**
   * Filter the given incremental partitions.
   * @param writeConfig
   * @param partitions
   * @return Pair of final processing partition paths and filtered partitions which will be recorded as missing partitions.
   * Different strategies can individually implement whether to record, or which partitions to record as missing partitions.
   */
  Pair<List<String>, List<String>> filterPartitionPaths(HoodieWriteConfig writeConfig, List<String> partitions);
}
